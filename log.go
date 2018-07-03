/*
 * Copyright 2018 Marco Helmich
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package main

import (
	"container/list"
	"fmt"
	"sync"
	"time"

	"github.com/mhelmich/copycat"
	"github.com/mhelmich/metamorphosis/pb"
	"github.com/sirupsen/logrus"
)

// recommended reading:
// https://www.cockroachlabs.com/blog/serializable-lockless-distributed-isolation-cockroachdb/
// https://www.cockroachlabs.com/blog/how-cockroachdb-distributes-atomic-transactions/
// https://www.cockroachlabs.com/blog/sql-in-cockroachdb-mapping-table-data-to-key-value-storage/

// Log entries are appended to the existing log.
// That happens in a two-step process:
// 1. Write an intent to CopyCat (log entry consisting key, value, and the intent flag set - no offset).
//    The rountine appending waits for the entry to be replayed.
// 2. When the entry gets replayed, the position in the CopyCat log relative to the last
//    committed entry will determine its offset. Upon computation of the offset,
//    a rountine to write entry (inlcuding offset) to CopyCat is scheduled and the offset is returned.

type log struct {
	// the log is just a linked list today
	log *list.List
	// this mutex guards the log and snapshot
	mutex *sync.RWMutex
	// contains the compacted log
	// resembles map[string][]byte
	snapshot *sync.Map
	// these two switches define when compaction will happen
	anticipatedLogLen                 int
	highWatermarkLogSizeMultiplicator int
	// poor mans future
	// makes a routine wait for a commit entry to be replayed
	contextWait *contextWait
	// a log entry offset is determined by the position of its intent in the log
	// the offset will be the number of all previous intents + 1
	// this will be part of the copycat snapshot that is being created from time to time
	baseOffset uint64

	proposeCh        chan<- []byte
	commitCh         <-chan []byte
	errorCh          <-chan error
	snapshotConsumer copycat.SnapshotConsumer
}

func newLog(dataStructureId string, cc copycat.CopyCat) (*log, error) {
	l := &log{
		log:                               &list.List{},
		mutex:                             &sync.RWMutex{},
		snapshot:                          &sync.Map{},
		anticipatedLogLen:                 1000,
		highWatermarkLogSizeMultiplicator: 3,
		contextWait:                       newContextWait(),
	}

	var err error
	l.proposeCh, l.commitCh, l.errorCh, l.snapshotConsumer, err = cc.SubscribeToDataStructureWithStringID(dataStructureId, l.snapshotProvider)
	if err != nil {
		return nil, err
	}

	go l.serveChannels()
	return l, nil
}

func (l *log) serveChannels() {
	for {
		select {
		case data, ok := <-l.commitCh:
			if !ok {
				return
			}

			if data == nil {
				bites, err := l.snapshotConsumer()
				if err != nil {
					logrus.Errorf("Error getting snapshot: %s", err.Error())
				}

				protobuf := &pb.MetamorphosisLog{}
				err = protobuf.Unmarshal(bites)
				if err != nil {
					logrus.Errorf("Error unmarshaling snapshot: %s", err.Error())
				}

				wg := &sync.WaitGroup{}
				wg.Add(2)

				newSnap := &sync.Map{}
				go func() {
					for k, v := range protobuf.State {
						newSnap.Store(k, v)
					}
					wg.Done()
				}()

				newLog := &list.List{}
				go func() {
					for _, item := range protobuf.Log {
						newLog.PushBack(item)
					}
					wg.Done()
				}()

				wg.Wait()
				// set the internal state of the log
				l.mutex.Lock()
				l.log = newLog
				l.snapshot = newSnap
				l.baseOffset = protobuf.BaseOffset
				l.mutex.Unlock()
			} else {
				le := &pb.LogEntry{}
				err := le.Unmarshal(data)
				if err != nil {
					logrus.Errorf("Can't unmarshal log entry: %s", err.Error())
				}

				l.mutex.Lock()
				if le.IsIntent {
					// if the log entry is an intent, slap it at the back of the log
					// its position will determine this log entries offset
					l.log.PushBack(le)
				} else {
					// if the log entry is not an intent, find the position in the log,
					// that's right for it
					inserted := false
					for e := l.log.Back(); e != nil; e = e.Prev() {
						lle := e.Value.(*pb.LogEntry)
						if !lle.IsIntent && lle.Offset < le.Offset {
							l.log.InsertAfter(le, e)
							inserted = true
							break
						}
					}

					// if I haven't put the log entry anywhere yet
					// (because for example there are only intents left in the log)
					// I will be progressive and push the log entry at the tail of the log
					// this should make inserting new log entries fast
					if !inserted {
						l.log.PushBack(le)
					}
				}
				l.mutex.Unlock()
				l.contextWait.trigger(le.Context)
			}

			go l.maybeCompact()
		}
	}
}

func (l *log) maybeCompact() {
	// compact when the log is X times its anticipated size
	if l.log.Len() > l.anticipatedLogLen*l.highWatermarkLogSizeMultiplicator {
		l.mutex.Lock()
		if l.log.Len() > l.anticipatedLogLen*l.highWatermarkLogSizeMultiplicator {
			for e := l.log.Front(); e != nil && l.log.Len() > l.anticipatedLogLen; e = l.log.Front() {
				le := e.Value.(*pb.LogEntry)
				if le.IsIntent {
					l.baseOffset++
				} else {
					l.snapshot.Store(string(le.Key), le.Value)
				}
				l.log.Remove(e)
			}
		}
		l.mutex.Unlock()
	}
}

func (l *log) snapshotProvider() ([]byte, error) {
	wg := &sync.WaitGroup{}
	wg.Add(2)
	done := make(chan struct{})

	l.mutex.RLock()
	defer l.mutex.RUnlock()
	log := make([]*pb.LogEntry, l.log.Len())
	m := make(map[string][]byte)

	// serialize the snapshot
	go func() {
		l.snapshot.Range(func(key interface{}, value interface{}) bool {
			k := key.(string)
			v := value.([]byte)
			m[k] = v
			return true
		})
		wg.Done()
	}()

	// serialize the log
	go func() {
		i := 0
		for e := l.log.Front(); e != nil; e = e.Next() {
			le := e.Value.(*pb.LogEntry)
			if !le.IsIntent {
				log[i] = le
				i++
			}
		}
		// filtered all intents and shrink the slice down
		log = log[:i]
		wg.Done()
	}()

	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		daLog := &pb.MetamorphosisLog{
			State:      m,
			Log:        log,
			BaseOffset: l.baseOffset,
		}
		return daLog.Marshal()
	case <-time.After(1 * time.Second):
		return nil, fmt.Errorf("Snapshotting this log took longer than 1s")
	}
}

func (l *log) append(key []byte, value []byte) (uint64, error) {
	msgContext := randomContext()
	le := &pb.LogEntry{
		Key:      key,
		Value:    value,
		Context:  msgContext,
		IsIntent: true,
		// will be filled by the second iteration
		// Offset: l.nextOffset,
	}
	bites, err := le.Marshal()
	if err != nil {
		return 0, err
	}

	wait := l.contextWait.register(msgContext)
	l.proposeCh <- bites

	select {
	case <-wait:
		// compute my offset
		newOffset := l.computeNewOffset(msgContext)
		// schedule writing the committed entry
		l.writeCommittedEntry(key, value, newOffset)
		// dash out to unblock the caller
		return newOffset, nil
	case <-time.After(3 * time.Second):
		return 0, fmt.Errorf("Timed out waiting for append %d", msgContext)
	}
}

func (l *log) computeNewOffset(msgContext uint64) uint64 {
	var numIntents uint64
	var e *list.Element

	l.mutex.RLock()
	// find my intent and count the number of intents along the way
	for e = l.log.Front(); e != nil; e = e.Next() {
		le := e.Value.(*pb.LogEntry)

		if le.IsIntent {
			numIntents++
		}

		if le.Context == msgContext {
			break
		}
	}

	l.mutex.RUnlock()
	// the number of intents in the log before my intent is the offset
	return l.baseOffset + numIntents
}

func (l *log) writeCommittedEntry(key []byte, value []byte, newOffset uint64) {
	le := &pb.LogEntry{
		Key:      key,
		Value:    value,
		IsIntent: false,
		Offset:   newOffset,
	}
	bites, err := le.Marshal()
	if err != nil {
		logrus.Errorf("Can't serialize uncommitted log entry: %s", err.Error())
	}
	l.proposeCh <- bites
}

func (l *log) readFromOffset(startingOffset uint64, maxBatchSize int) ([]*pb.LogEntry, error) {
	l.mutex.RLock()
	defer l.mutex.RUnlock()

	head := l.log.Front()
	headEntry := head.Value.(*pb.LogEntry)

	if headEntry.Offset > startingOffset {
		return nil, fmt.Errorf("Offset too old")
	}

	entries := make([]*pb.LogEntry, maxBatchSize)
	idx := 0
	for e := head; e != nil && idx < maxBatchSize; e = e.Next() {
		entry := e.Value.(*pb.LogEntry)
		if entry.Offset >= startingOffset && !entry.IsIntent {
			entries[idx] = entry
			idx++
		}
	}

	return entries[:idx], nil
}
