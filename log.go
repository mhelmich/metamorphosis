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
	"bytes"
	"container/list"
	"sync"
	"time"

	"github.com/mhelmich/copycat"
	"github.com/mhelmich/metamorphosis/pb"
	"github.com/sirupsen/logrus"
)

type log struct {
	log        *list.List
	mutex      *sync.RWMutex
	snapshot   *sync.Map
	nextOffset uint64
	maxLogSize int

	proposeCh        chan<- []byte
	commitCh         <-chan []byte
	errorCh          <-chan error
	snapshotConsumer copycat.SnapshotConsumer
}

func newLog(cc copycat.CopyCat) (*log, error) {
	l := &log{
		log:        &list.List{},
		mutex:      &sync.RWMutex{},
		snapshot:   &sync.Map{},
		maxLogSize: 1000,
	}

	var err error
	l.proposeCh, l.commitCh, l.errorCh, l.snapshotConsumer, err = cc.SubscribeToDataStructureWithStringID("01CFRDSD7PBQZXV8N515RVYTZQ", l.snapshotProvider)
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

				newSnap := &sync.Map{}
				for _, v := range protobuf.State {
					newSnap.Store(v.Key, v.Value)
				}

				newLog := &list.List{}
				for _, item := range protobuf.Log {
					newLog.PushBack(item)
				}

				l.mutex.Lock()
				l.log = newLog
				l.snapshot = newSnap
				l.mutex.Unlock()
			} else {
				le := &pb.LogEntry{}
				err := le.Unmarshal(data)
				if err != nil {
					logrus.Errorf("Can't unmarshal log entry: %s", err.Error())
				}

				l.mutex.Lock()
				l.log.PushBack(le)
				l.mutex.Unlock()
			}

			defer l.maybeCompact()
		}
	}
}

func (l *log) maybeCompact() {
	if l.log.Len() > l.maxLogSize {
		l.mutex.Lock()
		if l.log.Len() > l.maxLogSize {
			i := 0
			var prevElement *list.Element
			for e := l.log.Front(); e != nil || i >= l.maxLogSize; e = e.Next() {
				if prevElement != nil {
					l.log.Remove(prevElement)
				}
				entry := e.Value.(*pb.LogEntry)
				l.snapshot.Store(entry.Key, entry.Value)
				prevElement = e
				i++
			}
			l.log.Remove(prevElement)
		}
		l.mutex.Unlock()
	}
}

func (l *log) snapshotProvider() ([]byte, error) {
	m := make(map[string]*pb.LogEntry)
	wg := &sync.WaitGroup{}
	wg.Add(2)
	done := make(chan struct{})

	l.mutex.RLock()
	defer l.mutex.RUnlock()
	log := make([]*pb.LogEntry, l.log.Len())

	go func() {
		l.snapshot.Range(func(key interface{}, value interface{}) bool {
			entry := value.(*pb.LogEntry)
			m[string(entry.Key)] = entry
			return true
		})
		wg.Done()
	}()

	go func() {
		i := 0
		for e := l.log.Front(); e != nil; e = e.Next() {
			entry := e.Value.(*pb.LogEntry)
			log[i] = entry
			i++
		}
		wg.Done()
	}()

	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(1 * time.Second):
	}

	daLog := &pb.MetamorphosisLog{
		State: m,
		Log:   log,
	}
	return daLog.Marshal()
}

func (l *log) append(key []byte, value []byte) uint64 {
	entry := &pb.LogEntry{
		Key:   key,
		Value: value,
	}

	l.mutex.Lock()
	entry.Offset = l.nextOffset
	l.nextOffset++
	l.log.PushBack(entry)
	l.mutex.Unlock()

	return entry.Offset
}

func (l *log) readKey(key []byte, startingOffset uint64) *pb.LogEntry {
	l.mutex.RLock()
	defer l.mutex.RUnlock()

	head := l.log.Front()
	if head == nil {
		return nil
	}

	headEntry := head.Value.(*pb.LogEntry)
	if headEntry.Offset < startingOffset {
		for e := head; e != nil; e = e.Next() {
			entry := e.Value.(*pb.LogEntry)
			if entry.Offset >= startingOffset && bytes.Compare(entry.Key, key) == 0 {
				return entry
			}
		}
	} else {
		v, ok := l.snapshot.Load(key)
		if !ok {
			return nil
		}

		return v.(*pb.LogEntry)
	}

	return nil
}
