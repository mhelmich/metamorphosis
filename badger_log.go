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
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"
	"unsafe"

	"github.com/dgraph-io/badger"
	"github.com/mhelmich/copycat"
	"github.com/mhelmich/metamorphosis/pb"
	"github.com/sirupsen/logrus"
)

var (
	// HACK: I'm exploiting the fact that 0 is not a valid offset
	// at this point the zero byte is some kind of reserved parameter
	// *sigh*
	// I'm not particularly happy with this either but it allows me
	// to keep the state map and the log in one place :)
	// this leads to a little bit of snafu where compaction constantly
	// deletes and re-creates this key over and over again
	// but I'm willing to live with it for now
	snapshotKey = []byte{0}

	errLogEmpty = errors.New("The log is empty")
)

type badgerLog struct {
	topicName string
	// will hold all the data
	// the log will have an uint64 offset
	// the snapshot will be behind one static key (snapshotKey)
	db               *badger.DB
	proposeCh        chan<- []byte
	commitCh         <-chan []byte
	errorCh          <-chan error
	snapshotConsumer copycat.SnapshotConsumer
	// a log entry offset is determined by the offset of the previous entry
	// if there is no previous entry (say at the beginning or after a compaction or a snapshot restore)
	// we fall back to using this integer
	// this will be part of the copycat snapshot that is being created from time to time
	// defauls to 0 which makes the first offset be 1
	baseOffset uint64
	// poor mans future
	// makes a routine wait for a commit entry to be replayed
	contextWait *contextWait
	// go routines will be waiting on this channel to be closed
	// if there are no log entries for them to read
	waitCh chan interface{}
	// the good enough size of the log
	// this is only used to determine whether we should compact the log or not
	// defaults to zero
	currentLogSize int
	desiredLogSize int
	compactLock    *sync.Mutex
	alreadyClosed  int32 // atomic boolean indicating whether this log has been closed or not
}

func newBadgerLog(dataDir string, topicName string, dataStructureId string, cc copycat.CopyCat) (*badgerLog, error) {
	var err error
	dir := dataDir + topicName
	err = createDirIfNotExists(dir)
	if err != nil {
		return nil, err
	}

	opts := badger.DefaultOptions
	opts.Dir = dir
	opts.ValueDir = dir
	db, err := badger.Open(opts)
	if err != nil {
		return nil, err
	}

	l := &badgerLog{
		db:             db,
		topicName:      topicName,
		contextWait:    newContextWait(),
		desiredLogSize: 5000,
		compactLock:    &sync.Mutex{},
	}

	l.proposeCh, l.commitCh, l.errorCh, l.snapshotConsumer, err = cc.SubscribeToDataStructureWithStringID(dataStructureId, l.snapshotProvider)
	if err != nil {
		defer db.Close()
		return nil, err
	}

	go l.serveChannels()
	return l, nil
}

func (l *badgerLog) serveChannels() {
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

				protobuf := &pb.LogSnapshot{}
				err = protobuf.Unmarshal(bites)
				if err != nil {
					logrus.Errorf("Error unmarshaling snapshot: %s", err.Error())
				}

				// START OF RW TXN
				err = l.db.Update(func(txn *badger.Txn) error {
					var txnErr error
					opts := badger.DefaultIteratorOptions
					opts.PrefetchValues = false
					it := txn.NewIterator(opts)
					defer it.Close()

					// clear the entire log
					for it.Rewind(); it.Valid(); it.Next() {
						txnErr = txn.Delete(it.Item().Key())
						if txnErr != nil {
							return txnErr
						}
					}

					// fill the log
					var txnBites []byte
					for _, le := range protobuf.Log {
						txnBites, txnErr = le.Marshal()
						if txnErr != nil {
							return txnErr
						}

						txnErr = txn.Set(uint64ToBytes(le.Offset), txnBites)
						if txnErr != nil {
							return txnErr
						}
					}

					// flash in the snapshot data
					txnBites, txnErr = protobuf.State.Marshal()
					if txnErr != nil {
						return txnErr
					}
					return txn.Set(snapshotKey, txnBites)
				})
				// END OF TXN
				if err != nil {
					logrus.Errorf("Can't override state with new snapshot: %s", err.Error())
				}

				l.currentLogSize = len(protobuf.Log)
				l.baseOffset = protobuf.BaseOffset
			} else {
				le := &pb.LogEntry{}
				err := le.Unmarshal(data)
				if err != nil {
					logrus.Errorf("Can't unmarshal log entry: %s", err.Error())
				}

				newOffset, err := l.computeNewOffset()
				if err != nil {
					logrus.Errorf("Can't compute offset: %s", err.Error())
				}

				keyBites := uint64ToBytes(newOffset)
				le.Offset = newOffset
				valueBites, err := le.Marshal()
				if err != nil {
					logrus.Errorf("Can't marshal log entry: %s", err.Error())
				}

				err = l.db.Update(func(txn *badger.Txn) error {
					return txn.Set(keyBites, valueBites)
				})
				if err != nil {
					logrus.Errorf("Can't set new entry: %s", err.Error())
				}

				l.currentLogSize++
				l.contextWait.trigger(le.Context, newOffset)
				// release waiters after a new log entry (not intent) arrived
				l.closeWaitChannel()
			}

			// may compact in a different go routine
			// can't use defer here because we never leave this method
			go l.maybeCompact()
		}
	}
}

func (l *badgerLog) computeNewOffset() (uint64, error) {
	var lastOffset uint64
	err := l.db.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.Reverse = true
		opts.PrefetchValues = false
		it := txn.NewIterator(opts)
		defer it.Close()
		it.Rewind()
		if !it.Valid() {
			return errLogEmpty
		}

		bites := it.Item().Key()
		lastOffset = bytesToUint64(bites)
		return nil
	})

	// if there's no log yet, there are two different scenarios
	// 1. we are at the very beginning and we should return zero to be the first offset
	// 2. we just compacted, therefore fall back to the baseOffset and increment
	if err == errLogEmpty {
		if l.baseOffset == 0 {
			return 0, nil
		}
		return l.baseOffset + 1, nil
	}

	return lastOffset + 1, err
}

func (l *badgerLog) maybeCompact() {
	if l.currentLogSize > l.desiredLogSize*3 {
		l.compactLock.Lock()
		defer l.compactLock.Unlock()

		if l.currentLogSize > l.desiredLogSize*3 {
			var err error
			numItemsToClean := l.currentLogSize - l.desiredLogSize
			lesToClean := make([]*pb.LogEntry, numItemsToClean)
			snapshot := &pb.LogSnapshot_SnapState{}

			err = l.db.View(func(txn *badger.Txn) error {
				var txnErr error
				var bites []byte
				opts := badger.DefaultIteratorOptions
				opts.PrefetchSize = numItemsToClean
				it := txn.NewIterator(opts)
				defer it.Close()

				// COLLECT LOG ENTRIES TO CLEAN UP
				i := 0
				it.Rewind()
				// HACK - account for leading state key
				it.Next()
				for ; i < numItemsToClean && it.Valid(); it.Next() {
					bites, txnErr = it.Item().Value()
					if txnErr != nil {
						return txnErr
					}

					leToClean := &pb.LogEntry{}
					txnErr = leToClean.Unmarshal(bites)
					if txnErr != nil {
						return txnErr
					}
					lesToClean[i] = leToClean
					i++
				}

				// GET THE SNAPSHOT
				item, txnErr := txn.Get(snapshotKey)
				if txnErr == badger.ErrKeyNotFound {
					// if the key doesn't exist, I'm here the first time
					// that means we can dash out of the txn and let downstream
					// code take care of creating this key
					return nil
				} else if txnErr != nil {
					return txnErr
				}

				bites, txnErr = item.Value()
				if txnErr != nil {
					return txnErr
				}
				return snapshot.Unmarshal(bites)
			})

			if err != nil {
				logrus.Errorf("Compaction failed: %s", err.Error())
				return
			}

			if snapshot.M == nil {
				snapshot.M = make(map[string][]byte)
			}

			for idx := range lesToClean {
				if lesToClean[idx] == nil {
					break
				}

				snapshot.M[string(lesToClean[idx].Key)] = lesToClean[idx].Value
			}

			bites, err := snapshot.Marshal()
			if err != nil {
				logrus.Errorf("Compaction failed: %s", err.Error())
				return
			}

			err = l.db.Update(func(txn *badger.Txn) error {
				for idx := range lesToClean {
					if lesToClean[idx] != nil {
						txnErr := txn.Delete(uint64ToBytes(lesToClean[idx].Offset))
						if txnErr != nil {
							return txnErr
						}
					}
				}

				return txn.Set(snapshotKey, bites)
			})
			if err != nil {
				logrus.Errorf("Compaction failed: %s", err.Error())
				return
			}

			// set current size
			l.currentLogSize = l.currentLogSize - numItemsToClean

			// run badger GC
			l.db.RunValueLogGC(0.5)
		} // END CHECK LOCK CHECK
	}
}

func (l *badgerLog) snapshotProvider() ([]byte, error) {
	logSnap := &pb.LogSnapshot{
		State: &pb.LogSnapshot_SnapState{
			M: make(map[string][]byte),
		},
		Log: make([]*pb.LogEntry, l.currentLogSize),
	}

	err := l.db.View(func(txn *badger.Txn) error {
		var bites []byte
		var txnErr error
		it := txn.NewIterator(badger.DefaultIteratorOptions)
		defer it.Close()

		it.Rewind()
		// HACK - account for leading state key
		it.Next()
		idx := 0
		for ; it.Valid(); it.Next() {
			bites, txnErr = it.Item().Value()
			if txnErr != nil {
				return txnErr
			}

			le := &pb.LogEntry{}
			txnErr = le.Unmarshal(bites)
			if txnErr != nil {
				return txnErr
			}
			logSnap.Log[idx] = le
			idx++
		}

		var item *badger.Item
		item, txnErr = txn.Get(snapshotKey)
		if txnErr != nil && txnErr == badger.ErrKeyNotFound {
			// if we can't find the key, we're here the first time
			// no compaction has run ever, etc.
			// set an empty map and return
			logSnap.State = &pb.LogSnapshot_SnapState{
				M: make(map[string][]byte),
			}
			return nil
		} else if txnErr != nil {
			return txnErr
		}

		bites, txnErr = item.Value()
		if txnErr != nil {
			return txnErr
		}

		snap := &pb.LogSnapshot_SnapState{}
		txnErr = snap.Unmarshal(bites)
		if txnErr != nil {
			return txnErr
		}

		logSnap.State = snap
		return nil
	})
	if err != nil {
		return nil, err
	}

	return logSnap.Marshal()
}

func (l *badgerLog) append(key []byte, value []byte) (uint64, error) {
	msgContext := randomContext()
	le := &pb.LogEntry{
		Key:     key,
		Value:   value,
		Context: msgContext,
		// will be filled by serveChannels when the entry is committed
		// Offset: l.nextOffset,
	}
	bites, err := le.Marshal()
	if err != nil {
		return 0, err
	}

	wait := l.contextWait.register(msgContext)
	l.proposeCh <- bites

	select {
	case v := <-wait:
		newOffset := v.(uint64)
		return newOffset, nil
	case <-time.After(3 * time.Second):
		return 0, fmt.Errorf("Timed out waiting for append %d", msgContext)
	}
}

func (l *badgerLog) readFromOffset(startingOffset uint64, maxBatchSize int) ([]*pb.LogEntry, error) {
	if startingOffset < l.baseOffset {
		return nil, fmt.Errorf("Snapshot too old. Earliest offset %d want %d", l.baseOffset, startingOffset)
	}

	entries := make([]*pb.LogEntry, maxBatchSize)
	idx := 0
	err := l.db.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.PrefetchSize = maxBatchSize
		it := txn.NewIterator(opts)
		defer it.Close()

		it.Seek(uint64ToBytes(startingOffset))
		if !it.Valid() && !l.waitForAppend(5*time.Second) {
			return errLogEmpty
		}

		it.Seek(uint64ToBytes(startingOffset))
		for ; idx < maxBatchSize && it.Valid(); idx++ {
			// later on unmarshal will make a copy of the data anyways
			// no need to make two copies of the data
			bites, txnErr := it.Item().Value()
			if txnErr != nil {
				return txnErr
			}

			le := &pb.LogEntry{}
			txnErr = le.Unmarshal(bites)
			if txnErr != nil {
				return txnErr
			}

			entries[idx] = le
			it.Next()
		}

		return nil
	})
	if err == errLogEmpty {
		return make([]*pb.LogEntry, 0), nil
	} else if err != nil {
		return nil, err
	}

	return entries[:idx], nil
}

func (l *badgerLog) waitForAppend(d time.Duration) bool {
	select {
	case <-l.casWaitChannel():
		return true
	case <-time.After(d):
		return false
	}
}

func (l *badgerLog) casWaitChannel() chan interface{} {
	ch := make(chan interface{})
	if atomic.CompareAndSwapPointer(
		(*unsafe.Pointer)(unsafe.Pointer(&l.waitCh)),
		unsafe.Pointer(nil),
		unsafe.Pointer(&ch)) {
		return ch
	}

	chP := (*chan interface{})(atomic.LoadPointer((*unsafe.Pointer)(unsafe.Pointer(&l.waitCh))))
	return *chP
}

func (l *badgerLog) closeWaitChannel() {
	chP := (*chan interface{})(atomic.LoadPointer((*unsafe.Pointer)(unsafe.Pointer(&l.waitCh))))
	if chP != nil {
		atomic.CompareAndSwapPointer(
			(*unsafe.Pointer)(unsafe.Pointer(&l.waitCh)),
			unsafe.Pointer(chP),
			unsafe.Pointer(nil))
		close(*chP)
	}
}

func (l *badgerLog) getTopicName() string {
	return l.topicName
}

// NB: never call this directly!
// always let the manager take care of this
func (l *badgerLog) close() error {
	// closing a closed channel causes a panic
	// better be safe than sorry
	if atomic.CompareAndSwapInt32(&l.alreadyClosed, int32(0), int32(1)) {
		// this cleans up all underlying resources
		close(l.proposeCh)
		return l.db.Close()
	}
	return nil
}
