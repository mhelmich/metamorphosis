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
	"crypto/rand"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/mhelmich/metamorphosis/pb"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
)

func TestLogAppendReadBasic(t *testing.T) {
	var err error
	var offset uint64
	cc := newMockCopyCat()
	log, err := newLog("01CFRDSD7PBQZXV8N515RVYTZQ", cc)
	assert.Nil(t, err)

	keyPrefix := "key_"
	valuePrefix := "value_"
	offsets := make([]uint64, 0)

	for i := 1; i <= 99; i++ {
		key := []byte(keyPrefix + strconv.Itoa(i))
		value := []byte(valuePrefix + strconv.Itoa(i))
		offset, err = log.append(key, value)
		assert.Nil(t, err)
		assert.True(t, offset > uint64(0))
		offsets = append(offsets, offset)
	}

	logSize := 0
	for i := 0; i < 3 && len(offsets)*2 != logSize; i++ {
		log.mutex.Lock()
		logSize = log.log.Len()
		log.mutex.Unlock()
		time.Sleep(5 * time.Millisecond)
	}

	assert.Equal(t, 0, sizeOfMap(log.snapshot))
	assert.Equal(t, len(offsets)*2, log.log.Len())

	for e := log.log.Front(); e != nil; e = e.Next() {
		le := e.Value.(*pb.LogEntry)
		if !le.IsIntent {
			ko := strings.Split(string(le.Key), "_")[1]
			var keyOffset uint64
			keyOffset, err = strconv.ParseUint(ko, 10, 64)
			assert.Nil(t, err)
			assert.Equal(t, le.Offset, keyOffset)
		}
	}

	for i := 1; i < len(offsets); i++ {
		assert.Equal(t, offsets[i-1]+1, offsets[i])
	}

	assert.Equal(t, 99, len(offsets))
	entries, err := log.readFromOffset(uint64(45), 49)
	assert.Nil(t, err)
	assert.Equal(t, 49, len(entries))
	assert.Equal(t, []byte(keyPrefix+"45"), entries[0].Key)
	assert.Equal(t, []byte(keyPrefix+"46"), entries[1].Key)
	assert.Equal(t, []byte(keyPrefix+"93"), entries[48].Key)
}

func TestLogComputeNewOffset(t *testing.T) {
	var newOffset uint64

	cc := newMockCopyCat()
	log, err := newLog("01CFRDSD7PBQZXV8N515RVYTZQ", cc)
	assert.Nil(t, err)
	le1I := &pb.LogEntry{
		Context:  45,
		IsIntent: true,
	}
	log.log.PushBack(le1I)
	newOffset = log.computeNewOffset(65)
	assert.Equal(t, uint64(1), newOffset)

	le1C := &pb.LogEntry{
		Offset:   newOffset,
		IsIntent: false,
	}
	le2I := &pb.LogEntry{
		Context:  65,
		IsIntent: true,
	}

	log.log.PushBack(le1C)
	log.log.PushBack(le2I)
	newOffset = log.computeNewOffset(65)
	assert.Equal(t, uint64(2), newOffset)

	le2C := &pb.LogEntry{
		Offset:   newOffset,
		IsIntent: false,
	}
	le3I := &pb.LogEntry{
		IsIntent: true,
	}
	le4I := &pb.LogEntry{
		IsIntent: true,
	}
	le5I := &pb.LogEntry{
		Context:  99,
		IsIntent: true,
	}
	log.log.PushBack(le2C)
	log.log.PushBack(le3I)
	log.log.PushBack(le4I)
	log.log.PushBack(le5I)
	newOffset = log.computeNewOffset(99)
	assert.Equal(t, uint64(5), newOffset)
}

func TestLogComputeNewOffsetOutOfOrderCommits(t *testing.T) {
	cc := newMockCopyCat()
	log, err := newLog("01CFRDSD7PBQZXV8N515RVYTZQ", cc)
	assert.Nil(t, err)
	log.log.PushBack(&pb.LogEntry{
		Context:  95,
		IsIntent: true,
	})
	log.log.PushBack(&pb.LogEntry{
		Offset:  5,
		Context: 96,
	})
	log.log.PushBack(&pb.LogEntry{
		Offset:  4,
		Context: 97,
	})
	log.log.PushBack(&pb.LogEntry{
		Context:  98,
		IsIntent: true,
	})
	log.log.PushBack(&pb.LogEntry{
		Context:  99,
		IsIntent: true,
	})

	newOffset := log.computeNewOffset(99)
	assert.Equal(t, uint64(3), newOffset)
}

func TestLogCompactionOffset(t *testing.T) {
	cc := newMockCopyCat()
	log, err := newLog("01CFRDSD7PBQZXV8N515RVYTZQ", cc)
	assert.Nil(t, err)

	log.log.PushBack(&pb.LogEntry{
		Context:  95,
		IsIntent: true,
	})
	log.log.PushBack(&pb.LogEntry{
		Context:  96,
		IsIntent: true,
	})
	log.log.PushBack(&pb.LogEntry{
		Context:  97,
		Offset:   23,
		IsIntent: false,
	})
	log.log.PushBack(&pb.LogEntry{
		Context:  98,
		IsIntent: true,
	})
	log.log.PushBack(&pb.LogEntry{
		Context:  99,
		IsIntent: true,
	})

	newOffset := log.computeNewOffset(99)
	assert.Equal(t, uint64(4), newOffset)
	log.anticipatedLogLen = 1
	log.maybeCompact()

	assert.Equal(t, 1, log.log.Len())
}

func TestLogBasic(t *testing.T) {
	var err error
	var offset uint64
	cc := newMockCopyCat()
	log, err := newLog("01CFRDSD7PBQZXV8N515RVYTZQ", cc)
	assert.Nil(t, err)

	keyPrefix := "key_"
	valuePrefix := "value_"
	offsets := make([]uint64, 0)

	for i := 1; i <= 99; i++ {
		key := []byte(keyPrefix + strconv.Itoa(i))
		value := []byte(valuePrefix + strconv.Itoa(i))
		offset, err = log.append(key, value)
		assert.Nil(t, err)
		assert.True(t, offset > uint64(0))
		offsets = append(offsets, offset)
	}

	assert.Equal(t, 99, len(offsets))
	logSize := 0
	for i := 0; i < 3 && len(offsets) != logSize; i++ {
		log.mutex.Lock()
		logSize = log.log.Len()
		log.mutex.Unlock()
		time.Sleep(5 * time.Millisecond)
	}
	log.mutex.Lock()
	assert.Equal(t, 0, sizeOfMap(log.snapshot))
	assert.Equal(t, 198, log.log.Len())
	log.mutex.Unlock()
	// test compaction
	log.anticipatedLogLen = 33
	log.highWatermarkLogSizeMultiplicator = 1
	log.maybeCompact()

	log.mutex.Lock()
	assert.Equal(t, 99, sizeOfMap(log.snapshot))
	assert.Equal(t, 33, log.log.Len())
	log.mutex.Unlock()

	// test snapshot provider
	bites, err := log.snapshotProvider()
	assert.Nil(t, err)
	daLog := &pb.MetamorphosisLog{}
	err = daLog.Unmarshal(bites)
	assert.Nil(t, err)
	assert.Equal(t, 0, len(daLog.Log))
	assert.Equal(t, 99, len(daLog.State))

	// test snapshot consumer
	log.snapshotConsumer = func() ([]byte, error) {
		return bites, nil
	}

	log.log = &list.List{}
	log.snapshot = &sync.Map{}
	// send nil to invoke snapshot consumption
	log.proposeCh <- nil

	logSize = 0
	for i := 0; i < 3 && logSize != 32; i++ {
		time.Sleep(5 * time.Millisecond)
		logSize = log.log.Len()
	}

	assert.Equal(t, 99, sizeOfMap(log.snapshot))
	assert.Equal(t, 0, log.log.Len())
}

func TestLogConcurrentAppend(t *testing.T) {
	var err error
	cc := newMockCopyCat()
	log, err := newLog("01CFRDSD7PBQZXV8N515RVYTZQ", cc)
	assert.Nil(t, err)

	threads := 33
	keys := 222
	wg := &sync.WaitGroup{}
	wg.Add(threads)
	dupMap := &sync.Map{}
	// effectively turn off compaction
	log.anticipatedLogLen = threads * keys

	for i := 0; i < threads; i++ {
		routineId := i
		go func() {
			var offset uint64
			var err2 error
			for j := 0; j < keys; j++ {
				k := fmt.Sprintf("key_%d_%d", routineId, j)
				value := make([]byte, 4)
				rand.Read(value)
				offset, err2 = log.append([]byte(k), value)
				assert.Nil(t, err2)
				_, exists := dupMap.Load(offset)
				assert.False(t, exists)
				dupMap.Store(offset, false)
			}
			wg.Done()
		}()
	}

	logSize := threads * keys
	wg.Wait()
	for i := 0; i < 3 && log.log.Len() != logSize; i++ {
		log.mutex.Lock()
		logSize = log.log.Len()
		log.mutex.Unlock()
		time.Sleep(5 * time.Millisecond)
	}

	entries := make([]*pb.LogEntry, threads*keys)
	i := 0
	for e := log.log.Front(); e != nil; e = e.Next() {
		le := e.Value.(*pb.LogEntry)
		if !le.IsIntent {
			entries[i] = le
			i++
		}
	}

	assert.True(t, sort.IsSorted(idRange(entries)))

	bites, err := log.snapshotProvider()
	assert.Nil(t, err)

	daLog := &pb.MetamorphosisLog{}
	err = daLog.Unmarshal(bites)
	assert.Nil(t, err)
	assert.Equal(t, 0, len(daLog.State))
	assert.True(t, sort.IsSorted(idRange(daLog.Log)))

	logrus.Infof("Overall: state %d log %d => %d", len(daLog.State), len(daLog.Log), len(daLog.State)+len(daLog.Log))
}

// dummy implementation to ease tests to reason about whether log entries are sorted
type idRange []*pb.LogEntry

func (a idRange) Len() int           { return len(a) }
func (a idRange) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a idRange) Less(i, j int) bool { return a[i].Offset < a[j].Offset }
