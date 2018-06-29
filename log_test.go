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
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/mhelmich/metamorphosis/pb"
	"github.com/stretchr/testify/assert"
)

func TestLogAppendReadBasic(t *testing.T) {
	cc := newMockCopyCat()
	log, err := newLog(cc)
	assert.Nil(t, err)

	keyPrefix := "key_"
	valuePrefix := "value_"
	offsets := make([]uint64, 0)

	for i := 1; i <= 99; i++ {
		key := []byte(keyPrefix + strconv.Itoa(i))
		value := []byte(valuePrefix + strconv.Itoa(i))
		offset := log.append(key, value)
		offsets = append(offsets, offset)
	}

	assert.Equal(t, 0, sizeOfMap(log.snapshot))
	assert.Equal(t, 99, log.log.Len())

	assert.Equal(t, 99, len(offsets))
	entries, err := log.readFromOffset(uint64(45), 49)
	assert.Nil(t, err)
	assert.Equal(t, 49, len(entries))
	assert.Equal(t, []byte(keyPrefix+"46"), entries[0].Key)
	assert.Equal(t, []byte(keyPrefix+"47"), entries[1].Key)
	assert.Equal(t, []byte(keyPrefix+"94"), entries[48].Key)
}

func TestLogBasic(t *testing.T) {
	cc := newMockCopyCat()
	log, err := newLog(cc)
	assert.Nil(t, err)
	log.maxLogSize = 33

	keyPrefix := "key_"
	valuePrefix := "value_"
	offsets := make([]uint64, 0)

	for i := 1; i <= 99; i++ {
		key := []byte(keyPrefix + strconv.Itoa(i))
		value := []byte(valuePrefix + strconv.Itoa(i))
		offset := log.append(key, value)
		offsets = append(offsets, offset)
	}

	assert.Equal(t, 99, len(offsets))
	logSize := 0
	for i := 0; i < 3 && len(offsets) != logSize; i++ {
		log.mutex.Lock()
		logSize = log.log.Len()
		log.mutex.Unlock()
		time.Sleep(50 * time.Millisecond)
	}
	log.mutex.Lock()
	assert.Equal(t, 0, sizeOfMap(log.snapshot))
	assert.Equal(t, 99, log.log.Len())
	log.mutex.Unlock()
	// test compaction
	log.maybeCompact()

	log.mutex.Lock()
	assert.Equal(t, 67, sizeOfMap(log.snapshot))
	assert.Equal(t, 32, log.log.Len())
	log.mutex.Unlock()

	// test snapshot provider
	bites, err := log.snapshotProvider()
	assert.Nil(t, err)
	daLog := &pb.MetamorphosisLog{}
	err = daLog.Unmarshal(bites)
	assert.Nil(t, err)
	assert.Equal(t, 32, len(daLog.Log))
	assert.Equal(t, 67, len(daLog.State))

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
		time.Sleep(1 * time.Millisecond)
		logSize = log.log.Len()
	}

	assert.Equal(t, 67, sizeOfMap(log.snapshot))
	assert.Equal(t, 32, log.log.Len())
}
