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
	"os"
	"sort"
	"strconv"
	"testing"
	"time"

	"github.com/dgraph-io/badger"
	"github.com/mhelmich/metamorphosis/pb"
	"github.com/stretchr/testify/assert"
)

func TestBadgerLogAppendReadBasic(t *testing.T) {
	var err error
	var log *badgerLog
	var offset uint64
	dir := "./TestBadgerLogAppendReadBasic/"
	assert.Nil(t, os.RemoveAll(dir))
	cc := newMockCopyCat()
	log, err = newBadgerLog(dir, "T1", "01CFRDSD7PBQZXV8N515RVYTZQ", cc)
	assert.Nil(t, err)
	defer log.close()

	numEntries := 1111
	keyPrefix := "key_"
	valuePrefix := "value_"
	offsets := make([]uint64, 0)

	for i := 1; i <= numEntries; i++ {
		key := []byte(keyPrefix + strconv.Itoa(i))
		value := []byte(valuePrefix + strconv.Itoa(i))
		offset, err = log.append(key, value)
		assert.Nil(t, err)
		assert.True(t, offset >= uint64(0))
		offsets = append(offsets, offset)
	}

	assert.True(t, sort.IsSorted(offsetArray(offsets)))
	assert.Equal(t, numEntries, countLogEntries(log))

	entries, err := log.readFromOffset(uint64(45), 49)
	assert.Nil(t, err)
	assert.Equal(t, 49, len(entries))
	assert.Equal(t, []byte(keyPrefix+"46"), entries[0].Key)
	assert.Equal(t, []byte(keyPrefix+"47"), entries[1].Key)
	assert.Equal(t, []byte(keyPrefix+"94"), entries[48].Key)

	assert.Nil(t, log.close())
	assert.Nil(t, os.RemoveAll(dir))
}

func TestBadgerLogCompaction(t *testing.T) {
	var err error
	var log *badgerLog
	var offset uint64
	dir := "./TestBadgerLogCompaction/"
	assert.Nil(t, os.RemoveAll(dir))
	cc := newMockCopyCat()
	log, err = newBadgerLog(dir, "T1", "01CFRDSD7PBQZXV8N515RVYTZQ", cc)
	assert.Nil(t, err)
	defer log.close()

	numEntries := 1111
	keyPrefix := "key_"
	valuePrefix := "value_"
	offsets := make([]uint64, 0)

	for i := 1; i <= numEntries; i++ {
		key := []byte(keyPrefix + strconv.Itoa(i))
		value := []byte(valuePrefix + strconv.Itoa(i))
		offset, err = log.append(key, value)
		assert.Nil(t, err)
		assert.True(t, offset >= uint64(0))
		offsets = append(offsets, offset)
	}

	assert.True(t, sort.IsSorted(offsetArray(offsets)))
	assert.Equal(t, numEntries, countLogEntries(log))
	assert.Equal(t, numEntries, log.currentLogSize)

	log.desiredLogSize = 100
	log.maybeCompact()
	assert.Equal(t, log.desiredLogSize+1, countLogEntries(log))
	assert.Equal(t, numEntries-log.desiredLogSize, countEntriesInState(log))

	log.desiredLogSize = 10000
	for i := 1; i <= numEntries; i++ {
		key := []byte(keyPrefix + strconv.Itoa(i))
		value := []byte(valuePrefix + strconv.Itoa(i))
		offset, err = log.append(key, value)
		assert.Nil(t, err)
		assert.True(t, offset > uint64(0))
		offsets = append(offsets, offset)
	}

	assert.Equal(t, numEntries+101, countLogEntries(log))

	assert.Nil(t, log.close())
	assert.Nil(t, os.RemoveAll(dir))
}

func TestBadgerLogSnapshotProviderConsumer(t *testing.T) {
	var err error
	var log1 *badgerLog
	var offset uint64
	dir := "./TestBadgerLogSnapshotProviderConsumer/"
	assert.Nil(t, os.RemoveAll(dir))
	cc := newMockCopyCat()
	log1, err = newBadgerLog(dir, "T1", "01CFRDSD7PBQZXV8N515RVYTZQ", cc)
	assert.Nil(t, err)
	defer log1.close()

	log1.desiredLogSize = 300

	numEntries := 1111
	keyPrefix := "key_"
	valuePrefix := "value_"
	offsets := make([]uint64, 0)

	for i := 1; i <= numEntries; i++ {
		key := []byte(keyPrefix + strconv.Itoa(i))
		value := []byte(valuePrefix + strconv.Itoa(i))
		offset, err = log1.append(key, value)
		assert.Nil(t, err)
		assert.True(t, offset >= uint64(0))
		offsets = append(offsets, offset)
	}

	assert.True(t, len(offsets) > 0)
	assert.Equal(t, 601, countEntriesInState(log1))
	// HACK - compaction ran therefore we have our reserved key in there as well
	assert.Equal(t, 510+1, countLogEntries(log1))

	bites, err := log1.snapshotProvider()
	assert.Nil(t, err)
	assert.True(t, len(bites) > 0)
	logSnap := &pb.LogSnapshot{}
	err = logSnap.Unmarshal(bites)
	assert.Nil(t, err)
	assert.Equal(t, 510, len(logSnap.Log))
	assert.Equal(t, 601, len(logSnap.State.M))

	var log2 *badgerLog
	log2, err = newBadgerLog(dir, "T2", "01CFRDSD7PBQZXV8N515RVYTZQ", cc)
	assert.Nil(t, err)
	defer log2.close()
	log2.snapshotConsumer = func() ([]byte, error) {
		return bites, nil
	}
	log2.proposeCh <- nil

	// wait for the log to load itself from the snapshot
	time.Sleep(500 * time.Millisecond)
	assert.Equal(t, 601, countEntriesInState(log2))
	// HACK - compaction ran therefore we have our reserved key in there as well
	assert.Equal(t, 510+1, countLogEntries(log2))

	assert.Nil(t, os.RemoveAll(dir))
}

func countEntriesInState(log *badgerLog) int {
	size := 0
	err := log.db.View(func(txn *badger.Txn) error {
		item, err := txn.Get(snapshotKey)
		if err != nil {
			return err
		}

		bites, err := item.Value()
		if err != nil {
			return err
		}

		snap := &pb.LogSnapshot_SnapState{}
		err = snap.Unmarshal(bites)
		if err != nil {
			return err
		}

		size = len(snap.M)
		return nil
	})

	if err != nil {
		return -1
	}

	return size
}

func countLogEntries(log *badgerLog) int {
	size := 0

	err := log.db.View(func(txn *badger.Txn) error {
		it := txn.NewIterator(badger.DefaultIteratorOptions)
		defer it.Close()
		for it.Rewind(); it.Valid(); it.Next() {
			size++
		}
		return nil
	})

	if err != nil {
		return -1
	}

	return size
}

// dummy implementation to ease tests to reason about whether offsets are sorted
type offsetArray []uint64

func (a offsetArray) Len() int           { return len(a) }
func (a offsetArray) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a offsetArray) Less(i, j int) bool { return a[i] < a[j] }
