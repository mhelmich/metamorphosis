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
	"fmt"
	"sync"

	"github.com/mhelmich/copycat"
	"github.com/mhelmich/metamorphosis/pb"
	"github.com/sirupsen/logrus"
)

// in the real world, this should be a singleton
// I trust the developer to not create this more than once :)
func newTopicManager(dataDir string, cc copycat.CopyCat) (*topicManager, error) {
	tm := &topicManager{
		topicNamesToIds:  make(map[string]string),
		topicNamesToLogs: make(map[string]*logContainer),
		topicsMutex:      &sync.RWMutex{},
		cc:               cc,
		dataDir:          dataDir,
		logger:           logrus.WithFields(logrus.Fields{}),
	}

	var err error
	tm.proposeCh, tm.commitCh, tm.errorCh, tm.snapshotConsumer, err = cc.SubscribeToDataStructureWithStringID("01CFRDSD7PBQZXV8N515RVYTZQ", tm.snapshotProvider)
	if err != nil {
		return nil, err
	}

	go tm.serveChannels()
	return tm, nil
}

// topic manager is basically implementing a hashmap
// with topic names to logs
// it uses copycat to store the map and in order to keep it consistent
// there is a well-known copycat data structure id that is being used across
// all instances of metamorphosis to get topic information at the start
type topicManager struct {
	cc               copycat.CopyCat
	proposeCh        chan<- []byte
	commitCh         <-chan []byte
	errorCh          <-chan error
	snapshotConsumer copycat.SnapshotConsumer

	topicNamesToIds  map[string]string
	topicNamesToLogs map[string]*logContainer
	topicsMutex      *sync.RWMutex

	dataDir string

	logger *logrus.Entry
}

// copycat boiler plate
func (tm *topicManager) serveChannels() {
	for {
		select {
		case data, ok := <-tm.commitCh:
			if !ok {
				return
			}

			if data == nil {
				bites, err := tm.snapshotConsumer()
				if err != nil {
					tm.logger.Errorf("Error getting snapshot: %s", err.Error())
				}

				metadata := &pb.TopicMetadataSnapshot{}
				err = metadata.Unmarshal(bites)
				if err != nil {
					tm.logger.Errorf("Error unmarshaling snapshot: %s", err.Error())
				}

				tm.topicsMutex.Lock()
				tm.topicNamesToIds = metadata.TopicNameToDataStructureId
				tm.topicsMutex.Unlock()
			} else {
				op := &pb.TopicMetadataOperation{}
				err := op.Unmarshal(data)
				if err != nil {
					tm.logger.Errorf("Can't unmarshal log entry: %s", err.Error())
				}

				tm.topicsMutex.Lock()
				if op.DataStructureId == "" {
					delete(tm.topicNamesToIds, op.Name)
					lc, ok := tm.topicNamesToLogs[op.Name]
					if ok {
						delete(tm.topicNamesToLogs, op.Name)
						lc.l.close()
						// TODO - add the ability to delete a data structure in CC
					}
				} else {
					tm.topicNamesToIds[op.Name] = op.DataStructureId
				}
				tm.topicsMutex.Unlock()
			}
		}
	}
}

// copycat boiler plate
func (tm *topicManager) snapshotProvider() ([]byte, error) {
	tm.topicsMutex.RLock()
	snap := &pb.TopicMetadataSnapshot{
		TopicNameToDataStructureId: tm.topicNamesToIds,
	}
	bites, err := snap.Marshal()
	tm.topicsMutex.RUnlock()
	return bites, err
}

// creates a new topic with the given name
func (tm *topicManager) createTopic(name string) error {
	tm.topicsMutex.RLock()
	_, ok := tm.topicNamesToIds[name]
	tm.topicsMutex.RUnlock()
	if ok {
		return fmt.Errorf("Topic with name [%s] already exists", name)
	}

	// this allocates a copycat data structure but doesn't create a log structure yet
	id, err := tm.cc.AllocateNewDataStructure()
	if err != nil {
		return err
	}

	protobuf := &pb.TopicMetadataOperation{
		Name:            name,
		DataStructureId: id.String(),
	}

	bites, err := protobuf.Marshal()
	if err != nil {
		return err
	}

	tm.proposeCh <- bites
	return nil
}

// NB: obviously only do this when you know what you're doing
func (tm *topicManager) deleteTopic(name string) error {
	protobuf := &pb.TopicMetadataOperation{
		Name:            name,
		DataStructureId: "",
	}

	bites, err := protobuf.Marshal()
	if err != nil {
		return err
	}

	tm.proposeCh <- bites
	return nil
}

// NB: assumes you have the lock
// closes the topic on the local node
// doesn't not delete the topic everywhere
func (tm *topicManager) closeTopic(l log) error {
	delete(tm.topicNamesToLogs, l.getTopicName())
	defer l.close()
	return nil
}

func (tm *topicManager) returnToPool(l log) error {
	tm.topicsMutex.Lock()
	defer tm.topicsMutex.Unlock()
	lc, ok := tm.topicNamesToLogs[l.getTopicName()]
	if !ok {
		err := fmt.Errorf("You're trying to return topic I can't find anymore: %s", l.getTopicName())
		tm.logger.Panicf("%s", err.Error())
		return err
	}

	lc.refCount--
	if lc.refCount == 0 {
		n := l.getTopicName()
		tm.logger.Infof("closing topic %s", n)
		tm.closeTopic(l)
	} else if lc.refCount < 0 {
		err := fmt.Errorf("You're trying to return topic with ref count 0: %s", l.getTopicName())
		tm.logger.Panicf("%s", err.Error())
		return err
	}
	return nil
}

// NB: make sure to release your locks properly
func (tm *topicManager) getTopicForName(name string) (log, error) {
	tm.topicsMutex.Lock()
	defer tm.topicsMutex.Unlock()

	lc, ok := tm.topicNamesToLogs[name]
	if ok {
		lc.refCount++
		return lc.l, nil
	}

	id, ok := tm.topicNamesToIds[name]
	if !ok {
		return nil, fmt.Errorf("No topic with name [%s]", name)
	}

	return tm.createNewTopic(name, id)
}

// NB: assumes you have the lock
func (tm *topicManager) createNewTopic(name string, id string) (log, error) {
	lc, ok := tm.topicNamesToLogs[name]
	if ok {
		lc.refCount++
		return lc.l, nil
	}

	// lazily create the log if we know about the topic/cc ID
	l, err := newBadgerLog(tm.dataDir, name, id, tm.cc)
	if err != nil {
		return nil, err
	}

	tm.topicNamesToLogs[name] = &logContainer{
		l:        l,
		refCount: 1,
	}
	return l, nil
}

type log interface {
	append(key []byte, value []byte) (uint64, error)
	readFromOffset(startingOffset uint64, maxBatchSize int) ([]*pb.LogEntry, error)
	getTopicName() string
	close() error
	// TODO - this has to go
	snapshotProvider() ([]byte, error)
}

// poor man's ref counting container
type logContainer struct {
	l        log
	refCount int
}
