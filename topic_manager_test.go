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
	"testing"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
)

func TestTopicManagerBasic(t *testing.T) {
	topic2 := "testing_test__22"
	cc := newMockCopyCat()
	dir := "./TestTopicManagerBasic/"
	assert.Nil(t, os.RemoveAll(dir))
	tm, err := newTopicManager(dir, cc)
	assert.Nil(t, err)
	err = tm.createTopic("testing_test")
	assert.Nil(t, err)
	time.Sleep(5 * time.Millisecond)
	id1, ok := tm.topicNamesToIds["testing_test"]
	assert.True(t, ok)
	assert.Equal(t, 1, len(tm.topicNamesToIds))
	logrus.Infof("new id [%s]", id1)

	err = tm.createTopic(topic2)
	assert.Nil(t, err)
	time.Sleep(5 * time.Millisecond)
	id2, ok := tm.topicNamesToIds[topic2]
	assert.True(t, ok)
	assert.Equal(t, 2, len(tm.topicNamesToIds))
	logrus.Infof("new id [%s]", id2)

	l, err := tm.getTopicForName(topic2)
	assert.Nil(t, err)
	assert.NotNil(t, l)
	err = tm.returnToPool(l)
	assert.Nil(t, err)
	assert.Nil(t, os.RemoveAll(dir))
}

func TestTopicManagerCheckinOut(t *testing.T) {
	topicName := "testing_test__22"
	cc := newMockCopyCat()
	dir := "./TestTopicManagerCheckinOut/"
	assert.Nil(t, os.RemoveAll(dir))
	tm, err := newTopicManager(dir, cc)
	assert.Nil(t, err)

	err = tm.createTopic(topicName)
	assert.Nil(t, err)
	time.Sleep(5 * time.Millisecond)
	_, ok := tm.topicNamesToIds[topicName]
	assert.True(t, ok)
	assert.Equal(t, 1, len(tm.topicNamesToIds))

	l1, err := tm.getTopicForName(topicName)
	assert.Nil(t, err)
	assert.NotNil(t, l1)
	l2, err := tm.getTopicForName(topicName)
	assert.Nil(t, err)
	assert.NotNil(t, l2)

	lc, ok := tm.topicNamesToLogs[topicName]
	assert.True(t, ok)
	assert.Equal(t, 2, lc.refCount)

	err = tm.returnToPool(l1)
	assert.Nil(t, err)

	lc, ok = tm.topicNamesToLogs[topicName]
	assert.True(t, ok)
	assert.Equal(t, 1, lc.refCount)

	err = tm.returnToPool(l2)
	assert.Nil(t, err)

	lc, ok = tm.topicNamesToLogs[topicName]
	assert.False(t, ok)

	assert.Nil(t, os.RemoveAll(dir))
}
