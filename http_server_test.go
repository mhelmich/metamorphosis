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
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/mhelmich/copycat"
	"github.com/mhelmich/metamorphosis/pb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

func TestHttpServerDebugTopicMetadata(t *testing.T) {
	cc := new(mockCopyCat)
	proposeCh := make(chan []byte)
	commitCh := make(chan []byte)
	errorCh := make(chan error)
	metadata := &pb.TopicMetadataSnapshot{
		TopicNameToDataStructureId: make(map[string]string),
	}
	metadata.TopicNameToDataStructureId["narf"] = "moep"
	bites, err := metadata.Marshal()
	assert.Nil(t, err)
	var snapshotConsumer copycat.SnapshotConsumer = func() ([]byte, error) {
		return bites, nil
	}
	var mPropose chan<- []byte = proposeCh
	var mCommit <-chan []byte = commitCh
	var mError <-chan error = errorCh
	cc.
		On("SubscribeToDataStructureWithStringID", mock.Anything, mock.Anything).
		Return(mPropose, mCommit, mError, snapshotConsumer, nil)

	tm, err := newTopicManager("./TestHttpServerDebugTopicMetadata/", cc)
	assert.Nil(t, err)

	httpServer, err := startNewHttpServer(9999, tm)
	assert.Nil(t, err)

	// trigger loading from our snapshot consumer
	commitCh <- nil
	time.Sleep(10 * time.Millisecond)
	req, err := http.NewRequest("GET", "/debug/metamorphosis/topics", nil)
	assert.Nil(t, err)

	rr := httptest.NewRecorder()
	handler := http.HandlerFunc(httpServer.debugTopicMetadata)

	handler.ServeHTTP(rr, req)
	assert.Equal(t, http.StatusOK, rr.Code)

	md := &pb.TopicMetadataSnapshot{}
	err = json.Unmarshal(rr.Body.Next(rr.Body.Len()), md)
	assert.Nil(t, err)
	assert.Equal(t, 1, len(md.TopicNameToDataStructureId))
	v, ok := md.TopicNameToDataStructureId["narf"]
	assert.True(t, ok)
	assert.Equal(t, "moep", v)
}
