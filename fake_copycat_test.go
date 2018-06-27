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
	"crypto/rand"

	"github.com/mhelmich/copycat"
	"github.com/stretchr/testify/mock"
)

func newMockCopyCat() copycat.CopyCat {
	proposeCh := make(chan []byte)
	commitCh := make(chan []byte)
	go func() {
		for {
			bites, ok := <-proposeCh
			if !ok {
				return
			}
			commitCh <- bites
		}
	}()

	var returnedProposeCh chan<- []byte = proposeCh
	var returnedCommitCh <-chan []byte = commitCh

	mockCat := new(mockCopyCat)
	mockCat.On("AllocateNewDataStructure", mock.Anything).Return(newRandomId(), nil)
	mockCat.On("SubscribeToDataStructureWithStringID", mock.Anything, mock.Anything).Return(returnedProposeCh, returnedCommitCh, nil, nil, nil)
	mockCat.On("Shutdown").Return()
	return mockCat
}

func newRandomId() *copycat.ID {
	var id copycat.ID
	rand.Reader.Read(id[:])
	return &id
}
