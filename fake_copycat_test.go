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
)

func newMockCopyCat() copycat.CopyCat {
	return &fakeCopyCat{}
}

func newRandomCopyCatId() *copycat.ID {
	var id copycat.ID
	rand.Reader.Read(id[:])
	return &id
}

type fakeCopyCat struct{}

func (cc *fakeCopyCat) NewDataStructureID() (*copycat.ID, error) {
	return newRandomCopyCatId(), nil
}

func (cc *fakeCopyCat) AllocateNewDataStructure(opts ...copycat.AllocationOption) (*copycat.ID, error) {
	return cc.NewDataStructureID()
}

func (cc *fakeCopyCat) SubscribeToDataStructureWithStringID(id string, provider copycat.SnapshotProvider) (chan<- []byte, <-chan []byte, <-chan error, copycat.SnapshotConsumer, error) {
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
	return proposeCh, commitCh, nil, nil, nil
}

func (cc *fakeCopyCat) SubscribeToDataStructure(id *copycat.ID, provider copycat.SnapshotProvider) (chan<- []byte, <-chan []byte, <-chan error, copycat.SnapshotConsumer, error) {
	return nil, nil, nil, nil, nil
}

func (cc *fakeCopyCat) Shutdown() {}
