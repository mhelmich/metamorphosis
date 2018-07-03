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
	"sync"

	"github.com/sirupsen/logrus"
)

func newContextWait() *contextWait {
	return &contextWait{
		m:    make(map[uint64]chan interface{}),
		lock: &sync.Mutex{},
	}
}

type contextWait struct {
	m    map[uint64]chan interface{}
	lock *sync.Mutex
}

func (w *contextWait) register(id uint64) <-chan interface{} {
	w.lock.Lock()
	_, ok := w.m[id]
	if ok {
		logrus.Panic("dup id")
	}

	ch := make(chan interface{})
	w.m[id] = ch
	w.lock.Unlock()
	return ch
}

func (w *contextWait) trigger(id uint64) {
	w.lock.Lock()
	ch, ok := w.m[id]
	if ok {
		delete(w.m, id)
		close(ch)
	}
	w.lock.Unlock()
}
