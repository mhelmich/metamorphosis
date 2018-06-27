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
	"encoding/json"
	"io"
	"net/http"

	"github.com/mhelmich/metamorphosis/pb"
)

type httpServer struct {
	theLog *log
}

// naive implementation
func (s *httpServer) inspectLog(w http.ResponseWriter, r *http.Request) {
	bites, err := s.theLog.snapshotProvider()
	if err != nil {
		http.Error(w, err.Error(), 500)
		return
	}

	daLog := &pb.MetamorphosisLog{}
	err = daLog.Unmarshal(bites)
	if err != nil {
		http.Error(w, err.Error(), 500)
		return
	}

	a := make([]interface{}, 2)
	a[0] = daLog.State
	a[1] = daLog.Log

	j, err := json.Marshal(a)
	if err != nil {
		http.Error(w, err.Error(), 500)
		return
	}

	io.WriteString(w, string(j))
}

func (s *httpServer) appendLogEntry(w http.ResponseWriter, r *http.Request) {
	key := make([]byte, 16)
	value := make([]byte, 16)
	rand.Read(key)
	rand.Read(value)
	offset := s.theLog.append(key, value)
	j, err := json.Marshal(offset)
	if err != nil {
		http.Error(w, err.Error(), 500)
		return
	}

	io.WriteString(w, string(j))
}
