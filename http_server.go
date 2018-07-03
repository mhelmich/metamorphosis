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
	"fmt"
	"hash/fnv"
	"io"
	"net/http"
	netpprof "net/http/pprof"
	"time"

	"github.com/gorilla/mux"
	"github.com/mhelmich/copycat"
	"github.com/mhelmich/metamorphosis/pb"
	"github.com/sirupsen/logrus"
)

func newHttpServer(port int, cc copycat.CopyCat) *httpServer {
	l, err := newLog("01CFRDSD7PBQZXV8N515RVYTZQ", cc)
	if err != nil {
		logrus.Panicf("Can't create log: %s", err.Error())
	}

	router := mux.NewRouter().StrictSlash(true)
	httpServer := &httpServer{
		Server: http.Server{
			Addr:         fmt.Sprintf(":%d", port),
			Handler:      router,
			WriteTimeout: time.Second * 60,
			ReadTimeout:  time.Second * 60,
			IdleTimeout:  time.Second * 60,
		},
		theLog: l,
		cc:     cc,
	}

	// set up routes
	router.
		Methods("GET").
		Path("/inspectLog").
		HandlerFunc(httpServer.inspectLog).
		Name("inspectLog")

	router.
		Methods("GET").
		Path("/appendLogEntry").
		HandlerFunc(httpServer.appendLogEntry).
		Name("appendLogEntry")

	router.
		Methods("GET").
		Path("/createTopic/{topic}").
		HandlerFunc(httpServer.createTopic).
		Name("createTopic")

	// drag in pprof endpoints
	router.
		Path("/debug/pprof/cmdline").
		HandlerFunc(netpprof.Cmdline)

	router.
		Path("/debug/pprof/profile").
		HandlerFunc(netpprof.Profile)

	router.
		Path("/debug/pprof/symbol").
		HandlerFunc(netpprof.Symbol)

	router.
		Path("/debug/pprof/trace").
		HandlerFunc(netpprof.Trace)

	// at last register the prefix
	router.
		PathPrefix("/debug/pprof/").
		HandlerFunc(netpprof.Index)

	return httpServer
}

type httpServer struct {
	http.Server
	cc     copycat.CopyCat
	theLog *log
}

func (s *httpServer) createTopic(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)

	// TODO - hash a topic name to create a copycat id???
	// let's think this through again
	h := fnv.New128()
	h.Write([]byte(vars["topic"]))
	bites := h.Sum(nil)

	a := make([]interface{}, 2)
	a[0] = len(bites)
	a[1] = bites

	j, err := json.Marshal(a)
	if err != nil {
		http.Error(w, err.Error(), 500)
		return
	}

	w.WriteHeader(http.StatusOK)
	w.Header().Set("Content-Type", "application/json")
	io.WriteString(w, string(j))
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

	w.WriteHeader(http.StatusOK)
	w.Header().Set("Content-Type", "application/json")
	io.WriteString(w, string(j))
}

func (s *httpServer) appendLogEntry(w http.ResponseWriter, r *http.Request) {
	key := make([]byte, 16)
	value := make([]byte, 16)
	rand.Read(key)
	rand.Read(value)
	offset, err := s.theLog.append(key, value)
	if err != nil {
		http.Error(w, err.Error(), 500)
		return
	}

	j, err := json.Marshal(offset)
	if err != nil {
		http.Error(w, err.Error(), 500)
		return
	}

	w.WriteHeader(http.StatusOK)
	w.Header().Set("Content-Type", "application/json")
	io.WriteString(w, string(j))
}
