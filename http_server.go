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
	"net/http"
	netpprof "net/http/pprof"
	"time"

	"github.com/gorilla/mux"
	"github.com/mhelmich/metamorphosis/pb"
)

type httpServer struct {
	http.Server
	tm *topicManager
}

func startNewHttpServer(port int, tm *topicManager) (*httpServer, error) {
	router := mux.NewRouter().StrictSlash(true)
	httpServer := &httpServer{
		Server: http.Server{
			Addr:         fmt.Sprintf(":%d", port),
			Handler:      router,
			WriteTimeout: time.Second * 60,
			ReadTimeout:  time.Second * 60,
			IdleTimeout:  time.Second * 60,
		},
		tm: tm,
	}

	// set up routes
	router.
		Methods("GET").
		Path("/inspectLog/{topic}").
		HandlerFunc(httpServer.inspectLog).
		Name("inspectLog")

	router.
		Methods("GET").
		Path("/appendLogEntry/{topic}").
		HandlerFunc(httpServer.appendLogEntry).
		Name("appendLogEntry")

	router.
		Methods("GET").
		Path("/createTopic/{topic}").
		HandlerFunc(httpServer.createTopic).
		Name("createTopic")

	router.
		Path("/debug/metamorphosis/topics").
		HandlerFunc(httpServer.debugTopicMetadata)

	router.
		Path("/debug/metamorphosis/metrics").
		HandlerFunc(httpServer.metrics)

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
	// NB: the trailing slash ("/") is a concession
	// to how gorilla does routing
	// when you want to go to the pprof page, you need to add
	// the trailing slash to your URL
	router.
		PathPrefix("/debug/pprof/").
		HandlerFunc(netpprof.Index)

	go httpServer.ListenAndServe()
	return httpServer, nil
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

	w.WriteHeader(http.StatusOK)
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(a)
}

// naive implementation
func (s *httpServer) inspectLog(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	l, err := s.tm.getTopicForName(vars["topic"])
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	bites, err := l.snapshotProvider()
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	daLog := &pb.LogSnapshot{}
	err = daLog.Unmarshal(bites)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	a := make([]interface{}, 2)
	a[0] = daLog.State
	a[1] = daLog.Log

	w.WriteHeader(http.StatusOK)
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(a)
}

func (s *httpServer) appendLogEntry(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	l, err := s.tm.getTopicForName(vars["topic"])
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	key := make([]byte, 16)
	value := make([]byte, 16)
	rand.Read(key)
	rand.Read(value)
	offset, err := l.append(key, value)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusOK)
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(offset)
}

func (s *httpServer) debugTopicMetadata(w http.ResponseWriter, r *http.Request) {
	// TODO - make this less inefficient
	bites, err := s.tm.snapshotProvider()
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	md := &pb.TopicMetadataSnapshot{}
	err = md.Unmarshal(bites)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusOK)
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(md)
}

func (s *httpServer) metrics(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(http.StatusOK)
	w.Header().Set("Content-Type", "application/json")
	writeMetricsAsJSON(w)
}
