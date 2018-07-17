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
	"context"

	"github.com/mhelmich/metamorphosis/pb"
)

func newMetadataServer() *metadataServer {
	return &metadataServer{}
}

type metadataServer struct{}

func (ms *metadataServer) CreateTopic(context.Context, *pb.CreateTopicRequest) (*pb.CreateTopicResponse, error) {
	return nil, nil
}

func (ms *metadataServer) ListTopics(context.Context, *pb.ListTopicsRequest) (*pb.ListTopicsResponse, error) {
	return nil, nil
}
