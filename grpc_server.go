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
	"fmt"
	"io"
	"net"

	"github.com/mhelmich/metamorphosis/pb"
	"github.com/sirupsen/logrus"
	"google.golang.org/grpc"
)

type grpcServer struct {
	srvr *grpc.Server
	tm   *topicManager
}

func startNewGrpcServer(port int, tm *topicManager) (*grpcServer, error) {
	myAddress := fmt.Sprintf(":%d", port)
	lis, err := net.Listen("tcp", myAddress)
	if err != nil {
		return nil, err
	}

	s := &grpcServer{
		srvr: grpc.NewServer(),
		tm:   tm,
	}

	pb.RegisterMetamorphosisPubSubServiceServer(s.srvr, s)
	pb.RegisterMetamorphosisMetadataServiceServer(s.srvr, s)
	go s.srvr.Serve(lis)
	return s, nil
}

func (s *grpcServer) Publish(stream pb.MetamorphosisPubSubService_PublishServer) error {
	var request *pb.PublishRequest
	var l log
	var err error

	for { //ever...
		request, err = stream.Recv()
		if err == io.EOF {
			return nil
		} else if err != nil {
			return err
		}

		logrus.Infof("received publish request: %s", request.String())

		if l == nil {
			l, err = s.getLogWithName(request.Topic)
			if err != nil {
				stream.Send(&pb.PublishResponse{
					Error: err.Error(),
				})
				return err
			}
		}

		offset, err := l.append(request.EntryKey, request.EntryData)
		if err != nil {
			stream.Send(&pb.PublishResponse{
				Error: err.Error(),
			})
			// TODO - figure out how to give a log back without closing it
			return err
		}

		err = stream.Send(&pb.PublishResponse{
			Offset:            offset,
			CommittedEntryKey: request.EntryKey,
		})
		if err != nil {
			// TODO - figure out how to give a log back without closing it
			return err
		}
	}
}

func (s *grpcServer) Subscribe(stream pb.MetamorphosisPubSubService_SubscribeServer) error {
	var request *pb.SubscribeRequest
	var l log
	var err error
	var entries []*pb.LogEntry

	for { //ever...
		request, err = stream.Recv()
		if err == io.EOF {
			return nil
		} else if err != nil {
			return err
		}

		logrus.Infof("received subscribe request: %s", request.String())

		if l == nil {
			l, err = s.getLogWithName(request.Topic)
			if err != nil {
				stream.Send(&pb.SubscribeResponse{
					Error: err.Error(),
				})
				return err
			}
		}

		logrus.Infof("log %s", l.getTopicName())
		entries, err = l.readFromOffset(request.StartingOffset, 10)
		if err != nil {
			stream.Send(&pb.SubscribeResponse{
				Error: err.Error(),
			})
			// TODO - figure out how to give a log back without closing it
			return err
		}

		keys := make([][]byte, len(entries))
		values := make([][]byte, len(entries))
		for idx := range entries {
			keys[idx] = entries[idx].Key
			values[idx] = entries[idx].Value
		}

		var startingOffset uint64
		if len(entries) > 0 {
			startingOffset = entries[0].Offset
		} // else startingOffset => 0

		err = stream.Send(&pb.SubscribeResponse{
			StartingOffset: startingOffset,
			EntryKeys:      keys,
			EntryValues:    values,
		})
		if err != nil {
			// TODO - figure out how to give a log back without closing it
			return err
		}
	}
}

func (s *grpcServer) getLogWithName(name string) (log, error) {
	if name == "" {
		return nil, fmt.Errorf("topic name can't be empty on first request")
	}

	return s.tm.getTopicForName(name)
}

func (s *grpcServer) CreateTopic(ctx context.Context, request *pb.CreateTopicRequest) (*pb.CreateTopicResponse, error) {
	err := s.tm.createTopic(request.TopicName)
	if err != nil {
		return nil, err
	}
	return &pb.CreateTopicResponse{
		Ok: true,
	}, nil
}

func (s *grpcServer) ListTopics(ctx context.Context, request *pb.ListTopicsRequest) (*pb.ListTopicsResponse, error) {
	return &pb.ListTopicsResponse{}, nil
}

func (s *grpcServer) stop() {
	s.srvr.Stop()
}
