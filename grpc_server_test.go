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
	"crypto/rand"
	"fmt"
	"os"
	"sync"
	"testing"

	"github.com/mhelmich/metamorphosis/pb"
	"github.com/stretchr/testify/assert"
	"google.golang.org/grpc"
)

func TestGrpcServerBasic(t *testing.T) {
	topicName := "T1"
	port := 9999
	cc := newMockCopyCat()
	dir := "./TestGrpcServerBasic/"
	assert.Nil(t, os.RemoveAll(dir))
	tm, err := newTopicManager(dir, cc)
	assert.Nil(t, err)
	srvr, err := startNewGrpcServer(port, tm)
	defer srvr.stop()
	assert.Nil(t, err)

	conn, err := grpc.Dial(fmt.Sprintf(":%d", port), grpc.WithInsecure())
	assert.Nil(t, err)
	defer conn.Close()
	metadataClient := pb.NewMetamorphosisMetadataServiceClient(conn)
	createResp, err := metadataClient.CreateTopic(context.TODO(), &pb.CreateTopicRequest{TopicName: topicName})
	assert.Nil(t, err)
	assert.True(t, createResp.Ok)

	pubSubClient := pb.NewMetamorphosisPubSubServiceClient(conn)
	pubStream, err := pubSubClient.Publish(context.TODO())
	assert.Nil(t, err)

	key := make([]byte, 33)
	value := make([]byte, 33)

	rand.Read(key)
	rand.Read(value)

	err = pubStream.Send(&pb.PublishRequest{
		Topic:     "T1",
		EntryKey:  key,
		EntryData: value,
	})
	assert.Nil(t, err)
	resp, err := pubStream.Recv()
	assert.Nil(t, err)
	assert.Equal(t, "", resp.Error)
	assert.Equal(t, uint64(0), resp.Offset)
	pubStream.CloseSend()

	subStream, err := pubSubClient.Subscribe(context.TODO())
	assert.Nil(t, err)
	err = subStream.Send(&pb.SubscribeRequest{
		Topic:          topicName,
		StartingOffset: 0,
	})
	assert.Nil(t, err)
	subResp, err := subStream.Recv()
	assert.Nil(t, err)
	assert.Equal(t, 1, len(subResp.Records))
	assert.Equal(t, key, subResp.Records[0].Key)
	assert.Equal(t, value, subResp.Records[0].Value)
	err = subStream.CloseSend()
	assert.Nil(t, err)

	assert.Nil(t, os.RemoveAll(dir))
}

func TestGrpcServerConcurrent(t *testing.T) {
	topicName := "T1"
	port := 9998
	cc := newMockCopyCat()
	dir := "./TestGrpcServerConcurrent/"
	assert.Nil(t, os.RemoveAll(dir))
	tm, err := newTopicManager(dir, cc)
	assert.Nil(t, err)
	srvr, err := startNewGrpcServer(port, tm)
	defer srvr.stop()
	assert.Nil(t, err)

	conn, err := grpc.Dial(fmt.Sprintf(":%d", port), grpc.WithInsecure())
	assert.Nil(t, err)
	defer conn.Close()
	metadataClient := pb.NewMetamorphosisMetadataServiceClient(conn)
	createResp, err := metadataClient.CreateTopic(context.TODO(), &pb.CreateTopicRequest{TopicName: topicName})
	assert.Nil(t, err)
	assert.True(t, createResp.Ok)

	numConsumers := 10
	producerWg := &sync.WaitGroup{}
	producerWg.Add(1)
	consumerWg := &sync.WaitGroup{}
	consumerWg.Add(numConsumers)

	go func() {
		pubSubClient := pb.NewMetamorphosisPubSubServiceClient(conn)
		pubStream, err := pubSubClient.Publish(context.TODO())
		assert.Nil(t, err)

		for i := 0; i < 1000; i++ {

			key := uint64ToBytes(uint64(i))
			value := make([]byte, 33)

			rand.Read(key)
			rand.Read(value)

			err = pubStream.Send(&pb.PublishRequest{
				Topic:     "T1",
				EntryKey:  key,
				EntryData: value,
			})
			assert.Nil(t, err)
			resp, err := pubStream.Recv()
			assert.Nil(t, err)
			assert.Equal(t, "", resp.Error)
			assert.Equal(t, uint64(i), resp.Offset)
		}

		pubStream.CloseSend()
		producerWg.Done()
	}()

	for j := 0; j < numConsumers; j++ {
		go func() {
			myConn, err := grpc.Dial(fmt.Sprintf(":%d", port), grpc.WithInsecure())
			assert.Nil(t, err)
			defer myConn.Close()
			pubSubClient := pb.NewMetamorphosisPubSubServiceClient(conn)
			subStream, err := pubSubClient.Subscribe(context.TODO())
			assert.Nil(t, err)

			offset := uint64(0)
			var subResp *pb.SubscribeResponse

			for offset < 1000 {
				err = subStream.Send(&pb.SubscribeRequest{
					Topic:          topicName,
					StartingOffset: offset,
				})
				assert.Nil(t, err)
				subResp, err = subStream.Recv()
				assert.Nil(t, err)
				if len(subResp.Records) > 0 {
					offset = subResp.Records[len(subResp.Records)-1].Offset + 1
				}
			}

			err = subStream.CloseSend()
			assert.Nil(t, err)
			consumerWg.Done()
		}()
	}

	producerWg.Wait()
	consumerWg.Wait()
	assert.Nil(t, os.RemoveAll(dir))
}
