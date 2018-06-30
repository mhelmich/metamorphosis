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
	"bytes"
	"context"
	"flag"
	"io/ioutil"
	"net"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/mhelmich/copycat"
	"github.com/sirupsen/logrus"
	"github.com/spf13/viper"
)

func main() {
	logrus.Infof("Starting metamorphosis!")

	configPath := flag.String("config", "./default_config.yml", "path to config file for metamorphosis to use")
	flag.Parse()
	if !flag.Parsed() {
		logrus.Panic("Couldn't parse command line")
	}

	viperConfig, err := loadConfig(*configPath)
	if err != nil {
		logrus.Panicf("Can't load configs: %s", err)
	}

	copycatConfig := copycat.DefaultConfig()
	// set host name to external IP address
	// copycatConfig.Hostname = getOutboundIP().To4().String()
	// DEBUG - hardwire loopback for now
	copycatConfig.Hostname = "127.0.0.1"
	copycatConfig.CopyCatPort = viperConfig.GetInt("copycat-port")
	copycatConfig.GossipPort = viperConfig.GetInt("gossip-port")
	copycatConfig.CopyCatDataDir = viperConfig.GetString("copycat-dir")
	// in viper env vars are case sensitive
	// docker only allows for capitilized env vars!?!
	copycatConfig.PeersToContact = viperConfig.GetStringSlice("COPYCAT_PEERS")
	httpPort := viperConfig.GetInt("http-port")
	grpcPort := viperConfig.GetInt("grpc-port")
	logrus.Infof("Starting copycat with: %d %d %s %s %s", copycatConfig.CopyCatPort, copycatConfig.GossipPort, copycatConfig.CopyCatDataDir, copycatConfig.Hostname, strings.Join(copycatConfig.PeersToContact, ", "))
	logrus.Infof("Starting metamorphosis with: %d %d", httpPort, grpcPort)

	// register shutdown hook and call cleanup
	c := make(chan os.Signal, 1)
	signal.Notify(c, syscall.SIGINT, syscall.SIGTERM)

	cc, err := copycat.NewCopyCat(copycatConfig)
	if err != nil {
		logrus.Panicf("Can't start app: %s", err.Error())
	}

	httpServer := newHttpServer(httpPort, cc)
	go func() {
		logrus.Infof("Firing up http server on %s", httpServer.Addr)
		if err := httpServer.ListenAndServe(); err != nil {
			logrus.Errorf("%s", err.Error())
		}
	}()

	sig := <-c
	cleanup(sig, copycatConfig.CopyCatDataDir, cc, httpServer)
}

// get preferred outbound ip of this machine
func getOutboundIP() net.IP {
	// google dns
	// the address may not exist
	conn, err := net.Dial("udp", "8.8.8.8:80")
	if err != nil {
		logrus.Fatal(err)
	}
	defer conn.Close()
	localAddr := conn.LocalAddr().(*net.UDPAddr)
	// get the IP from an open connection
	return localAddr.IP
}

// func startGrpcServer(port int) {
// 	logrus.Infof("Firing up grpc server...")
// 	lis, err := net.Listen("tcp", fmt.Sprintf("127.0.0.1:%d", port))
// 	if err != nil {
// 		logrus.Panicf("%s", err.Error())
// 	}
//
// 	grpcServer := grpc.NewServer()
// 	pb.RegisterPubSubServiceServer(grpcServer, &pubSubServer{})
// 	grpcServer.Serve(lis)
// 	logrus.Error("Stopped grpc server!")
// }

func loadConfig(configPath string) (*viper.Viper, error) {
	logrus.Infof("Loading config at %s", configPath)
	viperConfig := viper.New()
	viperConfig.AutomaticEnv()
	viperConfig.SetConfigType("yaml")
	bites, err := ioutil.ReadFile(configPath)
	if err != nil {
		return nil, err
	}

	err = viperConfig.ReadConfig(bytes.NewBuffer(bites))
	return viperConfig, err
}

func cleanup(sig os.Signal, dataDir string, cc copycat.CopyCat, httpServer *httpServer) {
	logrus.Info("This node is going down gracefully\n")
	logrus.Infof("Received signal: %s\n", sig)

	cc.Shutdown()

	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()
	httpServer.Shutdown(ctx)

	err := os.RemoveAll(dataDir)
	if err != nil {
		logrus.Errorf("Can't clean up after myself: %s", err.Error())
	}
	logrus.Exit(0)
}
