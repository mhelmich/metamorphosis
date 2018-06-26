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
	"fmt"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"github.com/mhelmich/copycat"
	"github.com/sirupsen/logrus"
	"github.com/spf13/viper"
)

func main() {
	logrus.Infof("Starting metamorphosis!")

	viperConfig := viper.New()
	viperConfig.AutomaticEnv()

	copycatConfig := copycat.DefaultConfig()
	// set host name to external IP address
	copycatConfig.Hostname = getOutboundIP().To4().String()
	copycatConfig.CopyCatPort = viperConfig.GetInt("copycat-port")
	copycatConfig.GossipPort = viperConfig.GetInt("gossip-port")
	copycatConfig.CopyCatDataDir = viperConfig.GetString("copycat-dir")
	// in viper env vars are case sensitive
	// docker only allows for capitilized env vars!?!
	copycatConfig.PeersToContact = viperConfig.GetStringSlice("COPYCAT_PEERS")
	httpPort := viperConfig.GetInt("http-port")
	_ = viperConfig.GetInt("grpc-port")
	logrus.Infof("Starting copycat with: %d %d %s %s %s", copycatConfig.CopyCatPort, copycatConfig.GossipPort, copycatConfig.CopyCatDataDir, copycatConfig.Hostname, strings.Join(copycatConfig.PeersToContact, ", "))

	// register shutdown hook and call cleanup
	c := make(chan os.Signal, 1)
	signal.Notify(c, syscall.SIGINT, syscall.SIGTERM)

	cc, err := copycat.NewCopyCat(copycatConfig)
	if err != nil {
		logrus.Panicf("Can't start app: %s", err.Error())
	}

	go func() {
		sig := <-c
		cleanup(sig, copycatConfig.CopyCatDataDir, cc)
	}()

	startHTTPServer(httpPort)
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

func startHTTPServer(port int) {
	logrus.Infof("Firing up http server...")
	ws := &httpServer{
		theLog: &log{},
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/inspectLog", ws.inspectLog)
	mux.HandleFunc("/appendLogEntry", ws.appendLogEntry)
	http.ListenAndServe(fmt.Sprintf("127.0.0.1:%d", port), mux)
	logrus.Error("Stopped http server!")
}

func cleanup(sig os.Signal, dataDir string, cc copycat.CopyCat) {
	logrus.Info("This node is going down gracefully\n")
	logrus.Infof("Received signal: %s\n", sig)
	cc.Shutdown()
	err := os.RemoveAll(dataDir)
	if err != nil {
		logrus.Errorf("Can't clean up after myself: %s", err.Error())
	}
	logrus.Exit(0)
}
