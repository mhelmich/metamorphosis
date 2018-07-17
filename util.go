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
	"encoding/binary"
	"os"

	"github.com/sirupsen/logrus"
)

// converts bytes to an unsinged 64 bit integer
func bytesToUint64(b []byte) uint64 {
	return binary.BigEndian.Uint64(b)
}

// converts a uint64 to a byte slice
func uint64ToBytes(u uint64) []byte {
	buf := make([]byte, 8)
	binary.BigEndian.PutUint64(buf, u)
	return buf
}

func randomContext() uint64 {
	bites := make([]byte, 8)
	_, err := rand.Read(bites)
	if err != nil {
		logrus.Panicf("Can't read from random: %s", err.Error())
	}
	return bytesToUint64(bites)
}

func createDirIfNotExists(dir string) error {
	if !dirExists(dir) {
		return os.MkdirAll(dir, os.ModePerm)
	}
	return nil
}

func dirExists(dir string) bool {
	if _, err := os.Stat(dir); os.IsNotExist(err) {
		return false
	} else if err != nil {
		return false
	}

	return true
}
