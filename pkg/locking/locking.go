/*
 Licensed to the Apache Software Foundation (ASF) under one
 or more contributor license agreements.  See the NOTICE file
 distributed with this work for additional information
 regarding copyright ownership.  The ASF licenses this file
 to you under the Apache License, Version 2.0 (the
 "License"); you may not use this file except in compliance
 with the License.  You may obtain a copy of the License at

     http://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing, software
 distributed under the License is distributed on an "AS IS" BASIS,
 WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 See the License for the specific language governing permissions and
 limitations under the License.
*/

package locking

import (
	"fmt"
	"os"
	"strconv"
	"sync"
	"sync/atomic"

	godeadlock "github.com/sasha-s/go-deadlock"

	corelocking "github.com/apache/yunikorn-core/pkg/locking"
)

// EnvClassOrderEnabled turns the lock class order check on, see lockclass.go. It sits next to the
// DEADLOCK_* variables that the core locking package reads, the check is a part of the same
// diagnostic build: it is only compiled in under the "deadlock" build tag and only active when this
// is set.
const EnvClassOrderEnabled = "DEADLOCK_CLASS_ORDER_ENABLED"

var once sync.Once

func init() {
	once.Do(func() {
		// call into core locking package to ensure that all locks are globally configured
		corelocking.IsTrackingEnabled()
		initClassOrder()
	})
}

// initClassOrder reads the switch of the lock class order check. The reporting itself goes through
// the go-deadlock options that the core locking package has just configured, so that an order
// violation is reported and acted on exactly like a detected deadlock.
func initClassOrder() {
	classOrder, err := strconv.ParseBool(os.Getenv(EnvClassOrderEnabled))
	if err != nil {
		classOrder = false
	}
	classOrderEnabled.Store(classOrder && classOrderSupported)
	if classOrder && classOrderSupported {
		// written before anything else is initialised, same as the core message
		// no way to handle errors just ignore
		_, _ = fmt.Fprintf(os.Stderr, "=== Lock class order checking enabled ===\n")
	}
}

type Mutex struct {
	mu godeadlock.Mutex
	// class is the ordering class, see lockclass.go. Zero means the lock is not ordered.
	class atomic.Uint32
}

type RWMutex struct {
	mu godeadlock.RWMutex
	// class is the ordering class, see lockclass.go. Zero means the lock is not ordered.
	class atomic.Uint32
}
