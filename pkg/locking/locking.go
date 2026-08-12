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

// Package locking holds the lock wrappers of the shim and the taxonomy of the lock order.
//
// The order below is the shim lock order of docs/how-yunikorn-works.md rule 9.3, extended with
// the scheduler cache as its leaf, and it is the same taxonomy the runtime check in
// lockclass.go carries: the class names are the names that check prints, and the edges are the
// ones it declares. The two are kept in step by TestLockOrderAnnotationsMatchRuntime.
//
// The placeholder manager has a class but no edge. The only relation the code shows is manager
// first, and that is a ledgered finding rather than a documented order, so neither direction is
// declared, see the note on declaredOrder in lockclass_deadlock.go. There is no annotation for
// "this pair is deliberately unordered": leaving a class out of every edge is how it is said,
// and this comment is what records that it was a decision.
//
// +lockorder:cache.Context < cache.Application
// +lockorder:cache.Application < cache.Task
// +lockorder:cache.Task < external.SchedulerCache
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

// Mutex, and RWMutex below it, declare themselves lock primitives to the checklocks analysis.
// Without the declaration the analysis recognises a lock by its type name only, so the
// forwarders in forwarders.go read as ordinary methods that take a lock and return without
// releasing it and every one of them needs a "+checklocksignore" to silence that; see
// forwarders.go for what those ignores cost. Whether a type behaves as a Mutex or an RWMutex
// is taken from the type itself: it has an RLock method or it does not.
//
// +checklockslocktype
type Mutex struct {
	mu godeadlock.Mutex
	// class is the ordering class, see lockclass.go. Zero means the lock is not ordered.
	class atomic.Uint32
}

// +checklockslocktype
type RWMutex struct {
	mu godeadlock.RWMutex
	// class is the ordering class, see lockclass.go. Zero means the lock is not ordered.
	class atomic.Uint32
}
