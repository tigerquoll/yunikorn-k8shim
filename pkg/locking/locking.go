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

// Package locking holds the lock wrappers of the shim and the order in which its lock classes
// may be taken, in the form the static analysis reads. The order is declared here, in the
// package that owns the lock types, so that every package holding one of these locks picks it
// up through its import rather than restating it. The classes themselves are declared on the
// types that carry the locks, see the "+lockclass" annotations there.
//
// The same order is enforced at runtime by the class order check of lockclass_deadlock.go,
// which reads its own copy of the graph from "declaredOrder". The two declarations are kept in
// step by TestLockOrderAnnotationsMatchRuntime, which also checks that the class each type is
// annotated with is the class its constructor registers with SetClass. A drift between them is
// otherwise silent: the static side would pass what the runtime side rejects, or the other way
// round, with no build ever failing.
//
// The relation is a partial order and it is closed transitively: a pair the closure does not
// relate is not checked, because the taxonomy says nothing about it. Only the classes whose
// order the shim actually settles are declared.
//
// The context, application and task edges are the shim lock order as it is written down: the
// context lock is taken before the application lock, which is taken before the task lock. The
// reverse, a task or an application callback reaching back into the context, is what produces
// the freezes this order exists to keep out.
//
// The scheduler cache is not part of that rule but its place follows from the code, and it is
// the leaf. The context owns the cache and takes the cache lock while holding its own
// (IsPodFitNode read locks the context and then calls GetPod, GetNode and LockForReads on the
// cache), and a task does the same one level down (updateAllocation runs under the task lock and
// reaches GetPriorityClass through Context.IsPreemptSelfAllowed). Nothing inside the cache
// reaches back up to a context, an application or a task, so the single edge below gives the
// context and the application the same relation to it through the closure.
//
// +lockorder:cache.Context < cache.Application
// +lockorder:cache.Application < cache.Task
// +lockorder:cache.Task < external.SchedulerCache
//
// Two of the classes carry no edge, both on purpose. There is no annotation for "this pair is
// deliberately unordered": leaving a class out of every edge is how it is said, and this is what
// records that it was a decision rather than an omission.
//
// The admission controller runs as a process of its own and never holds a cache lock, so its
// webhook manager has nothing to be ordered against.
//
// The placeholder manager is the one that could be ordered and is not. The only relation the
// code shows is manager first: cleanUp and createAppPlaceholders hold the manager lock and then
// call the application accessors, which take the application lock. Declaring that as the order
// would sanction it, and it is not sanctioned anywhere: the application side of the same
// relation is written the other way round on purpose, every call from an application into the
// manager is wrapped in a "go" (onReserving, handleFailApplicationEvent and
// handleCompleteApplicationEvent all start a goroutine to call cleanUp), which is what a caller
// does when the reverse direction is known to be unsafe. Two goroutines, one in each direction,
// is the classic ABBA.
//
// YUNIKORN-XXXX: rank the placeholder manager once the ABBA is fixed. Until then the pair stays
// unordered, which means neither direction is reported rather than one of them being blessed.
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
