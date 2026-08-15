//go:build deadlock

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
	"runtime"
	"strings"
	"sync"
	"sync/atomic"

	godeadlock "github.com/sasha-s/go-deadlock"

	"github.com/petermattis/goid"
)

// classOrderSupported reports that the order check is compiled into this build.
const classOrderSupported = true

// hierarchical marks the classes for which nesting two locks of the same class is by design. The
// shim has no such class: the core queue hierarchy has no counterpart here, every class below is a
// single object or a set of objects that are never nested. The array is kept so that a class which
// does need the exemption can be added with its justification. It is the runtime half of the
// "+lockhierarchical" annotation, which locking.go therefore carries none of.
var hierarchical = [numClasses]bool{}

// sameClassWithheld marks the classes whose same class rule is not enforced in this version. This
// would not be an exemption on merit like hierarchical above: it means the rule holds, the code
// breaks it, and the check is withheld until the code is fixed so that this branch can ship green.
// Nothing is withheld: the first full run of this checker over the shim test suite reported no
// same class nesting at all. It is the runtime half of the "+lockorderwithheld" annotation, which
// locking.go therefore carries none of either.
var sameClassWithheld = [numClasses]bool{}

// declaredOrder holds the direct edges of the shim lock order: an edge from a to b means a is
// acquired before b, so acquiring a while b is already held is a violation. It is the runtime half
// of the "+lockorder" annotations in locking.go, edge for edge, and
// TestLockOrderAnnotationsMatchRuntime is what holds the two halves together. The reasoning behind
// each edge, and behind the classes that carry no edge at all, is written down there.
var declaredOrder = [][2]Class{
	{ClassContext, ClassApplication},
	{ClassApplication, ClassTask},
	// the cache is the leaf. This single edge gives Context and Application the same relation to
	// it through the closure.
	{ClassTask, ClassSchedulerCache},
}

// precedes is the transitive closure of declaredOrder: precedes[a][b] means a must be acquired
// before b. Pairs that are unrelated in the closure are not checked, the taxonomy does not order
// them and guessing would only produce noise.
var precedes [numClasses][numClasses]bool

// reported keeps the check to one report per ordered pair so a mis-ordered call in a hot path
// cannot flood the log.
var reported [numClasses][numClasses]atomic.Bool

// reportCount counts the reports that were actually emitted, it makes the report once behaviour
// observable for the tests.
var reportCount atomic.Int32

func init() {
	for _, e := range declaredOrder {
		precedes[e[0]][e[1]] = true
	}
	// Warshall: the order is a tiny DAG and this runs once, the cubic loop does not matter.
	for k := Class(0); k < numClasses; k++ {
		for i := Class(0); i < numClasses; i++ {
			for j := Class(0); j < numClasses; j++ {
				if precedes[i][k] && precedes[k][j] {
					precedes[i][j] = true
				}
			}
		}
	}
}

// held tracks the classes held by one goroutine. It is a multiset: the same class can be held more
// than once, legitimately for a hierarchical class and by mistake otherwise, and locks are released
// in an arbitrary order, so a release drops a count rather than an entry.
type held struct {
	counts [numClasses]int32
}

// heldShardCount must stay a power of two: shardFor masks the goroutine id with it rather than
// taking a remainder, which keeps the index in range for any id without a signed conversion.
const heldShardCount = 64

// heldShard is guarded by a plain sync.Mutex on purpose: using the wrapper types of this package
// would recurse straight back into the check.
type heldShard struct {
	lock       sync.Mutex
	goroutines map[int64]*held
}

var heldShards [heldShardCount]heldShard

func init() {
	for i := range heldShards {
		heldShards[i].goroutines = make(map[int64]*held)
	}
}

func shardFor(gid int64) *heldShard {
	return &heldShards[gid&(heldShardCount-1)]
}

// enterClass checks the class about to be acquired against everything this goroutine already holds
// and then records it. The check runs before the acquisition so that a mis-ordered acquisition that
// blocks forever has still been reported.
func enterClass(c Class) {
	if c == ClassNone || c >= numClasses {
		return
	}
	gid := goid.Get()
	shard := shardFor(gid)
	shard.lock.Lock()
	h := shard.goroutines[gid]
	if h == nil {
		h = &held{}
		shard.goroutines[gid] = h
	}
	var conflicts []Class
	for other := Class(1); other < numClasses; other++ {
		if h.counts[other] == 0 {
			continue
		}
		if other == c {
			// nesting two locks of the same class: by design for a hierarchy, withheld for the
			// classes that are known to break the rule, a violation for everything else
			if !hierarchical[c] && !sameClassWithheld[c] {
				conflicts = append(conflicts, other)
			}
			continue
		}
		// acquiring upward: the class being taken comes before one that is already held
		if precedes[c][other] {
			conflicts = append(conflicts, other)
		}
	}
	h.counts[c]++
	shard.lock.Unlock()

	for _, other := range conflicts {
		reportOrderViolation(other, c)
	}
}

// leaveClass drops one count of the class for this goroutine. Locks are not released in
// acquisition order so the count, not a position, is what is tracked.
func leaveClass(c Class) {
	if c == ClassNone || c >= numClasses {
		return
	}
	gid := goid.Get()
	shard := shardFor(gid)
	shard.lock.Lock()
	defer shard.lock.Unlock()
	h := shard.goroutines[gid]
	if h == nil {
		// the switch was turned on while this lock was held, there is nothing to drop
		return
	}
	if h.counts[c] > 0 {
		h.counts[c]--
	}
	for i := Class(1); i < numClasses; i++ {
		if h.counts[i] != 0 {
			return
		}
	}
	// nothing held any more: drop the goroutine so the map cannot grow without bound
	delete(shard.goroutines, gid)
}

// reportOrderViolation pushes the violation through the same path the go-deadlock reports use, so
// the exit on deadlock and testing mode behaviour is identical for both kinds of finding. The
// options are the ones the core locking package installs, this package deliberately keeps no
// reporting state of its own.
func reportOrderViolation(heldClass, acquired Class) {
	if !reported[heldClass][acquired].CompareAndSwap(false, true) {
		return
	}
	reportCount.Add(1)
	// the same class nesting is a rule of its own rather than an edge: there is no order to name,
	// and naming one would read as the class being declared before itself
	var msg string
	if heldClass == acquired {
		msg = fmt.Sprintf("POTENTIAL DEADLOCK: lock order violation: acquiring %s while holding %s\n"+
			"two locks of one class must not nest, %s is not declared hierarchical in pkg/locking\n",
			acquired, heldClass, acquired)
	} else {
		msg = fmt.Sprintf("POTENTIAL DEADLOCK: lock order violation: acquiring %s while holding %s\n"+
			"the declared order (the \"+lockorder\" taxonomy in pkg/locking) has %s before %s\n",
			acquired, heldClass, acquired, heldClass)
	}
	msg += callerStack()
	if buf := godeadlock.Opts.LogBuf; buf != nil {
		// the buffer only appends to a string, there is no error to handle
		_, _ = buf.Write([]byte(msg)) //nolint:errcheck
	}
	if report := godeadlock.Opts.OnPotentialDeadlock; report != nil {
		report()
	}
}

// lockingPkgPrefix is the function name prefix of everything in this package.
const lockingPkgPrefix = "github.com/apache/yunikorn-k8shim/pkg/locking."

// callerStack renders the stack of the acquisition without the reporting and forwarding frames of
// this package. Skipping a fixed number of frames would break as soon as anything here is
// refactored, so the frames are dropped by name instead. Test functions of this package are kept:
// the checker has its own tests and dropping them would leave the report without any context.
func callerStack() string {
	var pcs [64]uintptr
	n := runtime.Callers(2, pcs[:])
	frames := runtime.CallersFrames(pcs[:n])
	var out string
	skipping := true
	for {
		frame, more := frames.Next()
		if skipping {
			if isLockingInternalFrame(frame.Function) {
				if !more {
					break
				}
				continue
			}
			skipping = false
		}
		out += fmt.Sprintf("  %s\n    %s:%d\n", frame.Function, frame.File, frame.Line)
		if !more {
			break
		}
	}
	return out
}

// isLockingInternalFrame reports whether the frame is one of the check or forwarding functions of
// this package, which carry no information about where the mis-ordered acquisition came from.
func isLockingInternalFrame(fn string) bool {
	if !strings.HasPrefix(fn, lockingPkgPrefix) {
		return false
	}
	name := fn[len(lockingPkgPrefix):]
	return !strings.HasPrefix(name, "Test") && !strings.HasPrefix(name, "Benchmark")
}

// ClassOf returns the ordering class of the lock. It is how a test in another package checks that
// the constructor of its object assigned the class it is supposed to: a constructor that loses its
// SetClass call leaves the lock classless, which silently removes it from the order check without
// any other symptom. Only the deadlock tagged build has this method.
func (m *Mutex) ClassOf() Class {
	return Class(m.class.Load())
}

// ClassOf returns the ordering class of the lock, see Mutex.ClassOf.
func (m *RWMutex) ClassOf() Class {
	return Class(m.class.Load())
}

// EnableClassOrderForTest turns the order check on and clears everything it remembers: the classes
// held per goroutine and the pairs it has already reported. It returns a function that restores the
// previous state of the switch and clears the state again, so that a test which deliberately trips
// the check cannot blind the rest of its package through the report once behaviour.
//
// It exists for the tests of the packages that own the classed objects, the tests of the checker
// itself use the unexported state directly. Only the deadlock tagged build has this function.
func EnableClassOrderForTest() func() {
	previous := classOrderEnabled.Swap(true)
	resetClassOrderState()
	return func() {
		classOrderEnabled.Store(previous)
		resetClassOrderState()
	}
}

// resetClassOrderState drops the per goroutine held classes and the reported pairs.
func resetClassOrderState() {
	for i := range heldShards {
		heldShards[i].lock.Lock()
		heldShards[i].goroutines = make(map[int64]*held)
		heldShards[i].lock.Unlock()
	}
	for i := Class(0); i < numClasses; i++ {
		for j := Class(0); j < numClasses; j++ {
			reported[i][j].Store(false)
		}
	}
	reportCount.Store(0)
}

// enterClass is the outlined slow path called from the forwarder.
func (m *Mutex) enterClass() {
	enterClass(Class(m.class.Load()))
}

// leaveClass is the outlined slow path called from the forwarder.
func (m *Mutex) leaveClass() {
	leaveClass(Class(m.class.Load()))
}

// enterClass is the outlined slow path called from the forwarder.
func (m *RWMutex) enterClass() {
	enterClass(Class(m.class.Load()))
}

// leaveClass is the outlined slow path called from the forwarder.
func (m *RWMutex) leaveClass() {
	leaveClass(Class(m.class.Load()))
}
