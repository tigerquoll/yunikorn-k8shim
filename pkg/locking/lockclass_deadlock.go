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
// does need the exemption can be added with its justification, exactly like the core does for the
// queue.
var hierarchical = [numClasses]bool{}

// sameClassWithheld marks the classes whose same class rule is not enforced in this version. This
// would not be an exemption on merit like hierarchical above: it means the rule holds, the code
// breaks it, and the check is withheld until the code is fixed so that this branch can ship green.
// Nothing is withheld: the first full run of this checker over the shim test suite reported no
// same class nesting at all.
var sameClassWithheld = [numClasses]bool{}

// declaredOrder holds the direct edges of the shim lock order: an edge from a to b means a is
// acquired before b, so acquiring a while b is already held is a violation.
//
// The Context, Application and Task edges are rule 9.3 of docs/how-yunikorn-works.md verbatim:
// "Shim lock order is Context.lock -> Application.lock -> Task.lock. The reverse (task/app callback
// reaching back into the context) is what produces the D1/D2 freezes."
//
// The scheduler cache is not named in the lock order but its place follows from the code and from
// rule 9.6 ("the scheduler cache needs a stable view for predicates"): the Context owns the cache
// and takes the cache lock while holding its own (Context.IsPodFitNode read locks the context and
// then calls SchedulerCache.GetPod/GetNode and LockForReads), and a task does the same one level
// down (Task.updateAllocation runs under the task lock and reaches SchedulerCache.GetPriorityClass
// through Context.IsPreemptSelfAllowed). Nothing inside the cache reaches back up to a context, an
// application or a task, so the cache is the leaf of the order.
//
// The placeholder manager is deliberately absent, see the note below.
var declaredOrder = [][2]Class{
	// rule 9.3: "Context.lock -> Application.lock"
	{ClassContext, ClassApplication},
	// rule 9.3: "Application.lock -> Task.lock"
	{ClassApplication, ClassTask},
	// the cache is the leaf, see the comment above. This single edge gives Context and
	// Application the same relation to it through the closure.
	{ClassTask, ClassSchedulerCache},
}

// The placeholder manager has a class but no edge in this version, on purpose.
//
// The only relation the code shows is manager first: PlaceholderManager.cleanUp and
// createAppPlaceholders hold the manager lock and then call the application accessors, which take
// the application lock. Declaring that as the order would sanction it, and it is not sanctioned
// anywhere: the rules doc does not rank the placeholder manager at all, and the application side
// of the same relation is written the other way round on purpose. Every call from an application
// into the manager is wrapped in a "go" (Application.onReserving, handleFailApplicationEvent and
// handleCompleteApplicationEvent all start a goroutine to call cleanUp), which is rule 4.4,
// escaping the order with go, and is what a caller does when the reverse direction is known to be
// unsafe. Two goroutines, one in each direction, is the classic ABBA and the reason the manager
// side is a ledgered finding rather than a documented order.
//
// YUNIKORN-XXXX: rank the placeholder manager once that finding is resolved. Until then the pair
// stays unordered, which means the checker reports neither direction rather than blessing one.

// precedes is the transitive closure of declaredOrder: precedes[a][b] means a must be acquired
// before b. Pairs that are unrelated in the closure are not checked, the document does not order
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
	return &heldShards[uint64(gid)%heldShardCount]
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
	msg := fmt.Sprintf("POTENTIAL DEADLOCK: lock order violation: acquiring %s while holding %s\n"+
		"the documented order (docs/how-yunikorn-works.md rule 9.3) has %s before %s\n",
		acquired, heldClass, acquired, heldClass)
	msg += callerStack()
	if buf := godeadlock.Opts.LogBuf; buf != nil {
		_, _ = buf.Write([]byte(msg))
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
