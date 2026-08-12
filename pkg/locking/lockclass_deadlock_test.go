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
	"strings"
	"sync"
	"testing"

	godeadlock "github.com/sasha-s/go-deadlock"

	"gotest.tools/v3/assert"
)

// resetClassCheck puts the checker back into a known state: no goroutine holds anything and no
// pair has been reported.
//
// It also takes over the go-deadlock report callback for the duration of the test. That callback
// belongs to the core locking package and exits the process when DEADLOCK_EXIT is set, which is
// what "make test" sets: a test that deliberately trips the checker would take the whole test
// binary down with it. The tests assert on the reported pairs instead, the real callback is
// restored afterwards.
func resetClassCheck(t *testing.T, enabled bool) {
	t.Helper()
	resetClassOrderState()
	classOrderEnabled.Store(enabled)

	realReport := godeadlock.Opts.OnPotentialDeadlock
	godeadlock.Opts.OnPotentialDeadlock = func() {}
	t.Cleanup(func() {
		classOrderEnabled.Store(false)
		godeadlock.Opts.OnPotentialDeadlock = realReport
	})
}

// tripped reports whether the given ordered pair has been reported by the checker.
func tripped(heldClass, acquired Class) bool {
	return reported[heldClass][acquired].Load()
}

// anyTripped reports whether the checker reported any pair at all.
func anyTripped() bool {
	for i := Class(0); i < numClasses; i++ {
		for j := Class(0); j < numClasses; j++ {
			if reported[i][j].Load() {
				return true
			}
		}
	}
	return false
}

// heldGoroutines counts the goroutines the checker is currently tracking.
func heldGoroutines() int {
	total := 0
	for i := range heldShards {
		heldShards[i].lock.Lock()
		total += len(heldShards[i].goroutines)
		heldShards[i].lock.Unlock()
	}
	return total
}

func classed(c Class) *RWMutex {
	m := &RWMutex{}
	m.SetClass(c)
	return m
}

// TestClassOrderUpwardIsViolation takes a lock lower in the order and then one above it.
func TestClassOrderUpwardIsViolation(t *testing.T) {
	resetClassCheck(t, true)
	task := classed(ClassTask)
	app := classed(ClassApplication)

	task.Lock()
	assert.Assert(t, !tripped(ClassTask, ClassApplication), "holding the task lock alone is not a violation")
	app.Lock()
	assert.Assert(t, tripped(ClassTask, ClassApplication), "taking the application lock under the task lock must be reported")
	app.Unlock()
	task.Unlock()
}

// TestClassOrderDownwardIsAllowed walks the documented order from the top down.
func TestClassOrderDownwardIsAllowed(t *testing.T) {
	resetClassCheck(t, true)
	context := classed(ClassContext)
	app := classed(ClassApplication)
	task := classed(ClassTask)
	cache := classed(ClassSchedulerCache)

	context.Lock()
	app.Lock()
	task.Lock()
	cache.RLock()
	assert.Assert(t, !anyTripped(), "the documented order must not be reported")
	cache.RUnlock()
	task.Unlock()
	app.Unlock()
	context.Unlock()
}

// TestClassOrderTransitive checks the closure: the only declared edge to the scheduler cache is
// from the task, the context still precedes it through the closure.
func TestClassOrderTransitive(t *testing.T) {
	resetClassCheck(t, true)
	cache := classed(ClassSchedulerCache)
	context := classed(ClassContext)

	cache.Lock()
	context.Lock()
	assert.Assert(t, tripped(ClassSchedulerCache, ClassContext), "the context under the scheduler cache is an upward acquisition")
	context.Unlock()
	cache.Unlock()
}

// TestClassOrderUnrelatedPairSilent uses the placeholder manager, which has no edge in this
// version. Guessing an order there would only produce noise or bless a ledgered finding.
func TestClassOrderUnrelatedPairSilent(t *testing.T) {
	resetClassCheck(t, true)
	manager := classed(ClassPlaceholderManager)
	app := classed(ClassApplication)

	manager.Lock()
	app.Lock()
	assert.Assert(t, !anyTripped(), "the placeholder manager is not ordered against the application")
	app.Unlock()
	manager.Unlock()

	// and the other way round
	resetClassCheck(t, true)
	app.Lock()
	manager.Lock()
	assert.Assert(t, !anyTripped(), "the placeholder manager is not ordered against the application")
	manager.Unlock()
	app.Unlock()
}

// TestClassOrderClassless leaves the locks without a class: they must not be tracked at all.
func TestClassOrderClassless(t *testing.T) {
	resetClassCheck(t, true)
	one := &RWMutex{}
	two := &RWMutex{}

	one.Lock()
	two.Lock()
	assert.Assert(t, !anyTripped(), "locks without a class are not ordered")
	assert.Equal(t, 0, heldGoroutines(), "a classless lock must not create goroutine state")
	two.Unlock()
	one.Unlock()
}

// TestClassOrderSameClass nests two locks of the same class. That is a violation unless the class
// is a hierarchy or its same class rule is withheld.
func TestClassOrderSameClass(t *testing.T) {
	resetClassCheck(t, true)
	first := classed(ClassTask)
	second := classed(ClassTask)

	first.Lock()
	second.Lock()
	assert.Assert(t, tripped(ClassTask, ClassTask), "nesting two task locks must be reported")
	second.Unlock()
	first.Unlock()
}

// TestClassOrderSameClassExemptions covers the two exemption arrays. Neither is used by the shim,
// the test states that and keeps the mechanism covered: flipping an entry must silence the pair.
func TestClassOrderSameClassExemptions(t *testing.T) {
	for i := Class(0); i < numClasses; i++ {
		assert.Assert(t, !hierarchical[i], "no shim class is a hierarchy in this version")
		assert.Assert(t, !sameClassWithheld[i], "no shim class has its same class rule withheld")
	}

	resetClassCheck(t, true)
	hierarchical[ClassContext] = true
	t.Cleanup(func() { hierarchical[ClassContext] = false })
	first := classed(ClassContext)
	second := classed(ClassContext)

	first.Lock()
	second.Lock()
	assert.Assert(t, !anyTripped(), "a hierarchical class is exempt from the same class rule")
	second.Unlock()
	first.Unlock()

	resetClassCheck(t, true)
	sameClassWithheld[ClassApplication] = true
	t.Cleanup(func() { sameClassWithheld[ClassApplication] = false })
	firstApp := classed(ClassApplication)
	secondApp := classed(ClassApplication)

	firstApp.Lock()
	secondApp.Lock()
	assert.Assert(t, !anyTripped(), "a withheld same class rule is not reported")
	secondApp.Unlock()
	firstApp.Unlock()
}

// TestClassOrderDisabled repeats the violation case with the checker turned off.
func TestClassOrderDisabled(t *testing.T) {
	resetClassCheck(t, false)
	task := classed(ClassTask)
	app := classed(ClassApplication)

	task.Lock()
	app.Lock()
	assert.Assert(t, !anyTripped(), "nothing is checked while the switch is off")
	app.Unlock()
	task.Unlock()

	// and nothing was tracked either
	assert.Equal(t, 0, heldGoroutines(), "no goroutine state may be kept while the switch is off")
}

// TestClassOrderPerGoroutine holds a low lock on one goroutine while another goroutine takes a
// high one: the two must not see each other's held set.
func TestClassOrderPerGoroutine(t *testing.T) {
	resetClassCheck(t, true)
	task := classed(ClassTask)
	app := classed(ClassApplication)

	taken := make(chan struct{})
	release := make(chan struct{})
	done := make(chan struct{})
	go func() {
		defer close(done)
		task.Lock()
		close(taken)
		<-release
		task.Unlock()
	}()

	<-taken
	// this goroutine holds nothing, so the application lock is fine even though the other
	// goroutine is sitting on a task lock
	app.Lock()
	assert.Assert(t, !anyTripped(), "held classes must not leak between goroutines")
	app.Unlock()
	close(release)
	<-done
}

// TestClassOrderUnlockOutOfOrder releases the locks in acquisition order rather than in reverse:
// the multiset must still end up empty.
func TestClassOrderUnlockOutOfOrder(t *testing.T) {
	resetClassCheck(t, true)
	context := classed(ClassContext)
	app := classed(ClassApplication)
	task := classed(ClassTask)

	context.Lock()
	app.Lock()
	task.Lock()
	// release in the same order they were taken
	context.Unlock()
	app.Unlock()
	task.Unlock()
	assert.Assert(t, !anyTripped(), "the documented order must not be reported")
	assert.Equal(t, 0, heldGoroutines(), "the goroutine must be dropped once it holds nothing")
}

// TestClassOrderReportOnce trips the same pair twice and checks only one report is produced.
func TestClassOrderReportOnce(t *testing.T) {
	resetClassCheck(t, true)
	task := classed(ClassTask)
	app := classed(ClassApplication)

	task.Lock()
	app.Lock()
	app.Unlock()
	task.Unlock()
	assert.Assert(t, tripped(ClassTask, ClassApplication), "the pair is marked as reported")
	assert.Equal(t, int32(1), reportCount.Load(), "the first violation is reported")

	reportCount.Store(0)
	task.Lock()
	app.Lock()
	assert.Equal(t, int32(0), reportCount.Load(), "the same pair must only be reported once")
	app.Unlock()
	task.Unlock()
}

// TestClassOrderReadLocksOrder checks the read side is ordered the same way as the write side.
func TestClassOrderReadLocksOrder(t *testing.T) {
	resetClassCheck(t, true)
	task := classed(ClassTask)
	context := classed(ClassContext)

	task.RLock()
	context.RLock()
	assert.Assert(t, tripped(ClassTask, ClassContext), "the context read lock under a task read lock is upward")
	context.RUnlock()
	task.RUnlock()
}

// TestClassOrderReportContents checks the report names both classes and carries the stack of the
// acquisition without the forwarding frames of this package.
func TestClassOrderReportContents(t *testing.T) {
	resetClassCheck(t, true)
	buf := &strings.Builder{}
	realBuf := godeadlock.Opts.LogBuf
	godeadlock.Opts.LogBuf = buf
	t.Cleanup(func() { godeadlock.Opts.LogBuf = realBuf })

	task := classed(ClassTask)
	app := classed(ClassApplication)
	task.Lock()
	app.Lock()
	app.Unlock()
	task.Unlock()

	out := buf.String()
	assert.Assert(t, strings.Contains(out, "POTENTIAL DEADLOCK: lock order violation"), "the report is written to the shared buffer: %s", out)
	assert.Assert(t, strings.Contains(out, "acquiring cache.Application while holding cache.Task"), "the report names both classes: %s", out)
	assert.Assert(t, strings.Contains(out, "TestClassOrderReportContents"), "the report carries the stack of the acquisition: %s", out)
	assert.Assert(t, !strings.Contains(out, "locking.(*RWMutex).Lock"), "the forwarding frames of this package are stripped: %s", out)
}

// TestClassOrderConcurrentNoRace hammers the checker from several goroutines, it is here to be run
// under the race detector.
func TestClassOrderConcurrentNoRace(t *testing.T) {
	resetClassCheck(t, true)
	app := classed(ClassApplication)
	task := classed(ClassTask)

	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 200; j++ {
				app.Lock()
				task.Lock()
				task.Unlock()
				app.Unlock()
			}
		}()
	}
	wg.Wait()
	assert.Assert(t, !anyTripped(), "the documented order must not be reported")
	assert.Equal(t, 0, heldGoroutines(), "every goroutine must be dropped once it holds nothing")
}
