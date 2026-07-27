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

package cache

// [YUNIKORN-XXXX] Deterministic reproduction of the recursive FSM state
// read-lock deadlock ("D2").
//
// Shared helpers (reproProbeBlocked, reproSetup, ...) live in
// d1_deadlock_repro_test.go; the two repros are kept in separate files so each
// deadlock can be read, run and reviewed on its own.
//
// THE CYCLE
// ---------
//   G1: holds fsm stateMu.RLock #1 and blocks taking stateMu.RLock #2
//   W:  blocks on stateMu.Lock(), queued behind RLock #1
//
// Go's sync.RWMutex explicitly forbids recursive read locking: "If a goroutine
// holds a RWMutex for reading and another goroutine might call Lock, no
// goroutine should expect to be able to acquire a read lock until the initial
// read lock is released." A queued writer blocks all new readers, so G1's
// second RLock can never be granted, and the writer can never be granted
// because G1 still holds the first RLock. Neither side is at fault in
// isolation; the recursive RLock is.
//
// THE PRODUCT LINES IT DEPENDS ON (all on the commit under test)
// --------------------------------------------------------------
//   fsm@v1.0.3/fsm.go:311-312   FSM.Event() takes f.stateMu.RLock() with a
//                               deferred RUnlock and calls the before-callbacks
//                               while holding it. -> RLock #1
//   fsm@v1.0.3/fsm.go:436       FSM.beforeEventCallbacks runs the callbacks.
//   pkg/cache/task_state.go:437-440
//                               before_CompleteTask -> Task.beforeTaskCompleted.
//   pkg/cache/task.go:445-446   beforeTaskCompleted -> releaseAllocation(false).
//   pkg/cache/task.go:457       releaseAllocation calls shouldAppRelease()
//                               FIRST, ...
//   pkg/cache/task.go:470,478-479,487
//                               ... and only then re-reads the task state via
//                               task.GetTaskState() (five separate calls).
//   pkg/cache/task.go:148-150   GetTaskState -> task.sm.Current() ->
//                               fsm@v1.0.3/fsm.go:209 stateMu.RLock().
//                               -> RLock #2, on the same goroutine and the same
//                               RWMutex as RLock #1.
//   pkg/cache/task.go:266-267   MarkPreviouslyAllocated calls
//                               task.sm.SetState(...) -> fsm@v1.0.3/fsm.go:224
//                               stateMu.Lock(). -> the queued writer.
//
// Note that the first GetTaskState() is an argument to a log.Debug() call. Go
// evaluates call arguments unconditionally, so it runs at any log level.
//
// In production the writer is driven by the scheduler core's RMProxy callback
// goroutine: AsyncRMCallback.UpdateAllocation (pkg/cache/scheduler_callback.go:49)
// calls Task.MarkPreviouslyAllocated at scheduler_callback.go:86 for a pod that
// was already assigned to a node. That is why the shim informer's serialisation
// of pod events does not prevent this: the two goroutines come from different
// subsystems entirely.
//
// WHY THE STAGING IS LEGITIMATE, NOT A HARNESS ARTEFACT
// -----------------------------------------------------
// No product code is modified, wrapped, instrumented or patched, and no
// callback or sleep is injected into it. The test only:
//
//   1. Acquires app.lock -- an ordinary application lock held by many product
//      paths (Application.SetState, GetNewTasks, addTask, removeTask, the
//      application FSM callbacks, and tryAddReleasableTask itself). Because
//      releaseAllocation calls shouldAppRelease -> tryAddReleasableTask BEFORE
//      its GetTaskState() calls, holding app.lock parks G1 inside the FSM's
//      stateMu read window and before the recursive read. The test chooses the
//      moment; the lock, the order and the call graph are entirely the
//      product's.
//   2. Calls two product entry points, Task.handle() and
//      Task.MarkPreviouslyAllocated(), from two goroutines -- exactly what the
//      dispatcher and the core's RMProxy callback goroutine do.
//
// The staging observation is likewise made through real lock semantics rather
// than stack inspection: while G1 is pinned it holds stateMu.RLock #1 and
// cannot release it, so no writer can be *active*; therefore a fresh
// stateMu.RLock that blocks proves a writer is *queued*. That is a
// biconditional, not a heuristic.
//
// OUTCOMES
// --------
//   FAIL "DEADLOCK REPRODUCED"  -- the bug is present in this build, with a
//                                  full goroutine dump.
//   FAIL "COULD NOT STAGE"      -- the harness assumptions no longer hold.
//                                  Never a skip.
//   PASS                        -- the bug is absent: either the pinned release
//                                  path and the queued state writer both
//                                  completed, or the release path no longer
//                                  runs inside the FSM's lock window at all so
//                                  there is no window to pin it in.
//
// See d1_deadlock_repro_test.go for the note on intentionally leaked
// goroutines; the same applies here.

import (
	"sync"
	"testing"
	"time"

	"gotest.tools/v3/assert"
)

// reproAssertReleaseAllocationReadsTaskState is a harness self-check. On a
// throwaway app/task pair, with no locks held by the test and the application
// in the Accepted state, it proves that task.handle(CompleteTask) drives
// releaseAllocation past shouldAppRelease() and all the way to the
// SchedulerAPI.UpdateAllocation() call -- i.e. through the region that contains
// the GetTaskState() re-reads.
//
// Without this check, a build in which the release hook had simply been deleted
// would produce a vacuous PASS.
func reproAssertReleaseAllocationReadsTaskState(t *testing.T) {
	t.Helper()
	app, task, apiProvider := reproSetup(t, ApplicationStates().Accepted)
	before := apiProvider.GetSchedulerAPIUpdateAllocationCount()
	err := task.handle(NewSimpleTaskEvent(app.applicationID, task.taskID, CompleteTask))
	assert.NilError(t, err, "harness precondition: CompleteTask must be handled")
	assert.Equal(t, task.GetTaskState(), TaskStates().Completed,
		"harness precondition: CompleteTask must reach the Completed state")
	assert.Assert(t, apiProvider.GetSchedulerAPIUpdateAllocationCount() > before,
		"harness precondition: releaseAllocation must run past shouldAppRelease and send the release")
	assert.Equal(t, len(app.releaseableTasks), 0,
		"harness precondition: an Accepted application must not defer the release")
}

// TestReleaseAllocationRecursiveFSMStateReadLockDeadlock reproduces the
// recursive fsm stateMu read-lock deadlock. See the file header for the full
// cycle, the product lines involved, and why the staging is legitimate.
//
// A FAILURE OF THIS TEST MEANS THE BUG IS PRESENT IN THE BUILD UNDER TEST.
func TestReleaseAllocationRecursiveFSMStateReadLockDeadlock(t *testing.T) {
	reproAssertReleaseAllocationReadsTaskState(t)

	// Accepted, so tryAddReleasableTask returns false and releaseAllocation
	// carries on to its GetTaskState() re-reads instead of returning early.
	app, task, _ := reproSetup(t, ApplicationStates().Accepted)

	// Step 1: hold the application lock. This is the pin.
	var unpinOnce sync.Once
	unpin := func() { unpinOnce.Do(app.lock.Unlock) }
	app.lock.Lock()
	defer unpin()

	// Step 2: G1 drives the release path (the dispatcher's role in production).
	g1Done := make(chan struct{})
	var g1Err error
	go func() {
		defer close(g1Done)
		g1Err = task.handle(NewSimpleTaskEvent(app.applicationID, task.taskID, CompleteTask))
	}()

	select {
	case <-g1Done:
		// G1 completed the whole CompleteTask event while the test held
		// app.lock. The release path therefore never blocked on app.lock from
		// inside the FSM callback window: there is no point at which it can be
		// parked while the FSM holds stateMu read-locked, so the recursive-read
		// window this test targets does not exist in this build.
		unpin()
		assert.NilError(t, g1Err, "CompleteTask must be handled")
		assert.Equal(t, task.GetTaskState(), TaskStates().Completed)
		t.Log("PASS: no recursive-read window. task.handle(CompleteTask) ran to completion while " +
			"the test held app.lock, so the release path does not acquire app.lock from inside " +
			"fsm.Event's stateMu read window and cannot be parked there.")
		return
	case <-time.After(reproParkWait):
	}

	// Step 3: confirm G1 is parked inside fsm.Event.
	//
	// A fresh FSM.Can() takes eventMu (fsm@v1.0.3/fsm.go:231). If it blocks,
	// eventMu is held, and FSM.Event is the only place holding it across other
	// work -- so G1 is inside FSM.Event, which means it is also holding
	// stateMu.RLock #1 (fsm.go:311-312). The only blocking call reachable from
	// there is the app.lock acquisition in tryAddReleasableTask, which the test
	// is holding, and which releaseAllocation reaches before any GetTaskState().
	if !reproWaitProbeBlocked(func() { _ = task.sm.Can(SubmitTask.String()) },
		reproProbeSettle, reproStageTimeout) {
		unpin()
		t.Fatalf("COULD NOT STAGE (harness assumption broken): task.handle(CompleteTask) neither "+
			"completed within %s nor left the FSM event mutex held, so G1 is blocked somewhere "+
			"this test does not model. Refusing to report a pass.\ngoroutine dump:\n%s",
			reproParkWait, reproGoroutineDump())
	}
	// Informational only: this cycle does not require the task lock to have been
	// dropped (MarkPreviouslyAllocated calls sm.SetState before it takes the
	// task lock), unlike the D1 cycle.
	t.Logf("staged: G1 parked inside fsm.Event with the event mutex held; task.lock currently free: %t",
		reproProbeCompletes(reproTaskLockProbe(task), reproProbeSuccess))

	// Step 4: queue the FSM state writer. In production this is the core's
	// RMProxy callback goroutine marking a pod that was already allocated.
	writerDone := make(chan struct{})
	go func() {
		defer close(writerDone)
		task.MarkPreviouslyAllocated("alloc-01", "node-1")
	}()

	// Step 5: confirm the writer is queued on stateMu.
	//
	// G1 holds stateMu.RLock #1 and is pinned, so it cannot release it; no
	// writer can therefore be *active*. A fresh stateMu.RLock (FSM.Current,
	// fsm.go:209) can only block if a writer is *queued*, and the only writer in
	// existence is MarkPreviouslyAllocated's SetState. So: probe blocked <=>
	// writer queued.
	if !reproWaitProbeBlocked(func() { _ = task.sm.Current() },
		reproProbeSettle, reproStageTimeout) {
		unpin()
		t.Fatalf("COULD NOT STAGE (harness assumption broken): no FSM state writer became queued, "+
			"so a fresh stateMu.RLock still succeeds. Refusing to report a pass."+
			"\ngoroutine dump:\n%s", reproGoroutineDump())
	}
	if reproDone(writerDone) {
		unpin()
		t.Fatalf("COULD NOT STAGE (harness assumption broken): MarkPreviouslyAllocated completed "+
			"while G1 still held the FSM state read lock. Refusing to report a pass."+
			"\ngoroutine dump:\n%s", reproGoroutineDump())
	}

	// Staged. Release the pin: G1 leaves tryAddReleasableTask, re-takes
	// task.lock, and runs on into releaseAllocation's GetTaskState().
	unpin()

	if !reproAwaitAll(reproResolveWait, g1Done, writerDone) {
		t.Fatalf(`DEADLOCK REPRODUCED -- recursive looplab/fsm stateMu read lock behind a queued writer.

Neither goroutine completed within %s after the application lock was released.
  G1 task.handle(CompleteTask):        %s
  W  task.MarkPreviouslyAllocated(...): %s

Cycle:
  G1 holds fsm stateMu.RLock #1, taken by FSM.Event at fsm@v1.0.3/fsm.go:311
     with a deferred RUnlock and held across the before-callbacks. Inside
     before_CompleteTask -> Task.beforeTaskCompleted (pkg/cache/task.go:445-446)
     -> Task.releaseAllocation, it calls task.GetTaskState()
     (pkg/cache/task.go:470) -> task.sm.Current() (pkg/cache/task.go:148-150) ->
     stateMu.RLock #2 on the same RWMutex, on the same goroutine.
  W  is queued on stateMu.Lock() from Task.MarkPreviouslyAllocated ->
     task.sm.SetState (pkg/cache/task.go:266-267, fsm@v1.0.3/fsm.go:224),
     behind G1's RLock #1.

Go's sync.RWMutex blocks new readers once a writer is queued, so RLock #2 can
never be granted, and RLock #1 is never released, so the writer can never be
granted either. In production W is the scheduler core's RMProxy callback
goroutine, so the shim's informer serialisation does not prevent this.

The test held only app.lock -- a lock ordinary product code holds -- and called
only Task.handle() and Task.MarkPreviouslyAllocated(). No product code was
modified or instrumented.

goroutine dump:
%s`, reproResolveWait, reproState(g1Done), reproState(writerDone), reproGoroutineDump())
	}

	t.Logf("PASS: the staged interleave resolved. G1 (CompleteTask) returned %v and the queued "+
		"FSM state writer completed.", g1Err)
}
