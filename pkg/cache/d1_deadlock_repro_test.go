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

// [YUNIKORN-XXXX] Deterministic reproduction of the task-lock / FSM-event-mutex
// ABBA deadlock ("D1").
//
// THE CYCLE
// ---------
//   G1: holds looplab/fsm eventMu, wants task.lock
//   G2: holds task.lock,           wants looplab/fsm eventMu
//
// THE PRODUCT LINES IT DEPENDS ON (all on the commit under test)
// --------------------------------------------------------------
//   pkg/cache/task.go:118-120   Task.handle() takes task.lock.Lock() and holds
//                               it across task.sm.Event(...).
//   fsm@v1.0.3/fsm.go:299       FSM.Event() takes f.eventMu.Lock() and holds it
//                               across the before-callbacks.
//   fsm@v1.0.3/fsm.go:436       FSM.beforeEventCallbacks runs the callbacks.
//   pkg/cache/task_state.go:437-440
//                               before_CompleteTask -> Task.beforeTaskCompleted.
//   pkg/cache/task.go:445-446   beforeTaskCompleted -> releaseAllocation(false).
//   pkg/cache/task.go:457       releaseAllocation -> task.shouldAppRelease().
//   pkg/cache/task.go:508-511   shouldAppRelease does
//                                   task.lock.Unlock(); defer task.lock.Lock()
//                               around Application.tryAddReleasableTask(task),
//                               i.e. it drops task.lock while fsm still holds
//                               eventMu, and re-takes it on the way out.
//   pkg/cache/application.go:703-704
//                               tryAddReleasableTask takes app.lock.Lock().
//
// So between the Unlock at task.go:509 and the deferred Lock at task.go:510
// (which runs when shouldAppRelease returns) there is a window in which a goroutine
// running a task FSM event holds eventMu but NOT task.lock. Any other goroutine
// that calls Task.handle() in that window takes the free task.lock and then
// blocks forever on eventMu, while the first goroutine blocks forever trying to
// re-take task.lock.
//
// In production the two goroutines are:
//   G1 the dispatcher (pkg/dispatcher/dispatcher.go:220) delivering e.g. a
//      CompleteTask event, and
//   G2 the scheduling ticker (pkg/shim/scheduler.go:132 wait.Until(ss.schedule)
//      -> Application.Schedule (pkg/cache/application.go:353) -> scheduleTasks
//      (application.go:397) -> task.handle(InitTask) at application.go:407-408).
// Nothing serialises them: GetNewTasks (pkg/cache/application.go:261-264) takes
// app.lock.RLock(), builds the slice and releases the lock on return, handing
// out bare *Task pointers that scheduleTasks then drives without any lock.
//
// WHY THE STAGING IS LEGITIMATE, NOT A HARNESS ARTEFACT
// -----------------------------------------------------
// This test does not modify, wrap, instrument or monkey-patch any product code,
// and it injects no callbacks or sleeps into the product. It only:
//
//   1. Acquires app.lock -- an application lock that real product code paths
//      acquire all the time (Application.SetState, GetNewTasks, addTask,
//      removeTask, tryAddReleasableTask itself, and every application FSM
//      callback). Holding it is not an exotic state; it is the normal state of
//      the world whenever another goroutine is inside the application.
//   2. Calls two exported/product entry points, Task.handle(), from two
//      goroutines -- exactly what the dispatcher and the scheduling ticker do.
//
// The test therefore only chooses *when* a perfectly ordinary lock is held. It
// makes the interleave reliable; it does not create it. Every lock in the cycle
// is a product lock, acquired by product code, in the product's own order.
//
// Consequently, the pin also doubles as the oracle: on a build where the
// release path does not acquire app.lock from inside the FSM callback window
// (or does not drop task.lock there), G1 simply runs to completion while the
// test holds app.lock, and the test reports PASS. That branch is a positive
// observation, not a skip.
//
// OUTCOMES
// --------
//   FAIL "DEADLOCK REPRODUCED"  -- the bug is present in this build. The
//                                  message names the cycle and includes a full
//                                  goroutine dump.
//   FAIL "COULD NOT STAGE"      -- the harness assumptions no longer hold. This
//                                  is deliberately a failure and never a skip,
//                                  so that a broken repro can never be misread
//                                  as evidence that the bug is gone.
//   PASS                        -- the bug is absent: either both goroutines
//                                  completed with the interleave fully staged,
//                                  or the FSM callback window in which the task
//                                  lock is dropped no longer exists.
//
// NOTE ON LEAKED GOROUTINES
// -------------------------
// When the deadlock reproduces, G1 and G2 (and the probe goroutines that
// observed them) stay blocked for the lifetime of the test binary -- that is
// what a deadlock is. This is safe for `go test`: no t.Fatal is ever called
// from a non-test goroutine, the test goroutine itself always returns, and the
// remaining tests in the package still run to completion. The blocked
// goroutines hold only locks belonging to this test's own throwaway
// Application/Task, so they cannot wedge any other test.

import (
	"runtime"
	"sync"
	"testing"
	"time"

	"gotest.tools/v3/assert"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"

	"github.com/apache/yunikorn-k8shim/pkg/client"
)

// Timings. These are not synchronisation: every staging step is confirmed by a
// positive observation of a real lock's state (see reproProbeBlocked). The
// durations only bound how long the test is willing to wait for an observation
// that, in practice, is available within microseconds.
const (
	// how long a goroutine that is expected to be blocked is given to finish
	// before the test concludes that it is in fact blocked
	reproParkWait = 2 * time.Second
	// how long a probe must stay blocked before it counts as "blocked"
	reproProbeSettle = 300 * time.Millisecond
	// how long a probe that is expected to succeed is given to succeed
	reproProbeSuccess = 2 * time.Second
	// overall budget for observing a staging condition
	reproStageTimeout = 5 * time.Second
	// how long both goroutines are given to finish once the pin is released
	reproResolveWait = 5 * time.Second
	// poll interval while waiting for a staging condition to appear
	reproPollInterval = time.Millisecond
)

// ---------------------------------------------------------------------------
// Shared helpers. Also used by d2_deadlock_repro_test.go.
// ---------------------------------------------------------------------------

// reproProbeBlocked runs fn on a fresh goroutine and reports whether fn was
// still blocked once settle had elapsed.
//
// This is the staging device used throughout both repros: it observes the state
// of a *real product lock* by trying to take it, rather than inspecting
// goroutine stacks. If fn blocks, the probe goroutine stays blocked; that is
// intentional and documented in the file header.
func reproProbeBlocked(fn func(), settle time.Duration) bool {
	done := make(chan struct{})
	go func() {
		fn()
		close(done)
	}()
	select {
	case <-done:
		return false
	case <-time.After(settle):
		return true
	}
}

// reproWaitProbeBlocked polls until fn is observed to block for settle, or until
// timeout expires. Returns true if blocking was observed.
func reproWaitProbeBlocked(fn func(), settle, timeout time.Duration) bool {
	deadline := time.Now().Add(timeout)
	for {
		if reproProbeBlocked(fn, settle) {
			return true
		}
		if time.Now().After(deadline) {
			return false
		}
		time.Sleep(reproPollInterval)
	}
}

// reproProbeCompletes reports whether fn ran to completion within timeout.
func reproProbeCompletes(fn func(), timeout time.Duration) bool {
	return !reproProbeBlocked(fn, timeout)
}

// reproTaskLockProbe returns a probe that takes and immediately releases
// task.lock in read mode. The critical section is intentionally empty: the
// probe exists to observe whether the lock can be acquired at all, not to read
// any state under it. Whether it blocks is the entire signal.
func reproTaskLockProbe(task *Task) func() {
	return func() {
		task.lock.RLock()
		task.lock.RUnlock() //nolint:staticcheck // SA2001: the empty critical section is the probe
	}
}

// reproDone reports whether ch is already closed, without blocking.
func reproDone(ch <-chan struct{}) bool {
	select {
	case <-ch:
		return true
	default:
		return false
	}
}

// reproAwaitAll waits for every channel to close, returning false on timeout.
func reproAwaitAll(timeout time.Duration, chans ...chan struct{}) bool {
	deadline := time.After(timeout)
	pending := make([]<-chan struct{}, 0, len(chans))
	for _, c := range chans {
		pending = append(pending, c)
	}
	for {
		remaining := pending[:0]
		for _, c := range pending {
			if !reproDone(c) {
				remaining = append(remaining, c)
			}
		}
		pending = remaining
		if len(pending) == 0 {
			return true
		}
		select {
		case <-pending[0]:
		case <-deadline:
			return false
		}
	}
}

// reproGoroutineDump returns a dump of every goroutine in the process. It is
// used only to make a reproduced deadlock legible in the failure output; it is
// never used to drive or detect the staging.
func reproGoroutineDump() string {
	buf := make([]byte, 1<<20)
	n := runtime.Stack(buf, true)
	return string(buf[:n])
}

func reproState(ch <-chan struct{}) string {
	if reproDone(ch) {
		return "finished"
	}
	return "STILL BLOCKED"
}

// reproPod builds a minimal, non-terminated, non-placeholder pod so that
// NewTask leaves the task in the New state.
func reproPod(name, uid string) *v1.Pod {
	return &v1.Pod{
		TypeMeta: metav1.TypeMeta{
			Kind:       "Pod",
			APIVersion: "v1",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name: name,
			UID:  types.UID(uid),
		},
		Spec: v1.PodSpec{
			Containers: []v1.Container{{Name: "container-01"}},
		},
	}
}

// reproSetup builds a throwaway Application/Task pair driven by the standard
// mocked API provider used across this package's tests.
func reproSetup(t *testing.T, appState string) (*Application, *Task, *client.MockedAPIProvider) {
	t.Helper()
	ctx, apiProvider := initContextAndAPIProviderForTest()
	app := NewApplication("app01", "root.default", "bob", testGroups, map[string]string{},
		apiProvider.GetAPIs().SchedulerAPI)
	app.SetState(appState)
	task := NewTask("task01", app, ctx, reproPod("pod-01", "task01"))
	assert.Equal(t, task.GetTaskState(), TaskStates().New)
	return app, task, apiProvider
}

// reproAssertReleasePathReachesAppLockHolder is a harness self-check. On a
// throwaway app/task pair, with no locks held by the test, it proves that
// task.handle(CompleteTask) really does drive the release path all the way into
// Application.tryAddReleasableTask.
//
// With the application in the New state, tryAddReleasableTask appends the task
// to app.releaseableTasks and returns true, so observing a non-empty
// releaseableTasks slice is direct evidence that the release path ran and
// reached the function whose app.lock acquisition the main test pins on.
//
// Without this check, a build in which the release hook had simply been deleted
// would produce a vacuous PASS.
func reproAssertReleasePathReachesTryAddReleasableTask(t *testing.T) {
	t.Helper()
	app, task, _ := reproSetup(t, ApplicationStates().New)
	err := task.handle(NewSimpleTaskEvent(app.applicationID, task.taskID, CompleteTask))
	assert.NilError(t, err, "harness precondition: CompleteTask must be handled")
	assert.Equal(t, task.GetTaskState(), TaskStates().Completed,
		"harness precondition: CompleteTask must reach the Completed state")
	assert.Equal(t, len(app.releaseableTasks), 1,
		"harness precondition: the CompleteTask release path must reach Application.tryAddReleasableTask")
}

// ---------------------------------------------------------------------------
// D1
// ---------------------------------------------------------------------------

// TestTaskLockFSMEventMutexDeadlock reproduces the ABBA deadlock between
// Task.lock and the looplab/fsm event mutex. See the file header for the full
// cycle, the product lines involved, and why the staging is legitimate.
//
// A FAILURE OF THIS TEST MEANS THE BUG IS PRESENT IN THE BUILD UNDER TEST.
func TestTaskLockFSMEventMutexDeadlock(t *testing.T) {
	reproAssertReleasePathReachesTryAddReleasableTask(t)

	app, task, _ := reproSetup(t, ApplicationStates().Accepted)

	// Step 1: hold the application lock. This is the pin. tryAddReleasableTask
	// wants it in write mode, so G1 will stop there.
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
		// inside the FSM callback window, so the window in which task.lock is
		// dropped while eventMu is held does not exist in this build. There is
		// nothing for a second goroutine to slip into.
		unpin()
		assert.NilError(t, g1Err, "CompleteTask must be handled")
		assert.Equal(t, task.GetTaskState(), TaskStates().Completed)
		g2Err := task.handle(NewSimpleTaskEvent(app.applicationID, task.taskID, InitTask))
		t.Logf("PASS: no ABBA window. task.handle(CompleteTask) ran to completion while the test "+
			"held app.lock, so the release path did not acquire app.lock with task.lock dropped "+
			"from inside fsm.Event. (second handle() returned: %v)", g2Err)
		return
	case <-time.After(reproParkWait):
	}

	// Step 3: confirm G1 is parked inside fsm.Event.
	//
	// A fresh FSM.Can() call takes eventMu (fsm@v1.0.3/fsm.go:231). If it
	// blocks, eventMu is held; G1 is the only goroutine that touches this FSM,
	// and FSM.Event is the only place that holds eventMu across other work, so
	// G1 is inside fsm.Event.
	if !reproWaitProbeBlocked(func() { _ = task.sm.Can(SubmitTask.String()) },
		reproProbeSettle, reproStageTimeout) {
		unpin()
		t.Fatalf("COULD NOT STAGE (harness assumption broken): task.handle(CompleteTask) neither "+
			"completed within %s nor left the FSM event mutex held, so G1 is blocked somewhere "+
			"this test does not model. Refusing to report a pass.\ngoroutine dump:\n%s",
			reproParkWait, reproGoroutineDump())
	}

	// Step 4: confirm G1 has released task.lock. A fresh RLock that succeeds
	// proves task.lock is not write-held. Combined with step 3 (G1 is inside
	// fsm.Event, and Task.handle holds task.lock across fsm.Event) this pins G1
	// to exactly one place in the product: past the task.lock.Unlock() at
	// task.go:509, inside tryAddReleasableTask, blocked on the app.lock the
	// test is holding.
	taskLockFree := reproProbeCompletes(reproTaskLockProbe(task), reproProbeSuccess)
	if !taskLockFree {
		// G1 is blocked inside the FSM callback but is still holding task.lock.
		// No other goroutine can enter Task.handle, so this specific ABBA cycle
		// cannot form.
		unpin()
		if !reproAwaitAll(reproResolveWait, g1Done) {
			t.Fatalf("COULD NOT STAGE (harness assumption broken): G1 held task.lock while blocked "+
				"on app.lock, and did not finish within %s after the pin was released."+
				"\ngoroutine dump:\n%s", reproResolveWait, reproGoroutineDump())
		}
		t.Logf("PASS: no ABBA window. The release path blocked on app.lock from inside fsm.Event "+
			"but kept task.lock held throughout, so no other goroutine can acquire task.lock and "+
			"block on eventMu. (handle returned: %v)", g1Err)
		return
	}

	// Step 5: G2 takes the momentarily free task.lock and blocks on eventMu.
	// This is exactly what the scheduling ticker does via
	// Application.scheduleTasks -> task.handle(InitTask).
	g2Done := make(chan struct{})
	var g2Err error
	go func() {
		defer close(g2Done)
		g2Err = task.handle(NewSimpleTaskEvent(app.applicationID, task.taskID, InitTask))
	}()

	// Step 6: confirm G2 has taken task.lock. A fresh RLock blocks only if a
	// writer holds the lock or is queued on it; task.lock was observed free in
	// step 4, a free RWMutex grants Lock() immediately, and G1 cannot re-take it
	// while pinned. So a blocked RLock means G2 holds task.lock in write mode --
	// i.e. G2 is inside Task.handle past task.go:118, and the only thing it can
	// be waiting on is eventMu, which G1 holds.
	if !reproWaitProbeBlocked(reproTaskLockProbe(task), reproProbeSettle, reproStageTimeout) {
		unpin()
		t.Fatalf("COULD NOT STAGE (harness assumption broken): the second task.handle() call never "+
			"took task.lock. Refusing to report a pass.\ngoroutine dump:\n%s", reproGoroutineDump())
	}
	if reproDone(g2Done) {
		unpin()
		t.Fatalf("COULD NOT STAGE (harness assumption broken): the second task.handle() call "+
			"completed while G1 was still inside fsm.Event holding the event mutex. Refusing to "+
			"report a pass.\ngoroutine dump:\n%s", reproGoroutineDump())
	}

	// Staged. G1: holds eventMu, wants task.lock. G2: holds task.lock, wants
	// eventMu. Release the pin and let G1 run into it.
	unpin()

	if !reproAwaitAll(reproResolveWait, g1Done, g2Done) {
		t.Fatalf(`DEADLOCK REPRODUCED -- ABBA between Task.lock and the looplab/fsm event mutex.

Neither goroutine completed within %s after the application lock was released.
  G1 task.handle(CompleteTask): %s
  G2 task.handle(InitTask):     %s

Cycle:
  G1 holds fsm eventMu (fsm@v1.0.3/fsm.go:299, taken by FSM.Event and held
     across the before-callbacks) and, having dropped task.lock at
     pkg/cache/task.go:509 (Task.shouldAppRelease), is now blocked re-taking
     task.lock via the deferred Lock() at pkg/cache/task.go:510.
  G2 holds task.lock (taken at pkg/cache/task.go:118 by Task.handle and held
     across task.sm.Event) and is blocked acquiring fsm eventMu inside
     FSM.Event.

In production G1 is the dispatcher (pkg/dispatcher/dispatcher.go:220) and G2 is
the scheduling ticker (pkg/shim/scheduler.go:132 -> Application.Schedule ->
scheduleTasks -> task.handle(InitTask), pkg/cache/application.go:407-408). Nothing
serialises them: Application.GetNewTasks (pkg/cache/application.go:261-264)
releases app.lock before its callers drive the returned tasks.

The test held only app.lock -- a lock ordinary product code holds -- and called
only Task.handle(). No product code was modified or instrumented.

goroutine dump:
%s`, reproResolveWait, reproState(g1Done), reproState(g2Done), reproGoroutineDump())
	}

	t.Logf("PASS: the staged interleave resolved. G1 (CompleteTask) returned %v, G2 (InitTask) "+
		"returned %v.", g1Err, g2Err)
}
