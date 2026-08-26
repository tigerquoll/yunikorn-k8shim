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

import (
	"testing"
	"time"

	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/apache/yunikorn-scheduler-interface/lib/go/si"
)

// This file holds the invariant-level regression guard for the task-release
// path: releaseAllocation() and everything it calls must run with none of the
// task FSM's internal locks held.
//
// It is deliberately complementary to the two reproduction tests in
// d1_deadlock_repro_test.go and d2_deadlock_repro_test.go rather than a
// duplicate of them. Those two stage a specific interleave and prove that a
// specific cycle can form; this one asserts the property that makes every such
// cycle impossible, without staging anything and without depending on the shape
// of either cycle. A future refactor that reintroduces release work inside an
// FSM callback window fails here even if it does not reproduce either of the
// two original cycles.

// TestReleaseAllocationRunsWithFSMStateUnlocked asserts the invariant directly
// and deterministically: while the release's UpdateAllocation call is
// executing, an independent goroutine must be able to take the task FSM's
// internal write lock (SetState) without blocking.
//
// looplab/fsm holds stateMu read-locked across its before-callbacks. If
// releaseAllocation still ran from a before-callback (the pre-fix behaviour),
// that writer would block for as long as the callback runs, which is exactly
// the condition that turns releaseAllocation's own GetTaskState() re-read into
// a recursive read-lock behind a queued writer -- a permanent deadlock. Running
// the release from an after-callback removes the read lock from the picture
// entirely.
//
// The writer is spawned at the UpdateAllocation call, which on the pre-fix code
// is reached only after the GetTaskState() re-reads have already happened, so
// this test observes the blocked writer and reports it rather than wedging
// itself on the recursive read.
func TestReleaseAllocationRunsWithFSMStateUnlocked(t *testing.T) {
	ctx, apiProvider := initContextAndAPIProviderForTest()

	app := NewApplication("app-d2", "root.default", "bob", testGroups,
		map[string]string{}, newMockSchedulerAPI())
	// Accepted => shouldAppRelease() returns false, so releaseAllocation() runs
	// all the way through to UpdateAllocation instead of deferring the release.
	app.SetState(ApplicationStates().Accepted)

	pod := &v1.Pod{
		TypeMeta:   metav1.TypeMeta{Kind: "Pod", APIVersion: "v1"},
		ObjectMeta: metav1.ObjectMeta{Name: "pod-d2", UID: "UID-d2"},
		Spec:       v1.PodSpec{},
	}
	task := NewTask("task-d2", app, ctx, pod)

	writerDone := make(chan struct{})
	writerBlocked := false

	apiProvider.MockSchedulerAPIUpdateAllocationFn(func(*si.AllocationRequest) error {
		// We are now executing inside releaseAllocation. Spawn an independent
		// goroutine that takes the FSM write lock. On the fixed build the FSM
		// state lock is free here, so it returns immediately; on a regression
		// (release inside a before-callback) it blocks behind the FSM read lock
		// held across the callback.
		started := make(chan struct{})
		go func() {
			close(started)
			task.sm.SetState(TaskStates().Completed)
			close(writerDone)
		}()
		<-started
		select {
		case <-writerDone:
			// writer acquired the FSM write lock => the state lock was not held (good).
		case <-time.After(2 * time.Second):
			writerBlocked = true
		}
		return nil
	})

	if err := task.handle(NewSimpleTaskEvent(task.applicationID, task.taskID, CompleteTask)); err != nil {
		// Reported, not fatal: the writer goroutine below must still be drained.
		t.Errorf("CompleteTask must be handled: %v", err)
	}

	// Ensure the writer goroutine has finished (it unblocks at the latest once the
	// FSM releases its state lock) so the test does not leak a goroutine.
	<-writerDone

	if writerBlocked {
		t.Fatal("regression: a concurrent FSM SetState writer was blocked while " +
			"releaseAllocation ran, i.e. the release still holds the task FSM state lock " +
			"(it must run from an after-callback, not a before-callback).")
	}
}
