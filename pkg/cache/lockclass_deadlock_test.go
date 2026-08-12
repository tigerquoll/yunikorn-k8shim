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

package cache

import (
	"strings"
	"testing"

	godeadlock "github.com/sasha-s/go-deadlock"
	"gotest.tools/v3/assert"
	v1 "k8s.io/api/core/v1"

	"github.com/apache/yunikorn-k8shim/pkg/cache/external"
	"github.com/apache/yunikorn-k8shim/pkg/client"
	"github.com/apache/yunikorn-k8shim/pkg/locking"
)

// The tests below cover the wiring of the lock class order check into this package rather than the
// checker itself, which has its own tests in pkg/locking. A constructor that loses its SetClass
// call leaves its lock classless: the object silently drops out of the order check and nothing else
// changes, so only a test that looks at the class or drives a known violation notices.

// startClassOrderCheck turns the check on and takes over the two go-deadlock reporting options for
// the duration of the test. The report callback belongs to the core locking package and exits the
// process when DEADLOCK_EXIT is set, which is what "make test" sets, so a test that deliberately
// trips the check has to hold it. The returned buffer collects the reports.
func startClassOrderCheck(t *testing.T) *strings.Builder {
	t.Helper()
	restore := locking.EnableClassOrderForTest()
	reports := &strings.Builder{}
	realBuf := godeadlock.Opts.LogBuf
	realReport := godeadlock.Opts.OnPotentialDeadlock
	godeadlock.Opts.LogBuf = reports
	godeadlock.Opts.OnPotentialDeadlock = func() {}
	t.Cleanup(func() {
		godeadlock.Opts.LogBuf = realBuf
		godeadlock.Opts.OnPotentialDeadlock = realReport
		restore()
	})
	return reports
}

// newClassedObjects builds one of each object that carries an ordering class, every one of them
// through the constructor the shim itself uses.
func newClassedObjects(t *testing.T) (*Context, *Application, *Task, *external.SchedulerCache, *PlaceholderManager) {
	t.Helper()
	context := initContextForTest()
	app := NewApplication("app-class", "root.default", "test-user", testGroups, map[string]string{}, newMockSchedulerAPI())
	pod := newPodHelper("pod-class", "default", "pod-class-uid", "", "app-class", v1.PodPending)
	task := NewTask("task-class", app, context, pod)
	schedulerCache := external.NewSchedulerCache(client.NewMockedAPIProvider(false).GetAPIs())
	manager := NewPlaceholderManager(client.NewMockedAPIProvider(false).GetAPIs())
	return context, app, task, schedulerCache, manager
}

// TestLockClassAssignedByConstructors checks the class of every lock this package hands out. The
// scheduler cache keeps its lock unexported in its own package, its class is checked there.
func TestLockClassAssignedByConstructors(t *testing.T) {
	context, app, task, _, manager := newClassedObjects(t)

	assert.Equal(t, locking.ClassContext, context.lock.ClassOf(), "the context constructor must class its lock")
	assert.Equal(t, locking.ClassApplication, app.lock.ClassOf(), "the application constructor must class its lock")
	assert.Equal(t, locking.ClassTask, task.lock.ClassOf(), "the task constructor must class its lock")
	assert.Equal(t, locking.ClassPlaceholderManager, manager.ClassOf(), "the placeholder manager constructor must class its lock")
}

// TestLockClassOrderEdges drives one violation for every edge of the declared order, using the real
// objects. A missing class on either side of an edge shows up here as a report that never arrives.
func TestLockClassOrderEdges(t *testing.T) {
	tests := []struct {
		name     string
		violate  func(*Context, *Application, *Task, *external.SchedulerCache)
		expected string
	}{
		{
			// rule 9.3: the application lock comes before the task lock
			name: "application under task",
			violate: func(_ *Context, app *Application, task *Task, _ *external.SchedulerCache) {
				task.lock.Lock()
				app.lock.Lock()
				app.lock.Unlock()
				task.lock.Unlock()
			},
			expected: "acquiring cache.Application while holding cache.Task",
		},
		{
			// rule 9.3: the context lock comes before the application lock
			name: "context under application",
			violate: func(context *Context, app *Application, _ *Task, _ *external.SchedulerCache) {
				app.lock.Lock()
				context.lock.Lock()
				context.lock.Unlock()
				app.lock.Unlock()
			},
			expected: "acquiring cache.Context while holding cache.Application",
		},
		{
			// the scheduler cache is the leaf of the order, nothing may be taken under it
			name: "task under scheduler cache",
			violate: func(_ *Context, _ *Application, task *Task, schedulerCache *external.SchedulerCache) {
				schedulerCache.LockForReads()
				task.lock.Lock()
				task.lock.Unlock()
				schedulerCache.UnlockForReads()
			},
			expected: "acquiring cache.Task while holding external.SchedulerCache",
		},
		{
			// and the closure reaches all the way up from the leaf
			name: "context under scheduler cache",
			violate: func(context *Context, _ *Application, _ *Task, schedulerCache *external.SchedulerCache) {
				schedulerCache.LockForReads()
				context.lock.Lock()
				context.lock.Unlock()
				schedulerCache.UnlockForReads()
			},
			expected: "acquiring cache.Context while holding external.SchedulerCache",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// fresh objects for every case: go-deadlock tracks the order of lock instances and
			// would report the pair itself once two cases take the same two locks the other way
			// round, which is a different finding from the one under test here
			context, app, task, schedulerCache, _ := newClassedObjects(t)
			reports := startClassOrderCheck(t)
			tt.violate(context, app, task, schedulerCache)
			assert.Assert(t, strings.Contains(reports.String(), tt.expected),
				"expected a report of %q, got: %s", tt.expected, reports.String())
		})
	}
}

// TestLockClassPlaceholderManagerUnordered covers the pair that is deliberately left out of the
// order: the manager is classed but has no edge, so neither direction is reported. Adding an edge
// for it would bless the manager over application nesting that the application side of the code
// avoids by starting a goroutine, see the note in pkg/locking/lockclass_deadlock.go.
func TestLockClassPlaceholderManagerUnordered(t *testing.T) {
	t.Run("application under manager", func(t *testing.T) {
		_, app, _, _, manager := newClassedObjects(t)
		reports := startClassOrderCheck(t)
		manager.Lock()
		app.lock.Lock()
		app.lock.Unlock()
		manager.Unlock()
		assertNoOrderViolation(t, reports)
	})

	// fresh objects again: taking the same two locks in the opposite order is what go-deadlock
	// itself reports, and that report is a different finding from the class order check. That it
	// fires here is the reason the edge is withheld rather than declared.
	t.Run("manager under application", func(t *testing.T) {
		_, app, _, _, manager := newClassedObjects(t)
		reports := startClassOrderCheck(t)
		app.lock.Lock()
		manager.Lock()
		manager.Unlock()
		app.lock.Unlock()
		assertNoOrderViolation(t, reports)
	})
}

// assertNoOrderViolation fails when the class order check reported anything. It looks for the
// message of the check rather than an empty buffer: the buffer is the shared go-deadlock report
// buffer and any other goroutine of the test binary can write to it as well.
func assertNoOrderViolation(t *testing.T, reports *strings.Builder) {
	t.Helper()
	assert.Assert(t, !strings.Contains(reports.String(), "lock order violation"),
		"no class order violation expected, got: %s", reports.String())
}
