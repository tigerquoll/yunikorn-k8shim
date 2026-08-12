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
	"fmt"
	"testing"
	"time"

	"gotest.tools/v3/assert"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"

	"github.com/apache/yunikorn-k8shim/pkg/client"
	"github.com/apache/yunikorn-k8shim/pkg/common/events"
	"github.com/apache/yunikorn-k8shim/pkg/dispatcher"
	"github.com/apache/yunikorn-k8shim/pkg/locking"
)

const placeholderFailedAction = "PlaceholderCreateFailed"

// actionRecorder captures the action of every event, the mocked recorder of the events package
// only counts them and the tests below need to tell the two events of onReserving apart.
type actionRecorder struct {
	lock    locking.Mutex
	actions []string
	seen    chan string
}

func newActionRecorder() *actionRecorder {
	return &actionRecorder{seen: make(chan string, 16)}
}

func (r *actionRecorder) Eventf(_ runtime.Object, _ runtime.Object, _, _, action, _ string, _ ...interface{}) {
	r.lock.Lock()
	r.actions = append(r.actions, action)
	r.lock.Unlock()
	select {
	case r.seen <- action:
	default:
	}
}

func (r *actionRecorder) Event(_ runtime.Object, _, _, _ string) {}

func (r *actionRecorder) AnnotatedEventf(_ runtime.Object, _ map[string]string, _, _, _ string, _ ...interface{}) {
}

func (r *actionRecorder) PastEventf(_ runtime.Object, _ metav1.Time, _, _, _ string, _ ...interface{}) {
}

// waitForAction waits for an event with the given action to be recorded.
func (r *actionRecorder) waitForAction(t *testing.T, action string) {
	t.Helper()
	deadline := time.After(5 * time.Second)
	for {
		select {
		case seen := <-r.seen:
			if seen == action {
				return
			}
		case <-deadline:
			t.Fatalf("no %s event was recorded", action)
		}
	}
}

// newReservingTestApp builds an application with one task group and a placeholder manager whose
// pod creation always fails, which is the path onReserving hands to its background routine.
func newReservingTestApp(t *testing.T, appID string) (*Application, *Task) {
	t.Helper()
	mockedAPIProvider := client.NewMockedAPIProvider(false)
	mockedAPIProvider.MockCreateFn(func(pod *v1.Pod) (*v1.Pod, error) {
		return nil, fmt.Errorf("failed to create placeholder")
	})
	NewPlaceholderManager(mockedAPIProvider.GetAPIs())

	app := NewApplication(appID, "root.abc", "test-user", testGroups, map[string]string{},
		mockedAPIProvider.GetAPIs().SchedulerAPI)
	app.setTaskGroups([]TaskGroup{
		{
			Name:      "test-group-1",
			MinMember: 1,
			MinResource: map[string]resource.Quantity{
				v1.ResourceCPU.String():    resource.MustParse("500m"),
				v1.ResourceMemory.String(): resource.MustParse("500Mi"),
			},
		},
	})
	app.setPlaceholderOwnerReferences([]metav1.OwnerReference{{Name: appID, UID: "originator-uid"}})

	pod := &v1.Pod{
		TypeMeta: metav1.TypeMeta{Kind: "Pod", APIVersion: "v1"},
		ObjectMeta: metav1.ObjectMeta{
			Name: "originator", Namespace: "default", UID: "originator-uid",
		},
		Spec: v1.PodSpec{},
	}
	return app, NewTask("originator-task", app, nil, pod)
}

// TestOnReservingOriginatingTaskRace drives the placeholder creation failure path of onReserving
// against a concurrent setOriginatingTask. onReserving is called the way the state machine calls
// it, with the application lock held, so the only unsynchronised access left is the one in the
// routine it starts: the routine outlives that lock.
//
// The write is triggered from the application event handler, because the routine dispatches an
// event immediately before it reads the field. That puts the write in the window between the
// dispatch and the read, where no lock of the application orders the two.
//
// Run with -race. On the unfixed code this reports a data race between the read in the routine and
// the write in setOriginatingTask.
func TestOnReservingOriginatingTaskRace(t *testing.T) {
	recorder := newActionRecorder()
	events.SetRecorder(recorder)
	defer events.SetRecorder(events.NewMockedRecorder())

	apps := make(chan *Application, 1)
	tasks := make(chan *Task, 1)
	handled := make(chan struct{}, 64)
	dispatcher.RegisterEventHandler("TestReservingHandler", dispatcher.EventTypeApp, func(_ interface{}) {
		select {
		case app := <-apps:
			app.setOriginatingTask(<-tasks)
		default:
		}
		handled <- struct{}{}
	})
	defer dispatcher.UnregisterEventHandler("TestReservingHandler", dispatcher.EventTypeApp)
	dispatcher.Start()
	defer dispatcher.Stop()

	for i := 0; i < 25; i++ {
		app, task := newReservingTestApp(t, fmt.Sprintf("app-reserving-%d", i))
		apps <- app
		tasks <- task

		// the state machine holds the application lock over the callback
		app.lock.Lock()
		app.onReserving()
		app.lock.Unlock()

		// the routine dispatches the event just before it reads the field, so this is the point
		// where the write and the read overlap. Whether the failure event is then recorded
		// depends on which of the two wins, the test does not assert on it
		select {
		case <-handled:
		case <-time.After(5 * time.Second):
			t.Fatal("the routine of onReserving did not dispatch its event")
		}
	}

	// the routines are detached, give the reads room to land before the test returns
	time.Sleep(200 * time.Millisecond)
}

// TestOnReservingReportsFailureAgainstOriginatingTask checks that the routine still reports the
// placeholder creation failure against the originating task.
func TestOnReservingReportsFailureAgainstOriginatingTask(t *testing.T) {
	recorder := newActionRecorder()
	events.SetRecorder(recorder)
	defer events.SetRecorder(events.NewMockedRecorder())

	app, task := newReservingTestApp(t, "app-reserving-capture")
	app.setOriginatingTask(task)

	app.lock.Lock()
	app.onReserving()
	app.lock.Unlock()

	recorder.waitForAction(t, placeholderFailedAction)
	assert.Equal(t, task, app.GetOriginatingTask(), "the originating task must be unchanged")
}

// TestOnReservingWithoutOriginatingTask covers the branch where no originating task is set: the
// routine must report nothing against a pod and must not panic.
func TestOnReservingWithoutOriginatingTask(t *testing.T) {
	recorder := newActionRecorder()
	events.SetRecorder(recorder)
	defer events.SetRecorder(events.NewMockedRecorder())

	app, _ := newReservingTestApp(t, "app-reserving-no-originator")
	assert.Assert(t, app.GetOriginatingTask() == nil, "no originating task is set")

	app.lock.Lock()
	app.onReserving()
	app.lock.Unlock()

	// nothing to wait for, the routine has no pod to report against: give it room to run and
	// check that it recorded no failure event
	time.Sleep(time.Second)
	recorder.lock.Lock()
	defer recorder.lock.Unlock()
	for _, action := range recorder.actions {
		assert.Assert(t, action != placeholderFailedAction, "no failure event without an originating task")
	}
}
