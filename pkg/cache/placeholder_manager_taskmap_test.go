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
	"strconv"
	"sync"
	"testing"

	"gotest.tools/v3/assert"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"

	"github.com/apache/yunikorn-k8shim/pkg/client"
)

// newPlaceholderTestApp returns an application with one task group, which is what makes the
// placeholder manager walk the task map of the application.
func newPlaceholderTestApp(t *testing.T, appID string) (*Application, *PlaceholderManager) {
	t.Helper()
	mockedAPIProvider := client.NewMockedAPIProvider(false)
	mgr := NewPlaceholderManager(mockedAPIProvider.GetAPIs())

	app := NewApplication(appID, "root.abc", "test-user", testGroups, map[string]string{},
		mockedAPIProvider.GetAPIs().SchedulerAPI)
	app.setTaskGroups([]TaskGroup{
		{
			Name:      "test-group-1",
			MinMember: 2,
			MinResource: map[string]resource.Quantity{
				v1.ResourceCPU.String():    resource.MustParse("500m"),
				v1.ResourceMemory.String(): resource.MustParse("500Mi"),
			},
		},
	})
	return app, mgr
}

func newPlaceholderTestTask(app *Application, index int) *Task {
	pod := &v1.Pod{
		TypeMeta: metav1.TypeMeta{Kind: "Pod", APIVersion: "v1"},
		ObjectMeta: metav1.ObjectMeta{
			Name:      "pod-" + strconv.Itoa(index),
			Namespace: "default",
			UID:       types.UID("pod-uid-" + strconv.Itoa(index)),
		},
		Spec: v1.PodSpec{},
	}
	return NewTask("task-"+strconv.Itoa(index), app, nil, pod)
}

// TestCreateAppPlaceholdersConcurrentTaskAdd creates placeholders while tasks are added to the
// application. The manager holds its own lock over that work, which says nothing about the
// application: the task map is written under the application lock as pods arrive from the
// informers, so walking it without that lock is a race and iterating it while it is written is a
// fatal "concurrent map iteration and map write".
//
// Run with -race. On the unfixed code this reports a data race between the walk in
// getPlaceHolderTasks and the write in addTask.
func TestCreateAppPlaceholdersConcurrentTaskAdd(t *testing.T) {
	app, mgr := newPlaceholderTestApp(t, "app-placeholder-race")
	for i := 0; i < 4; i++ {
		app.addTask(newPlaceholderTestTask(app, i))
	}

	stop := make(chan struct{})
	var wg sync.WaitGroup

	// the informer path: keeps adding tasks under the application lock
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 4; ; i++ {
			select {
			case <-stop:
				return
			default:
			}
			app.addTask(newPlaceholderTestTask(app, i))
		}
	}()

	// the reserving path: the placeholder manager creating the placeholders of the application
	for i := 0; i < 200; i++ {
		assert.NilError(t, mgr.createAppPlaceholders(app))
	}

	close(stop)
	wg.Wait()
}

// TestCreateAppPlaceholdersCountsExisting checks the behaviour the walk is there for: placeholders
// that already exist are counted and only the missing ones are created.
func TestCreateAppPlaceholdersCountsExisting(t *testing.T) {
	created := newThreadSafePodsMap()
	mockedAPIProvider := client.NewMockedAPIProvider(false)
	mockedAPIProvider.MockCreateFn(func(pod *v1.Pod) (*v1.Pod, error) {
		created.add(pod)
		return pod, nil
	})
	mgr := NewPlaceholderManager(mockedAPIProvider.GetAPIs())

	app := NewApplication("app-placeholder-count", "root.abc", "test-user", testGroups,
		map[string]string{}, mockedAPIProvider.GetAPIs().SchedulerAPI)
	app.setTaskGroups([]TaskGroup{
		{
			Name:      "test-group-1",
			MinMember: 3,
			MinResource: map[string]resource.Quantity{
				v1.ResourceCPU.String():    resource.MustParse("500m"),
				v1.ResourceMemory.String(): resource.MustParse("500Mi"),
			},
		},
	})

	// no placeholders yet: all three are created
	assert.NilError(t, mgr.createAppPlaceholders(app))
	assert.Equal(t, 3, created.count(), "all placeholders of the task group must be created")

	// register one of them as a placeholder task of the application, one less is needed
	placeholder := NewTaskPlaceholder("placeholder-task", app, nil, &v1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: "placeholder", Namespace: "default", UID: "placeholder-uid"},
	})
	placeholder.setTaskGroupName("test-group-1")
	app.addTask(placeholder)

	second := newThreadSafePodsMap()
	mockedAPIProvider.MockCreateFn(func(pod *v1.Pod) (*v1.Pod, error) {
		second.add(pod)
		return pod, nil
	})
	assert.NilError(t, mgr.createAppPlaceholders(app))
	assert.Equal(t, 2, second.count(), "the existing placeholder must be counted")
}
