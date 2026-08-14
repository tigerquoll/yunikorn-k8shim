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

package external

import (
	"testing"

	"gotest.tools/v3/assert"

	"github.com/apache/yunikorn-k8shim/pkg/client"
	"github.com/apache/yunikorn-k8shim/pkg/locking"
)

// TestLockClassAssignedByConstructor checks that the cache constructor gives its lock the ordering
// class. The lock is unexported so the check lives here rather than with the rest of the wiring
// tests in pkg/cache. Losing the SetClass call would drop the cache out of the order check without
// any other symptom, the order edges themselves are driven from pkg/cache.
func TestLockClassAssignedByConstructor(t *testing.T) {
	cache := NewSchedulerCache(client.NewMockedAPIProvider(false).GetAPIs())
	assert.Equal(t, locking.ClassSchedulerCache, cache.lock.ClassOf(), "the scheduler cache constructor must class its lock")
}
