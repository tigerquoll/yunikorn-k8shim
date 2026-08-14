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

package admission

import (
	"testing"

	"gotest.tools/v3/assert"
	"k8s.io/client-go/kubernetes/fake"

	"github.com/apache/yunikorn-k8shim/pkg/locking"
)

// TestLockClassAssignedByConstructor checks that the webhook manager constructor gives its lock the
// ordering class. The class carries no edge, the admission controller is a process of its own and
// holds none of the other classed locks, so what the check does with it is the same class rule: two
// manager locks nested inside each other. Losing the SetClass call would drop that without any
// other symptom.
func TestLockClassAssignedByConstructor(t *testing.T) {
	wm := newWebhookManagerImpl(createConfig(), fake.NewSimpleClientset())
	assert.Equal(t, locking.ClassWebhookManager, wm.ClassOf(), "the webhook manager constructor must class its lock")
}
