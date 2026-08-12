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
	"sync/atomic"
)

// Class is the ordering class of a lock. Locks that are not given a class are not part of the
// order check at all: they are still covered by the go-deadlock detection.
//
// The classes and the order are the shim lock order as documented in
// docs/how-yunikorn-works.md rule 9.3. Only the objects that the document actually orders, plus
// the two caches whose place follows from the shim's own call graph, are modelled; everything
// else stays classless in this version. The order itself and the checking live in
// lockclass_deadlock.go: the check is only compiled into the deadlock tagged build so that the
// default build keeps the plain, inlinable forwarders, see forwarders.go.
type Class uint32

const (
	// ClassNone is the zero value: the lock is not part of the order check.
	ClassNone Class = iota
	ClassContext
	ClassApplication
	ClassTask
	ClassSchedulerCache
	ClassPlaceholderManager
	numClasses
)

var className = [numClasses]string{
	ClassNone:               "none",
	ClassContext:            "cache.Context",
	ClassApplication:        "cache.Application",
	ClassTask:               "cache.Task",
	ClassSchedulerCache:     "external.SchedulerCache",
	ClassPlaceholderManager: "cache.PlaceholderManager",
}

func (c Class) String() string {
	if c >= numClasses {
		return "unknown"
	}
	return className[c]
}

// classOrderEnabled is the master switch, set from DEADLOCK_CLASS_ORDER_ENABLED. It is read on
// every acquisition in the deadlock tagged build so it must stay a plain atomic load.
var classOrderEnabled atomic.Bool

// IsClassOrderEnabled returns true when the lock class order check is turned on. It is always
// false in a build that does not carry the check.
func IsClassOrderEnabled() bool {
	return classOrderEnabled.Load()
}

// SetClass gives the lock an ordering class. It is called once from the constructor of the object
// the lock belongs to, before the object is shared, so it does not need to be ordered against the
// acquisitions of that lock. It is a plain store in every build, the class is only read by the
// check in the deadlock tagged build.
func (m *Mutex) SetClass(c Class) {
	m.class.Store(uint32(c))
}

// SetClass gives the lock an ordering class, see Mutex.SetClass.
func (m *RWMutex) SetClass(c Class) {
	m.class.Store(uint32(c))
}
