//go:build checklocks_canary
// +build checklocks_canary

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

import "fmt"

// This file is never built, the build constraint above is never set. It is the fixture for
// the self test of the "checklocks" make target: files named on the go vet command line are
// analysed even when a build constraint excludes them, so the target can hand this file to
// the analysis without it ever becoming part of the shim.
//
// The fixture holds one violation of each annotation class that is in use: setValue writes
// a guarded field without holding the lock and reEnter calls a method that must not be
// called with the lock held while holding it. The self test asserts that the analysis
// reports both. Without that assertion checklocks could silently stop finding anything at
// all and every run would still be green: the lock wrappers are recognised by their name,
// so renaming them, changing the forwarding methods or moving to a gvisor version that
// behaves differently all end in an analysis that reports nothing. The fixture uses the
// wrappers of this package, which makes the self test cover the whole chain: wrapper type,
// forwarding methods, field annotation and lock precondition.

// +lockclass:canary.Canary
type canary struct {
	lock RWMutex
	// +checklocks:lock
	value int
}

// setValueLocked writes the guarded field the way it is supposed to be done. It must never
// be reported, a violation here means the forwarding methods stopped working.
func (c *canary) setValueLocked(value int) {
	c.lock.Lock()
	defer c.lock.Unlock()
	c.value = value
}

// setValue writes the guarded field without holding the lock. The self test in the Makefile
// requires the analysis to report "invalid field access" for this line.
func (c *canary) setValue(value int) {
	c.value = value
}

// setValueSelfLocking takes the lock itself, which makes it invalid to call while the lock
// is held. It must never be reported itself.
// +checklocksexclude:c.lock
func (c *canary) setValueSelfLocking(value int) {
	c.lock.Lock()
	defer c.lock.Unlock()
	c.value = value
}

// reEnter calls a method that excludes the lock while holding it, the self deadlock shape.
// The self test in the Makefile requires the analysis to report "must not hold" for this
// line. The order analysis sees the same call as a nesting of two locks of one class; it is
// silenced here so that nestSameClass stays the only source of that diagnostic and the self
// test keeps one fixture per analysis.
// +lockorderignore
func (c *canary) reEnter(value int) {
	c.lock.Lock()
	defer c.lock.Unlock()
	c.setValueSelfLocking(value)
}

// nestSameClass locks two canaries at once. Two locks of one class must not nest, which the
// order analysis reports without needing an edge, so the fixture states it without adding
// anything to the taxonomy of the shim.
func (c *canary) nestSameClass(other *canary) {
	c.lock.Lock()
	defer c.lock.Unlock()
	other.lock.Lock()
	defer other.lock.Unlock()
}

// String reads a guarded field from a method that is evaluated wherever a log entry is encoded,
// which the stringer analysis reports. The guarded field check is silenced here on purpose: it
// is the usual way this hazard is hidden, and the stringer analysis is meant to report it
// anyway. That also leaves setValue as the only source of the guarded field diagnostic, so the
// self test covers one fixture per analysis.
// +checklocksignore
func (c *canary) String() string {
	return fmt.Sprintf("canary %d", c.value)
}

// waitUnderLock waits on a channel with the lock held, which the blocking analysis reports.
func (c *canary) waitUnderLock(ch chan int) int {
	c.lock.Lock()
	defer c.lock.Unlock()
	return <-ch
}
