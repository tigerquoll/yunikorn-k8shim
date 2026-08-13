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
// The fixture holds a violation of each class that is in use: setValue writes
// a guarded field without holding the lock, reEnter calls a method that must not be called
// with the lock held while holding it, doubleLock takes one lock twice, and the callback
// table reaches the second of those from a body whose lock is named by the value it asserts.
// The self test asserts that the analysis reports all four. Without that assertion checklocks could
// silently stop finding anything at all and every run would still be green: the lock
// wrappers are recognised by their declaration in locking.go, so removing it, renaming the
// types, changing the forwarding methods or moving to a version that behaves differently
// all end in an analysis that reports less than it did. The fixture uses the wrappers of
// this package, which makes the self test cover the whole chain: wrapper type, forwarding
// methods, field annotation and lock precondition.

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

// relock takes and releases the lock twice over, which is balanced. It must never be reported.
func (c *canary) relock(value int) {
	c.lock.Lock()
	c.value = value
	c.lock.Unlock()
	c.lock.Lock()
	c.value = value
	c.lock.Unlock()
}

// doubleLock takes the lock a second time on the same path, the self deadlock shape at its
// simplest. The self test in the Makefile requires the analysis to report "already locked"
// for this line.
//
// The diagnostic only exists while the wrappers declare themselves lock primitives. Without
// that declaration the forwarding methods need a "+checklocksignore" each, and an ignore is
// read at every call site of the function that carries it, so the whole class disappears for
// every wrapper lock in the shim while every other message of this fixture stays exactly as
// it is. The order analysis sees the same line as a nesting of two locks of one class; that
// is silenced so nestSameClass stays the only source of that diagnostic.
func (c *canary) doubleLock(value int) {
	c.lock.Lock()
	defer c.lock.Unlock()
	c.lock.Lock() // +lockorderignore
	c.value = value
}

// callbackCanary is the subject of the callback fixture below. Its guarded field is named
// apart from the one above so a diagnostic about it can only have come from there. It carries
// no lock class: the order analysis has its own fixture and should not see this one.
type callbackCanary struct {
	lock RWMutex
	// +checklocks:lock
	callbackValue int
}

// callbackSelfLocking takes the subject's own lock, so holding it on entry would deadlock.
// +checklocksexclude:c.lock
func (c *callbackCanary) callbackSelfLocking(value int) {
	c.lock.Lock()
	defer c.lock.Unlock()
	c.callbackValue = value
}

// callbackEvent stands in for what a state machine library hands a callback: the subject
// arrives inside an interface and the body recovers it by asserting a type.
type callbackEvent struct {
	Args []any
}

// callbackTable is the shape the fsm callbacks in pkg/cache have: a table of literals handed
// to a library, each stating the lock its caller holds by naming the value its own body
// recovers, because that value exists nowhere else to be named.
//
// Both polarities are in the one literal. The write through the asserted subject is correct
// and must never be reported, and the call below it must be, because the guard put that
// subject's lock in scope and the callee takes it again.
//
// The second is what the self test requires, and it is the only message here that a guard
// which stopped binding would take with it: a guard matching nothing records no lock,
// silently, and then the call is fine while the write is reported instead. Requiring the
// report on the WRITE would pass in both worlds and prove nothing.
func callbackTable() map[string]func(*callbackEvent) {
	return map[string]func(*callbackEvent){
		// +checklocks:event.Args[0].(*callbackCanary).lock
		"enter": func(event *callbackEvent) {
			subject := event.Args[0].(*callbackCanary)
			subject.callbackValue = 1
			subject.callbackSelfLocking(2)
		},
	}
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
