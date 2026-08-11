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

// This file is never built, the build constraint above is never set. It is the fixture for
// the self test of the "checklocks" make target: files named on the go vet command line are
// analysed even when a build constraint excludes them, so the target can hand this file to
// the analysis without it ever becoming part of the shim.
//
// The setValue method below writes a guarded field without holding the lock. The self test
// asserts that the analysis reports it. Without that assertion checklocks could silently
// stop finding anything at all and every run would still be green: the lock wrappers are
// recognised by their name, so renaming them, changing the forwarding methods or moving to
// a gvisor version that behaves differently all end in an analysis that reports nothing.
// The fixture uses the wrappers of this package, which makes the self test cover the whole
// chain: wrapper type, forwarding methods and field annotation.

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
