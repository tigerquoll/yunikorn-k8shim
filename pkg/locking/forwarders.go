//go:build !deadlock

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

// The methods below forward to the inner lock, they are the complete lock API of this
// package. Two things about the shape are deliberate:
//
// The inner lock is a named field and not embedded. Embedding promotes the whole
// go-deadlock API, which includes TryLock, TryRLock and RLocker. The first two would be
// tracked against the inner field by the gVisor checklocks analysis instead of the wrapper
// and the last one hands out a plain sync.Locker that cannot be tracked at all. With a
// named field none of them exist on the wrapper: using one becomes a compile error and thus
// a deliberate decision instead of a silent hole in the analysis.
//
// The forwarding itself exists so that checklocks (see the "checklocks" make target) tracks
// the locking.Mutex and locking.RWMutex fields directly. When the inner lock is called
// directly the analysis attributes the acquisition to the inner field, i.e. "lock.mu"
// instead of "lock", and all "+checklocks:" field annotations fail to match. The forwarders
// are ignored by the analysis: a lock method acquires a lock and returns while holding it,
// which is exactly what its lock balance check flags. The ignore is not entirely free, it
// also suppresses the "already locked" and "unlock without lock" diagnostics at every call
// site. The lock state tracking itself is unaffected: guarded field access, the lock
// preconditions of a function and the lock balance of the calling function are all still
// checked.
//
// The forwarding costs nothing at runtime, the methods are inlined. The only visible effect
// is one extra stack frame in a go-deadlock report: the "<<<<<" marker points at the
// forwarder in this file with the real caller one frame below it.

// This is the default build: the lock class order check (see lockclass.go) is only compiled into
// the deadlock tagged build, so these forwarders are exactly the plain forwarding calls and stay
// inlinable. The instrumented copies live in forwarders_deadlock.go.

// +checklocksignore
func (m *Mutex) Lock() {
	m.mu.Lock()
}

// +checklocksignore
func (m *Mutex) Unlock() {
	m.mu.Unlock()
}

// +checklocksignore
func (m *RWMutex) Lock() {
	m.mu.Lock()
}

// +checklocksignore
func (m *RWMutex) Unlock() {
	m.mu.Unlock()
}

// +checklocksignore
func (m *RWMutex) RLock() {
	m.mu.RLock()
}

// +checklocksignore
func (m *RWMutex) RUnlock() {
	m.mu.RUnlock()
}
