//go:build tools
// +build tools

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

// Package tools pins the build dependencies of the checklocks vet tool. It is a module of
// its own so that the analyser does not become a dependency of the shim itself, the module is
// never built as part of the shim. See the "checklocks" target in the Makefile.
//
// The analyser is gVisor's tools/checklocks, which gVisor does not publish as an importable
// module. github.com/tigerquoll/checklocks is a standalone extraction of it, taken from gvisor
// commit 1919d963, carrying three fixes that the shim annotations need and that are on their
// way upstream: the panic on cross package use of unexported global guards (filed as
// google/gvisor#14078), resolving pointer typed global guards through the pointer, and guard
// annotations on package level variable declarations. That repository says of itself that it is
// a temporary home, so expect this pin to move once the fixes land upstream.
package tools

import (
	_ "github.com/tigerquoll/checklocks/cmd/checklocks"
)
