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
// its own so that gvisor does not become a dependency of the shim itself, the module is
// never built as part of the shim. See the "checklocks" target in the Makefile.
package tools

import (
	_ "gvisor.dev/gvisor/tools/checklocks/cmd/checklocks"
)
