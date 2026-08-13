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

package locking

import (
	"go/parser"
	"go/token"
	"sort"
	"strings"
	"testing"

	"gotest.tools/v3/assert"
)

// The lock order exists twice: as the "+lockorder" annotations in the package documentation,
// which the static analysis reads, and as declaredOrder in lockclass_deadlock.go, which the
// runtime check walks. Nothing in either tool compares them, so this test does.
//
// The classes are named in the annotations exactly as the runtime prints them, which is what
// makes the comparison a string comparison rather than a mapping that could itself drift.

// staticOnlyClasses are the classes that the annotations declare and the runtime taxonomy
// deliberately does not carry. They exist so that the analyses which only need to know that a
// lock is held can see one; they take no part in the order. Adding to this list is a decision,
// which is why the test names them rather than skipping unknown classes.
var staticOnlyClasses = map[string]string{
	"admission.WebhookManager": "classed for the blocking analysis, outside the order",
	"canary.Canary":            "the self test fixture, never built",
}

// TestLockOrderAnnotationsMatchRuntime compares the edges in the package documentation with the
// edges the runtime check declares.
func TestLockOrderAnnotationsMatchRuntime(t *testing.T) {
	annotated := parseOrderAnnotations(t)

	runtime := make([]string, 0, len(declaredOrder))
	for _, e := range declaredOrder {
		runtime = append(runtime, e[0].String()+" < "+e[1].String())
	}
	sort.Strings(runtime)
	sort.Strings(annotated)

	assert.DeepEqual(t, runtime, annotated)
}

// TestLockOrderClassesMatchRuntime checks the class names on both sides: every class the
// annotations use in an edge must be a class the runtime knows, and every class the runtime
// knows must either appear in an edge or be one the runtime deliberately leaves unordered.
func TestLockOrderClassesMatchRuntime(t *testing.T) {
	inEdges := make(map[string]bool)
	for _, edge := range parseOrderAnnotations(t) {
		for _, name := range strings.Split(edge, " < ") {
			inEdges[strings.TrimSpace(name)] = true
		}
	}

	runtimeNames := make(map[string]bool)
	for c := Class(1); c < numClasses; c++ {
		runtimeNames[c.String()] = true
	}

	for name := range inEdges {
		if _, ok := staticOnlyClasses[name]; ok {
			t.Errorf("class %s is declared static only but appears in an order edge", name)
			continue
		}
		assert.Assert(t, runtimeNames[name], "the annotations order %s, which the runtime does not know", name)
	}

	// every runtime class that takes no part in the order is a withheld pair, and the reason has
	// to be written down next to the declaration
	unordered := make([]string, 0)
	for name := range runtimeNames {
		if !inEdges[name] {
			unordered = append(unordered, name)
		}
	}
	sort.Strings(unordered)
	assert.DeepEqual(t, []string{"cache.PlaceholderManager"}, unordered)
}

// parseOrderAnnotations reads the "+lockorder" lines out of the package documentation of this
// package, the same text the static analysis reads.
func parseOrderAnnotations(t *testing.T) []string {
	t.Helper()
	fset := token.NewFileSet()
	pkgs, err := parser.ParseDir(fset, ".", nil, parser.ParseComments)
	assert.NilError(t, err, "cannot parse the package source")
	pkg, ok := pkgs["locking"]
	assert.Assert(t, ok, "package locking not found in the parsed source")

	edges := make([]string, 0)
	for _, file := range pkg.Files {
		if file.Doc == nil {
			continue
		}
		for _, c := range file.Doc.List {
			text := strings.TrimSpace(c.Text)
			if !strings.HasPrefix(text, "// +lockorder:") {
				continue
			}
			payload := strings.TrimPrefix(text, "// +lockorder:")
			parts := strings.Split(payload, "<")
			assert.Equal(t, 2, len(parts), "malformed order annotation: %s", text)
			edges = append(edges, strings.TrimSpace(parts[0])+" < "+strings.TrimSpace(parts[1]))
		}
	}
	assert.Assert(t, len(edges) > 0, "no order annotations found in the package documentation")
	return edges
}
