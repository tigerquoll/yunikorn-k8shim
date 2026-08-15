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
	"go/ast"
	"go/build"
	"go/parser"
	"go/token"
	"io/fs"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"testing"

	godeadlock "github.com/sasha-s/go-deadlock"

	"gotest.tools/v3/assert"
)

// annotationClass maps the name a class carries in the static annotations to the class of the
// runtime checker. The names are the same text on both sides, the runtime prints a class exactly
// as an annotation spells it, so this map is only what says which names exist.
var annotationClass = map[string]Class{
	"cache.Context":            ClassContext,
	"cache.Application":        ClassApplication,
	"cache.Task":               ClassTask,
	"external.SchedulerCache":  ClassSchedulerCache,
	"cache.PlaceholderManager": ClassPlaceholderManager,
	"admission.WebhookManager": ClassWebhookManager,
}

// classIdent maps the name of the constant that names a class to the class itself, so that a
// "SetClass(locking.ClassTask)" call can be read back out of the source without type information.
// The constant names cannot be derived from the annotation names: an annotation qualifies a class
// with the package of the type it sits on and a Go identifier cannot carry the dot.
var classIdent = map[string]Class{
	"ClassContext":            ClassContext,
	"ClassApplication":        ClassApplication,
	"ClassTask":               ClassTask,
	"ClassSchedulerCache":     ClassSchedulerCache,
	"ClassPlaceholderManager": ClassPlaceholderManager,
	"ClassWebhookManager":     ClassWebhookManager,
}

// orderAnnotations parses the order the static analyzer reads out of the package doc of
// locking.go: the edges, the hierarchical classes and the classes whose same class rule is
// withheld. The file is parsed rather than the annotations being repeated here, so that the test
// compares the two declarations instead of comparing a copy of one with the other.
func orderAnnotations(t *testing.T) (edges [][2]Class, hier, withheld map[Class]bool) {
	t.Helper()
	fset := token.NewFileSet()
	file, err := parser.ParseFile(fset, "locking.go", nil, parser.ParseComments)
	assert.NilError(t, err, "parsing the file that declares the order")
	assert.Assert(t, file.Doc != nil, "locking.go must carry the annotated order in its package doc")

	class := func(name string) Class {
		t.Helper()
		c, ok := annotationClass[name]
		assert.Assert(t, ok, "the annotations name a class the runtime checker does not have: %s", name)
		return c
	}
	hier = make(map[Class]bool)
	withheld = make(map[Class]bool)
	for _, c := range file.Doc.List {
		text := strings.TrimSpace(strings.TrimPrefix(c.Text, "//"))
		switch {
		case strings.HasPrefix(text, "+lockorder:"):
			before, after, ok := strings.Cut(strings.TrimPrefix(text, "+lockorder:"), "<")
			assert.Assert(t, ok, "an order annotation must read \"A < B\": %s", text)
			edges = append(edges, [2]Class{class(strings.TrimSpace(before)), class(strings.TrimSpace(after))})
		case strings.HasPrefix(text, "+lockhierarchical:"):
			hier[class(strings.TrimSpace(strings.TrimPrefix(text, "+lockhierarchical:")))] = true
		case strings.HasPrefix(text, "+lockorderwithheld:"):
			withheld[class(strings.TrimSpace(strings.TrimPrefix(text, "+lockorderwithheld:")))] = true
		}
	}
	return edges, hier, withheld
}

// classSet turns the flag array of the checker into a set, so both sides of a comparison are
// written the same way.
func classSet(flags [numClasses]bool) map[Class]bool {
	set := make(map[Class]bool)
	for c := Class(1); c < numClasses; c++ {
		if flags[c] {
			set[c] = true
		}
	}
	return set
}

// edgeStrings renders a set of edges as sorted text, which makes a mismatch readable.
func edgeStrings(edges [][2]Class) []string {
	out := make([]string, 0, len(edges))
	for _, e := range edges {
		out = append(out, e[0].String()+" < "+e[1].String())
	}
	sort.Strings(out)
	return out
}

// moduleRoot is the root of the module, relative to this package.
const moduleRoot = "../.."

// classWiring walks the module for the two halves of a class declaration: the "+lockclass"
// annotation the type carries, and the SetClass call its constructor makes. Both are collected as
// the set of classes of a directory, which is as far as a parse without type information can tie a
// call to the type it is made on, and is far enough: no package annotates one class and registers
// another. A set rather than a list because a class is annotated once and registered by every
// constructor of its type.
//
// Only the files of a normal build are read. The canary of this package sits behind its own build
// tag and declares classes that deliberately belong to it alone, and test files build their own
// objects, neither of which says anything about how the shim is wired.
func classWiring(t *testing.T) (annotated, registered map[string][]string) {
	t.Helper()
	annotated = make(map[string][]string)
	registered = make(map[string][]string)

	add := func(into map[string][]string, dir string, c Class) {
		name := c.String()
		for _, have := range into[dir] {
			if have == name {
				return
			}
		}
		into[dir] = append(into[dir], name)
	}
	annotatedClass := func(name string) Class {
		t.Helper()
		c, ok := annotationClass[name]
		assert.Assert(t, ok, "a type is annotated with a class the runtime checker does not have: %s", name)
		return c
	}
	registeredClass := func(name string) Class {
		t.Helper()
		c, ok := classIdent[name]
		assert.Assert(t, ok, "SetClass is called with something that is not a class constant: %s", name)
		return c
	}
	err := filepath.WalkDir(moduleRoot, func(path string, entry fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if entry.IsDir() {
			// the root is reached as "..", so it is only the directories below it that a
			// leading dot marks as none of the module's own source
			if path != moduleRoot && strings.HasPrefix(entry.Name(), ".") {
				return fs.SkipDir
			}
			return nil
		}
		if !strings.HasSuffix(entry.Name(), ".go") || strings.HasSuffix(entry.Name(), "_test.go") {
			return nil
		}
		dir := filepath.Dir(path)
		built, err := build.Default.MatchFile(dir, entry.Name())
		if err != nil || !built {
			return err
		}
		file, err := parser.ParseFile(token.NewFileSet(), path, nil, parser.ParseComments)
		if err != nil {
			return err
		}
		dir, err = filepath.Rel(moduleRoot, dir)
		if err != nil {
			return err
		}
		for _, group := range file.Comments {
			for _, c := range group.List {
				text := strings.TrimSpace(strings.TrimPrefix(c.Text, "//"))
				if name, ok := strings.CutPrefix(text, "+lockclass:"); ok {
					add(annotated, dir, annotatedClass(strings.TrimSpace(name)))
				}
			}
		}
		ast.Inspect(file, func(n ast.Node) bool {
			call, ok := n.(*ast.CallExpr)
			if !ok {
				return true
			}
			fun, ok := call.Fun.(*ast.SelectorExpr)
			if !ok || fun.Sel.Name != "SetClass" || len(call.Args) != 1 {
				return true
			}
			var name string
			switch arg := call.Args[0].(type) {
			case *ast.SelectorExpr:
				name = arg.Sel.Name
			case *ast.Ident:
				name = arg.Name
			}
			add(registered, dir, registeredClass(name))
			return true
		})
		return nil
	})
	assert.NilError(t, err, "walking the module for the class declarations")
	for _, classes := range annotated {
		sort.Strings(classes)
	}
	for _, classes := range registered {
		sort.Strings(classes)
	}
	return annotated, registered
}

// TestLockOrderAnnotationsMatchRuntime is the guard on the one thing that keeps the runtime
// checker in this package and the static analysers of the "vetlock" make target enforcing the same
// rule: the order the annotations of locking.go declare and the order enforced by
// lockclass_deadlock.go must be the same graph, and the class a type is annotated with must be the
// class its constructor registers. Either half can be edited on its own, and a drift between them
// is silent: the static side would then pass the code the runtime side rejects, or the other way
// round, with no build ever failing.
func TestLockOrderAnnotationsMatchRuntime(t *testing.T) {
	annotatedEdges, annotatedHier, annotatedWithheld := orderAnnotations(t)

	assert.DeepEqual(t, edgeStrings(annotatedEdges), edgeStrings(declaredOrder))
	assert.DeepEqual(t, annotatedHier, classSet(hierarchical))
	assert.DeepEqual(t, annotatedWithheld, classSet(sameClassWithheld))

	// A class the annotations have no name for cannot be kept in step, so adding one to the
	// checker must fail here rather than quietly leave the static side without it.
	for c := Class(1); c < numClasses; c++ {
		named := false
		for _, mapped := range annotationClass {
			if mapped == c {
				named = true
				break
			}
		}
		assert.Assert(t, named, "class %s has no name in the annotations", c)
	}

	// The order above only binds the classes to each other. What binds a class to a lock is the
	// SetClass call in the constructor, and an annotated type whose constructor forgets it, or
	// registers a different class, leaves the runtime check reading an order the annotations do
	// not describe.
	annotated, registered := classWiring(t)
	assert.DeepEqual(t, annotated, registered)
}

// resetClassCheck puts the checker back into a known state: no goroutine holds anything and no
// pair has been reported.
//
// It also takes over the go-deadlock report callback for the duration of the test. That callback
// belongs to the core locking package and exits the process when DEADLOCK_EXIT is set, which is
// what "make test" sets: a test that deliberately trips the checker would take the whole test
// binary down with it. The tests assert on the reported pairs instead, the real callback is
// restored afterwards.
func resetClassCheck(t *testing.T, enabled bool) {
	t.Helper()
	resetClassOrderState()
	classOrderEnabled.Store(enabled)

	realReport := godeadlock.Opts.OnPotentialDeadlock
	godeadlock.Opts.OnPotentialDeadlock = func() {}
	t.Cleanup(func() {
		classOrderEnabled.Store(false)
		godeadlock.Opts.OnPotentialDeadlock = realReport
	})
}

// reportBuf collects the reports while it stands in for the go-deadlock report buffer. That buffer
// is shared with every goroutine of the test binary, so the writes have to be guarded: this is the
// shape of the errorBuf the core locking package installs, with a plain sync.Mutex for the same
// reason heldShard carries one, a wrapper lock of this package would recurse into the check.
type reportBuf struct {
	lock sync.Mutex
	data string
}

func (b *reportBuf) Write(p []byte) (int, error) {
	b.lock.Lock()
	defer b.lock.Unlock()
	b.data += string(p)
	return len(p), nil
}

func (b *reportBuf) String() string {
	b.lock.Lock()
	defer b.lock.Unlock()
	return b.data
}

// tripped reports whether the given ordered pair has been reported by the checker.
func tripped(heldClass, acquired Class) bool {
	return reported[heldClass][acquired].Load()
}

// anyTripped reports whether the checker reported any pair at all.
func anyTripped() bool {
	for i := Class(0); i < numClasses; i++ {
		for j := Class(0); j < numClasses; j++ {
			if reported[i][j].Load() {
				return true
			}
		}
	}
	return false
}

// heldGoroutines counts the goroutines the checker is currently tracking.
func heldGoroutines() int {
	total := 0
	for i := range heldShards {
		heldShards[i].lock.Lock()
		total += len(heldShards[i].goroutines)
		heldShards[i].lock.Unlock()
	}
	return total
}

func classed(c Class) *RWMutex {
	m := &RWMutex{}
	m.SetClass(c)
	return m
}

// TestClassOrderUpwardIsViolation takes a lock lower in the order and then one above it.
func TestClassOrderUpwardIsViolation(t *testing.T) {
	resetClassCheck(t, true)
	task := classed(ClassTask)
	app := classed(ClassApplication)

	task.Lock()
	assert.Assert(t, !tripped(ClassTask, ClassApplication), "holding the task lock alone is not a violation")
	app.Lock()
	assert.Assert(t, tripped(ClassTask, ClassApplication), "taking the application lock under the task lock must be reported")
	app.Unlock()
	task.Unlock()
}

// TestClassOrderDownwardIsAllowed walks the declared order from the top down.
func TestClassOrderDownwardIsAllowed(t *testing.T) {
	resetClassCheck(t, true)
	context := classed(ClassContext)
	app := classed(ClassApplication)
	task := classed(ClassTask)
	cache := classed(ClassSchedulerCache)

	context.Lock()
	app.Lock()
	task.Lock()
	cache.RLock()
	assert.Assert(t, !anyTripped(), "the declared order must not be reported")
	cache.RUnlock()
	task.Unlock()
	app.Unlock()
	context.Unlock()
}

// TestClassOrderTransitive checks the closure: the only declared edge to the scheduler cache is
// from the task, the context still precedes it through the closure.
func TestClassOrderTransitive(t *testing.T) {
	resetClassCheck(t, true)
	cache := classed(ClassSchedulerCache)
	context := classed(ClassContext)

	cache.Lock()
	context.Lock()
	assert.Assert(t, tripped(ClassSchedulerCache, ClassContext), "the context under the scheduler cache is an upward acquisition")
	context.Unlock()
	cache.Unlock()
}

// TestClassOrderUnrelatedPairSilent uses the placeholder manager, which has no edge in this
// version. Guessing an order there would only produce noise or bless a ledgered finding.
func TestClassOrderUnrelatedPairSilent(t *testing.T) {
	resetClassCheck(t, true)
	manager := classed(ClassPlaceholderManager)
	app := classed(ClassApplication)

	manager.Lock()
	app.Lock()
	assert.Assert(t, !anyTripped(), "the placeholder manager is not ordered against the application")
	app.Unlock()
	manager.Unlock()

	// and the other way round
	resetClassCheck(t, true)
	app.Lock()
	manager.Lock()
	assert.Assert(t, !anyTripped(), "the placeholder manager is not ordered against the application")
	manager.Unlock()
	app.Unlock()
}

// TestClassOrderClassless leaves the locks without a class: they must not be tracked at all.
func TestClassOrderClassless(t *testing.T) {
	resetClassCheck(t, true)
	one := &RWMutex{}
	two := &RWMutex{}

	one.Lock()
	two.Lock()
	assert.Assert(t, !anyTripped(), "locks without a class are not ordered")
	assert.Equal(t, 0, heldGoroutines(), "a classless lock must not create goroutine state")
	two.Unlock()
	one.Unlock()
}

// TestClassOrderSameClass nests two locks of the same class. That is a violation unless the class
// is a hierarchy or its same class rule is withheld.
func TestClassOrderSameClass(t *testing.T) {
	resetClassCheck(t, true)
	first := classed(ClassTask)
	second := classed(ClassTask)

	first.Lock()
	second.Lock()
	assert.Assert(t, tripped(ClassTask, ClassTask), "nesting two task locks must be reported")
	second.Unlock()
	first.Unlock()
}

// TestClassOrderSameClassExemptions covers the two exemption arrays. Neither is used by the shim,
// the test states that and keeps the mechanism covered: flipping an entry must silence the pair.
func TestClassOrderSameClassExemptions(t *testing.T) {
	for i := Class(0); i < numClasses; i++ {
		assert.Assert(t, !hierarchical[i], "no shim class is a hierarchy in this version")
		assert.Assert(t, !sameClassWithheld[i], "no shim class has its same class rule withheld")
	}

	resetClassCheck(t, true)
	hierarchical[ClassContext] = true
	t.Cleanup(func() { hierarchical[ClassContext] = false })
	first := classed(ClassContext)
	second := classed(ClassContext)

	first.Lock()
	second.Lock()
	assert.Assert(t, !anyTripped(), "a hierarchical class is exempt from the same class rule")
	second.Unlock()
	first.Unlock()

	resetClassCheck(t, true)
	sameClassWithheld[ClassApplication] = true
	t.Cleanup(func() { sameClassWithheld[ClassApplication] = false })
	firstApp := classed(ClassApplication)
	secondApp := classed(ClassApplication)

	firstApp.Lock()
	secondApp.Lock()
	assert.Assert(t, !anyTripped(), "a withheld same class rule is not reported")
	secondApp.Unlock()
	firstApp.Unlock()
}

// TestClassOrderDisabled repeats the violation case with the checker turned off.
func TestClassOrderDisabled(t *testing.T) {
	resetClassCheck(t, false)
	task := classed(ClassTask)
	app := classed(ClassApplication)

	task.Lock()
	app.Lock()
	assert.Assert(t, !anyTripped(), "nothing is checked while the switch is off")
	app.Unlock()
	task.Unlock()

	// and nothing was tracked either
	assert.Equal(t, 0, heldGoroutines(), "no goroutine state may be kept while the switch is off")
}

// TestClassOrderPerGoroutine holds a low lock on one goroutine while another goroutine takes a
// high one: the two must not see each other's held set.
func TestClassOrderPerGoroutine(t *testing.T) {
	resetClassCheck(t, true)
	task := classed(ClassTask)
	app := classed(ClassApplication)

	taken := make(chan struct{})
	release := make(chan struct{})
	done := make(chan struct{})
	go func() {
		defer close(done)
		task.Lock()
		close(taken)
		<-release
		task.Unlock()
	}()

	<-taken
	// this goroutine holds nothing, so the application lock is fine even though the other
	// goroutine is sitting on a task lock
	app.Lock()
	assert.Assert(t, !anyTripped(), "held classes must not leak between goroutines")
	app.Unlock()
	close(release)
	<-done
}

// TestClassOrderUnlockOutOfOrder releases the locks in acquisition order rather than in reverse:
// the multiset must still end up empty.
func TestClassOrderUnlockOutOfOrder(t *testing.T) {
	resetClassCheck(t, true)
	context := classed(ClassContext)
	app := classed(ClassApplication)
	task := classed(ClassTask)

	context.Lock()
	app.Lock()
	task.Lock()
	// release in the same order they were taken
	context.Unlock()
	app.Unlock()
	task.Unlock()
	assert.Assert(t, !anyTripped(), "the declared order must not be reported")
	assert.Equal(t, 0, heldGoroutines(), "the goroutine must be dropped once it holds nothing")
}

// TestClassOrderReportOnce trips the same pair twice and checks only one report is produced.
func TestClassOrderReportOnce(t *testing.T) {
	resetClassCheck(t, true)
	task := classed(ClassTask)
	app := classed(ClassApplication)

	task.Lock()
	app.Lock()
	app.Unlock() //nolint:staticcheck // SA2001: the critical section is empty on purpose, the acquisition is what is checked
	task.Unlock()
	assert.Assert(t, tripped(ClassTask, ClassApplication), "the pair is marked as reported")
	assert.Equal(t, int32(1), reportCount.Load(), "the first violation is reported")

	reportCount.Store(0)
	task.Lock()
	app.Lock()
	assert.Equal(t, int32(0), reportCount.Load(), "the same pair must only be reported once")
	app.Unlock()
	task.Unlock()
}

// TestClassOrderReadLocksOrder checks the read side is ordered the same way as the write side.
func TestClassOrderReadLocksOrder(t *testing.T) {
	resetClassCheck(t, true)
	task := classed(ClassTask)
	context := classed(ClassContext)

	task.RLock()
	context.RLock()
	assert.Assert(t, tripped(ClassTask, ClassContext), "the context read lock under a task read lock is upward")
	context.RUnlock()
	task.RUnlock()
}

// TestClassOrderReportContents checks the report names both classes and carries the stack of the
// acquisition without the forwarding frames of this package.
func TestClassOrderReportContents(t *testing.T) {
	resetClassCheck(t, true)
	buf := &reportBuf{}
	realBuf := godeadlock.Opts.LogBuf
	godeadlock.Opts.LogBuf = buf
	t.Cleanup(func() { godeadlock.Opts.LogBuf = realBuf })

	task := classed(ClassTask)
	app := classed(ClassApplication)
	task.Lock()
	app.Lock()
	app.Unlock() //nolint:staticcheck // SA2001: the critical section is empty on purpose, the acquisition is what is checked
	task.Unlock()

	out := buf.String()
	assert.Assert(t, strings.Contains(out, "POTENTIAL DEADLOCK: lock order violation"), "the report is written to the shared buffer: %s", out)
	assert.Assert(t, strings.Contains(out, "acquiring cache.Application while holding cache.Task"), "the report names both classes: %s", out)
	assert.Assert(t, strings.Contains(out, "TestClassOrderReportContents"), "the report carries the stack of the acquisition: %s", out)
	assert.Assert(t, !strings.Contains(out, "locking.(*RWMutex).Lock"), "the forwarding frames of this package are stripped: %s", out)
}

// TestClassOrderSameClassReportContents checks the report of a same class nesting. The rule is the
// class itself rather than an edge of the order, so the report must not describe it as one: there
// is no order that has a class before itself.
func TestClassOrderSameClassReportContents(t *testing.T) {
	resetClassCheck(t, true)
	buf := &reportBuf{}
	realBuf := godeadlock.Opts.LogBuf
	godeadlock.Opts.LogBuf = buf
	t.Cleanup(func() { godeadlock.Opts.LogBuf = realBuf })

	first := classed(ClassTask)
	second := classed(ClassTask)
	first.Lock()
	second.Lock()
	second.Unlock() //nolint:staticcheck // SA2001: the critical section is empty on purpose, the acquisition is what is checked
	first.Unlock()

	out := buf.String()
	assert.Assert(t, strings.Contains(out, "acquiring cache.Task while holding cache.Task"), "the report names the class: %s", out)
	assert.Assert(t, strings.Contains(out, "two locks of one class must not nest"), "the report states the rule that was broken: %s", out)
	assert.Assert(t, !strings.Contains(out, "the declared order"), "a same class nesting is not an edge of the order: %s", out)
}

// TestClassOrderConcurrentNoRace hammers the checker from several goroutines, it is here to be run
// under the race detector.
func TestClassOrderConcurrentNoRace(t *testing.T) {
	resetClassCheck(t, true)
	app := classed(ClassApplication)
	task := classed(ClassTask)

	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 200; j++ {
				app.Lock()
				task.Lock()
				task.Unlock() //nolint:staticcheck // SA2001: the critical section is empty on purpose, the acquisition is what is checked
				app.Unlock()
			}
		}()
	}
	wg.Wait()
	assert.Assert(t, !anyTripped(), "the declared order must not be reported")
	assert.Equal(t, 0, heldGoroutines(), "every goroutine must be dropped once it holds nothing")
}
