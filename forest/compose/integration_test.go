package compose_test

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Leeminjing/go-forest/forest"
	"github.com/Leeminjing/go-forest/forest/compose"
)

type source interface {
	Read(context.Context) (string, error)
}
type forwarder struct{ source forest.Ref[source] }

func (s forwarder) Read(ctx context.Context) (string, error) {
	var result string
	err := s.source.Use(ctx, func(next source) error { var err error; result, err = next.Read(ctx); return err })
	return result, err
}

type valueSource struct {
	label   string
	counter atomic.Int32
	entered chan struct{}
	release chan struct{}
	once    *sync.Once
}

func (s *valueSource) Read(ctx context.Context) (string, error) {
	if s.entered != nil {
		s.once.Do(func() { close(s.entered) })
	}
	if s.release != nil {
		select {
		case <-s.release:
		case <-ctx.Done():
			return "", ctx.Err()
		}
	}
	return fmt.Sprintf("%s/%d", s.label, s.counter.Add(1)), nil
}

func forwardingDefinition(name string) forest.Definition {
	return forest.Definition{TypeID: name, Version: "1", HotSwappable: true, PreserveChildren: true, Reparentable: true,
		Ports:        []forest.PortDescriptor{forest.Port[source]("source", forest.Required())},
		Capabilities: []forest.CapabilityDescriptor{forest.Capability[source]("read", forest.Public|forest.Ancestors)},
		Build: func(ctx context.Context, env *forest.BuildEnv) (forest.Lifecycle, error) {
			ref, err := forest.Reference[source](env, "source")
			if err != nil {
				return nil, err
			}
			return forest.Hooks{}, forest.Provide(env, "read", forwarder{source: ref})
		}}
}
func valueDefinition(version, label string, stops *atomic.Int32, entered, release chan struct{}) forest.Definition {
	signalOnce := &sync.Once{}
	return forest.Definition{TypeID: "value", Version: version, HotSwappable: true,
		Capabilities: []forest.CapabilityDescriptor{forest.Capability[source]("read", forest.Public|forest.Ancestors)},
		Build: func(ctx context.Context, env *forest.BuildEnv) (forest.Lifecycle, error) {
			err := forest.Provide(env, "read", &valueSource{label: label, entered: entered, release: release, once: signalOnce})
			return forest.Hooks{OnStop: func(context.Context) error {
				if stops != nil {
					stops.Add(1)
				}
				return nil
			}}, err
		}}
}

func newForest(t *testing.T, definitions ...forest.Definition) *forest.Forest {
	t.Helper()
	f := forest.New(forest.Options{})
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		if err := f.Close(ctx); err != nil {
			t.Error(err)
		}
	})
	for _, d := range definitions {
		if err := f.Register(d); err != nil {
			t.Fatal(err)
		}
	}
	return f
}
func apply(t *testing.T, f *forest.Forest, operations ...forest.Operation) {
	t.Helper()
	if _, err := f.Apply(context.Background(), forest.ChangePlan{BaseGeneration: f.Snapshot().Generation, Operations: operations}); err != nil {
		t.Fatal(err)
	}
}
func placement(t *testing.T, b *compose.Blueprint, id forest.TreeID) *compose.Instance {
	t.Helper()
	i, err := b.Instantiate(id, "")
	if err != nil {
		t.Fatal(err)
	}
	return i
}
func tree(t *testing.T, i *compose.Instance, path string) forest.TreeID {
	t.Helper()
	id, err := i.Tree(path)
	if err != nil {
		t.Fatal(err)
	}
	return id
}
func read(f *forest.Forest, id forest.TreeID) (string, error) {
	var result string
	err := f.Invoke(context.Background(), func(ctx context.Context) error {
		return forest.Use[source](ctx, f, id, "read", func(s source) error { var err error; result, err = s.Read(ctx); return err })
	})
	return result, err
}
func expectRead(t *testing.T, f *forest.Forest, id forest.TreeID, want string) {
	t.Helper()
	got, err := read(f, id)
	if err != nil || got != want {
		t.Fatalf("read %s: got=%q err=%v want=%q", id, got, err, want)
	}
}
func wait(t *testing.T, f *forest.Forest) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := f.Wait(ctx); err != nil {
		t.Fatal(err)
	}
}
func receive[T any](t *testing.T, ch <-chan T) T {
	t.Helper()
	select {
	case value := <-ch:
		return value
	case <-time.After(3 * time.Second):
		t.Fatal("test synchronization timed out")
		var zero T
		return zero
	}
}

func engine(t *testing.T) *compose.Blueprint {
	return mustBlueprint(t, compose.Spec{Root: ref("engine"), Trees: []compose.Tree{{ID: "worker", Definition: ref("worker")}, {ID: "source", Definition: ref("value")}},
		Bindings: []forest.Link{{ID: "worker", Port: "source", Provider: "worker", Capability: "read"}, {ID: "source", Consumer: "worker", Port: "source", Provider: "source", Capability: "read"}}})
}
func recursive(t *testing.T) *compose.Blueprint {
	inner := engine(t)
	middle := mustBlueprint(t, compose.Spec{Root: ref("module"), Trees: []compose.Tree{{ID: "engine", Composite: inner}}, Bindings: []forest.Link{{ID: "engine", Port: "source", Provider: "engine", Capability: "read"}}})
	return mustBlueprint(t, compose.Spec{Root: ref("application"), Trees: []compose.Tree{{ID: "module", Composite: middle}}, Bindings: []forest.Link{{ID: "module", Port: "source", Provider: "module", Capability: "read"}}})
}
func definitions(value forest.Definition) []forest.Definition {
	return []forest.Definition{forwardingDefinition("engine"), forwardingDefinition("worker"), forwardingDefinition("module"), forwardingDefinition("application"), forwardingDefinition("consumer"), value}
}

func TestRepeatedCompositeInstantiationIsRecursiveAndIndependent(t *testing.T) {
	b := recursive(t)
	for range 2 {
		f := newForest(t, definitions(valueDefinition("1", "v1", nil, nil, nil))...)
		left, right := placement(t, b, "left"), placement(t, b, "right")
		operations := append(left.Plan(0).Operations, right.Plan(0).Operations...)
		apply(t, f, operations...)
		if s := f.Snapshot(); s.Generation != 1 || len(s.Trees) != 10 || len(s.Links) != 2 || len(s.NestBindings) != 6 {
			t.Fatalf("expanded structure: trees=%d links=%d local=%d", len(s.Trees), len(s.Links), len(s.NestBindings))
		}
		expectRead(t, f, left.Root(), "v1/1")
		expectRead(t, f, right.Root(), "v1/1")
		expectRead(t, f, left.Root(), "v1/2")
		if a, z := f.Snapshot().Trees[tree(t, left, "module/engine/source")], f.Snapshot().Trees[tree(t, right, "module/engine/source")]; a.InstanceID == z.InstanceID {
			t.Fatal("shared physical instance")
		}
		// Existing identities are never silently overwritten by another planting.
		if _, err := f.Apply(context.Background(), left.Plan(1)); !errors.Is(err, forest.ErrInvalidOperation) || f.Snapshot().Generation != 1 {
			t.Fatal(err)
		}
	}
}

func TestExternalConsumerUsesOnlyCompositeRootBoundary(t *testing.T) {
	f := newForest(t, definitions(valueDefinition("1", "value", nil, nil, nil))...)
	i := placement(t, recursive(t), "module")
	ops := append(i.Plan(0).Operations, forest.AddRoot("client", ref("consumer")), forest.Connect(forest.Link{ID: "client", Consumer: "client", Port: "source", Provider: i.Root(), Capability: "read"}))
	apply(t, f, ops...)
	expectRead(t, f, "client", "value/1")
	if _, err := read(f, tree(t, i, "module/engine/source")); !errors.Is(err, forest.ErrMissingCapability) {
		t.Fatal(err)
	}
	// Even a public leaf cannot be linked across the closed composite boundary.
	op := forest.Rebind(forest.Link{ID: "client", Consumer: "client", Port: "source", Provider: tree(t, i, "module/engine/source"), Capability: "read"})
	if _, err := f.Apply(context.Background(), forest.ChangePlan{BaseGeneration: 1, Operations: []forest.Operation{op}}); !errors.Is(err, forest.ErrMissingCapability) || f.Snapshot().Generation != 1 {
		t.Fatal(err)
	}
	expectRead(t, f, "client", "value/2")
}

func TestNestedCompositeConsumesAnExternalRootPort(t *testing.T) {
	f := newForest(t, definitions(valueDefinition("1", "injected", nil, nil, nil))...)
	// The inner module declares its required dependency at its root. Its parent
	// supplies it without naming or modifying any inner implementation node.
	inner := mustBlueprint(t, compose.Spec{Root: ref("engine")})
	outer := mustBlueprint(t, compose.Spec{Root: ref("application"), Trees: []compose.Tree{{ID: "engine", Composite: inner}, {ID: "provider", Definition: ref("value")}}, Bindings: []forest.Link{
		{ID: "output", Port: "source", Provider: "engine", Capability: "read"},
		{ID: "input", Consumer: "engine", Port: "source", Provider: "provider", Capability: "read"},
	}})
	i := placement(t, outer, "outer")
	apply(t, f, i.Plan(0).Operations...)
	expectRead(t, f, i.Root(), "injected/1")
	id, err := i.Link("input")
	if err != nil {
		t.Fatal(err)
	}
	if s := f.Snapshot(); s.Links[id].Consumer != "outer/engine" || s.Links[id].Provider != "outer/provider" {
		t.Fatal("external root port not linked")
	}
	if _, err := f.Apply(context.Background(), forest.ChangePlan{BaseGeneration: 1, Operations: []forest.Operation{forest.Unlink(id)}}); !errors.Is(err, forest.ErrDependencyInUse) {
		t.Fatal(err)
	}
	expectRead(t, f, i.Root(), "injected/2")
}

func TestCompositeInternalReplacementPinsOldCalls(t *testing.T) {
	entered, release := make(chan struct{}), make(chan struct{})
	defer func() {
		select {
		case <-release:
		default:
			close(release)
		}
	}()
	var oldStops atomic.Int32
	f := newForest(t, definitions(valueDefinition("1", "old", &oldStops, entered, release))...)
	v2 := valueDefinition("2", "new", nil, nil, nil)
	if err := f.Register(v2); err != nil {
		t.Fatal(err)
	}
	b := engine(t)
	left, right := placement(t, b, "left"), placement(t, b, "right")
	apply(t, f, append(left.Plan(0).Operations, right.Plan(0).Operations...)...)
	result := make(chan string, 1)
	fail := make(chan error, 1)
	go func() { value, err := read(f, left.Root()); result <- value; fail <- err }()
	receive(t, entered)
	apply(t, f, forest.Replace(tree(t, left, "source"), v2.Ref(), false))
	if oldStops.Load() != 0 {
		t.Fatal("old provider released during call")
	}
	expectRead(t, f, left.Root(), "new/1")
	close(release)
	if got := receive(t, result); got != "old/1" {
		t.Fatal(got)
	}
	if err := receive(t, fail); err != nil {
		t.Fatal(err)
	}
	expectRead(t, f, right.Root(), "old/1")
	wait(t, f)
	if oldStops.Load() != 1 {
		t.Fatal("wrong old instance stopped")
	}
}

func TestCompositeRemovalProtectsExternalDependenciesAndActiveCalls(t *testing.T) {
	entered, release := make(chan struct{}), make(chan struct{})
	defer func() {
		select {
		case <-release:
		default:
			close(release)
		}
	}()
	var stops atomic.Int32
	f := newForest(t, definitions(valueDefinition("1", "old", &stops, entered, release))...)
	v2 := valueDefinition("2", "new", nil, nil, nil)
	if err := f.Register(v2); err != nil {
		t.Fatal(err)
	}
	b := engine(t)
	old, next := placement(t, b, "old"), placement(t, b, "next")
	apply(t, f, append(old.Plan(0).Operations, forest.AddRoot("client", ref("consumer")), forest.Connect(forest.Link{ID: "client", Consumer: "client", Port: "source", Provider: old.Root(), Capability: "read"}))...)
	finished := make(chan error, 1)
	go func() { _, err := read(f, "client"); finished <- err }()
	receive(t, entered)
	if _, err := f.Apply(context.Background(), forest.ChangePlan{BaseGeneration: 1, Operations: []forest.Operation{forest.RemoveSubtree(old.Root())}}); !errors.Is(err, forest.ErrDependencyInUse) {
		t.Fatal(err)
	}
	operations := append(next.Plan(1).Operations, forest.Replace(tree(t, next, "source"), v2.Ref(), false), forest.Rebind(forest.Link{ID: "client", Consumer: "client", Port: "source", Provider: next.Root(), Capability: "read"}), forest.RemoveSubtree(old.Root()))
	apply(t, f, operations...)
	if stops.Load() != 0 {
		t.Fatal("removed provider stopped during old call")
	}
	expectRead(t, f, "client", "new/1")
	close(release)
	if err := receive(t, finished); err != nil {
		t.Fatal(err)
	}
	wait(t, f)
	if stops.Load() != 1 {
		t.Fatal("old provider not released")
	}
	if s := f.Snapshot(); len(s.Trees) != 4 || s.Generation != 2 {
		t.Fatal("incomplete composite removal")
	}
}

func TestCompositeRollbackOnInvalidContractAndStartupFailure(t *testing.T) {
	for _, kind := range []string{"contract", "startup", "missing-required", "invalid-port"} {
		t.Run(kind, func(t *testing.T) {
			var cleanup atomic.Int32
			value := valueDefinition("1", "healthy", nil, nil, nil)
			f := newForest(t, definitions(value)...)
			existing := placement(t, engine(t), "existing")
			apply(t, f, existing.Plan(0).Operations...)
			bad := valueDefinition("bad", "bad", &cleanup, nil, nil)
			if kind == "contract" {
				bad.Capabilities = []forest.CapabilityDescriptor{forest.Capability[int]("read", forest.Public|forest.Ancestors)}
				bad.Build = func(ctx context.Context, env *forest.BuildEnv) (forest.Lifecycle, error) {
					return forest.Hooks{OnStop: func(context.Context) error { cleanup.Add(1); return nil }}, forest.Provide(env, "read", 42)
				}
			}
			if kind == "startup" {
				build := bad.Build
				bad.Build = func(ctx context.Context, env *forest.BuildEnv) (forest.Lifecycle, error) {
					_, err := build(ctx, env)
					return forest.Hooks{OnStart: func(context.Context) error { return errors.New("startup failure") }, OnStop: func(context.Context) error { cleanup.Add(1); return nil }}, err
				}
			}
			if err := f.Register(bad); err != nil {
				t.Fatal(err)
			}
			broken := mustBlueprint(t, compose.Spec{Root: ref("engine"), Trees: []compose.Tree{{ID: "source", Definition: bad.Ref()}}, Bindings: []forest.Link{{ID: "source", Port: "source", Provider: "source", Capability: "read"}}})
			if kind == "missing-required" {
				broken = mustBlueprint(t, compose.Spec{Root: ref("engine")})
			}
			if kind == "invalid-port" {
				broken = mustBlueprint(t, compose.Spec{Root: ref("engine"), Trees: []compose.Tree{{ID: "source", Definition: bad.Ref()}}, Bindings: []forest.Link{{ID: "source", Port: "unknown", Provider: "source", Capability: "read"}}})
			}
			i := placement(t, broken, "broken")
			_, err := f.Apply(context.Background(), i.Plan(1))
			want := forest.ErrContractMismatch
			if kind == "startup" {
				want = forest.ErrPrepareFailed
			}
			if kind == "missing-required" {
				want = forest.ErrMissingCapability
			}
			if kind == "invalid-port" {
				want = forest.ErrInvalidLink
			}
			if !errors.Is(err, want) || f.Snapshot().Generation != 1 || len(f.Snapshot().Trees) != 3 {
				t.Fatalf("rollback: %v", err)
			}
			wait(t, f)
			if (kind == "contract" || kind == "startup") && cleanup.Load() != 1 {
				t.Fatal("new resources not cleaned")
			}
			expectRead(t, f, existing.Root(), "healthy/1")
		})
	}
}

func TestCompositePreparationPublishesAllNodesAtOnce(t *testing.T) {
	f := newForest(t, definitions(valueDefinition("1", "healthy", nil, nil, nil))...)
	existing := placement(t, engine(t), "existing")
	apply(t, f, existing.Plan(0).Operations...)
	entered, release := make(chan struct{}), make(chan struct{})
	defer func() {
		select {
		case <-release:
		default:
			close(release)
		}
	}()
	warming := valueDefinition("warming", "ready", nil, nil, nil)
	build := warming.Build
	warming.Build = func(ctx context.Context, env *forest.BuildEnv) (forest.Lifecycle, error) {
		object, err := build(ctx, env)
		return forest.Hooks{OnReady: func(context.Context) error { close(entered); <-release; return nil }, OnStop: object.Stop}, err
	}
	if err := f.Register(warming); err != nil {
		t.Fatal(err)
	}
	b := mustBlueprint(t, compose.Spec{Root: ref("engine"), Trees: []compose.Tree{{ID: "source", Definition: warming.Ref()}}, Bindings: []forest.Link{{ID: "source", Port: "source", Provider: "source", Capability: "read"}}})
	i := placement(t, b, "new")
	finished := make(chan error, 1)
	go func() { _, err := f.Apply(context.Background(), i.Plan(1)); finished <- err }()
	receive(t, entered)
	if s := f.Snapshot(); s.Generation != 1 || len(s.Trees) != 3 || s.Trees[i.Root()].ID != "" {
		t.Fatal("partially published composite")
	}
	expectRead(t, f, existing.Root(), "healthy/1")
	close(release)
	if err := receive(t, finished); err != nil {
		t.Fatal(err)
	}
	if s := f.Snapshot(); s.Generation != 2 || len(s.Trees) != 5 {
		t.Fatal("incomplete committed composite")
	}
	expectRead(t, f, i.Root(), "ready/1")
}

func TestConcurrentCompositePlansConflictWithoutPartialPlanting(t *testing.T) {
	f := newForest(t, definitions(valueDefinition("1", "value", nil, nil, nil))...)
	b := engine(t)
	start := make(chan struct{})
	results := make(chan error, 2)
	for _, id := range []forest.TreeID{"left", "right"} {
		i := placement(t, b, id)
		go func() { <-start; _, err := f.Apply(context.Background(), i.Plan(0)); results <- err }()
	}
	close(start)
	success, conflict := 0, 0
	for range 2 {
		err := receive(t, results)
		if err == nil {
			success++
		} else if errors.Is(err, forest.ErrGenerationConflict) {
			conflict++
		} else {
			t.Fatal(err)
		}
	}
	if s := f.Snapshot(); success != 1 || conflict != 1 || s.Generation != 1 || len(s.Trees) != 3 {
		t.Fatal("lost update or partial planting")
	}
}

func TestConcurrentBlueprintInstantiationAndCalls(t *testing.T) {
	b := engine(t)
	f := newForest(t, definitions(valueDefinition("1", "old", nil, nil, nil))...)
	v2 := valueDefinition("2", "new", nil, nil, nil)
	if err := f.Register(v2); err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	results := make(chan *compose.Instance, 16)
	errs := make(chan error, 16)
	for n := 0; n < 16; n++ {
		wg.Add(1)
		go func(n int) {
			defer wg.Done()
			i, err := b.Instantiate(forest.TreeID(fmt.Sprintf("instance-%d", n)), "")
			if err != nil {
				errs <- err
				return
			}
			results <- i
		}(n)
	}
	wg.Wait()
	close(results)
	close(errs)
	for err := range errs {
		t.Fatal(err)
	}
	var operations []forest.Operation
	for i := range results {
		operations = append(operations, i.Plan(0).Operations...)
	}
	apply(t, f, operations...)
	if len(f.Snapshot().Trees) != 48 {
		t.Fatal("namespace collision")
	}
	callErrors := make(chan error, 8)
	for range 8 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for range 50 {
				if err := f.Invoke(context.Background(), func(ctx context.Context) error {
					var version string
					for n := 0; n < 2; n++ {
						if err := forest.Use[source](ctx, f, "instance-0", "read", func(s source) error {
							got, err := s.Read(ctx)
							if err != nil {
								return err
							}
							label := strings.Split(got, "/")[0]
							if n == 0 {
								version = label
							} else if version != label {
								return errors.New("mixed composite generations")
							}
							return nil
						}); err != nil {
							return err
						}
					}
					return nil
				}); err != nil {
					callErrors <- err
					return
				}
			}
		}()
	}
	for n := 0; n < 15; n++ {
		definition := ref("value")
		if n%2 == 0 {
			definition = v2.Ref()
		}
		apply(t, f, forest.Replace("instance-0/source", definition, false))
	}
	wg.Wait()
	close(callErrors)
	for err := range callErrors {
		t.Error(err)
	}
	wait(t, f)
}
