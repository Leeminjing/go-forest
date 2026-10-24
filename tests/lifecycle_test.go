package tests

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Leeminjing/go-forest/forest"
)

func receive[T any](t *testing.T, ch <-chan T) T {
	t.Helper()
	select {
	case value := <-ch:
		return value
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for test synchronization")
		var zero T
		return zero
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

type worker interface {
	Work(context.Context) (string, error)
}
type workService struct {
	label   string
	entered chan struct{}
	release chan struct{}
	once    sync.Once
}

func (s *workService) Work(ctx context.Context) (string, error) {
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
	return s.label, nil
}
func workDef(version string, s *workService, stopped *atomic.Int32) forest.Definition {
	return forest.Definition{TypeID: "work", Version: version, HotSwappable: true,
		Capabilities: []forest.CapabilityDescriptor{forest.Capability[worker]("work", forest.Public)},
		Build: func(ctx context.Context, env *forest.BuildEnv) (forest.Lifecycle, error) {
			err := forest.Provide(env, "work", s)
			return forest.Hooks{OnStop: func(context.Context) error { stopped.Add(1); return nil }}, err
		}}
}
func callWork(f *forest.Forest) (string, error) {
	var result string
	err := f.Invoke(context.Background(), func(ctx context.Context) error {
		return forest.Use[worker](ctx, f, "work", "work", func(w worker) error { var err error; result, err = w.Work(ctx); return err })
	})
	return result, err
}
func TestHotSwapPinsOldCallAndDrainsAfterReturn(t *testing.T) {
	f := newForest(t)
	var stopped1, stopped2 atomic.Int32
	old := &workService{label: "v1", entered: make(chan struct{}), release: make(chan struct{})}
	v1 := register(t, f, workDef("1", old, &stopped1))
	v2 := register(t, f, workDef("2", &workService{label: "v2"}, &stopped2))
	apply(t, f, forest.AddRoot("work", v1))
	result := make(chan string, 1)
	fail := make(chan error, 1)
	go func() { value, err := callWork(f); result <- value; fail <- err }()
	receive(t, old.entered)
	oldID := f.Snapshot().Trees["work"].InstanceID
	apply(t, f, forest.Replace("work", v2, false))
	if stopped1.Load() != 0 {
		t.Fatal("old provider freed while call was active")
	}
	if got, err := callWork(f); err != nil || got != "v2" {
		t.Fatalf("new: %s %v", got, err)
	}
	found := false
	for _, i := range f.Snapshot().Instances {
		if i.ID == oldID && i.State == forest.Draining {
			found = true
		}
	}
	if !found {
		t.Fatal("old instance not inspectable as draining")
	}
	close(old.release)
	if got := receive(t, result); got != "v1" {
		t.Fatal(got)
	}
	if err := receive(t, fail); err != nil {
		t.Fatal(err)
	}
	wait(t, f)
	if stopped1.Load() != 1 || stopped2.Load() != 0 {
		t.Fatal("incorrect instance ownership")
	}
}

func TestDeleteWaitsForActiveCall(t *testing.T) {
	f := newForest(t)
	var stopped atomic.Int32
	s := &workService{label: "finished", entered: make(chan struct{}), release: make(chan struct{})}
	d := register(t, f, workDef("1", s, &stopped))
	apply(t, f, forest.AddRoot("work", d))
	finished := make(chan error, 1)
	go func() { _, err := callWork(f); finished <- err }()
	receive(t, s.entered)
	apply(t, f, forest.Remove("work"))
	if stopped.Load() != 0 {
		t.Fatal("premature Stop")
	}
	if _, err := callWork(f); !errors.Is(err, forest.ErrNotFound) {
		t.Fatal(err)
	}
	close(s.release)
	if err := receive(t, finished); err != nil {
		t.Fatal(err)
	}
	wait(t, f)
	if stopped.Load() != 1 {
		t.Fatal("not stopped")
	}
}

func TestPrepareFailureAndIncompatibleReplacementPreserveService(t *testing.T) {
	f := newForest(t)
	var oldStops, cleanup atomic.Int32
	v1 := register(t, f, workDef("1", &workService{label: "healthy"}, &oldStops))
	apply(t, f, forest.AddRoot("work", v1))
	bad := workDef("bad", &workService{label: "bad"}, &cleanup)
	build := bad.Build
	bad.Build = func(ctx context.Context, env *forest.BuildEnv) (forest.Lifecycle, error) {
		_, err := build(ctx, env)
		return forest.Hooks{OnStart: func(context.Context) error { return errors.New("startup failure") }, OnStop: func(context.Context) error { cleanup.Add(1); return nil }}, err
	}
	d := register(t, f, bad)
	_, err := f.Apply(context.Background(), forest.ChangePlan{BaseGeneration: 1, Operations: []forest.Operation{forest.Replace("work", d, false)}})
	if !errors.Is(err, forest.ErrPrepareFailed) {
		t.Fatal(err)
	}
	var detail *forest.Error
	if !errors.As(err, &detail) || detail.Tree != "work" {
		t.Fatal("lost failure context")
	}
	wait(t, f)
	if cleanup.Load() != 1 || oldStops.Load() != 0 || f.Snapshot().Generation != 1 {
		t.Fatal("rollback failure")
	}
	if got, err := callWork(f); err != nil || got != "healthy" {
		t.Fatal(got, err)
	}
	incompatible := emptyDef()
	incompatible.TypeID = "work"
	incompatible.Version = "incompatible"
	i := register(t, f, incompatible)
	_, err = f.Apply(context.Background(), forest.ChangePlan{BaseGeneration: 1, Operations: []forest.Operation{forest.Replace("work", i, false)}})
	if !errors.Is(err, forest.ErrContractMismatch) || f.Snapshot().Generation != 1 {
		t.Fatal(err)
	}
}

func TestExclusiveReplacementAndChildrenRequireExplicitSafety(t *testing.T) {
	f := newForest(t)
	d := emptyDef()
	d.HotSwappable = false
	unsafe := register(t, f, d)
	apply(t, f, forest.AddRoot("exclusive", unsafe))
	d.Version = "2"
	d.HotSwappable = true
	safe := register(t, f, d)
	_, err := f.Apply(context.Background(), forest.ChangePlan{BaseGeneration: 1, Operations: []forest.Operation{forest.Replace("exclusive", safe, false)}})
	if !errors.Is(err, forest.ErrUnsupportedHotReplace) {
		t.Fatal(err)
	}
	apply(t, f, forest.AddRoot("parent", safe), forest.Nest("parent", "child", safe))
	d.Version = "3"
	next := register(t, f, d)
	_, err = f.Apply(context.Background(), forest.ChangePlan{BaseGeneration: 2, Operations: []forest.Operation{forest.Replace("parent", next, false)}})
	if !errors.Is(err, forest.ErrUnsupportedHotReplace) {
		t.Fatal(err)
	}
	child := f.Snapshot().Trees["child"].InstanceID
	apply(t, f, forest.Replace("parent", next, true))
	if f.Snapshot().Trees["child"].InstanceID != child {
		t.Fatal("child instance replaced")
	}
}

func TestDrainDeadlineDoesNotReleaseOwner(t *testing.T) {
	f := forest.New(forest.Options{DrainTimeout: 20 * time.Millisecond})
	entered, release := make(chan struct{}), make(chan struct{})
	var ownerStops atomic.Int32
	parent := emptyDef()
	parent.TypeID = "parent"
	parent.Build = func(context.Context, *forest.BuildEnv) (forest.Lifecycle, error) {
		return forest.Hooks{OnStop: func(context.Context) error { ownerStops.Add(1); return nil }}, nil
	}
	child := emptyDef()
	child.TypeID = "child"
	child.Build = func(context.Context, *forest.BuildEnv) (forest.Lifecycle, error) {
		return forest.Hooks{OnStop: func(ctx context.Context) error { close(entered); <-release; return nil }}, nil
	}
	p := register(t, f, parent)
	c := register(t, f, child)
	apply(t, f, forest.AddRoot("parent", p), forest.Nest("parent", "child", c))
	apply(t, f, forest.RemoveSubtree("parent"))
	receive(t, entered)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := f.Wait(ctx); !errors.Is(err, forest.ErrDrainTimeout) {
		t.Fatal(err)
	}
	if ownerStops.Load() != 0 {
		t.Fatal("owner released while child Stop was still running")
	}
	close(release)
	// Wait may still report the recorded timeout until late Stop completes.
	deadline := time.After(3 * time.Second)
	for ownerStops.Load() == 0 {
		select {
		case <-deadline:
			t.Fatal("owner never released")
		default:
			time.Sleep(time.Millisecond)
		}
	}
	if err := f.Close(ctx); err != nil {
		t.Fatal(err)
	}
}
