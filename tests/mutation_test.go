package tests

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Leeminjing/go-forest/forest"
)

func TestAddRebindRemoveIsOneObservableVersion(t *testing.T) {
	f := newForest(t)
	c1 := register(t, f, clockDef("1", "old", forest.Public))
	p := register(t, f, printerDef(true))
	l := binding("l", "printer", "clock", "old", "now")
	apply(t, f, forest.AddRoot("old", c1), forest.AddRoot("printer", p), forest.Connect(l))
	preparing, proceed := make(chan struct{}), make(chan struct{})
	d2 := clockDef("2", "new", forest.Public)
	build := d2.Build
	d2.Build = func(ctx context.Context, env *forest.BuildEnv) (forest.Lifecycle, error) {
		_, err := build(ctx, env)
		return forest.Hooks{OnReady: func(context.Context) error { close(preparing); <-proceed; return nil }}, err
	}
	c2 := register(t, f, d2)
	l.Provider = "new"
	finished := make(chan error, 1)
	go func() {
		_, err := f.Apply(context.Background(), forest.ChangePlan{BaseGeneration: 1, Operations: []forest.Operation{forest.AddRoot("new", c2), forest.Rebind(l), forest.Remove("old")}})
		finished <- err
	}()
	receive(t, preparing)
	s := f.Snapshot()
	if s.Generation != 1 || s.Trees["old"].ID != "old" || s.Trees["new"].ID != "" || s.Links["l"].Provider != "old" {
		t.Fatal("candidate became visible")
	}
	if got := readPrint(t, f); got != "printed: old" {
		t.Fatal(got)
	}
	prepared := false
	for _, i := range s.Instances {
		if i.TreeID == "new" && i.State == forest.Preparing {
			prepared = true
		}
	}
	if !prepared {
		t.Fatal("preparing instance not observable")
	}
	close(proceed)
	if err := receive(t, finished); err != nil {
		t.Fatal(err)
	}
	if s := f.Snapshot(); s.Generation != 2 || len(s.Trees) != 2 || s.Trees["old"].ID != "" || s.Links["l"].Provider != "new" {
		t.Fatal(s)
	}
	if got := readPrint(t, f); got != "printed: new" {
		t.Fatal(got)
	}
}

func TestEventsConflictsGapsAndSnapshotRecovery(t *testing.T) {
	f := forest.New(forest.Options{EventCapacity: 2})
	defer f.Close(context.Background())
	d := register(t, f, emptyDef())
	apply(t, f, forest.AddRoot("a", d))
	watch := f.Watch(0)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	e, err := watch.Next(ctx)
	if err != nil || e.Sequence != 1 || e.Generation != 1 || e.Result != "success" {
		t.Fatal(e, err)
	}
	_, err = f.Apply(ctx, forest.ChangePlan{Operations: []forest.Operation{forest.AddRoot("b", d)}})
	if !errors.Is(err, forest.ErrGenerationConflict) {
		t.Fatal(err)
	}
	e, err = watch.Next(ctx)
	if err != nil || e.Sequence != 2 || e.Generation != 1 || e.Result != "conflict" {
		t.Fatal(e, err)
	}
	apply(t, f, forest.AddRoot("b", d))
	apply(t, f, forest.Remove("a"))
	if _, err = f.Watch(0).Next(ctx); !errors.Is(err, forest.ErrEventGap) {
		t.Fatal(err)
	}
	s := f.Snapshot()
	if s.Generation != 3 || s.EventSequence != 4 || len(s.Trees) != 1 {
		t.Fatal(s)
	}
	watch = f.Watch(s.EventSequence)
	apply(t, f, forest.AddRoot("c", d))
	e, err = watch.Next(ctx)
	if err != nil || e.Sequence != 5 || e.Generation != 4 {
		t.Fatal(e, err)
	}
	history := f.Events()
	history[0].Trees[0] = "corrupted"
	if f.Events()[0].Trees[0] == "corrupted" {
		t.Fatal("mutable event history")
	}
}

func TestMoveRequiresSafeResourceTransferAndValidNewBindings(t *testing.T) {
	f := newForest(t)
	root := emptyDef()
	root.TypeID = "root"
	r := register(t, f, root)
	unsafe := emptyDef()
	unsafe.TypeID = "unsafe"
	unsafe.Reparentable = false
	u := register(t, f, unsafe)
	apply(t, f, forest.AddRoot("a", r), forest.AddRoot("b", r), forest.Nest("a", "child", u))
	_, err := f.Apply(context.Background(), forest.ChangePlan{BaseGeneration: 1, Operations: []forest.Operation{forest.Move("child", "b")}})
	if !errors.Is(err, forest.ErrReparentUnsupported) || f.Snapshot().Trees["child"].ParentID != "a" {
		t.Fatal(err)
	}
	apply(t, f, forest.Nest("a", "safe", r))
	id := f.Snapshot().Trees["safe"].InstanceID
	apply(t, f, forest.Move("safe", "b"))
	if s := f.Snapshot(); s.Trees["safe"].ParentID != "b" || s.Trees["safe"].InstanceID != id {
		t.Fatal(s)
	}
}

func TestRuntimeLinkCycleAllowedButConstructionCycleRejected(t *testing.T) {
	for _, phase := range []forest.Phase{forest.Runtime, forest.Construction, forest.Startup} {
		t.Run(string(rune('0'+phase)), func(t *testing.T) {
			f := newForest(t)
			d := clockDef("1", "value", forest.Public)
			d.Ports = []forest.PortDescriptor{forest.Port[clock]("peer", forest.Required(), forest.During(phase))}
			ref := register(t, f, d)
			_, err := f.Apply(context.Background(), forest.ChangePlan{Operations: []forest.Operation{forest.AddRoot("a", ref), forest.AddRoot("b", ref), forest.Connect(binding("ab", "a", "peer", "b", "now")), forest.Connect(binding("ba", "b", "peer", "a", "now"))}})
			if phase == forest.Runtime {
				if err != nil {
					t.Fatal(err)
				}
			} else if !errors.Is(err, forest.ErrPrepareFailed) || f.Snapshot().Generation != 0 {
				t.Fatal(err)
			}
		})
	}
}

func TestConstructionResourceCannotRebindWithoutReplacingConsumer(t *testing.T) {
	f := newForest(t)
	c1 := register(t, f, clockDef("1", "first", forest.Public))
	c2 := register(t, f, clockDef("2", "second", forest.Public))
	var prepared atomic.Bool
	pd := printerDef(true)
	pd.Ports[0].Phase = forest.Construction
	original := pd.Build
	pd.Build = func(ctx context.Context, env *forest.BuildEnv) (forest.Lifecycle, error) {
		ref, err := forest.Reference[clock](env, "clock")
		if err != nil {
			return nil, err
		}
		if err = ref.Use(ctx, func(c clock) error { prepared.Store(c.Now(ctx) == "first"); return nil }); err != nil {
			return nil, err
		}
		return original(ctx, env)
	}
	p := register(t, f, pd)
	l := binding("l", "printer", "clock", "first", "now")
	apply(t, f, forest.AddRoot("first", c1), forest.AddRoot("second", c2), forest.AddRoot("printer", p), forest.Connect(l))
	if !prepared.Load() {
		t.Fatal("constructor prerequisite unavailable")
	}
	l.Provider = "second"
	_, err := f.Apply(context.Background(), forest.ChangePlan{BaseGeneration: 1, Operations: []forest.Operation{forest.Rebind(l)}})
	if !errors.Is(err, forest.ErrDependencyInUse) {
		t.Fatal(err)
	}
	apply(t, f, forest.Replace("printer", p, false), forest.Rebind(l), forest.Remove("first"))
	if got := readPrint(t, f); got != "printed: second" {
		t.Fatal(got)
	}
}

func TestSemanticContractsAndReadinessFailure(t *testing.T) {
	f := newForest(t)
	c := clockDef("1", "ok", forest.Public)
	c.Capabilities[0].Contract = forest.Contract{ID: "clock/v1", Version: 2}
	provider := register(t, f, c)
	p := printerDef(true)
	p.Ports[0].ContractID = "clock/v1"
	p.Ports[0].MinVersion = 3
	consumer := register(t, f, p)
	_, err := f.Apply(context.Background(), forest.ChangePlan{Operations: []forest.Operation{forest.AddRoot("clock", provider), forest.AddRoot("printer", consumer), forest.Connect(binding("l", "printer", "clock", "clock", "now"))}})
	if !errors.Is(err, forest.ErrContractMismatch) {
		t.Fatal(err)
	}
	bad := emptyDef()
	bad.TypeID = "not-ready"
	var stops atomic.Int32
	bad.Build = func(context.Context, *forest.BuildEnv) (forest.Lifecycle, error) {
		return forest.Hooks{OnReady: func(context.Context) error { return errors.New("not ready") }, OnStop: func(context.Context) error { stops.Add(1); return nil }}, nil
	}
	d := register(t, f, bad)
	_, err = f.Apply(context.Background(), forest.ChangePlan{Operations: []forest.Operation{forest.AddRoot("bad", d)}})
	if !errors.Is(err, forest.ErrPrepareFailed) || f.Snapshot().Generation != 0 {
		t.Fatal(err)
	}
	wait(t, f)
	if stops.Load() != 1 {
		t.Fatal("readiness failure resources leaked")
	}
}

func TestInvalidDefinitionsAndRequestsReturnErrors(t *testing.T) {
	f := newForest(t)
	bad := emptyDef()
	bad.Ports = []forest.PortDescriptor{forest.Port[int]("not-interface")}
	if err := f.Register(bad); !errors.Is(err, forest.ErrInvalidDefinition) {
		t.Fatal(err)
	}
	d := register(t, f, emptyDef())
	for _, op := range []forest.Operation{forest.AddRoot("", d), forest.Connect(forest.Link{}), forest.Operation{Kind: "unknown"}, forest.Move("missing", "parent")} {
		if _, err := f.Apply(context.Background(), forest.ChangePlan{Operations: []forest.Operation{op}}); err == nil {
			t.Fatal("invalid request succeeded")
		}
	}
	apply(t, f, forest.AddRoot("tree", d))
	s := f.Snapshot()
	if gen, err := f.Apply(context.Background(), forest.ChangePlan{BaseGeneration: s.Generation}); err != nil || gen != s.Generation {
		t.Fatal(gen, err)
	}
}
