package tests

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/Leeminjing/go-forest/forest"
)

func register(t *testing.T, f *forest.Forest, d forest.Definition) forest.DefinitionRef {
	t.Helper()
	if err := f.Register(d); err != nil {
		t.Fatal(err)
	}
	return d.Ref()
}
func apply(t *testing.T, f *forest.Forest, ops ...forest.Operation) {
	t.Helper()
	if _, err := f.Apply(context.Background(), forest.ChangePlan{BaseGeneration: f.Snapshot().Generation, Operations: ops}); err != nil {
		t.Fatal(err)
	}
}
func emptyDef() forest.Definition {
	return forest.Definition{TypeID: "branch", Version: "1", HotSwappable: true, Reparentable: true, PreserveChildren: true,
		Build: func(context.Context, *forest.BuildEnv) (forest.Lifecycle, error) { return forest.Hooks{}, nil }}
}
func newForest(t *testing.T) *forest.Forest {
	t.Helper()
	f := forest.New(forest.Options{})
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		if err := f.Close(ctx); err != nil {
			t.Error(err)
		}
	})
	return f
}

func TestEmptyForestAndDynamicNesting(t *testing.T) {
	f := newForest(t)
	if s := f.Snapshot(); s.Generation != 0 || len(s.Trees) != 0 || len(s.Links) != 0 {
		t.Fatalf("empty: %+v", s)
	}
	d := register(t, f, emptyDef())
	apply(t, f, forest.AddRoot("root", d), forest.Nest("root", "child", d), forest.Nest("child", "leaf", d))
	s := f.Snapshot()
	if s.Generation != 1 || s.Trees["leaf"].ParentID != "child" || s.Trees["child"].ParentID != "root" {
		t.Fatalf("nest: %+v", s)
	}
	_, err := f.Apply(context.Background(), forest.ChangePlan{BaseGeneration: 1, Operations: []forest.Operation{forest.Move("root", "leaf")}})
	if !errors.Is(err, forest.ErrNestCycle) || f.Snapshot().Generation != 1 {
		t.Fatalf("cycle: %v", err)
	}
	_, err = f.Apply(context.Background(), forest.ChangePlan{BaseGeneration: 1, Operations: []forest.Operation{forest.Remove("root")}})
	if !errors.Is(err, forest.ErrDependencyInUse) {
		t.Fatalf("remove parent: %v", err)
	}
	apply(t, f, forest.Unnest("leaf"), forest.RemoveSubtree("root"))
	if s := f.Snapshot(); len(s.Trees) != 1 || s.Trees["leaf"].ParentID != "" {
		t.Fatalf("remaining: %+v", s)
	}
	// Returned inspection maps are detached from runtime topology.
	s.Trees["injected"] = forest.TreeInfo{}
	if len(f.Snapshot().Trees) != 1 {
		t.Fatal("snapshot alias")
	}
}
