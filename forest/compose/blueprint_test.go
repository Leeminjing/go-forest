package compose_test

import (
	"errors"
	"testing"

	"github.com/Leeminjing/go-forest/forest"
	"github.com/Leeminjing/go-forest/forest/compose"
)

func ref(name string) forest.DefinitionRef { return forest.DefinitionRef{TypeID: name, Version: "1"} }
func mustBlueprint(t *testing.T, spec compose.Spec) *compose.Blueprint {
	t.Helper()
	b, err := compose.New(spec)
	if err != nil {
		t.Fatal(err)
	}
	return b
}

func TestInstantiateRecursiveBlueprintNamesAndPlans(t *testing.T) {
	inner := mustBlueprint(t, compose.Spec{Root: ref("inner"), Trees: []compose.Tree{{ID: "model", Definition: ref("model")}}})
	trees := []compose.Tree{{ID: "adapter", Definition: ref("adapter")}, {ID: "engine", Composite: inner}, {ID: "owned", Parent: "adapter", Definition: ref("owned")}}
	bindings := []forest.Link{{ID: "output", Consumer: "", Port: "output", Provider: "adapter", Capability: "output"}, {ID: "engine", Consumer: "adapter", Port: "engine", Provider: "engine", Capability: "run"}}
	b := mustBlueprint(t, compose.Spec{Root: ref("root"), Trees: trees, Bindings: bindings})
	trees[0].ID = "mutated"
	bindings[0].Port = "mutated"
	// Embedding captures the child template, rather than retaining the caller's
	// replaceable Blueprint variable as part of the parent's declaration.
	*inner = compose.Blueprint{}
	instance, err := b.Instantiate("alpha", "")
	if err != nil {
		t.Fatal(err)
	}
	for path, want := range map[string]forest.TreeID{"": "alpha", "adapter": "alpha/adapter", "engine": "alpha/engine", "engine/model": "alpha/engine/model", "owned": "alpha/owned"} {
		got, err := instance.Tree(path)
		if err != nil || got != want {
			t.Fatalf("path %q: got=%q err=%v", path, got, err)
		}
	}
	if got, err := instance.Link("output"); err != nil || got != "alpha/@output" {
		t.Fatal(got, err)
	}
	plan := instance.Plan(7)
	if plan.BaseGeneration != 7 || len(plan.Operations) != 7 {
		t.Fatalf("plan: %+v", plan)
	}
	if plan.Operations[0].Kind != forest.OpAddRoot || plan.Operations[5].Kind != forest.OpBindNest || plan.Operations[6].Kind != forest.OpLink {
		t.Fatalf("classification: %+v", plan.Operations)
	}
	if plan.Operations[5].Binding.Port != "output" {
		t.Fatal("mutable blueprint bindings")
	}
	plan.Operations[0].Tree = "corrupted"
	if instance.Plan(7).Operations[0].Tree != "alpha" {
		t.Fatal("mutable instance plan")
	}
	other, err := b.Instantiate("beta", "existing-parent")
	if err != nil {
		t.Fatal(err)
	}
	if op := other.Plan(0).Operations[0]; op.Kind != forest.OpNest || op.Parent != "existing-parent" || op.Tree != "beta" {
		t.Fatal(op)
	}
	if _, err := instance.Tree("missing"); !errors.Is(err, forest.ErrNotFound) {
		t.Fatal(err)
	}
	if _, err := instance.Link("missing"); !errors.Is(err, forest.ErrNotFound) {
		t.Fatal(err)
	}
}

func TestBlueprintRejectsInvalidStructureAndBoundaryCrossing(t *testing.T) {
	inner := mustBlueprint(t, compose.Spec{Root: ref("inner")})
	for _, tc := range []struct {
		name string
		spec compose.Spec
		want error
	}{
		{"missing-root", compose.Spec{}, compose.ErrInvalidBlueprint},
		{"duplicate-name", compose.Spec{Root: ref("root"), Trees: []compose.Tree{{ID: "a", Definition: ref("leaf")}, {ID: "a", Definition: ref("leaf")}}}, compose.ErrInvalidBlueprint},
		{"ambiguous-node", compose.Spec{Root: ref("root"), Trees: []compose.Tree{{ID: "a", Definition: ref("leaf"), Composite: inner}}}, compose.ErrInvalidBlueprint},
		{"bad-name", compose.Spec{Root: ref("root"), Trees: []compose.Tree{{ID: "a/b", Definition: ref("leaf")}}}, compose.ErrInvalidName},
		{"missing-parent", compose.Spec{Root: ref("root"), Trees: []compose.Tree{{ID: "a", Parent: "missing", Definition: ref("leaf")}}}, compose.ErrInvalidBlueprint},
		{"cycle", compose.Spec{Root: ref("root"), Trees: []compose.Tree{{ID: "a", Parent: "b", Definition: ref("leaf")}, {ID: "b", Parent: "a", Definition: ref("leaf")}}}, forest.ErrNestCycle},
		{"nested-parent", compose.Spec{Root: ref("root"), Trees: []compose.Tree{{ID: "inner", Composite: inner}, {ID: "a", Parent: "inner", Definition: ref("leaf")}}}, compose.ErrBoundary},
		{"nested-endpoint", compose.Spec{Root: ref("root"), Trees: []compose.Tree{{ID: "inner", Composite: inner}}, Bindings: []forest.Link{{ID: "cross", Port: "p", Provider: "inner/hidden", Capability: "c"}}}, compose.ErrBoundary},
		{"missing-endpoint", compose.Spec{Root: ref("root"), Bindings: []forest.Link{{ID: "bad", Port: "p", Provider: "missing", Capability: "c"}}}, compose.ErrInvalidBlueprint},
		{"empty-port", compose.Spec{Root: ref("root"), Trees: []compose.Tree{{ID: "leaf", Definition: ref("leaf")}}, Bindings: []forest.Link{{ID: "bad", Provider: "leaf", Capability: "c"}}}, compose.ErrInvalidBlueprint},
		{"duplicate-link", compose.Spec{Root: ref("root"), Trees: []compose.Tree{{ID: "leaf", Definition: ref("leaf")}}, Bindings: []forest.Link{{ID: "same", Port: "p", Provider: "leaf", Capability: "c"}, {ID: "same", Port: "p", Provider: "leaf", Capability: "c"}}}, compose.ErrInvalidBlueprint},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := compose.New(tc.spec); !errors.Is(err, tc.want) {
				t.Fatalf("got %v, want %v", err, tc.want)
			}
		})
	}
	b := mustBlueprint(t, compose.Spec{Root: ref("root")})
	for _, id := range []forest.TreeID{"", "a/b", ".", ".."} {
		if _, err := b.Instantiate(id, ""); !errors.Is(err, compose.ErrInvalidName) {
			t.Fatalf("root %q: %v", id, err)
		}
	}
	var zero compose.Blueprint
	if _, err := zero.Instantiate("root", ""); !errors.Is(err, compose.ErrInvalidBlueprint) {
		t.Fatal(err)
	}
}
