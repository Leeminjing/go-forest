package topologymodel_test

import (
	m "github.com/Leeminjing/go-forest/tests/internal/topologymodel"
	"reflect"
	"testing"
)

func TestAtomicNestingAndRejectedCycle(t *testing.T) {
	model := m.New([]m.Definition{{ID: "safe", Version: "1", Hot: true, Move: true, Keep: true, Export: true}})
	state, issues := model.Next(m.Plan{Ops: []m.Op{{Kind: "add-root", Tree: "a", Definition: "safe"}, {Kind: "nest", Tree: "b", Parent: "a", Definition: "safe"}}})
	if len(issues) != 0 || state.Generation != 1 || state.Nodes["b"].Parent != "a" {
		t.Fatal(state, issues)
	}
	model.State = state
	after, issues := model.Next(m.Plan{Base: 1, Ops: []m.Op{{Kind: "move", Tree: "a", Parent: "b"}}})
	if len(issues) == 0 || after.Generation != 1 || after.Nodes["a"].Parent != "" {
		t.Fatal(after, issues)
	}
	same, issues := model.Next(m.Plan{Base: 1, Ops: []m.Op{{Kind: "move", Tree: "b", Parent: "a"}}})
	if len(issues) != 0 || same.Generation != 2 || same.Nodes["b"].Instance != state.Nodes["b"].Instance {
		t.Fatal(same, issues)
	}
}

func TestBindingsOrderRetentionAndAtomicRepair(t *testing.T) {
	provider := m.Definition{ID: "p", Version: "1", Hot: true, Move: true, Keep: true, Export: true, Caps: []m.Capability{{ID: "value", Type: "value", Public: true, Down: true, Up: true}}}
	consumer := provider
	consumer.ID = "c"
	consumer.Ports = []m.Port{{ID: "input", Type: "value", Many: true, Phase: m.Construction}}
	model := m.New([]m.Definition{provider, consumer})
	link := func(id, target string) m.Op {
		return m.Op{Kind: "link", Binding: m.Binding{ID: id, Consumer: "c", Port: "input", Provider: target, Capability: "value"}}
	}
	first, issues := model.Next(m.Plan{Ops: []m.Op{{Kind: "add-root", Tree: "p1", Definition: "p"}, {Kind: "add-root", Tree: "p2", Definition: "p"}, {Kind: "add-root", Tree: "c", Definition: "c"}, link("a", "p1"), link("b", "p2")}})
	if len(issues) != 0 {
		t.Fatal(issues)
	}
	model.State = first
	ops := []m.Op{{Kind: "unlink", Binding: m.Binding{ID: "a"}}, link("a", "p1")}
	failed, issues := model.Next(m.Plan{Base: 1, Ops: ops})
	if len(issues) == 0 || !reflect.DeepEqual(first, failed) {
		t.Fatal(failed, issues)
	}
	repaired, issues := model.Next(m.Plan{Base: 1, Ops: append(ops, m.Op{Kind: "replace", Tree: "c", Definition: "c"})})
	if len(issues) != 0 || repaired.Bindings["a"].Order != 3 || repaired.Bindings["b"].Order != 2 || repaired.Nodes["c"].Instance == first.Nodes["c"].Instance {
		t.Fatal(repaired, issues)
	}
}

func TestDisjointAddsCommuteAndMoveRoundTripRetainsIdentity(t *testing.T) {
	defs := []m.Definition{{ID: "safe", Version: "1", Hot: true, Move: true, Keep: true, Export: true}}
	a, b := m.Op{Kind: "add-root", Tree: "a", Definition: "safe"}, m.Op{Kind: "add-root", Tree: "b", Definition: "safe"}
	left, right := m.New(defs), m.New(defs)
	x, xi := left.Next(m.Plan{Ops: []m.Op{a, b}})
	y, yi := right.Next(m.Plan{Ops: []m.Op{b, a}})
	if len(xi) > 0 || len(yi) > 0 {
		t.Fatal(xi, yi)
	}
	for id, n := range x.Nodes {
		n.Instance = 0
		x.Nodes[id] = n
	}
	for id, n := range y.Nodes {
		n.Instance = 0
		y.Nodes[id] = n
	}
	if !reflect.DeepEqual(x, y) {
		t.Fatal("disjoint root additions do not commute", x, y)
	}
	original, issues := left.Next(m.Plan{Ops: []m.Op{a, b}})
	if len(issues) > 0 {
		t.Fatal(issues)
	}
	left.State = original
	moved, issues := left.Next(m.Plan{Base: 1, Ops: []m.Op{{Kind: "move", Tree: "a", Parent: "b"}}})
	if len(issues) > 0 {
		t.Fatal(issues)
	}
	left.State = moved
	restored, issues := left.Next(m.Plan{Base: 2, Ops: []m.Op{{Kind: "unnest", Tree: "a"}}})
	if len(issues) > 0 || restored.Generation != 3 || !reflect.DeepEqual(restored.Nodes, original.Nodes) {
		t.Fatal(restored, issues)
	}
}
