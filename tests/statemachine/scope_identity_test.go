package statemachine

import (
	"context"
	"errors"
	"testing"

	"github.com/Leeminjing/go-forest/forest"
	m "github.com/Leeminjing/go-forest/tests/internal/topologymodel"
)

func TestModelClosedScopeAndReusedTreeIdentity(t *testing.T) {
	h := newHarness(t, Catalog(), []string{"p", "c"})
	h.step(m.Plan{Ops: []m.Op{root("p", "provider-v1"), root("c", "runtime-required"), link("l", "c", "in", "p", false)}})
	var escaped context.Context
	if err := h.f.Invoke(context.Background(), func(c context.Context) error { escaped = c; return nil }); err != nil {
		t.Fatal(err)
	}
	if err := h.f.Invoke(escaped, func(context.Context) error { return nil }); !errors.Is(err, forest.ErrScopeClosed) {
		t.Fatal(err)
	}
	consumer := h.nativeInstances[h.model.State.Nodes["c"].Instance]
	oldRef := consumer.values["in"] // a declared Ref is copyable; no raw capability escapes.
	h.step(m.Plan{Base: 1, Ops: []m.Op{{Kind: "remove", Tree: "c"}}})
	h.step(m.Plan{Base: 2, Ops: []m.Op{root("c", "runtime-required"), link("l", "c", "in", "p", false)}})
	err := h.f.Invoke(context.Background(), func(c context.Context) error { return oldRef.Use(c, func(value) error { return nil }) })
	if !errors.Is(err, forest.ErrBindingUnavailable) {
		t.Fatal("old consumer Ref reached replacement", err)
	}
	snapshot := h.f.Snapshot()
	n := snapshot.Trees["c"]
	n.ParentID = "invented"
	n.Ports[0].ID = "invented"
	snapshot.Trees["c"] = n
	delete(snapshot.Links, "l")
	if err = h.check(); err != nil {
		t.Fatal("detached inspection changed actual graph", err)
	}
}
