package topology

import (
	"context"
	"errors"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/Leeminjing/go-forest/forest"
)

func TestColdHistoricalSelectionRequiresAnExitedController(t *testing.T) {
	ctx := context.Background()
	var builds atomic.Int32
	catalog, m := numberCatalog(t, &builds)
	first := catalog.entries[m.Trees[0].Definition]
	first.Definition.Capabilities[0].Contract = forest.Contract{ID: "number", Version: 1}
	catalog.entries[first.Definition.Ref()] = first
	second := first
	second.Definition.Version = "2"
	second.Definition.Capabilities = append([]forest.CapabilityDescriptor(nil), first.Definition.Capabilities...)
	second.Definition.Capabilities[0].Contract.Version = 2
	second.ImplementationDigest = strings.Repeat("b", 64)
	catalog.entries[second.Definition.Ref()] = second
	directory := t.TempDir()
	s, e := Open(directory, Options{})
	if e != nil {
		t.Fatal(e)
	}
	seed, e := s.Seed(ctx, Request{ID: "seed", Expected: s.Inspect().Position, Approval: "initial"}, m)
	if e != nil {
		t.Fatal(e)
	}
	f := forest.New(forest.Options{})
	c, e := NewController(f, s, catalog)
	if e != nil {
		t.Fatal(e)
	}
	_, e = c.Restore(ctx, Request{ID: "boot", Expected: s.Inspect().Position, Approval: "recreate"})
	if e != nil {
		t.Fatal(e)
	}
	target := detached(m)
	target.Trees[0].Definition = second.Definition.Ref()
	target.Trees[0].ImplementationDigest = second.ImplementationDigest
	p, e := c.Preview(target)
	if e != nil {
		t.Fatal(e)
	}
	_, e = c.Reconfigure(ctx, Request{ID: "upgrade", Expected: p.Expected, Approval: "stronger capability"}, p)
	if e != nil {
		t.Fatal(e)
	}
	p, e = c.PreviewRollback(seed.Revision)
	if e != nil {
		t.Fatal(e)
	}
	_, e = c.Rollback(ctx, Request{ID: "hot-back", Expected: p.Expected, Approval: "must reject weakened promise"}, p)
	if !errors.Is(e, forest.ErrContractMismatch) {
		t.Fatalf("unsafe hot rollback: %v", e)
	}
	if _, e = s.SelectHistorical(ctx, Request{ID: "cold", Expected: s.Inspect().Position, Approval: "not exited"}, seed.Revision); !errors.Is(e, ErrStoreBusy) {
		t.Fatalf("selected while active: %v", e)
	}
	if e = c.Close(ctx); e != nil {
		t.Fatal(e)
	}
	s, e = Open(directory, Options{})
	if e != nil {
		t.Fatal(e)
	}
	selected, e := s.SelectHistorical(ctx, Request{ID: "cold", Expected: s.Inspect().Position, Approval: "old controller and resources actually exited; accept downtime"}, seed.Revision)
	if e != nil {
		t.Fatal(e)
	}
	if selected.Revision <= seed.Revision || selected.Outcome != "selected" {
		t.Fatal("cold choice pretended to rewind history")
	}
	fresh := forest.New(forest.Options{})
	c, e = NewController(fresh, s, catalog)
	if e != nil {
		t.Fatal(e)
	}
	result, e := c.Restore(ctx, Request{ID: "new-boot", Expected: s.Inspect().Position, Approval: "allow new physical objects with old contract"})
	if e != nil {
		t.Fatal(e)
	}
	if result.Generation != 1 || result.Revision != selected.Revision || fresh.Snapshot().Trees["number"].Version != "1" {
		t.Fatalf("cold restore: %+v", result)
	}
	if e = c.Close(ctx); e != nil {
		t.Fatal(e)
	}
}
