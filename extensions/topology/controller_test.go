package topology

import (
	"context"
	"errors"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/Leeminjing/go-forest/forest"
)

func numberCatalog(t *testing.T, builds *atomic.Int32) (*Catalog, Manifest) {
	t.Helper()
	d := forest.Definition{TypeID: "number", Version: "1", HotSwappable: true, Capabilities: []forest.CapabilityDescriptor{forest.Capability[number]("number", forest.Public)}, Build: func(_ context.Context, e *forest.BuildEnv) (forest.Lifecycle, error) {
		builds.Add(1)
		return forest.Hooks{}, forest.Provide(e, "number", value(7))
	}}
	r := Registration{Definition: d, ContractKey: "number/1", ImplementationDigest: strings.Repeat("a", 64)}
	c, e := NewCatalog([]Registration{r})
	if e != nil {
		t.Fatal(e)
	}
	m := Empty()
	m.Trees = append(m.Trees, Tree{ID: "number", Definition: d.Ref(), ContractKey: r.ContractKey, ImplementationDigest: r.ImplementationDigest})
	return c, m
}
func TestColdRestoreUsesFreshForestAndExplicitAdmission(t *testing.T) {
	ctx := context.Background()
	var builds atomic.Int32
	c, m := numberCatalog(t, &builds)
	dir := t.TempDir()
	s, e := Open(dir, Options{})
	if e != nil {
		t.Fatal(e)
	}
	_, e = s.Seed(ctx, Request{ID: "seed", Expected: s.Inspect().Position, Approval: "initial"}, m)
	if e != nil {
		t.Fatal(e)
	}
	var oldSession string
	for attempt := 0; attempt < 2; attempt++ {
		if attempt > 0 {
			s, e = Open(dir, Options{})
			if e != nil {
				t.Fatal(e)
			}
		}
		f := forest.New(forest.Options{})
		controller, e := NewController(f, s, c)
		if e != nil {
			t.Fatal(e)
		}
		if e = controller.Invoke(ctx, func(context.Context) error { return nil }); !errors.Is(e, ErrAdmissionClosed) {
			t.Fatalf("premature admission: %v", e)
		}
		request := Request{ID: []string{"boot-1", "boot-2"}[attempt], Expected: s.Inspect().Position, Approval: "old process exited; resource recreation allowed"}
		receipt, e := controller.Restore(ctx, request)
		if e != nil {
			t.Fatal(e)
		}
		if receipt.Generation != 1 || receipt.HistoricalOnly || receipt.SessionID == oldSession {
			t.Fatalf("receipt: %+v", receipt)
		}
		oldSession = receipt.SessionID
		again, e := controller.Restore(ctx, request)
		if e != nil || again.Generation != 1 {
			t.Fatalf("retry %+v %v", again, e)
		}
		if e = s.Close(); !errors.Is(e, ErrStoreBusy) {
			t.Fatalf("live store close: %v", e)
		}
		if e = controller.OpenAdmission(ctx); e != nil {
			t.Fatal(e)
		}
		got := 0
		e = controller.Invoke(ctx, func(call context.Context) error {
			return forest.Use[number](call, f, "number", "number", func(n number) error { got = n.Number(); return nil })
		})
		if e != nil || got != 7 {
			t.Fatalf("call %d %v", got, e)
		}
		if e = controller.Close(ctx); e != nil {
			t.Fatal(e)
		}
	}
	if builds.Load() != 2 {
		t.Fatalf("builds=%d", builds.Load())
	}
}

func TestMissingCatalogDoesNotBuildOrPublish(t *testing.T) {
	var builds atomic.Int32
	c, m := numberCatalog(t, &builds)
	_ = c
	s, e := Open(t.TempDir(), Options{})
	if e != nil {
		t.Fatal(e)
	}
	defer s.Close()
	_, e = s.Seed(context.Background(), Request{ID: "seed", Expected: s.Inspect().Position, Approval: "initial"}, m)
	if e != nil {
		t.Fatal(e)
	}
	empty, _ := NewCatalog(nil)
	f := forest.New(forest.Options{})
	controller, e := NewController(f, s, empty)
	if e != nil {
		t.Fatal(e)
	}
	_, e = controller.Restore(context.Background(), Request{ID: "boot", Expected: s.Inspect().Position, Approval: "allow recreation"})
	if !errors.Is(e, ErrDefinitionUnavailable) {
		t.Fatalf("missing definition: %v", e)
	}
	if f.Snapshot().Generation != 0 || len(f.Snapshot().Trees) != 0 || builds.Load() != 0 {
		t.Fatal("failed restore changed graph")
	}
	if e = controller.Close(context.Background()); e != nil {
		t.Fatal(e)
	}
}

func TestHistoricalReceiptDoesNotRestoreOrAdmitANewForest(t *testing.T) {
	ctx := context.Background()
	var builds atomic.Int32
	catalog, m := numberCatalog(t, &builds)
	directory := t.TempDir()
	s, e := Open(directory, Options{})
	if e != nil {
		t.Fatal(e)
	}
	_, e = s.Seed(ctx, Request{ID: "seed", Expected: s.Inspect().Position, Approval: "initial"}, m)
	if e != nil {
		t.Fatal(e)
	}
	c, e := NewController(forest.New(forest.Options{}), s, catalog)
	if e != nil {
		t.Fatal(e)
	}
	original := Request{ID: "boot", Expected: s.Inspect().Position, Approval: "recreate"}
	_, e = c.Restore(ctx, original)
	if e != nil {
		t.Fatal(e)
	}
	if e = c.Close(ctx); e != nil {
		t.Fatal(e)
	}
	s, e = Open(directory, Options{})
	if e != nil {
		t.Fatal(e)
	}
	if !s.Inspect().LastObserved.HistoricalOnly {
		t.Fatal("closed-session observation presented as live")
	}
	fresh := forest.New(forest.Options{})
	c, e = NewController(fresh, s, catalog)
	if e != nil {
		t.Fatal(e)
	}
	defer c.Close(ctx)
	receipt, e := c.Restore(ctx, original)
	if e != nil || !receipt.HistoricalOnly || fresh.Snapshot().Generation != 0 || builds.Load() != 1 {
		t.Fatalf("historical retry became execution: %+v %v", receipt, e)
	}
	if e = c.OpenAdmission(ctx); !errors.Is(e, ErrAdmissionClosed) {
		t.Fatalf("historical receipt opened new ingress: %v", e)
	}
	_, e = c.Restore(ctx, Request{ID: "new-attempt", Expected: s.Inspect().Position, Approval: "new process resource recreation"})
	if e != nil || builds.Load() != 2 {
		t.Fatalf("new attempt: %v builds=%d", e, builds.Load())
	}
}

func TestEmptyRestoreAndNoopPreserveNativeGenerationZero(t *testing.T) {
	catalog, _ := NewCatalog(nil)
	c, s, f, seed := managed(t, catalog, Empty())
	if f.Snapshot().Generation != 0 {
		t.Fatal("empty restore invented a native version")
	}
	proposal, e := c.Preview(Empty())
	if e != nil {
		t.Fatal(e)
	}
	result, e := c.Reconfigure(context.Background(), Request{ID: "empty-noop", Expected: proposal.Expected, Approval: "identical empty target"}, proposal)
	if e != nil || !result.Noop || result.Generation != 0 || result.Revision != seed.Revision || len(s.History()) != 1 {
		t.Fatalf("empty no-op %+v %v", result, e)
	}
}
