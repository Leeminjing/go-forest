package topology

import (
	"context"
	"errors"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Leeminjing/go-forest/forest"
)

func TestHistoricalRollbackIsANewCommitAndPinsOldScope(t *testing.T) {
	ctx := context.Background()
	var builds atomic.Int32
	catalog, m := numberCatalog(t, &builds)
	v2 := catalog.entries[m.Trees[0].Definition]
	v2.Definition.Version = "2"
	v2.ImplementationDigest = strings.Repeat("d", 64)
	v2.Definition.Build = func(_ context.Context, env *forest.BuildEnv) (forest.Lifecycle, error) {
		builds.Add(1)
		return forest.Hooks{}, forest.Provide(env, "number", value(9))
	}
	catalog.entries[v2.Definition.Ref()] = v2
	next := detached(m)
	next.Trees[0].Definition = v2.Definition.Ref()
	next.Trees[0].ImplementationDigest = v2.ImplementationDigest
	s, e := Open(t.TempDir(), Options{})
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
	defer c.Close(ctx)
	_, e = c.Restore(ctx, Request{ID: "boot", Expected: s.Inspect().Position, Approval: "recreate"})
	if e != nil {
		t.Fatal(e)
	}
	preview, e := c.Preview(next)
	if e != nil {
		t.Fatal(e)
	}
	if preview.Mode != "reconfigure" || len(preview.RebuildTrees) != 1 || len(preview.Diff.ChangedTrees) != 1 {
		t.Fatalf("preview: %+v", preview)
	}
	changed, e := c.Reconfigure(ctx, Request{ID: "change", Expected: preview.Expected, Approval: "replace full graph"}, preview)
	if e != nil {
		t.Fatal(e)
	}
	if changed.Generation != 2 || changed.Revision == seed.Revision {
		t.Fatalf("change %+v", changed)
	}
	if e = c.OpenAdmission(ctx); e != nil {
		t.Fatal(e)
	}
	entered, release, done := make(chan struct{}), make(chan struct{}), make(chan error, 1)
	var released atomic.Bool
	defer func() {
		if released.CompareAndSwap(false, true) {
			close(release)
		}
	}()
	go func() {
		done <- c.Invoke(ctx, func(call context.Context) error {
			g, e := f.Generation(call)
			if e != nil {
				return e
			}
			if g != 2 {
				return errors.New("old generation not 2")
			}
			return forest.Use[number](call, f, "number", "number", func(n number) error {
				if n.Number() != 9 {
					return errors.New("old value")
				}
				close(entered)
				<-release
				g, e = f.Generation(call)
				if e != nil {
					return e
				}
				if g != 2 || n.Number() != 9 {
					return errors.New("old scope changed")
				}
				return nil
			})
		})
	}()
	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("old call never entered")
	}
	rollback, e := c.PreviewRollback(seed.Revision)
	if e != nil {
		t.Fatal(e)
	}
	rolled, e := c.Rollback(ctx, Request{ID: "rollback", Expected: rollback.Expected, Approval: "return to historic structure"}, rollback)
	if e != nil {
		if released.CompareAndSwap(false, true) {
			close(release)
		}
		<-done
		t.Fatal(e)
	}
	if rolled.Generation != 3 || rolled.Revision <= changed.Revision || rolled.Revision == seed.Revision {
		t.Fatalf("rollback rewound versions %+v", rolled)
	}
	if e = c.OpenAdmission(ctx); e != nil {
		if released.CompareAndSwap(false, true) {
			close(release)
		}
		<-done
		t.Fatal(e)
	}
	e = c.Invoke(ctx, func(call context.Context) error {
		return forest.Use[number](call, f, "number", "number", func(n number) error {
			if n.Number() != 7 {
				return errors.New("new value not historical")
			}
			return nil
		})
	})
	if e != nil {
		t.Error(e)
	}
	if released.CompareAndSwap(false, true) {
		close(release)
	}
	if e = <-done; e != nil {
		t.Fatal(e)
	}
	equal, e := c.Preview(m)
	if e != nil {
		t.Fatal(e)
	}
	noop, e := c.Reconfigure(ctx, Request{ID: "noop", Expected: equal.Expected, Approval: "same target"}, equal)
	if e != nil || !noop.Noop || noop.Generation != 3 || noop.Revision != rolled.Revision {
		t.Fatalf("noop %+v %v", noop, e)
	}
	if builds.Load() != 3 {
		t.Fatalf("builds %d", builds.Load())
	}
}
