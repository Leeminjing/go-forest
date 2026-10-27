package topology

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Leeminjing/go-forest/forest"
)

func TestRollbackKeepsNativeFilesAndDoesNotUndoBusinessEffects(t *testing.T) {
	ctx := context.Background()
	directory := t.TempDir()
	p := &probes{}
	var registrations []Registration
	for _, version := range []string{"1", "2"} {
		d := p.definition(directory, version)
		registrations = append(registrations, Registration{Definition: d, ContractKey: "resource/1", ImplementationDigest: strings.Repeat("e", 64)})
	}
	catalog, _ := NewCatalog(registrations)
	m := Empty()
	r := registrations[0]
	m.Trees = append(m.Trees, Tree{ID: "resource", Definition: r.Definition.Ref(), ContractKey: r.ContractKey, ImplementationDigest: r.ImplementationDigest})
	c, _, f, seed := managed(t, catalog, m)
	target := detached(m)
	target.Trees[0].Definition.Version = "2"
	preview, e := c.Preview(target)
	if e != nil {
		t.Fatal(e)
	}
	_, e = c.Reconfigure(ctx, Request{ID: "v2", Expected: preview.Expected, Approval: "replace"}, preview)
	if e != nil {
		t.Fatal(e)
	}
	if e = c.OpenAdmission(ctx); e != nil {
		t.Fatal(e)
	}
	entered := make(chan string, 1)
	release := make(chan struct{})
	var once sync.Once
	defer once.Do(func() { close(release) })
	done := make(chan error, 1)
	receiptPath := filepath.Join(directory, "business-receipt")
	go func() {
		done <- c.Invoke(ctx, func(call context.Context) error {
			return forest.Use[fileCapability](call, f, "resource", "file", func(v fileCapability) error {
				marker, e := v.Read(call)
				if e != nil {
					return e
				}
				effect, e := os.OpenFile(receiptPath, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
				if e != nil {
					return e
				}
				_, write := effect.WriteString("logical-effect:stable-1")
				e = errors.Join(write, effect.Sync(), effect.Close())
				if e != nil {
					return e
				}
				entered <- marker
				<-release
				generation, e := f.Generation(call)
				if e != nil {
					return e
				}
				again, e := v.Read(call)
				if e != nil || again != marker || generation != 2 {
					return fmt.Errorf("pinned resource changed: generation=%d error=%v", generation, e)
				}
				return nil
			})
		})
	}()
	var old string
	select {
	case old = <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("call failed to enter")
	}
	rollback, e := c.PreviewRollback(seed.Revision)
	if e != nil {
		t.Fatal(e)
	}
	_, e = c.Rollback(ctx, Request{ID: "back", Expected: rollback.Expected, Approval: "rollback topology only"}, rollback)
	if e != nil {
		t.Fatal(e)
	}
	select {
	case <-p.get(old).closed:
		t.Fatal("old resource closed while normal work entered")
	default:
	}
	deadline, cancel := context.WithTimeout(ctx, 20*time.Millisecond)
	defer cancel()
	if e = f.Wait(deadline); !errors.Is(e, forest.ErrDrainTimeout) {
		t.Fatalf("expected occupied history: %v", e)
	}
	if e = c.OpenAdmission(ctx); e != nil {
		t.Fatal(e)
	}
	var current string
	e = c.Invoke(ctx, func(call context.Context) error {
		return forest.Use[fileCapability](call, f, "resource", "file", func(v fileCapability) error { var err error; current, err = v.Read(call); return err })
	})
	if e != nil || !strings.HasPrefix(current, "1:") || current == old {
		t.Fatalf("new physical value %q: %v", current, e)
	}
	effect, e := os.ReadFile(receiptPath)
	if e != nil || string(effect) != "logical-effect:stable-1" {
		t.Fatal("topology rollback changed an external effect")
	}
	once.Do(func() { close(release) })
	if e = <-done; e != nil {
		t.Fatal(e)
	}
	if e = f.Wait(ctx); e != nil {
		t.Fatal(e)
	}
	select {
	case <-p.get(old).closed:
	default:
		t.Fatal("old physical file not closed after actual join")
	}
}

func TestConcurrentChangesCannotOverwriteTheSameHistoryPosition(t *testing.T) {
	var builds atomic.Int32
	catalog, m := numberCatalog(t, &builds)
	c, s, f, _ := managed(t, catalog, m)
	first, second := detached(m), detached(m)
	a, b := m.Trees[0], m.Trees[0]
	a.ID = "a"
	b.ID = "b"
	first.Trees = append(first.Trees, a)
	second.Trees = append(second.Trees, b)
	p1, e := c.Preview(first)
	if e != nil {
		t.Fatal(e)
	}
	p2, e := c.Preview(second)
	if e != nil {
		t.Fatal(e)
	}
	start := make(chan struct{})
	results := make(chan error, 2)
	for i, p := range []Proposal{p1, p2} {
		go func(i int, p Proposal) {
			<-start
			_, e := c.Reconfigure(context.Background(), Request{ID: fmt.Sprintf("writer-%d", i), Expected: p.Expected, Approval: "common base"}, p)
			results <- e
		}(i, p)
	}
	close(start)
	success, conflict := 0, 0
	for i := 0; i < 2; i++ {
		e := <-results
		if e == nil {
			success++
		} else if errors.Is(e, ErrHistoryConflict) {
			conflict++
		} else {
			t.Fatal(e)
		}
	}
	if success != 1 || conflict != 1 || f.Snapshot().Generation != 2 || len(s.History()) != 2 || len(f.Snapshot().Trees) != 2 {
		t.Fatalf("lost update: success=%d conflict=%d snapshot=%+v", success, conflict, f.Snapshot())
	}
}

func TestPreparingCandidateIsObservableAndCancellationIsNotCleanup(t *testing.T) {
	var builds atomic.Int32
	catalog, m := numberCatalog(t, &builds)
	r := catalog.entries[m.Trees[0].Definition]
	entered, release, stopped := make(chan struct{}), make(chan struct{}), make(chan struct{})
	var once sync.Once
	defer once.Do(func() { close(release) })
	r.Definition.Build = func(_ context.Context, env *forest.BuildEnv) (forest.Lifecycle, error) {
		builds.Add(1)
		e := forest.Provide(env, "number", value(7))
		return forest.Hooks{OnStart: func(ctx context.Context) error { close(entered); <-ctx.Done(); <-release; return ctx.Err() }, OnStop: func(context.Context) error { close(stopped); return nil }}, e
	}
	catalog.entries[r.Definition.Ref()] = r
	s, e := Open(t.TempDir(), Options{})
	if e != nil {
		t.Fatal(e)
	}
	_, e = s.Seed(context.Background(), Request{ID: "seed", Expected: s.Inspect().Position, Approval: "initial"}, m)
	if e != nil {
		t.Fatal(e)
	}
	f := forest.New(forest.Options{})
	c, e := NewController(f, s, catalog)
	if e != nil {
		t.Fatal(e)
	}
	defer func() {
		once.Do(func() { close(release) })
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := c.Close(ctx); err != nil {
			t.Error(err)
		}
	}()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() {
		_, e := c.Restore(ctx, Request{ID: "boot", Expected: s.Inspect().Position, Approval: "recreate"})
		done <- e
	}()
	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("prepare did not enter")
	}
	inspected := make(chan Status, 1)
	go func() { inspected <- c.Status() }()
	select {
	case status := <-inspected:
		if status.Storage.Pending == nil || status.Runtime.Generation != 0 || len(status.Runtime.Trees) != 0 || status.Admission {
			t.Fatalf("candidate exposed %+v", status)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("preparation blocked public inspection")
	}
	cancel()
	select {
	case <-stopped:
		t.Fatal("cancel released resource before Start exited")
	default:
	}
	select {
	case <-done:
		t.Fatal("Apply returned before actual preparation exit")
	default:
	}
	once.Do(func() { close(release) })
	if e = <-done; !errors.Is(e, context.Canceled) {
		t.Fatalf("cancel propagation: %v", e)
	}
	if e = f.Wait(context.Background()); e != nil {
		t.Fatal(e)
	}
	select {
	case <-stopped:
	default:
		t.Fatal("failed candidate did not actually stop")
	}
	if f.Snapshot().Generation != 0 || s.Inspect().Pending != nil {
		t.Fatal("failed candidate became topology")
	}
}
