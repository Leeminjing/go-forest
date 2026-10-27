package topology

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/Leeminjing/go-forest/forest"
)

func TestUnsafeTargetsLeaveTheActiveTopologyUnchanged(t *testing.T) {
	for _, kind := range []string{"not-hot", "start-failed", "exclusive", "weakened-contract"} {
		t.Run(kind, func(t *testing.T) {
			directory := t.TempDir()
			var builds atomic.Int32
			catalog, m := numberCatalog(t, &builds)
			original := catalog.entries[m.Trees[0].Definition]
			second := original
			second.Definition.Version = "2"
			second.ImplementationDigest = strings.Repeat("f", 64)
			switch kind {
			case "not-hot":
				original.Definition.HotSwappable = false
				catalog.entries[original.Definition.Ref()] = original
			case "start-failed":
				second.Definition.Build = func(_ context.Context, env *forest.BuildEnv) (forest.Lifecycle, error) {
					return forest.Hooks{OnStart: func(context.Context) error { return errors.New("native initialization failed") }}, forest.Provide(env, "number", value(9))
				}
			case "weakened-contract":
				second.Definition.Capabilities = []forest.CapabilityDescriptor{forest.Capability[number]("number", 0)}
			case "exclusive":
				exclusive := filepath.Join(directory, "exclusive")
				factory := func(_ context.Context, env *forest.BuildEnv) (forest.Lifecycle, error) {
					file, e := os.OpenFile(exclusive, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
					if e != nil {
						return nil, e
					}
					return forest.Hooks{OnStop: func(context.Context) error { return errors.Join(file.Close(), os.Remove(exclusive)) }}, forest.Provide(env, "number", value(7))
				}
				original.Definition.Build = factory
				second.Definition.Build = factory
				catalog.entries[original.Definition.Ref()] = original
			}
			catalog.entries[second.Definition.Ref()] = second
			c, s, f, _ := managed(t, catalog, m)
			before, _ := Capture(f.Snapshot(), catalog)
			beforeHash, _ := Digest(before)
			target := detached(m)
			target.Trees[0].Definition = second.Definition.Ref()
			target.Trees[0].ImplementationDigest = second.ImplementationDigest
			p, e := c.Preview(target)
			if e != nil {
				t.Fatal(e)
			}
			_, e = c.Reconfigure(context.Background(), Request{ID: "unsafe", Expected: p.Expected, Approval: "native checks must reject"}, p)
			want := forest.ErrPrepareFailed
			if kind == "not-hot" {
				want = forest.ErrUnsupportedHotReplace
			}
			if kind == "weakened-contract" {
				want = forest.ErrContractMismatch
			}
			if !errors.Is(e, want) {
				t.Fatalf("%s rejection %v", kind, e)
			}
			after, _ := Capture(f.Snapshot(), catalog)
			afterHash, _ := Digest(after)
			if result := c.Status().Storage.LastResult; result == nil || result.Outcome != "failed" || result.OperationID != "unsafe" {
				t.Fatalf("failed attempt not inspectable: %+v", result)
			}
			if beforeHash != afterHash || f.Snapshot().Generation != 1 || len(s.History()) != 1 || s.Inspect().Pending != nil {
				t.Fatal("failed plan changed confirmed topology")
			}
			if e = c.OpenAdmission(context.Background()); e != nil {
				t.Fatal(e)
			}
			e = c.Invoke(context.Background(), func(ctx context.Context) error {
				return forest.Use[number](ctx, f, "number", "number", func(n number) error {
					if n.Number() != 7 {
						return errors.New("old forest was damaged")
					}
					return nil
				})
			})
			if e != nil {
				t.Fatal(e)
			}
		})
	}
}

func TestManifestAndRegistryRejectionsOccurBeforeConstruction(t *testing.T) {
	for _, kind := range []string{"missing-version", "identity", "cycle", "missing-parent", "invalid-link", "required-port", "phase-cycle"} {
		t.Run(kind, func(t *testing.T) {
			var builds atomic.Int32
			catalog, m := numberCatalog(t, &builds)
			switch kind {
			case "missing-version":
				m.Trees[0].Definition.Version = "missing"
			case "identity":
				m.Trees[0].ImplementationDigest = strings.Repeat("b", 64)
			case "cycle":
				m.Trees[0].ParentID = m.Trees[0].ID
			case "missing-parent":
				m.Trees[0].ParentID = "missing"
			case "invalid-link":
				m.Bindings = []Binding{{ID: "invalid", Consumer: "number", Port: "nonexistent", Provider: "number", Capability: "number", Kind: "link", Ordinal: 1}}
			case "required-port", "phase-cycle":
				r := catalog.entries[m.Trees[0].Definition]
				r.Definition.Ports = []forest.PortDescriptor{forest.Port[number]("dependency", forest.Required(), forest.During(forest.Construction))}
				catalog.entries[r.Definition.Ref()] = r
				if kind == "phase-cycle" {
					q := m.Trees[0]
					q.ID = "second"
					m.Trees = append(m.Trees, q)
					m.Bindings = []Binding{{ID: "a", Consumer: "number", Port: "dependency", Provider: "second", Capability: "number", Kind: "link", Ordinal: 1}, {ID: "b", Consumer: "second", Port: "dependency", Provider: "number", Capability: "number", Kind: "link", Ordinal: 2}}
				}
			}
			s, e := Open(t.TempDir(), Options{})
			if e != nil {
				t.Fatal(e)
			}
			_, e = s.Seed(context.Background(), Request{ID: "seed", Expected: s.Inspect().Position, Approval: "initial"}, m)
			if e != nil {
				if kind != "cycle" && kind != "missing-parent" {
					t.Fatal(e)
				}
				s.Close()
				return
			}
			f := forest.New(forest.Options{})
			c, e := NewController(f, s, catalog)
			if e != nil {
				t.Fatal(e)
			}
			defer c.Close(context.Background())
			_, e = c.Restore(context.Background(), Request{ID: "boot", Expected: s.Inspect().Position, Approval: "recreate"})
			if e == nil {
				t.Fatalf("accepted %s", kind)
			}
			if len(f.Snapshot().Trees) != 0 || f.Snapshot().Generation != 0 || builds.Load() != 0 {
				t.Fatal("invalid target constructed or published")
			}
		})
	}
}

func TestDetectedOutOfBandMutationStopsManagedWrites(t *testing.T) {
	var builds atomic.Int32
	catalog, m := numberCatalog(t, &builds)
	c, s, f, _ := managed(t, catalog, m)
	if e := c.OpenAdmission(context.Background()); e != nil {
		t.Fatal(e)
	}
	_, e := f.Apply(context.Background(), forest.ChangePlan{BaseGeneration: 1, Operations: []forest.Operation{forest.AddRoot("outside", m.Trees[0].Definition)}})
	if e != nil {
		t.Fatal(e)
	}
	position := s.Inspect().Position
	if _, e = c.Preview(m); !errors.Is(e, ErrManagedForestDiverged) {
		t.Fatalf("unmanaged graph accepted: %v", e)
	}
	if e = c.OpenAdmission(context.Background()); !errors.Is(e, ErrManagedForestDiverged) {
		t.Fatal(e)
	}
	if c.Status().Admission || s.Inspect().Position != position {
		t.Fatal("divergence fabricated new history")
	}
}
