package topology

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/Leeminjing/go-forest/forest"
)

func TestPreviewPublishesTargetDigestAndRejectsTamperedClaim(t *testing.T) {
	catalog, m := orderedFixture(t, forest.Runtime)
	c, s, _, _ := managed(t, catalog, m)
	target := detached(m)
	target.Trees[0].Definition.Version = "2"
	p, e := c.Preview(target)
	if e != nil {
		t.Fatal(e)
	}
	encoded, _ := json.Marshal(p)
	var fields map[string]json.RawMessage
	_ = json.Unmarshal(encoded, &fields)
	expected, _ := Digest(target)
	var got string
	_ = json.Unmarshal(fields["target_digest"], &got)
	if got != expected {
		t.Fatalf("preview target digest = %q want %q", got, expected)
	}
	// Alter only an audit claim, retaining all actual target operations.
	fields["target_digest"] = json.RawMessage(`"` + strings.Repeat("b", 64) + `"`)
	altered, _ := json.Marshal(fields)
	var bad Proposal
	_ = json.Unmarshal(altered, &bad)
	position := s.Inspect().Position
	req := Request{ID: "digest", Expected: p.Expected, Approval: "exact target claim"}
	if _, e = c.Reconfigure(context.Background(), req, bad); e == nil || s.Inspect().Position != position {
		t.Fatal("tampered preview accepted", e)
	}
	if _, e = c.Reconfigure(context.Background(), req, p); e != nil {
		t.Fatal(e)
	}
	if _, e = c.Reconfigure(context.Background(), req, bad); !errors.Is(e, ErrRequestConflict) {
		t.Fatal("tampered retry lost identity conflict", e)
	}
}

func orderedFixture(t *testing.T, phase forest.Phase) (*Catalog, Manifest) {
	t.Helper()
	probes := &probes{}
	var calls atomic.Int32
	definitions := []forest.Definition{probes.definition(t.TempDir(), "1"), probes.definition(t.TempDir(), "2"), phaseReader(probes, phase, &calls)}
	registrations := []Registration{}
	for _, d := range definitions {
		registrations = append(registrations, Registration{Definition: d, ContractKey: d.TypeID, ImplementationDigest: strings.Repeat("a", 64)})
	}
	c, err := NewCatalog(registrations)
	if err != nil {
		t.Fatal(err)
	}
	m := Empty()
	for _, id := range []forest.TreeID{"a", "b", "reader"} {
		d := definitions[0]
		if id == "reader" {
			d = definitions[2]
		}
		m.Trees = append(m.Trees, Tree{ID: id, Definition: d.Ref(), ContractKey: d.TypeID, ImplementationDigest: strings.Repeat("a", 64)})
	}
	for i, id := range []forest.TreeID{"a", "b"} {
		m.Bindings = append(m.Bindings, Binding{ID: string(id), Consumer: "reader", Port: "files", Provider: id, Capability: "file", Kind: "link", Ordinal: uint64(i + 1)})
	}
	return c, m
}
func commitTarget(t *testing.T, c *Controller, target Manifest) Proposal {
	t.Helper()
	p, e := c.Preview(target)
	if e != nil {
		t.Fatal(e)
	}
	if _, e = c.Reconfigure(context.Background(), Request{ID: fmt.Sprintf("change-%d", p.Expected.Sequence), Expected: p.Expected, Approval: "explicit incremental change"}, p); e != nil {
		t.Fatal(e)
	}
	return p
}

func TestIncrementalRuntimeRebindAndManyOrder(t *testing.T) {
	catalog, m := orderedFixture(t, forest.Runtime)
	c, _, f, _ := managed(t, catalog, m)
	before := f.Snapshot()
	target := detached(m)
	target.Bindings[0].Provider = "b"
	p := commitTarget(t, c, target)
	if len(p.Plan.Operations) != 1 || p.Plan.Operations[0].Kind != forest.OpRebind || len(p.RebuildTrees) != 0 {
		t.Fatalf("not a runtime Rebind: %+v", p)
	}
	if f.Snapshot().Links["a"].Order != before.Links["a"].Order {
		t.Fatal("Rebind changed native order")
	}
	// Removing an earlier binding must not rebuild the shifted survivor.
	target.Bindings = target.Bindings[1:]
	target.Bindings[0].Ordinal = 1
	commitTarget(t, c, target)
	if f.Snapshot().Trees["reader"].InstanceID != before.Trees["reader"].InstanceID {
		t.Fatal("ordinal renumbering rebuilt runtime consumer")
	}
	target = detached(m)
	target.Bindings[0], target.Bindings[1] = target.Bindings[1], target.Bindings[0]
	target.Bindings[0].Ordinal = 1
	target.Bindings[1].Ordinal = 2
	commitTarget(t, c, target)
	if err := c.OpenAdmission(context.Background()); err != nil {
		t.Fatal(err)
	}
	var got []string
	err := c.Invoke(context.Background(), func(ctx context.Context) error {
		return forest.Use[readAll](ctx, f, "reader", "read", func(r readAll) error { var e error; got, e = r.ReadAll(ctx); return e })
	})
	if err != nil || len(got) != 2 || got[0] == got[1] {
		t.Fatal("real Many call failed", got, err)
	}
	var expected []string
	err = f.Invoke(context.Background(), func(ctx context.Context) error {
		for _, id := range []forest.TreeID{"b", "a"} {
			if e := forest.Use[fileCapability](ctx, f, id, "file", func(v fileCapability) error { s, e := v.Read(ctx); expected = append(expected, s); return e }); e != nil {
				return e
			}
		}
		return nil
	})
	if err != nil || !reflect.DeepEqual(got, expected) {
		t.Fatalf("Many order got %v want %v: %v", got, expected, err)
	}
	captured, _ := Capture(f.Snapshot(), catalog)
	normalized, _ := Normalize(target)
	if !reflect.DeepEqual(captured, normalized) {
		t.Fatal("target order not captured")
	}
	for id, old := range before.Trees {
		if f.Snapshot().Trees[id].InstanceID != old.InstanceID {
			t.Fatal("Runtime changes rebuilt", id)
		}
	}
}

func TestIncrementalPhaseConsumersRebuildForProviderAndPhysicalOrderChanges(t *testing.T) {
	for _, phase := range []forest.Phase{forest.Construction, forest.Startup} {
		t.Run(fmt.Sprint(phase), func(t *testing.T) {
			catalog, m := orderedFixture(t, phase)
			c, _, f, _ := managed(t, catalog, m)
			old := f.Snapshot()
			target := detached(m)
			target.Trees[0].Definition.Version = "2"
			p := commitTarget(t, c, target)
			if !reflect.DeepEqual(p.RebuildTrees, []forest.TreeID{"a", "reader"}) {
				t.Fatalf("missing dependent consumer %+v", p.RebuildTrees)
			}
			if f.Snapshot().Trees["b"].InstanceID != old.Trees["b"].InstanceID {
				t.Fatal("unrelated provider rebuilt")
			}
			originalOrder := f.Snapshot().Links["b"].Order
			target.Bindings = target.Bindings[1:]
			target.Bindings[0].Ordinal = 1
			commitTarget(t, c, target) // phase binding removal must rebuild reader
			reader := f.Snapshot().Trees["reader"].InstanceID
			// Add a different Runtime-free target binding BEFORE the survivor. Both
			// bindings will get new native order, forcing phase consumer reconstruction.
			target.Bindings = append([]Binding{{ID: "new", Consumer: "reader", Port: "files", Provider: "a", Capability: "file", Kind: "link", Ordinal: 1}}, target.Bindings...)
			target.Bindings[1].Ordinal = 2
			p = commitTarget(t, c, target)
			if !reflect.DeepEqual(p.RebuildTrees, []forest.TreeID{"reader"}) || f.Snapshot().Trees["reader"].InstanceID == reader || f.Snapshot().Links["b"].Order == originalOrder {
				t.Fatal("physical order change not rebuilt", p)
			}
		})
	}
}

func TestIncrementalBlockedClosureIsObservableAndDoesNotWrite(t *testing.T) {
	catalog, m := orderedFixture(t, forest.Construction)
	reader := catalog.entries[m.Trees[2].Definition]
	reader.Definition.HotSwappable = false
	catalog.entries[reader.Definition.Ref()] = reader
	c, s, f, _ := managed(t, catalog, m)
	before := s.Inspect().Position
	target := detached(m)
	target.Trees[0].Definition.Version = "2"
	p, e := c.Preview(target)
	if !errors.Is(e, ErrPlanBlocked) || !errors.Is(e, forest.ErrUnsupportedHotReplace) || len(p.Planning.Impact.Blockers) != 1 || !reflect.DeepEqual(p.RebuildTrees, []forest.TreeID{"a", "reader"}) {
		t.Fatal("missing complete blocker report", p, e)
	}
	if _, e = c.Reconfigure(context.Background(), Request{ID: "blocked", Expected: p.Expected, Approval: "must not bypass"}, p); !errors.Is(e, ErrPlanBlocked) {
		t.Fatal(e)
	}
	if s.Inspect().Position != before || f.Snapshot().Generation != 1 {
		t.Fatal("blocked plan changed storage/runtime")
	}
}

func TestIncrementalNestPreservationExpansionAndMove(t *testing.T) {
	for _, preserve := range []bool{true, false} {
		t.Run(fmt.Sprint(preserve), func(t *testing.T) {
			probes := &probes{}
			d1 := probes.definition(t.TempDir(), "1")
			d2 := probes.definition(t.TempDir(), "2")
			d2.PreserveChildren = preserve
			regs := []Registration{}
			for _, d := range []forest.Definition{d1, d2} {
				regs = append(regs, Registration{Definition: d, ContractKey: "file", ImplementationDigest: strings.Repeat("a", 64)})
			}
			catalog, e := NewCatalog(regs)
			if e != nil {
				t.Fatal(e)
			}
			m := Empty()
			for _, v := range []struct{ id, parent forest.TreeID }{{"parent", ""}, {"child", "parent"}, {"grand", "child"}} {
				m.Trees = append(m.Trees, Tree{ID: v.id, ParentID: v.parent, Definition: d1.Ref(), ContractKey: "file", ImplementationDigest: strings.Repeat("a", 64)})
			}
			c, _, f, _ := managed(t, catalog, m)
			old := f.Snapshot()
			target := detached(m)
			target.Trees[0].Definition = d2.Ref()
			p := commitTarget(t, c, target)
			want := []forest.TreeID{"child", "grand", "parent"}
			if preserve {
				want = []forest.TreeID{"parent"}
			}
			if !reflect.DeepEqual(p.RebuildTrees, want) {
				t.Fatal("wrong ownership closure", p.RebuildTrees, want)
			}
			for _, id := range []forest.TreeID{"child", "grand"} {
				if (f.Snapshot().Trees[id].InstanceID == old.Trees[id].InstanceID) != preserve {
					t.Fatal("child preservation mismatch", id)
				}
			}
			// Move a surviving subtree away before deleting only the former parent.
			target.Trees[0].ID = "new-parent"
			target.Trees[0].Definition = d1.Ref()
			target.Trees[1].ParentID = "new-parent"
			old = f.Snapshot()
			p = commitTarget(t, c, target)
			for _, op := range p.Plan.Operations {
				if op.Kind == forest.OpRemoveSubtree {
					t.Fatal("used implicit subtree removal")
				}
			}
			for _, id := range []forest.TreeID{"child", "grand"} {
				if f.Snapshot().Trees[id].InstanceID != old.Trees[id].InstanceID {
					t.Fatal("safe move rebuilt", id)
				}
			}
		})
	}
}

func TestIncrementalPhaseDependencyClosurePropagatesThroughConsumers(t *testing.T) {
	for _, phase := range []forest.Phase{forest.Construction, forest.Startup} {
		t.Run(fmt.Sprint(phase), func(t *testing.T) {
			catalog, m := orderedFixture(t, phase)
			relay := forest.Definition{TypeID: "relay", Version: "1", HotSwappable: true, Ports: []forest.PortDescriptor{forest.Port[readAll]("input", forest.Required(), forest.During(phase))}, Capabilities: []forest.CapabilityDescriptor{forest.Capability[readAll]("read", forest.Public)}}
			relay.Build = func(ctx context.Context, env *forest.BuildEnv) (forest.Lifecycle, error) {
				ref, e := forest.Reference[readAll](env, "input")
				if e != nil {
					return nil, e
				}
				read := readFunc(func(call context.Context) ([]string, error) {
					var out []string
					e := ref.Use(call, func(r readAll) error { var err error; out, err = r.ReadAll(call); return err })
					return out, e
				})
				check := func(call context.Context) error { _, e := read(call); return e }
				if phase == forest.Construction {
					if e = check(ctx); e != nil {
						return nil, e
					}
				}
				return forest.Hooks{OnStart: func(ctx context.Context) error {
					if phase == forest.Startup {
						return check(ctx)
					}
					return nil
				}}, forest.Provide(env, "read", read)
			}
			r := Registration{Definition: relay, ContractKey: "relay", ImplementationDigest: strings.Repeat("a", 64)}
			catalog.entries[relay.Ref()] = r
			m.Trees = append(m.Trees, Tree{ID: "relay", Definition: relay.Ref(), ContractKey: r.ContractKey, ImplementationDigest: r.ImplementationDigest})
			m.Bindings = append(m.Bindings, Binding{ID: "relay", Consumer: "relay", Port: "input", Provider: "reader", Capability: "read", Kind: "link", Ordinal: 3})
			c, _, _, _ := managed(t, catalog, m)
			target := detached(m)
			target.Trees[0].Definition.Version = "2"
			p := commitTarget(t, c, target)
			if !reflect.DeepEqual(p.RebuildTrees, []forest.TreeID{"a", "reader", "relay"}) {
				t.Fatalf("incomplete fixed point: %+v", p.RebuildTrees)
			}
			found := false
			for _, reason := range p.Planning.Impact.Reasons {
				if reason.Tree == "relay" && reason.Source == "reader" && reason.Code == "phase-provider-rebuilt" {
					found = true
				}
			}
			if !found {
				t.Fatal("lost causal edge")
			}
		})
	}
}

func TestIncrementalOptionalPhaseBindingAdditionAndRemoval(t *testing.T) {
	for _, phase := range []forest.Phase{forest.Construction, forest.Startup} {
		t.Run(fmt.Sprint(phase), func(t *testing.T) {
			catalog, m := orderedFixture(t, phase)
			reader := catalog.entries[m.Trees[2].Definition]
			reader.Definition.Ports = []forest.PortDescriptor{forest.Port[fileCapability]("files", forest.Many(), forest.During(phase))}
			reader.Definition.Build = func(ctx context.Context, env *forest.BuildEnv) (forest.Lifecycle, error) {
				ref, e := forest.Reference[fileCapability](env, "files")
				if e != nil {
					return nil, e
				}
				read := readFunc(func(call context.Context) ([]string, error) {
					out := []string{}
					e := ref.UseAll(call, func(values []fileCapability) error {
						for _, v := range values {
							s, e := v.Read(call)
							if e != nil {
								return e
							}
							out = append(out, s)
						}
						return nil
					})
					if errors.Is(e, forest.ErrBindingUnavailable) {
						return out, nil
					}
					return out, e
				})
				check := func(ctx context.Context) error { _, e := read(ctx); return e }
				if phase == forest.Construction {
					if e = check(ctx); e != nil {
						return nil, e
					}
				}
				return forest.Hooks{OnStart: func(ctx context.Context) error {
					if phase == forest.Startup {
						return check(ctx)
					}
					return nil
				}}, forest.Provide(env, "read", read)
			}
			catalog.entries[reader.Definition.Ref()] = reader
			original := detached(m)
			m.Bindings = []Binding{}
			c, _, f, _ := managed(t, catalog, m)
			old := f.Snapshot().Trees["reader"].InstanceID
			p := commitTarget(t, c, original)
			if !reflect.DeepEqual(p.RebuildTrees, []forest.TreeID{"reader"}) || old == f.Snapshot().Trees["reader"].InstanceID {
				t.Fatal("optional addition retained stale consumer")
			}
			p = commitTarget(t, c, m)
			if !reflect.DeepEqual(p.RebuildTrees, []forest.TreeID{"reader"}) {
				t.Fatal("optional removal retained stale consumer")
			}
		})
	}
}

func TestIncrementalBindNestClassificationAndUnsafeMove(t *testing.T) {
	for _, safe := range []bool{true, false} {
		t.Run(fmt.Sprint(safe), func(t *testing.T) {
			catalog, m := orderedFixture(t, forest.Runtime)
			r := catalog.entries[m.Trees[2].Definition]
			r.Definition.Reparentable = safe
			catalog.entries[r.Definition.Ref()] = r
			c, s, f, _ := managed(t, catalog, m)
			old := f.Snapshot()
			position := s.Inspect().Position
			target := detached(m)
			target.Trees[2].ParentID = "a"
			target.Bindings[0].Kind = "nest"
			p, e := c.Preview(target)
			if !safe {
				if !errors.Is(e, forest.ErrReparentUnsupported) || !errors.Is(e, ErrPlanBlocked) || s.Inspect().Position != position || f.Snapshot().Generation != 1 {
					t.Fatal("unsafe move was not blocked", p, e)
				}
				return
			}
			if e != nil {
				t.Fatal(e)
			}
			commitTarget(t, c, target)
			after := f.Snapshot()
			if after.Trees["reader"].InstanceID != old.Trees["reader"].InstanceID || len(after.NestBindings) != 1 || after.NestBindings["a"].Provider != "a" || len(after.Links) != 1 {
				t.Fatal("bad Nest conversion", after)
			}
		})
	}
}

func TestIncrementalReplaceRetainsUnchangedNonHotResource(t *testing.T) {
	var builds atomic.Int32
	catalog, m := numberCatalog(t, &builds)
	resource := Registration{Definition: forest.Definition{TypeID: "resource", Version: "1", Build: func(context.Context, *forest.BuildEnv) (forest.Lifecycle, error) { return forest.Hooks{}, nil }}, ContractKey: "resource", ImplementationDigest: strings.Repeat("b", 64)}
	catalog.entries[resource.Definition.Ref()] = resource
	m.Trees = append(m.Trees, Tree{ID: "resource", Definition: resource.Definition.Ref(), ContractKey: resource.ContractKey, ImplementationDigest: resource.ImplementationDigest})
	second := catalog.entries[m.Trees[0].Definition]
	second.Definition.Version = "2"
	second.ImplementationDigest = strings.Repeat("d", 64)
	catalog.entries[second.Definition.Ref()] = second
	c, s, f, _ := managed(t, catalog, m)
	before := f.Snapshot()
	target := detached(m)
	target.Trees[0].Definition = second.Definition.Ref()
	target.Trees[0].ImplementationDigest = second.ImplementationDigest
	p, e := c.Preview(target)
	if e != nil {
		t.Fatal(e)
	}
	if !reflect.DeepEqual(p.RebuildTrees, []forest.TreeID{"number"}) {
		t.Fatalf("unnecessary rebuilds: %v", p.RebuildTrees)
	}
	if len(p.Plan.Operations) != 1 || p.Plan.Operations[0].Kind != forest.OpReplace {
		t.Fatalf("not an incremental Replace: %+v", p.Plan)
	}
	position := s.Inspect().Position
	if again, e := c.Preview(target); e != nil || !reflect.DeepEqual(again, p) || s.Inspect().Position != position {
		t.Fatal("preview is not pure and deterministic", e)
	}
	if _, e = c.Reconfigure(context.Background(), Request{ID: "replace", Expected: p.Expected, Approval: "local change"}, p); e != nil {
		t.Fatal(e)
	}
	after := f.Snapshot()
	if before.Trees["resource"].InstanceID != after.Trees["resource"].InstanceID || before.Trees["number"].InstanceID == after.Trees["number"].InstanceID {
		t.Fatal("wrong instance retention")
	}
}
