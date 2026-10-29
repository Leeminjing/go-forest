package consumer

import (
	"context"
	"encoding/json"
	"fmt"
	topology "github.com/Leeminjing/go-forest/extensions/topology"
	"github.com/Leeminjing/go-forest/forest"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
)

type number interface{ Number() int }
type value int

func (v value) Number() int { return int(v) }

type reader interface {
	Read(context.Context) (int, error)
}
type readFunc func(context.Context) (int, error)

func (r readFunc) Read(ctx context.Context) (int, error) { return r(ctx) }
func must(e error) {
	if e != nil {
		panic(e)
	}
}
func TestPublishedIncrementalResourcesAndColdRecovery(t *testing.T) {
	ctx := context.Background()
	dir, e := os.MkdirTemp("", "topology-v2-")
	must(e)
	defer os.RemoveAll(dir)
	// These are exact released v0.1.0 bytes, not a newly encoded legacy record.
	legacy, e := os.ReadFile("testdata/legacy-history/journal")
	must(e)
	oldDir := filepath.Join(dir, "legacy")
	must(os.Mkdir(oldDir, 0700))
	must(os.WriteFile(filepath.Join(oldDir, "journal"), legacy, 0600))
	old, e := topology.Open(oldDir, topology.Options{})
	must(e)
	if len(old.History()) != 3 || old.Inspect().Pending != nil {
		panic("old history not readable")
	}
	must(old.Close())
	live := filepath.Join(dir, "live")
	must(os.Mkdir(live, 0700))
	f := forest.New(forest.Options{})
	regs := []topology.Registration{}
	for _, v := range []string{"1", "2"} {
		v := v
		d := forest.Definition{TypeID: "number", Version: v, HotSwappable: true, Capabilities: []forest.CapabilityDescriptor{forest.Capability[number]("number", forest.Public)}, Build: func(_ context.Context, env *forest.BuildEnv) (forest.Lifecycle, error) {
			n := value(1)
			if v == "2" {
				n = 2
			}
			return forest.Hooks{}, forest.Provide(env, "number", n)
		}}
		regs = append(regs, topology.Registration{Definition: d, ContractKey: "number", ImplementationDigest: strings.Repeat("a", 64)})
	}
	stableBuilds := 0
	resource := forest.Definition{TypeID: "stable", Version: "1", Build: func(context.Context, *forest.BuildEnv) (forest.Lifecycle, error) {
		stableBuilds++
		return forest.Hooks{}, nil
	}}
	consumer := forest.Definition{TypeID: "consumer", Version: "1", Ports: []forest.PortDescriptor{forest.Port[number]("input", forest.Required())}, Capabilities: []forest.CapabilityDescriptor{forest.Capability[reader]("read", forest.Public)}, Build: func(_ context.Context, env *forest.BuildEnv) (forest.Lifecycle, error) {
		ref, e := forest.Reference[number](env, "input")
		if e != nil {
			return nil, e
		}
		r := readFunc(func(ctx context.Context) (int, error) {
			n := 0
			e := ref.Use(ctx, func(v number) error { n = v.Number(); return nil })
			return n, e
		})
		return forest.Hooks{}, forest.Provide(env, "read", r)
	}}
	for _, d := range []forest.Definition{resource, consumer} {
		regs = append(regs, topology.Registration{Definition: d, ContractKey: d.TypeID, ImplementationDigest: strings.Repeat("a", 64)})
	}
	catalog, e := topology.NewCatalog(regs)
	must(e)
	m := topology.Empty()
	for _, x := range []struct {
		id    forest.TreeID
		index int
	}{{"number", 0}, {"other", 1}, {"stable", 2}, {"consumer", 3}} {
		r := regs[x.index]
		m.Trees = append(m.Trees, topology.Tree{ID: x.id, Definition: r.Definition.Ref(), ContractKey: r.ContractKey, ImplementationDigest: r.ImplementationDigest})
	}
	m.Bindings = []topology.Binding{{ID: "input", Consumer: "consumer", Port: "input", Provider: "number", Capability: "number", Kind: "link", Ordinal: 1}}
	store, e := topology.Open(live, topology.Options{})
	must(e)
	seed, e := store.Seed(ctx, topology.Request{ID: "seed", Expected: store.Inspect().Position, Approval: "external resource owner"}, m)
	must(e)
	c, e := topology.NewController(f, store, catalog)
	must(e)
	boot, e := c.Restore(ctx, topology.Request{ID: "boot", Expected: store.Inspect().Position, Approval: "new physical objects"})
	must(e)
	before := f.Snapshot()
	values := []int{}
	gens := []uint64{boot.Generation}
	call := func() {
		must(c.OpenAdmission(ctx))
		must(c.Invoke(ctx, func(ctx context.Context) error {
			return forest.Use[reader](ctx, f, "consumer", "read", func(r reader) error { n, e := r.Read(ctx); values = append(values, n); return e })
		}))
	}
	check := func() {
		s := f.Snapshot()
		if s.Trees["stable"].InstanceID != before.Trees["stable"].InstanceID || s.Trees["consumer"].InstanceID != before.Trees["consumer"].InstanceID || stableBuilds != 1 {
			panic("unchanged resources reconstructed")
		}
	}
	call()
	target := store.Inspect().Desired.Manifest
	for i := range target.Trees {
		if target.Trees[i].ID == "number" {
			target.Trees[i].Definition.Version = "2"
		}
	}
	p, e := c.Preview(target)
	must(e)
	if p.Planning.Version != 2 || len(p.Plan.Operations) != 1 || p.Plan.Operations[0].Kind != forest.OpReplace {
		panic("not incremental")
	}
	req := topology.Request{ID: "replace", Expected: p.Expected, Approval: "one replacement"}
	r, e := c.Reconfigure(ctx, req, p)
	must(e)
	gens = append(gens, r.Generation)
	check()
	call()
	again, e := c.Reconfigure(ctx, req, p)
	must(e)
	if again.Generation != r.Generation {
		panic("duplicate changed receipt")
	}
	p, e = c.PreviewRollback(seed.Revision)
	must(e)
	r, e = c.Rollback(ctx, topology.Request{ID: "rollback", Expected: p.Expected, Approval: "new incremental commit"}, p)
	must(e)
	gens = append(gens, r.Generation)
	check()
	call()
	target = store.Inspect().Desired.Manifest
	target.Bindings[0].Provider = "other"
	p, e = c.Preview(target)
	must(e)
	if len(p.Plan.Operations) != 1 || p.Plan.Operations[0].Kind != forest.OpRebind {
		panic("not a Rebind")
	}
	r, e = c.Reconfigure(ctx, topology.Request{ID: "rebind", Expected: p.Expected, Approval: "runtime routing"}, p)
	must(e)
	gens = append(gens, r.Generation)
	check()
	call()
	session := r.SessionID
	must(c.Close(ctx))
	store, e = topology.Open(live, topology.Options{})
	must(e)
	f = forest.New(forest.Options{})
	c, e = topology.NewController(f, store, catalog)
	must(e)
	cold, e := c.Restore(ctx, topology.Request{ID: "restart", Expected: store.Inspect().Position, Approval: "previous controller joined"})
	must(e)
	call()
	must(c.Close(ctx))
	result := struct {
		Values         []int
		Generations    []uint64
		ColdGeneration uint64
		NewSession     bool
	}{values, gens, cold.Generation, cold.SessionID != session}
	out, e := json.Marshal(result)
	must(e)
	if !reflect.DeepEqual(values, []int{1, 2, 1, 2, 2}) || !reflect.DeepEqual(gens, []uint64{1, 2, 3, 4}) || cold.Generation != 1 || cold.SessionID == session || stableBuilds != 2 {
		t.Fatalf("wrong actual execution: %s stable builds=%d", out, stableBuilds)
	}
	fmt.Println(string(out))
}
