// Package performance supplies synthetic, real Forest workloads, not an
// alternate graph implementation. No framework or business I/O is on the read path.
package performance

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/Leeminjing/go-forest/extensions/topology"
	"github.com/Leeminjing/go-forest/forest"
)

type Config struct {
	N, E, D, Ports int
	Many           bool
}
type Value interface {
	Value(context.Context) (uint64, error)
}
type scalar struct{ tag uint64 }

//go:noinline
func (s *scalar) Value(context.Context) (uint64, error) { return s.tag, nil }

//go:noinline
func Dispatch(ctx context.Context, v Value) (uint64, error) { return v.Value(ctx) }

type Runner interface {
	Read(context.Context) (uint64, error)
}
type reader struct {
	ref  forest.Ref[Value]
	many bool
}

func (r *reader) Read(ctx context.Context) (uint64, error) {
	var sum uint64
	if r.many {
		err := r.ref.UseAll(ctx, func(values []Value) error {
			for _, v := range values {
				n, e := v.Value(ctx)
				if e != nil {
					return e
				}
				sum += n
			}
			return nil
		})
		return sum, err
	}
	err := r.ref.Use(ctx, func(v Value) error { var e error; sum, e = v.Value(ctx); return e })
	return sum, err
}

type Fixture struct {
	F          *forest.Forest
	C          *topology.Controller
	Store      *topology.Store
	Catalog    *topology.Catalog
	Config     Config
	Expected   uint64
	generation uint64
	version    string
	rebound    bool
}

func Provider(i int) forest.TreeID { return forest.TreeID(fmt.Sprintf("p%05d", i)) }
func New(cfg Config, directory string) (*Fixture, error) {
	if cfg.N < 4 || cfg.E < 0 || cfg.D < 0 || cfg.D > cfg.N-2 || cfg.E < cfg.D || cfg.E-cfg.D > cfg.N-2 || (!cfg.Many && cfg.D > 1) || cfg.Ports < 1 {
		return nil, errors.New("invalid performance dimensions")
	}
	f := forest.New(forest.Options{})
	x := &Fixture{F: f, Config: cfg, version: "1"}
	entries := []topology.Registration{}
	for _, version := range []string{"1", "2"} {
		tag := uint64(1)
		if version == "2" {
			tag = 2
		}
		d := forest.Definition{TypeID: "perf.value", Version: version, HotSwappable: true, Capabilities: []forest.CapabilityDescriptor{forest.Capability[Value]("value", forest.Public)}, Build: func(_ context.Context, e *forest.BuildEnv) (forest.Lifecycle, error) {
			return forest.Hooks{}, forest.Provide(e, "value", &scalar{tag})
		}}
		entries = append(entries, topology.Registration{Definition: d, ContractKey: "value", ImplementationDigest: strings.Repeat("a", 64)})
	}
	ports := []forest.PortDescriptor{}
	for i := 1; i < cfg.Ports; i++ {
		ports = append(ports, forest.Port[Value](forest.PortID(fmt.Sprintf("unused-%d", i))))
	}
	opts := []forest.PortOption{}
	if cfg.D > 0 {
		opts = append(opts, forest.Required())
	}
	if cfg.Many {
		opts = append(opts, forest.Many())
	}
	ports = append(ports, forest.Port[Value]("input", opts...))
	consumer := forest.Definition{TypeID: "perf.consumer", Version: "1", Ports: ports, Capabilities: []forest.CapabilityDescriptor{forest.Capability[Runner]("read", forest.Public)}, Build: func(_ context.Context, e *forest.BuildEnv) (forest.Lifecycle, error) {
		ref, err := forest.Reference[Value](e, "input")
		if err != nil {
			return nil, err
		}
		return forest.Hooks{}, forest.Provide(e, "read", &reader{ref, cfg.Many})
	}}
	unrelated := forest.Definition{TypeID: "perf.unrelated", Version: "1", Ports: []forest.PortDescriptor{forest.Port[Value]("input", forest.Many())}, Build: func(context.Context, *forest.BuildEnv) (forest.Lifecycle, error) { return forest.Hooks{}, nil }}
	for _, d := range []forest.Definition{consumer, unrelated} {
		entries = append(entries, topology.Registration{Definition: d, ContractKey: d.TypeID, ImplementationDigest: strings.Repeat("a", 64)})
	}
	var err error
	x.Catalog, err = topology.NewCatalog(entries)
	if err != nil {
		return nil, err
	}
	for _, r := range entries {
		if err = f.Register(r.Definition); err != nil {
			return nil, err
		}
	}
	ops := []forest.Operation{forest.AddRoot("consumer", consumer.Ref()), forest.AddRoot("unrelated", unrelated.Ref())}
	for i := 0; i < cfg.N-2; i++ {
		version := "1"
		if i == 1 {
			version = "2"
		}
		ops = append(ops, forest.AddRoot(Provider(i), forest.DefinitionRef{TypeID: "perf.value", Version: version}))
	}
	for i := 0; i < cfg.D; i++ {
		ops = append(ops, forest.Connect(forest.Link{ID: fmt.Sprintf("target-%d", i), Consumer: "consumer", Port: "input", Provider: Provider(i), Capability: "value"}))
		x.Expected++
		if i == 1 {
			x.Expected++
		}
	}
	for i := 0; i < cfg.E-cfg.D; i++ {
		ops = append(ops, forest.Connect(forest.Link{ID: fmt.Sprintf("unrelated-%d", i), Consumer: "unrelated", Port: "input", Provider: Provider((i + 1) % (cfg.N - 2)), Capability: "value"}))
	}
	x.generation, err = f.Apply(context.Background(), forest.ChangePlan{Operations: ops})
	if err != nil {
		_ = f.Close(context.Background())
		return nil, err
	}
	snap := f.Snapshot()
	if len(snap.Trees) != cfg.N || len(snap.Links) != cfg.E {
		return nil, errors.New("actual fixture dimensions differ")
	}
	if directory != "" {
		m, e := topology.Capture(snap, x.Catalog)
		if e != nil {
			return nil, e
		}
		if e = f.Close(context.Background()); e != nil {
			return nil, e
		}
		x.Store, e = topology.Open(directory, topology.Options{})
		if e != nil {
			return nil, e
		}
		_, e = x.Store.Seed(context.Background(), topology.Request{ID: "seed", Expected: x.Store.Inspect().Position, Approval: "synthetic owned fixture"}, m)
		if e != nil {
			_ = x.Store.Close()
			return nil, e
		}
		x.F = forest.New(forest.Options{})
		x.C, e = topology.NewController(x.F, x.Store, x.Catalog)
		if e != nil {
			_ = x.Store.Close()
			return nil, e
		}
		_, e = x.C.Restore(context.Background(), topology.Request{ID: "restore", Expected: x.Store.Inspect().Position, Approval: "fresh synthetic fixture"})
		if e != nil {
			_ = x.C.Close(context.Background())
			return nil, e
		}
		if e = x.C.OpenAdmission(context.Background()); e != nil {
			return nil, e
		}
	}
	return x, nil
}
func (x *Fixture) Close(ctx context.Context) error {
	if x.C != nil {
		return x.C.Close(ctx)
	}
	return x.F.Close(ctx)
}
func (x *Fixture) Pinned(ctx context.Context, fn func(context.Context, Runner) error) error {
	return x.F.Invoke(ctx, func(call context.Context) error {
		return forest.Use[Runner](call, x.F, "consumer", "read", func(r Runner) error { return fn(call, r) })
	})
}
func (x *Fixture) Call(ctx context.Context) (uint64, error) {
	var out uint64
	invoke := x.F.Invoke
	if x.C != nil {
		invoke = x.C.Invoke
	}
	err := invoke(ctx, func(call context.Context) error {
		return forest.Use[Runner](call, x.F, "consumer", "read", func(r Runner) error { var e error; out, e = r.Read(call); return e })
	})
	return out, err
}
func (x *Fixture) Change(ctx context.Context, replace bool) error {
	var op forest.Operation
	if replace {
		if x.version == "1" {
			x.version = "2"
		} else {
			x.version = "1"
		}
		op = forest.Replace(Provider(0), forest.DefinitionRef{TypeID: "perf.value", Version: x.version}, false)
	} else {
		x.rebound = !x.rebound
		id := Provider(0)
		if x.rebound {
			id = Provider(1)
		}
		op = forest.Rebind(forest.Link{ID: "target-0", Consumer: "consumer", Port: "input", Provider: id, Capability: "value"})
	}
	g, e := x.F.Apply(ctx, forest.ChangePlan{BaseGeneration: x.generation, Operations: []forest.Operation{op}})
	if e == nil {
		x.generation = g
	}
	return e
}
func (x *Fixture) Target() (topology.Manifest, error) {
	m, e := topology.Capture(x.F.Snapshot(), x.Catalog)
	for i := range m.Trees {
		if m.Trees[i].ID == Provider(0) {
			m.Trees[i].Definition.Version = "2"
		}
	}
	return m, e
}
