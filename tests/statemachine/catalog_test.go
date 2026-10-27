package statemachine

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"

	"github.com/Leeminjing/go-forest/forest"
	m "github.com/Leeminjing/go-forest/tests/internal/topologymodel"
)

func Catalog() []m.Definition {
	base := m.Definition{ID: "provider-v1", Version: "1", Hot: true, Move: true, Keep: true, Export: true, Caps: []m.Capability{{ID: "value", Type: "value", Public: true, Down: true, Up: true, Contract: "sample", Version: 1}, {ID: "inspect", Type: "inspect", Public: true}}}
	defs := []m.Definition{base}
	add := func(id string, edit func(*m.Definition)) {
		d := base
		d.ID = id
		d.Caps = append([]m.Capability(nil), base.Caps...)
		edit(&d)
		defs = append(defs, d)
	}
	add("provider-v2", func(d *m.Definition) { d.Version = "2"; d.Caps[0].Version = 2 })
	add("provider-next", func(d *m.Definition) { d.Version = "2" })
	add("other", func(d *m.Definition) { d.Caps[0].Type = "other" })
	add("sealed", func(d *m.Definition) { d.Export = false })
	add("fixed", func(d *m.Definition) { d.Move = false })
	add("exclusive", func(d *m.Definition) { d.Hot = false })
	add("no-keep", func(d *m.Definition) { d.Keep = false })
	add("private", func(d *m.Definition) { d.Caps[0].Public = false; d.Caps[0].Down = false; d.Caps[0].Up = false })
	add("up", func(d *m.Definition) { d.Caps[0].Public = false; d.Caps[0].Down = false })
	add("down", func(d *m.Definition) { d.Caps[0].Public = false; d.Caps[0].Up = false })
	add("foreign-contract", func(d *m.Definition) { d.Caps[0].Contract = "foreign" })
	for _, phase := range []m.Phase{m.Runtime, m.Construction, m.Startup} {
		for _, mode := range []string{"optional", "required", "many"} {
			phase, mode := phase, mode
			add(string(phase)+"-"+mode, func(d *m.Definition) {
				d.Ports = []m.Port{{ID: "in", Type: "value", Phase: phase, Required: mode == "required", Many: mode == "many"}}
			})
		}
	}
	add("two-ports", func(d *m.Definition) {
		d.Ports = []m.Port{{ID: "in", Type: "value", Phase: m.Construction}, {ID: "peer", Type: "value", Phase: m.Runtime}}
	})
	add("parallel-build", func(d *m.Definition) {
		d.Ports = []m.Port{{ID: "in", Type: "value", Phase: m.Construction, Required: true}}
		d.Fail = "start"
	})
	add("semantic", func(d *m.Definition) {
		d.Ports = []m.Port{{ID: "in", Type: "value", Phase: m.Runtime, Required: true, Contract: "sample", Min: 2, Max: 3}}
	})
	add("other-consumer", func(d *m.Definition) { d.Ports = []m.Port{{ID: "in", Type: "other", Phase: m.Runtime}} })
	for _, stage := range []string{"build", "start", "readiness"} {
		stage := stage
		add("fail-"+stage, func(d *m.Definition) { d.Fail = stage })
	}
	return defs
}

type reading struct {
	Tree, Definition, Physical string
	Generation                 uint64
}
type value interface {
	Read(context.Context) (reading, error)
}
type other interface {
	Other(context.Context) (reading, error)
}
type inspector interface {
	Inspect(context.Context, string) ([]reading, error)
}
type valueObject struct{ o *object }

func (v valueObject) Read(c context.Context) (reading, error) { return v.o.read(c) }

type otherObject struct{ o *object }

func (v otherObject) Other(c context.Context) (reading, error) { return v.o.read(c) }

type phaseUse struct {
	Tree, Definition, Phase, Port string
	Generation                    uint64
	Values                        []reading
	Err                           error
}
type object struct {
	resourceMu    sync.Mutex
	resources     []reading
	h             *harness
	tree          string
	definition    m.Definition
	file          *os.File
	closed        chan struct{}
	key           string
	stops, closes atomic.Int32
	built, ready  atomic.Bool
	values        map[string]forest.Ref[value]
	others        map[string]forest.Ref[other]
}

type demandKey struct{}
type demand struct {
	Stage string
	Phase m.Phase
}

func (o *object) read(ctx context.Context) (reading, error) {
	if want, ok := ctx.Value(demandKey{}).(demand); ok {
		if want.Stage == "build" && !o.built.Load() {
			return reading{}, fmt.Errorf("provider constructor unfinished: %s", o.tree)
		}
		if want.Stage != "build" && !o.ready.Load() {
			return reading{}, fmt.Errorf("provider readiness unfinished: %s", o.tree)
		}
	}
	var b [1]byte
	if _, err := o.file.ReadAt(b[:], 0); err != nil {
		return reading{}, err
	}
	g, err := o.h.f.Generation(ctx)
	out := reading{Tree: o.tree, Definition: o.definition.ID, Physical: o.key, Generation: g}
	if err == nil && o.h.readHook != nil {
		err = o.h.readHook(ctx, o)
	}
	return out, err
}
func (o *object) Inspect(c context.Context, id string) (out []reading, err error) {
	var alive [1]byte
	if _, err = o.file.ReadAt(alive[:], 0); err != nil {
		return nil, err
	}
	p, ok := o.definition.Port(id)
	if !ok {
		return nil, forest.ErrInvalidOperation
	}
	if p.Type == "other" {
		r := o.others[id]
		fn := func(v other) error { x, e := v.Other(c); out = append(out, x); return e }
		if p.Many {
			err = r.UseAll(c, func(vs []other) error {
				for _, v := range vs {
					if e := fn(v); e != nil {
						return e
					}
				}
				return nil
			})
		} else {
			err = r.Use(c, fn)
		}
	} else {
		r := o.values[id]
		fn := func(v value) error { x, e := v.Read(c); out = append(out, x); return e }
		if p.Many {
			err = r.UseAll(c, func(vs []value) error {
				for _, v := range vs {
					if e := fn(v); e != nil {
						return e
					}
				}
				return nil
			})
		} else {
			err = r.Use(c, fn)
		}
	}
	return
}
func (o *object) phase(ctx context.Context, stage string) error {
	g, err := o.h.f.Generation(ctx)
	if err != nil {
		return err
	}
	if err := o.h.record(nativeEvent{Kind: "phase.begin." + stage, Tree: o.tree, Physical: o.key, Definition: o.definition.ID, Generation: &g}); err != nil {
		return err
	}
	for _, p := range o.definition.Ports {
		values, e := o.Inspect(context.WithValue(ctx, demandKey{}, demand{stage, p.Phase}), p.ID)
		if e == nil && p.Phase != m.Runtime {
			o.resourceMu.Lock()
			o.resources = append(o.resources, values...)
			o.resourceMu.Unlock()
		}
		event := nativeEvent{Kind: "phase." + stage, Tree: o.tree, Physical: o.key, Definition: o.definition.ID, Generation: &g, Values: values}
		if e != nil {
			event.Error = e.Error()
		}
		if err := o.h.record(event); err != nil {
			return err
		}
		o.h.mu.Lock()
		o.h.phases = append(o.h.phases, phaseUse{Tree: o.tree, Definition: o.definition.ID, Phase: stage, Port: p.ID, Generation: g, Values: values, Err: e})
		o.h.mu.Unlock()
		permitted := p.Phase == m.Construction || (stage != "build" && p.Phase == m.Startup)
		if !permitted {
			if !isUnavailable(e) {
				return fmt.Errorf("premature %s resolution during %s", p.Phase, stage)
			}
		} else if e != nil && !(!p.Required && len(values) == 0 && isUnavailable(e)) {
			return e
		}
	}
	return nil
}
func (o *object) Start(c context.Context) error {
	if err := o.phase(c, "start"); err != nil {
		return err
	}
	if o.h.startHook != nil {
		if err := o.h.startHook(c, o); err != nil {
			return err
		}
	}
	if o.definition.Fail == "start" {
		return errInjected
	}
	return nil
}
func (o *object) Ready(c context.Context) error {
	if err := o.phase(c, "readiness"); err != nil {
		return err
	}
	if o.h.readyHook != nil {
		if err := o.h.readyHook(c, o); err != nil {
			return err
		}
	}
	if o.definition.Fail == "readiness" {
		return errInjected
	}
	o.ready.Store(true)
	if err := o.h.record(nativeEvent{Kind: "ready.done", Tree: o.tree, Physical: o.key, Definition: o.definition.ID}); err != nil {
		return err
	}
	return nil
}
func (o *object) Stop(c context.Context) error {
	o.stops.Add(1)
	if err := o.h.checkResources(o); err != nil {
		return err
	}
	if err := o.h.record(nativeEvent{Kind: "stop.enter", Tree: o.tree, Physical: o.key, Definition: o.definition.ID}); err != nil {
		return err
	}
	if o.h.stopHook != nil {
		if err := o.h.stopHook(c, o); err != nil {
			return err
		}
	}
	if err := o.file.Close(); err != nil {
		return err
	}
	o.closes.Add(1)
	if err := o.h.record(nativeEvent{Kind: "resource.closed", Tree: o.tree, Physical: o.key, Definition: o.definition.ID}); err != nil {
		return err
	}
	close(o.closed)
	return nil
}

func visibility(c m.Capability) forest.Visibility {
	var v forest.Visibility
	if c.Public {
		v |= forest.Public
	}
	if c.Down {
		v |= forest.Descendants
	}
	if c.Up {
		v |= forest.Ancestors
	}
	return v
}
func phase(p m.Phase) forest.Phase {
	switch p {
	case m.Construction:
		return forest.Construction
	case m.Startup:
		return forest.Startup
	default:
		return forest.Runtime
	}
}
func definitionRef(tree string, d m.Definition) forest.DefinitionRef {
	return forest.DefinitionRef{TypeID: "state-machine." + tree + "." + d.ID, Version: d.Version}
}
func (h *harness) definition(tree string, d m.Definition) forest.Definition {
	ref := definitionRef(tree, d)
	out := forest.Definition{TypeID: ref.TypeID, Version: ref.Version, HotSwappable: d.Hot, Reparentable: d.Move, PreserveChildren: d.Keep, ExportChildren: d.Export}
	for _, p := range d.Ports {
		var opts []forest.PortOption
		if p.Required {
			opts = append(opts, forest.Required())
		}
		if p.Many {
			opts = append(opts, forest.Many())
		}
		opts = append(opts, forest.During(phase(p.Phase)))
		if p.Contract != "" {
			opts = append(opts, forest.RequiresContract(p.Contract, p.Min, p.Max))
		}
		if p.Type == "other" {
			out.Ports = append(out.Ports, forest.Port[other](forest.PortID(p.ID), opts...))
		} else {
			out.Ports = append(out.Ports, forest.Port[value](forest.PortID(p.ID), opts...))
		}
	}
	for _, cap := range d.Caps {
		v := visibility(cap)
		c := forest.Contract{ID: cap.Contract, Version: cap.Version}
		switch cap.Type {
		case "value":
			out.Capabilities = append(out.Capabilities, forest.Capability[value](forest.CapabilityID(cap.ID), v, c))
		case "other":
			out.Capabilities = append(out.Capabilities, forest.Capability[other](forest.CapabilityID(cap.ID), v, c))
		case "inspect":
			out.Capabilities = append(out.Capabilities, forest.Capability[inspector](forest.CapabilityID(cap.ID), v, c))
		}
	}
	out.Build = func(c context.Context, e *forest.BuildEnv) (forest.Lifecycle, error) {
		file, err := os.CreateTemp(h.dir, "native-")
		if err != nil {
			return nil, err
		}
		if _, err = file.WriteString("open"); err != nil {
			file.Close()
			return nil, err
		}
		o := &object{h: h, tree: tree, definition: d, file: file, closed: make(chan struct{}), key: filepath.Base(file.Name()), values: map[string]forest.Ref[value]{}, others: map[string]forest.Ref[other]{}}
		h.mu.Lock()
		h.objects = append(h.objects, o)
		h.mu.Unlock()
		for _, p := range d.Ports {
			if p.Type == "other" {
				r, err := forest.Reference[other](e, forest.PortID(p.ID))
				if err != nil {
					return o, err
				}
				o.others[p.ID] = r
			} else {
				r, err := forest.Reference[value](e, forest.PortID(p.ID))
				if err != nil {
					return o, err
				}
				o.values[p.ID] = r
			}
		}
		for _, cap := range d.Caps {
			var v any
			switch cap.Type {
			case "value":
				v = valueObject{o}
			case "other":
				v = otherObject{o}
			case "inspect":
				v = o
			}
			if err = forest.Provide(e, forest.CapabilityID(cap.ID), v); err != nil {
				return o, err
			}
		}
		if err = o.phase(c, "build"); err != nil {
			return o, err
		}
		if h.buildHook != nil {
			if err = h.buildHook(c, o); err != nil {
				return o, err
			}
		}
		if d.Fail == "build" {
			return o, errInjected
		}
		o.built.Store(true)
		if err := h.record(nativeEvent{Kind: "build.done", Tree: o.tree, Physical: o.key, Definition: o.definition.ID}); err != nil {
			return o, err
		}
		return o, nil
	}
	return out
}

// Hooks are configured before starting a controlled experiment, then immutable.
type hooks struct {
	events                                    []nativeEvent
	mu                                        sync.Mutex
	objects                                   []*object
	phases                                    []phaseUse
	buildHook, startHook, readyHook, stopHook func(context.Context, *object) error
	readHook                                  func(context.Context, *object) error
}
