package statemachine

import (
	"encoding/json"
	"errors"
	"github.com/Leeminjing/go-forest/forest"
	m "github.com/Leeminjing/go-forest/tests/internal/topologymodel"
	"os"
	"reflect"
)

type nativeEvent struct {
	Sequence                                uint64
	Kind, Actor, Tree, Physical, Definition string
	Generation                              *uint64
	Values                                  []reading
	Error                                   string
}

func snapshotFact(s forest.Snapshot) json.RawMessage {
	type port struct {
		ID, Type       string
		Required, Many bool
		Phase          forest.Phase
		Contract       string
		Min, Max       uint64
	}
	type capability struct {
		ID, Type   string
		Visibility forest.Visibility
		Contract   forest.Contract
	}
	type tree struct {
		Parent, Instance, Type, Version string
		Ports                           []port
		Caps                            []capability
	}
	type diagnostic struct {
		ID, Tree string
		State    forest.State
		Error    string
	}
	payload := struct {
		Generation, EventSequence uint64
		Trees                     map[string]tree
		Bindings                  map[string]forest.Binding
		LiveDiagnostics           []diagnostic
	}{Generation: s.Generation, EventSequence: s.EventSequence, Trees: map[string]tree{}, Bindings: project(s).Bindings}
	name := func(t reflect.Type) string {
		if t == nil {
			return "<nil>"
		}
		return t.String()
	}
	for id, n := range s.Trees {
		node := tree{Parent: string(n.ParentID), Instance: n.InstanceID, Type: n.TypeID, Version: n.Version}
		for _, p := range n.Ports {
			node.Ports = append(node.Ports, port{string(p.ID), name(p.Type), p.Required, p.Many, p.Phase, p.ContractID, p.MinVersion, p.MaxVersion})
		}
		for _, c := range n.Capabilities {
			node.Caps = append(node.Caps, capability{string(c.ID), name(c.Type), c.Visibility, c.Contract})
		}
		payload.Trees[string(id)] = node
	}
	for _, i := range s.Instances {
		d := diagnostic{ID: i.ID, Tree: string(i.TreeID), State: i.State}
		if i.Error != nil {
			d.Error = i.Error.Error()
		}
		payload.LiveDiagnostics = append(payload.LiveDiagnostics, d)
	}
	return jsonFact(payload)
}

func (h *harness) record(e nativeEvent) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	if len(h.events) >= 100000 {
		return errors.New("native evidence journal overflow")
	}
	e.Sequence = uint64(len(h.events) + 1)
	e.Values = append([]reading(nil), e.Values...)
	if e.Generation != nil {
		g := *e.Generation
		e.Generation = &g
	}
	h.events = append(h.events, e)
	if os.Getenv("FOREST_MODEL_WORKER_INPUT") != "" {
		return json.NewEncoder(os.Stdout).Encode(e)
	}
	return nil
}
func (h *harness) nativeEvents() []nativeEvent {
	h.mu.Lock()
	defer h.mu.Unlock()
	return append([]nativeEvent(nil), h.events...)
}

func (h *harness) installOwners(old, next m.State) {
	h.mu.Lock()
	defer h.mu.Unlock()
	for id, n := range old.Nodes {
		if after, ok := next.Nodes[id]; ok && after.Instance == n.Instance {
			continue
		}
		o := h.nativeInstances[n.Instance]
		if o == nil {
			continue
		}
		if _, set := h.stopOwners[o]; set {
			continue
		}
		var owner *object
		if p, ok := old.Nodes[n.Parent]; ok {
			owner = h.nativeInstances[p.Instance]
		}
		h.stopOwners[o] = owner
	}
}
func (h *harness) checkResources(o *object) error {
	o.resourceMu.Lock()
	rs := append([]reading(nil), o.resources...)
	o.resourceMu.Unlock()
	h.mu.Lock()
	var owners []*object
	if owner := h.stopOwners[o]; owner != nil {
		owners = append(owners, owner)
	}
	for _, r := range rs {
		for _, candidate := range h.objects {
			if candidate.key == r.Physical {
				owners = append(owners, candidate)
				break
			}
		}
	}
	h.mu.Unlock()
	for _, dep := range owners {
		var b [1]byte
		if _, err := dep.file.ReadAt(b[:], 0); err != nil {
			return mismatch("lifecycle-resource", dep.key, err)
		}
	}
	return nil
}

func (h *harness) checkLifecycleOrder(old, next m.State) error {
	events := h.nativeEvents()
	at := map[string]map[string]uint64{}
	for _, e := range events {
		if at[e.Physical] == nil {
			at[e.Physical] = map[string]uint64{}
		}
		if _, ok := at[e.Physical][e.Kind]; !ok {
			at[e.Physical][e.Kind] = e.Sequence
		}
	}
	before := func(provider uint64, finished string, consumer uint64, begin string) error {
		a, b := h.nativeInstances[provider], h.nativeInstances[consumer]
		if a == nil || b == nil {
			return mismatch("lifecycle-witness", "actual built instances", nil)
		}
		left, right := at[a.key][finished], at[b.key][begin]
		if left == 0 || right == 0 || left >= right {
			return mismatch("lifecycle-partial-order", []any{a.tree, finished, b.tree, begin}, []uint64{left, right})
		}
		return nil
	}
	isNew := func(id string) bool {
		n := next.Nodes[id]
		p, ok := old.Nodes[id]
		return !ok || p.Instance != n.Instance
	}
	for id, n := range next.Nodes {
		if isNew(id) && n.Parent != "" {
			if err := before(next.Nodes[n.Parent].Instance, "build.done", n.Instance, "phase.begin.build"); err != nil {
				return err
			}
		}
		if n.Parent != "" && isNew(n.Parent) {
			ancestorDependency := false
			for _, binding := range next.Bindings {
				if binding.Consumer != id {
					continue
				}
				p, _ := h.model.Catalog[n.Definition].Port(binding.Port)
				if p.Phase != m.Runtime && next.Reaches(binding.Provider, id) {
					ancestorDependency = true
				}
			}
			if !ancestorDependency {
				if err := before(n.Instance, "ready.done", next.Nodes[n.Parent].Instance, "phase.begin.start"); err != nil {
					return err
				}
			}
		}
	}
	for _, binding := range next.Bindings {
		if !isNew(binding.Consumer) {
			continue
		}
		consumer, provider := next.Nodes[binding.Consumer], next.Nodes[binding.Provider]
		p, _ := h.model.Catalog[consumer.Definition].Port(binding.Port)
		if p.Phase == m.Construction {
			if err := before(provider.Instance, "build.done", consumer.Instance, "phase.begin.build"); err != nil {
				return err
			}
		}
		if p.Phase != m.Runtime {
			if err := before(provider.Instance, "ready.done", consumer.Instance, "phase.begin.start"); err != nil {
				return err
			}
		}
	}
	return nil
}
