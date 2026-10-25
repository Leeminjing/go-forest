package observe

import (
	"context"
	"errors"
	"reflect"
	"sort"

	"github.com/Leeminjing/go-forest/forest"
)

func diagnostic(err error) *Diagnostic {
	if err == nil {
		return nil
	}
	d := &Diagnostic{Code: "internal_error"}
	known := []struct {
		err  error
		code string
	}{
		{forest.ErrGenerationConflict, "generation_conflict"}, {forest.ErrContractMismatch, "contract_mismatch"},
		{forest.ErrMissingCapability, "missing_capability"}, {forest.ErrInvalidLink, "invalid_link"},
		{forest.ErrPortAlreadyBound, "port_already_bound"}, {forest.ErrNestCycle, "nest_cycle"},
		{forest.ErrDependencyInUse, "dependency_in_use"}, {forest.ErrReparentUnsupported, "reparent_unsupported"},
		{forest.ErrUnsupportedHotReplace, "unsupported_hot_replace"}, {forest.ErrScopeClosed, "scope_closed"},
		{forest.ErrDrainTimeout, "drain_timeout"}, {forest.ErrClosed, "closed"},
		{forest.ErrInvalidDefinition, "invalid_definition"}, {forest.ErrInvalidOperation, "invalid_operation"},
		{forest.ErrNotFound, "not_found"}, {forest.ErrBindingUnavailable, "binding_unavailable"},
		{forest.ErrEventGap, "event_gap"}, {forest.ErrPrepareFailed, "prepare_failed"},
		{context.Canceled, "canceled"}, {context.DeadlineExceeded, "deadline_exceeded"},
	}
	for _, item := range known {
		if errors.Is(err, item.err) {
			d.Code = item.code
			break
		}
	}
	var detail *forest.Error
	if errors.As(err, &detail) {
		d.Stage, d.Tree, d.Port = detail.Stage, string(detail.Tree), string(detail.Port)
	}
	return d
}

func typeName(t reflect.Type) string {
	if t == nil {
		return ""
	}
	return t.String()
}

func snapshotDTO(s forest.Snapshot) Snapshot {
	out := Snapshot{Generation: s.Generation, ObservedEventSequence: s.EventSequence, Trees: []Tree{}, NestEdges: []NestEdge{}, Bindings: []Binding{}, Instances: []Instance{}}
	for _, t := range s.Trees {
		tree := Tree{ID: string(t.ID), ParentID: string(t.ParentID), InstanceID: t.InstanceID, TypeID: t.TypeID, Version: t.Version, SampledState: string(t.State), Ports: []Port{}, Capabilities: []Capability{}}
		for _, p := range t.Ports {
			phase := "runtime"
			if p.Phase == forest.Construction {
				phase = "construction"
			} else if p.Phase == forest.Startup {
				phase = "startup"
			}
			tree.Ports = append(tree.Ports, Port{string(p.ID), typeName(p.Type), p.Required, p.Many, phase, p.ContractID, p.MinVersion, p.MaxVersion})
		}
		for _, c := range t.Capabilities {
			v := []string{}
			if c.Visibility&forest.Public != 0 {
				v = append(v, "public")
			}
			if c.Visibility&forest.Descendants != 0 {
				v = append(v, "descendants")
			}
			if c.Visibility&forest.Ancestors != 0 {
				v = append(v, "ancestors")
			}
			tree.Capabilities = append(tree.Capabilities, Capability{string(c.ID), typeName(c.Type), v, c.Contract.ID, c.Contract.Version})
		}
		sort.Slice(tree.Ports, func(a, b int) bool { return tree.Ports[a].ID < tree.Ports[b].ID })
		sort.Slice(tree.Capabilities, func(a, b int) bool { return tree.Capabilities[a].ID < tree.Capabilities[b].ID })
		out.Trees = append(out.Trees, tree)
		if t.ParentID != "" {
			out.NestEdges = append(out.NestEdges, NestEdge{string(t.ParentID), string(t.ID)})
		}
	}
	for _, collection := range []map[string]forest.LinkInfo{s.Links, s.NestBindings} {
		for _, b := range collection {
			kind := "link"
			if b.Local {
				kind = "nest_binding"
				out.Counts.NestBindings++
			} else {
				out.Counts.Links++
			}
			out.Bindings = append(out.Bindings, Binding{b.ID, kind, string(b.Consumer), string(b.Port), string(b.Provider), string(b.Capability), b.Order})
		}
	}
	for _, i := range s.Instances {
		out.Instances = append(out.Instances, Instance{i.ID, string(i.TreeID), i.Definition.TypeID, i.Definition.Version, string(i.State), diagnostic(i.Error)})
		if i.State == forest.Draining {
			out.Counts.Draining++
		}
	}
	sort.Slice(out.Trees, func(a, b int) bool { return out.Trees[a].ID < out.Trees[b].ID })
	sort.Slice(out.NestEdges, func(a, b int) bool { return out.NestEdges[a].Child < out.NestEdges[b].Child })
	sort.Slice(out.Bindings, func(a, b int) bool { return out.Bindings[a].Order < out.Bindings[b].Order })
	sort.Slice(out.Instances, func(a, b int) bool { return out.Instances[a].ID < out.Instances[b].ID })
	out.Counts.Trees, out.Counts.NestEdges = uint64(len(out.Trees)), uint64(len(out.NestEdges))
	return out
}

func changeDTO(e forest.Event) Change {
	out := Change{Sequence: e.Sequence, Generation: e.Generation, Operations: []string{}, Trees: []string{}, Links: append([]string{}, e.Links...), Result: e.Result, Timestamp: e.Timestamp.UTC(), Diagnostic: diagnostic(e.Error)}
	for _, op := range e.Operations {
		out.Operations = append(out.Operations, string(op))
	}
	for _, id := range e.Trees {
		out.Trees = append(out.Trees, string(id))
	}
	return out
}
