package topology

import (
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"sort"

	"github.com/Leeminjing/go-forest/forest"
)

// DefinitionFacts are host assertions used by the planner, not serialized Go
// types or proof of a factory's behavior. Core still validates the actual graph.
type DefinitionFacts struct {
	Definition           forest.DefinitionRef `json:"definition"`
	ContractKey          string               `json:"contract_key"`
	ImplementationDigest string               `json:"implementation_digest"`
	HotSwappable         bool                 `json:"hot_swappable"`
	PreserveChildren     bool                 `json:"preserve_children"`
	Reparentable         bool                 `json:"reparentable"`
	Ports                []PortFacts          `json:"ports"`
}
type PortFacts struct {
	ID    forest.PortID `json:"id"`
	Phase forest.Phase  `json:"phase"`
}
type PlanningEvidence struct {
	Version int               `json:"version"`
	Facts   []DefinitionFacts `json:"facts"`
	Impact  Impact            `json:"impact"`
}
type Reason struct {
	Code     string        `json:"code"`
	Tree     forest.TreeID `json:"tree"`
	Port     forest.PortID `json:"port,omitempty"`
	Binding  string        `json:"binding,omitempty"`
	Provider forest.TreeID `json:"provider,omitempty"`
	Phase    forest.Phase  `json:"phase,omitempty"`
	Source   forest.TreeID `json:"source,omitempty"`
}
type Impact struct {
	RetainedTrees    []forest.TreeID `json:"retained_trees"`
	AddedTrees       []forest.TreeID `json:"added_trees"`
	RebuildTrees     []forest.TreeID `json:"rebuild_trees"`
	RemovedTrees     []forest.TreeID `json:"removed_trees"`
	MovedTrees       []forest.TreeID `json:"moved_trees"`
	RetainedBindings []string        `json:"retained_bindings"`
	ReboundBindings  []string        `json:"rebound_bindings"`
	RemovedBindings  []string        `json:"removed_bindings"`
	InsertedBindings []string        `json:"inserted_bindings"`
	Reasons          []Reason        `json:"reasons"`
	Blockers         []Reason        `json:"blockers"`
}

// PlanError accompanies a populated, non-submittable Proposal.
type PlanError struct{ Blockers []Reason }

func (e *PlanError) Error() string { return fmt.Sprintf("%v: %+v", ErrPlanBlocked, e.Blockers) }
func (e *PlanError) Unwrap() []error {
	out := []error{ErrPlanBlocked}
	for _, b := range e.Blockers {
		if b.Code == "not-reparentable" {
			out = append(out, forest.ErrReparentUnsupported)
		} else {
			out = append(out, forest.ErrUnsupportedHotReplace)
		}
	}
	return out
}
func factKey(f DefinitionFacts) string {
	b, _ := json.Marshal(struct {
		Definition          forest.DefinitionRef
		ContractKey, Digest string
	}{f.Definition, f.ContractKey, f.ImplementationDigest})
	return string(b)
}
func treeFactKey(t Tree) string {
	return factKey(DefinitionFacts{Definition: t.Definition, ContractKey: t.ContractKey, ImplementationDigest: t.ImplementationDigest})
}

func (c *Catalog) plan(before, target Manifest, g uint64) (Plan, *PlanningEvidence, error) {
	for _, m := range []Manifest{before, target} {
		if e := c.check(m); e != nil {
			return Plan{}, &PlanningEvidence{}, e
		}
	}
	seen := map[string]DefinitionFacts{}
	for _, m := range []Manifest{before, target} {
		for _, t := range m.Trees {
			r := c.entries[t.Definition]
			f := DefinitionFacts{Definition: t.Definition, ContractKey: t.ContractKey, ImplementationDigest: t.ImplementationDigest, HotSwappable: r.Definition.HotSwappable, PreserveChildren: r.Definition.PreserveChildren, Reparentable: r.Definition.Reparentable, Ports: []PortFacts{}}
			for _, p := range r.Definition.Ports {
				f.Ports = append(f.Ports, PortFacts{p.ID, p.Phase})
			}
			sort.Slice(f.Ports, func(i, j int) bool { return f.Ports[i].ID < f.Ports[j].ID })
			seen[factKey(f)] = f
		}
	}
	keys := make([]string, 0, len(seen))
	for k := range seen {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	facts := []DefinitionFacts{}
	for _, k := range keys {
		facts = append(facts, seen[k])
	}
	return planV2(before, target, g, facts)
}

func treeIndex(m Manifest) map[forest.TreeID]Tree {
	out := map[forest.TreeID]Tree{}
	for _, t := range m.Trees {
		out[t.ID] = t
	}
	return out
}
func bindingIndex(m Manifest) map[string]Binding {
	out := map[string]Binding{}
	for _, b := range m.Bindings {
		out[b.ID] = b
	}
	return out
}
func bindingLink(b Binding) forest.Link {
	return forest.Link{ID: b.ID, Consumer: b.Consumer, Port: b.Port, Provider: b.Provider, Capability: b.Capability}
}
func sameEndpoints(a, b Binding) bool { return bindingLink(a) == bindingLink(b) && a.Kind == b.Kind }
func orderedTrees(m Manifest, reverse bool) []Tree {
	nodes := treeIndex(m)
	depth := map[forest.TreeID]int{"": 0}
	for _, t := range m.Trees {
		var path []forest.TreeID
		id := t.ID
		for {
			if _, ok := depth[id]; ok {
				break
			}
			path = append(path, id)
			id = nodes[id].ParentID
		}
		for i := len(path) - 1; i >= 0; i-- {
			depth[path[i]] = depth[nodes[path[i]].ParentID] + 1
		}
	}
	out := append([]Tree(nil), m.Trees...)
	sort.Slice(out, func(i, j int) bool {
		if depth[out[i].ID] != depth[out[j].ID] {
			if reverse {
				return depth[out[i].ID] > depth[out[j].ID]
			}
			return depth[out[i].ID] < depth[out[j].ID]
		}
		return out[i].ID < out[j].ID
	})
	return out
}

func planV2(before, target Manifest, g uint64, facts []DefinitionFacts) (Plan, *PlanningEvidence, error) {
	p := Plan{BaseGeneration: g, Operations: []forest.Operation{}}
	e := &PlanningEvidence{Version: 2, Facts: facts, Impact: Impact{RetainedTrees: []forest.TreeID{}, AddedTrees: []forest.TreeID{}, RebuildTrees: []forest.TreeID{}, RemovedTrees: []forest.TreeID{}, MovedTrees: []forest.TreeID{}, RetainedBindings: []string{}, ReboundBindings: []string{}, RemovedBindings: []string{}, InsertedBindings: []string{}, Reasons: []Reason{}, Blockers: []Reason{}}}
	if facts == nil {
		return p, e, ErrInvalidManifest
	}
	at, bt := treeIndex(before), treeIndex(target)
	ab, bb := bindingIndex(before), bindingIndex(target)
	fm := map[string]DefinitionFacts{}
	needed := map[string]bool{}
	for _, m := range []Manifest{before, target} {
		for _, t := range m.Trees {
			needed[treeFactKey(t)] = true
		}
	}
	previous := ""
	for i, f := range facts {
		k := factKey(f)
		if !needed[k] || (i > 0 && k <= previous) || f.Ports == nil {
			return p, e, ErrInvalidManifest
		}
		previous = k
		for j, port := range f.Ports {
			if !validID(string(port.ID)) || port.Phase > forest.Startup || (j > 0 && f.Ports[j-1].ID >= port.ID) {
				return p, e, ErrInvalidManifest
			}
		}
		fm[k] = f
	}
	if len(fm) != len(needed) {
		return p, e, ErrInvalidManifest
	}
	phase := func(t Tree, port forest.PortID) forest.Phase {
		for _, v := range fm[treeFactKey(t)].Ports {
			if v.ID == port {
				return v.Phase
			}
		}
		return forest.Runtime
	}
	removed, rebuilt := map[forest.TreeID]bool{}, map[forest.TreeID]bool{}
	var queue []forest.TreeID
	mark := func(id forest.TreeID, r Reason) {
		if _, exists := at[id]; !exists {
			return
		}
		if _, exists := bt[id]; !exists {
			return
		}
		e.Impact.Reasons = append(e.Impact.Reasons, r)
		if !rebuilt[id] {
			rebuilt[id] = true
			queue = append(queue, id)
		}
	}
	for _, t := range before.Trees {
		if _, ok := bt[t.ID]; !ok {
			removed[t.ID] = true
			e.Impact.RemovedTrees = append(e.Impact.RemovedTrees, t.ID)
		}
	}
	for _, t := range target.Trees {
		if old, ok := at[t.ID]; !ok {
			e.Impact.AddedTrees = append(e.Impact.AddedTrees, t.ID)
		} else {
			if old.Definition != t.Definition {
				mark(t.ID, Reason{Code: "definition-changed", Tree: t.ID})
			}
			if old.ParentID != t.ParentID {
				e.Impact.MovedTrees = append(e.Impact.MovedTrees, t.ID)
			}
		}
	}
	children := map[forest.TreeID][]forest.TreeID{}
	for _, t := range target.Trees {
		children[t.ParentID] = append(children[t.ParentID], t.ID)
	}
	// Once a moved root has been checked, its descendants need not be revisited
	// for another move. Every surviving descendant must support reparenting.
	checked := map[forest.TreeID]bool{}
	for _, root := range e.Impact.MovedTrees {
		todo := []forest.TreeID{root}
		for len(todo) > 0 {
			id := todo[0]
			todo = todo[1:]
			if checked[id] {
				continue
			}
			checked[id] = true
			todo = append(todo, children[id]...)
			if old, exists := at[id]; exists && (!fm[treeFactKey(old)].Reparentable || !fm[treeFactKey(bt[id])].Reparentable) {
				e.Impact.Blockers = append(e.Impact.Blockers, Reason{Code: "not-reparentable", Tree: id, Source: root})
			}
		}
	}
	keep := map[string]bool{}
	last := uint64(0)
	for _, b := range target.Bindings {
		old, ok := ab[b.ID]
		if !ok || old.Consumer != b.Consumer || old.Port != b.Port || old.Kind != b.Kind || removed[old.Consumer] || removed[old.Provider] || old.Ordinal <= last {
			break
		}
		keep[b.ID] = true
		last = old.Ordinal
	}
	for _, b := range before.Bindings {
		if !keep[b.ID] {
			p.Operations = append(p.Operations, forest.Unlink(b.ID))
			e.Impact.RemovedBindings = append(e.Impact.RemovedBindings, b.ID)
		}
	}
	watchers := map[forest.TreeID][]Reason{}
	for _, b := range before.Bindings {
		if phase(at[b.Consumer], b.Port) != forest.Runtime {
			next, exists := bb[b.ID]
			if !exists || !keep[b.ID] || !sameEndpoints(b, next) {
				mark(b.Consumer, Reason{Code: "phase-binding-changed", Tree: b.Consumer, Port: b.Port, Binding: b.ID, Provider: b.Provider, Phase: phase(at[b.Consumer], b.Port)})
			}
		}
	}
	for _, b := range target.Bindings {
		ph := phase(bt[b.Consumer], b.Port)
		if ph != forest.Runtime {
			old, exists := ab[b.ID]
			if !exists || !keep[b.ID] || !sameEndpoints(old, b) {
				mark(b.Consumer, Reason{Code: "phase-binding-changed", Tree: b.Consumer, Port: b.Port, Binding: b.ID, Provider: b.Provider, Phase: ph})
			}
			watchers[b.Provider] = append(watchers[b.Provider], Reason{Code: "phase-provider-rebuilt", Tree: b.Consumer, Port: b.Port, Binding: b.ID, Provider: b.Provider, Phase: ph, Source: b.Provider})
		}
	}
	forced := map[forest.TreeID]bool{}
	for len(queue) > 0 {
		id := queue[0]
		queue = queue[1:]
		for _, r := range watchers[id] {
			mark(r.Tree, r)
		}
		if !fm[treeFactKey(bt[id])].PreserveChildren {
			todo := append([]forest.TreeID(nil), children[id]...)
			for len(todo) > 0 {
				child := todo[0]
				todo = todo[1:]
				if forced[child] {
					continue
				}
				forced[child] = true
				todo = append(todo, children[child]...)
				mark(child, Reason{Code: "parent-requires-rebuild", Tree: child, Source: id})
			}
		}
	}
	for _, t := range target.Trees {
		if old, exists := at[t.ID]; exists {
			if rebuilt[t.ID] {
				e.Impact.RebuildTrees = append(e.Impact.RebuildTrees, t.ID)
				if !fm[treeFactKey(old)].HotSwappable || !fm[treeFactKey(t)].HotSwappable {
					e.Impact.Blockers = append(e.Impact.Blockers, Reason{Code: "not-hot-swappable", Tree: t.ID})
				}
			} else {
				e.Impact.RetainedTrees = append(e.Impact.RetainedTrees, t.ID)
			}
		}
	}
	ordered := orderedTrees(target, false)
	for _, t := range ordered {
		if _, exists := at[t.ID]; !exists {
			if t.ParentID == "" {
				p.Operations = append(p.Operations, forest.AddRoot(t.ID, t.Definition))
			} else {
				p.Operations = append(p.Operations, forest.Nest(t.ParentID, t.ID, t.Definition))
			}
		}
	}
	for _, t := range ordered {
		if old, exists := at[t.ID]; exists && old.ParentID != t.ParentID {
			if t.ParentID == "" {
				p.Operations = append(p.Operations, forest.Unnest(t.ID))
			} else {
				p.Operations = append(p.Operations, forest.Move(t.ID, t.ParentID))
			}
		}
	}
	for _, t := range orderedTrees(before, true) {
		if removed[t.ID] {
			p.Operations = append(p.Operations, forest.Remove(t.ID))
		}
	}
	// A replaced ancestor may retain any depth of descendants. Compute that
	// fact once, bottom-up, rather than repeatedly scanning each subtree.
	hasRetained := map[forest.TreeID]bool{}
	for _, t := range orderedTrees(target, true) {
		if _, ok := at[t.ID]; ok && !rebuilt[t.ID] {
			hasRetained[t.ParentID] = true
		}
		if hasRetained[t.ID] {
			hasRetained[t.ParentID] = true
		}
	}
	for _, t := range ordered {
		if rebuilt[t.ID] {
			p.Operations = append(p.Operations, forest.Replace(t.ID, t.Definition, hasRetained[t.ID]))
		}
	}
	for _, b := range target.Bindings {
		if keep[b.ID] {
			if sameEndpoints(ab[b.ID], b) {
				e.Impact.RetainedBindings = append(e.Impact.RetainedBindings, b.ID)
			} else {
				e.Impact.ReboundBindings = append(e.Impact.ReboundBindings, b.ID)
				p.Operations = append(p.Operations, forest.Rebind(bindingLink(b)))
			}
		}
	}
	for _, b := range target.Bindings {
		if !keep[b.ID] {
			e.Impact.InsertedBindings = append(e.Impact.InsertedBindings, b.ID)
			if b.Kind == "nest" {
				p.Operations = append(p.Operations, forest.BindNest(bindingLink(b)))
			} else {
				p.Operations = append(p.Operations, forest.Connect(bindingLink(b)))
			}
		}
	}
	sortReasons := func(rs []Reason) []Reason {
		sort.Slice(rs, func(i, j int) bool {
			a, _ := json.Marshal(rs[i])
			b, _ := json.Marshal(rs[j])
			return string(a) < string(b)
		})
		out := rs[:0]
		for _, r := range rs {
			if len(out) == 0 || out[len(out)-1] != r {
				out = append(out, r)
			}
		}
		return out
	}
	e.Impact.Reasons = sortReasons(e.Impact.Reasons)
	e.Impact.Blockers = sortReasons(e.Impact.Blockers)
	if err := checkProjection(before, target, p); err != nil {
		return p, e, err
	}
	if len(e.Impact.Blockers) > 0 {
		return p, e, &PlanError{Blockers: detached(e.Impact.Blockers)}
	}
	return p, e, nil
}

// checkProjection checks inert topology effects only. Native contract checks,
// lifecycle ordering and resource ownership remain Forest's responsibility.
func checkProjection(before, target Manifest, p Plan) error {
	nodes := treeIndex(before)
	want := treeIndex(target)
	bindings := bindingIndex(before)
	order := uint64(len(before.Bindings))
	for _, op := range p.Operations {
		switch op.Kind {
		case forest.OpAddRoot, forest.OpNest:
			if _, exists := nodes[op.Tree]; exists {
				return ErrInvalidManifest
			}
			nodes[op.Tree] = want[op.Tree]
		case forest.OpMove, forest.OpUnnest:
			t := nodes[op.Tree]
			t.ParentID = op.Parent
			nodes[op.Tree] = t
		case forest.OpRemove:
			delete(nodes, op.Tree)
			for id, b := range bindings {
				if b.Consumer == op.Tree || b.Provider == op.Tree {
					delete(bindings, id)
				}
			}
		case forest.OpReplace:
			t := nodes[op.Tree]
			v := want[op.Tree]
			t.Definition, t.ContractKey, t.ImplementationDigest = v.Definition, v.ContractKey, v.ImplementationDigest
			nodes[op.Tree] = t
		case forest.OpUnlink:
			delete(bindings, op.Binding.ID)
		case forest.OpRebind:
			b := bindings[op.Binding.ID]
			l := op.Binding
			b.Consumer, b.Port, b.Provider, b.Capability = l.Consumer, l.Port, l.Provider, l.Capability
			bindings[b.ID] = b
		case forest.OpLink, forest.OpBindNest:
			order++
			l := op.Binding
			kind := "link"
			if op.Kind == forest.OpBindNest {
				kind = "nest"
			}
			bindings[l.ID] = Binding{ID: l.ID, Consumer: l.Consumer, Port: l.Port, Provider: l.Provider, Capability: l.Capability, Kind: kind, Ordinal: order}
		default:
			return ErrInvalidManifest
		}
	}
	out := Empty()
	for _, t := range nodes {
		out.Trees = append(out.Trees, t)
	}
	for _, b := range bindings {
		out.Bindings = append(out.Bindings, b)
	}
	sort.Slice(out.Bindings, func(i, j int) bool { return out.Bindings[i].Ordinal < out.Bindings[j].Ordinal })
	for i := range out.Bindings {
		out.Bindings[i].Ordinal = uint64(i + 1)
	}
	normalized, err := Normalize(out)
	if err != nil || !reflect.DeepEqual(normalized, target) {
		return errors.Join(ErrInvalidManifest, err)
	}
	return nil
}

func checkRetainedInstances(impact Impact, before, after *Receipt) error {
	if before == nil || before.SessionID != after.SessionID {
		return ErrCorruptJournal
	}
	old := map[string]string{}
	now := map[string]string{}
	oldIDs := map[string]bool{}
	for _, i := range before.Instances {
		old[i.TreeID] = i.InstanceID
		oldIDs[i.InstanceID] = true
	}
	for _, i := range after.Instances {
		now[i.TreeID] = i.InstanceID
	}
	for _, id := range impact.RetainedTrees {
		if old[string(id)] == "" || now[string(id)] != old[string(id)] {
			return ErrManagedForestDiverged
		}
	}
	for _, group := range [][]forest.TreeID{impact.RebuildTrees, impact.AddedTrees} {
		for _, id := range group {
			if now[string(id)] == "" || oldIDs[now[string(id)]] {
				return ErrManagedForestDiverged
			}
		}
	}
	return nil
}
