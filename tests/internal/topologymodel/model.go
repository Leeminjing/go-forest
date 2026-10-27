// Package topologymodel is an independent, finite-domain specification oracle.
// It deliberately imports neither Forest nor its descriptors/validation helpers.
package topologymodel

import "sort"

type Phase string

const (
	Runtime      Phase = "runtime"
	Construction Phase = "construction"
	Startup      Phase = "startup"
)

type Port struct {
	ID, Type       string
	Phase          Phase
	Required, Many bool
	Contract       string
	Min, Max       uint64
}
type Capability struct {
	ID, Type         string
	Public, Down, Up bool
	Contract         string
	Version          uint64
}
type Definition struct {
	ID, Version             string
	Ports                   []Port
	Caps                    []Capability
	Hot, Move, Keep, Export bool
	Fail                    string
}
type Node struct {
	Definition, Parent string
	Instance           uint64
}
type Binding struct {
	ID, Consumer, Port, Provider, Capability string
	Local                                    bool
	Order                                    uint64
}
type Op struct {
	Kind, Tree, Parent, Definition string
	Binding                        Binding
	KeepChildren                   bool
}
type Plan struct {
	Base uint64
	Ops  []Op
}
type State struct {
	Generation, NextInstance, NextOrder uint64
	Nodes                               map[string]Node
	Bindings                            map[string]Binding
}
type Model struct {
	Catalog map[string]Definition
	State   State
}
type Issue string

func New(defs []Definition) *Model {
	m := &Model{Catalog: map[string]Definition{}, State: State{Nodes: map[string]Node{}, Bindings: map[string]Binding{}}}
	for _, d := range defs {
		m.Catalog[d.ID] = d
	}
	return m
}
func (s State) Copy() State {
	n := s
	n.Nodes = map[string]Node{}
	n.Bindings = map[string]Binding{}
	for k, v := range s.Nodes {
		n.Nodes[k] = v
	}
	for k, v := range s.Bindings {
		n.Bindings[k] = v
	}
	return n
}
func IDs(s State) []string {
	out := make([]string, 0, len(s.Nodes))
	for id := range s.Nodes {
		out = append(out, id)
	}
	sort.Strings(out)
	return out
}
func (m *Model) Next(p Plan) (State, []Issue) {
	old := m.State
	reject := func(rule Issue) (State, []Issue) { return old.Copy(), []Issue{rule} }
	if p.Base != old.Generation {
		return reject("generation")
	}
	if len(p.Ops) == 0 {
		return old.Copy(), nil
	}
	s := old.Copy()
	keep := map[string]bool{}
	for _, op := range p.Ops {
		n, exists := s.Nodes[op.Tree]
		switch op.Kind {
		case "add-root", "nest":
			if exists || op.Tree == "" || (op.Kind == "nest" && op.Parent == "") {
				return reject("operation")
			}
			if _, ok := m.Catalog[op.Definition]; !ok {
				return reject("definition")
			}
			s.NextInstance++
			s.Nodes[op.Tree] = Node{Definition: op.Definition, Parent: op.Parent, Instance: s.NextInstance}
		case "move", "unnest":
			if !exists {
				return reject("not-found")
			}
			if op.Kind == "move" && op.Parent == "" {
				return reject("operation")
			}
			n.Parent = op.Parent
			s.Nodes[op.Tree] = n
		case "replace":
			if !exists {
				return reject("not-found")
			}
			d, ok := m.Catalog[op.Definition]
			if !ok {
				return reject("definition")
			}
			if !m.Catalog[n.Definition].Hot || !d.Hot {
				return reject("hot-replace")
			}
			s.NextInstance++
			n.Definition = op.Definition
			n.Instance = s.NextInstance
			s.Nodes[op.Tree] = n
			keep[op.Tree] = op.KeepChildren
		case "remove", "remove-subtree":
			if !exists {
				return reject("not-found")
			}
			removed := map[string]bool{op.Tree: true}
			if op.Kind == "remove-subtree" {
				for id := range s.Nodes {
					if s.Reaches(op.Tree, id) {
						removed[id] = true
					}
				}
			}
			for id := range removed {
				delete(s.Nodes, id)
			}
			for id, b := range s.Bindings {
				if removed[b.Consumer] || removed[b.Provider] {
					delete(s.Bindings, id)
				}
			}
		case "link", "bind-nest":
			b := op.Binding
			if _, ok := s.Bindings[b.ID]; ok || b.ID == "" {
				return reject("binding-id")
			}
			s.NextOrder++
			b.Local = op.Kind == "bind-nest"
			b.Order = s.NextOrder
			s.Bindings[b.ID] = b
		case "unlink":
			if _, ok := s.Bindings[op.Binding.ID]; !ok {
				return reject("not-found")
			}
			delete(s.Bindings, op.Binding.ID)
		case "rebind":
			b, ok := s.Bindings[op.Binding.ID]
			if !ok {
				return reject("not-found")
			}
			if b.Consumer != op.Binding.Consumer || b.Port != op.Binding.Port {
				return reject("binding-endpoint")
			}
			b.Provider = op.Binding.Provider
			b.Capability = op.Binding.Capability
			s.Bindings[b.ID] = b
		default:
			return reject("operation")
		}
	}
	for id, n := range s.Nodes {
		if n.Parent != "" {
			if _, ok := s.Nodes[n.Parent]; !ok {
				return reject("parent")
			}
		}
		if s.Reaches(id, id) {
			return reject("nest-cycle")
		}
	}
	if issues := m.validate(s, keep); len(issues) > 0 {
		return old.Copy(), issues
	}
	s.Generation++
	return s, nil
}

func (d Definition) Port(id string) (Port, bool) {
	for _, p := range d.Ports {
		if p.ID == id {
			return p, true
		}
	}
	return Port{}, false
}
func (d Definition) Cap(id string) (Capability, bool) {
	for _, c := range d.Caps {
		if c.ID == id {
			return c, true
		}
	}
	return Capability{}, false
}
func Ordered(s State, consumer, port string) []Binding {
	var bs []Binding
	for _, b := range s.Bindings {
		if b.Consumer == consumer && b.Port == port {
			bs = append(bs, b)
		}
	}
	sort.Slice(bs, func(i, j int) bool { return bs[i].Order < bs[j].Order })
	return bs
}
func (m *Model) Accessible(s State, consumer, provider string, c Capability, local bool) bool {
	if local {
		if s.Reaches(provider, consumer) {
			return c.Down
		}
		return s.Reaches(consumer, provider) && c.Up
	}
	if !c.Public {
		return false
	}
	for ancestor, n := range s.Nodes {
		if !s.Reaches(ancestor, provider) {
			continue
		}
		if consumer == ancestor || s.Reaches(ancestor, consumer) {
			continue
		}
		if !m.Catalog[n.Definition].Export {
			return false
		}
	}
	return true
}
func (m *Model) validate(s State, keep map[string]bool) []Issue {
	var issues []Issue
	add := func(x Issue) { issues = append(issues, x) }
	build, start, resource := map[string][]string{}, map[string][]string{}, map[string][]string{}
	for id := range s.Nodes {
		build[id] = nil
		start[id] = nil
		resource[id] = nil
	}
	for id, n := range s.Nodes {
		if n.Parent != "" {
			build[n.Parent] = append(build[n.Parent], id)
			resource[id] = append(resource[id], n.Parent)
		}
	}
	for _, b := range s.Bindings {
		cn, cok := s.Nodes[b.Consumer]
		pn, pok := s.Nodes[b.Provider]
		if !cok || !pok || b.Consumer == b.Provider {
			add("binding-endpoint")
			continue
		}
		related := s.Reaches(b.Consumer, b.Provider) || s.Reaches(b.Provider, b.Consumer)
		if related != b.Local {
			add("binding-relation")
		}
		p, ok := m.Catalog[cn.Definition].Port(b.Port)
		if !ok {
			add("port")
			continue
		}
		c, ok := m.Catalog[pn.Definition].Cap(b.Capability)
		if !ok || !m.Accessible(s, b.Consumer, b.Provider, c, b.Local) {
			add("visibility")
			continue
		}
		if p.Type != c.Type {
			add("type")
		}
		if p.Contract != "" && (p.Contract != c.Contract || c.Version < p.Min || (p.Max != 0 && c.Version > p.Max)) {
			add("contract")
		}
		if p.Phase == Construction {
			build[b.Provider] = append(build[b.Provider], b.Consumer)
		}
		if p.Phase != Runtime {
			start[b.Provider] = append(start[b.Provider], b.Consumer)
			resource[b.Consumer] = append(resource[b.Consumer], b.Provider)
		}
	}
	for id, n := range s.Nodes {
		d := m.Catalog[n.Definition]
		for _, p := range d.Ports {
			count := len(Ordered(s, id, p.ID))
			if p.Required && count == 0 {
				add("required")
			}
			if !p.Many && count > 1 {
				add("cardinality")
			}
		}
		if n.Parent != "" {
			ancestorDependency := false
			for _, b := range s.Bindings {
				if b.Consumer == id {
					p, _ := d.Port(b.Port)
					if p.Phase != Runtime && s.Reaches(b.Provider, id) {
						ancestorDependency = true
					}
				}
			}
			if !ancestorDependency {
				start[id] = append(start[id], n.Parent)
			}
		}
		before, exists := m.State.Nodes[id]
		if !exists {
			continue
		}
		if n.Instance == before.Instance {
			if n.Parent != before.Parent {
				for sub, sn := range s.Nodes {
					if sub == id || s.Reaches(id, sub) {
						if on, ok := m.State.Nodes[sub]; ok && on.Instance == sn.Instance && !m.Catalog[sn.Definition].Move {
							add("reparent")
						}
					}
				}
			}
			for _, p := range d.Ports {
				if p.Phase == Runtime {
					continue
				}
				a, b := Ordered(m.State, id, p.ID), Ordered(s, id, p.ID)
				if len(a) != len(b) {
					add("retained")
					continue
				}
				for i := range a {
					if a[i] != b[i] || m.State.Nodes[a[i].Provider].Instance != s.Nodes[b[i].Provider].Instance {
						add("retained")
					}
				}
			}
		} else {
			prior := m.Catalog[before.Definition]
			if !prior.Hot || !d.Hot {
				add("hot-replace")
			}
			for sub, sn := range s.Nodes {
				if on, ok := m.State.Nodes[sub]; ok && s.Reaches(id, sub) && on.Instance == sn.Instance && (!keep[id] || !d.Keep) {
					add("keep-children")
				}
			}
			for _, c := range prior.Caps {
				if !c.Public && !c.Down && !c.Up {
					continue
				}
				after, ok := d.Cap(c.ID)
				if !ok || after.Type != c.Type || (c.Public && !after.Public) || (c.Up && !after.Up) || (c.Down && !after.Down) || after.Contract != c.Contract || after.Version < c.Version {
					add("replacement-contract")
				}
			}
		}
	}
	if !acyclic(build) || !acyclic(start) || !acyclic(resource) {
		add("lifecycle-cycle")
	}
	if len(issues) == 0 {
		for id, n := range s.Nodes {
			if old, ok := m.State.Nodes[id]; !ok || old.Instance != n.Instance {
				if m.Catalog[n.Definition].Fail != "" {
					add("prepare")
				}
			}
		}
	}
	sort.Slice(issues, func(i, j int) bool { return issues[i] < issues[j] })
	return issues
}

// Kahn elimination checks partial orders without reproducing Core's DFS.
func acyclic(edges map[string][]string) bool {
	degree := map[string]int{}
	for a, bs := range edges {
		if _, ok := degree[a]; !ok {
			degree[a] = 0
		}
		for _, b := range bs {
			degree[b]++
		}
	}
	var ready []string
	for id, n := range degree {
		if n == 0 {
			ready = append(ready, id)
		}
	}
	count := 0
	for len(ready) > 0 {
		id := ready[0]
		ready = ready[1:]
		count++
		for _, b := range edges[id] {
			degree[b]--
			if degree[b] == 0 {
				ready = append(ready, b)
			}
		}
	}
	return count == len(degree)
}

// Resources is a semantic transitive closure, not an instance refcount model.
func (m *Model) Resources(s State, tree string) map[string]bool {
	edges := map[string][]string{}
	for id, n := range s.Nodes {
		if n.Parent != "" {
			edges[id] = append(edges[id], n.Parent)
		}
	}
	for _, b := range s.Bindings {
		n := s.Nodes[b.Consumer]
		p, ok := m.Catalog[n.Definition].Port(b.Port)
		if ok && p.Phase != Runtime {
			edges[b.Consumer] = append(edges[b.Consumer], b.Provider)
		}
	}
	out := map[string]bool{}
	queue := append([]string(nil), edges[tree]...)
	for len(queue) > 0 {
		id := queue[0]
		queue = queue[1:]
		if out[id] {
			continue
		}
		out[id] = true
		queue = append(queue, edges[id]...)
	}
	return out
}

// Reaches uses child adjacency and a visited frontier, rather than following
// Core's parent pointers. The starting vertex is not itself a descendant.
func (s State) Reaches(parent, child string) bool {
	seen := map[string]bool{}
	front := []string{parent}
	for len(front) > 0 {
		at := front[0]
		front = front[1:]
		if seen[at] {
			continue
		}
		seen[at] = true
		for id, n := range s.Nodes {
			if n.Parent == at {
				if id == child {
					return true
				}
				front = append(front, id)
			}
		}
	}
	return false
}
