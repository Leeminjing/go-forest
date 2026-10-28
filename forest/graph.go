package forest

import (
	"errors"
	"fmt"
	"sort"
)

// ancestor tolerates a malformed candidate so invalid plans cannot loop.
func ancestor(g *graph, parent, child TreeID) bool {
	seen := map[TreeID]bool{}
	for n := g.nodes[child]; n != nil && n.parent != ""; n = g.nodes[n.parent] {
		if n.parent == parent {
			return true
		}
		if seen[n.parent] {
			return false
		}
		seen[n.parent] = true
	}
	return false
}

func validateGraph(g *graph, actual bool) error {
	for _, id := range sortedIDs(g) {
		n := g.nodes[id]
		if n.parent != "" && g.nodes[n.parent] == nil {
			return failure("nest", id, "", errors.Join(ErrDependencyInUse, ErrNotFound))
		}
		if ancestor(g, id, id) {
			return failure("nest", id, "", ErrNestCycle)
		}
	}
	counts := map[struct {
		tree TreeID
		port PortID
	}]int{}
	for _, b := range sortedBindings(g) {
		consumer, provider := g.nodes[b.Consumer], g.nodes[b.Provider]
		if consumer == nil || provider == nil || consumer == provider {
			return failure("link", b.Consumer, b.Port, ErrInvalidLink)
		}
		related := ancestor(g, b.Consumer, b.Provider) || ancestor(g, b.Provider, b.Consumer)
		if related != b.Local {
			return failure("link", b.Consumer, b.Port, ErrInvalidLink)
		}
		p, ok := portOf(consumer.definition, b.Port)
		if !ok {
			return failure("link-port", b.Consumer, b.Port, ErrInvalidLink)
		}
		c, ok := capOf(provider.definition, b.Capability)
		if !ok || !accessible(g, b.Consumer, b.Provider, c, b.Local) {
			return failure("link-access", b.Consumer, b.Port, ErrMissingCapability)
		}
		if p.ContractID != "" && (p.ContractID != c.Contract.ID || c.Contract.Version < p.MinVersion || (p.MaxVersion != 0 && c.Contract.Version > p.MaxVersion)) {
			return failure("link-contract", b.Consumer, b.Port, ErrContractMismatch)
		}
		if actual && !compatible(typeOf(provider.instance.capabilities[c.ID]), p.Type) {
			return failure("link-type", b.Consumer, b.Port, ErrContractMismatch)
		}
		key := struct {
			tree TreeID
			port PortID
		}{b.Consumer, b.Port}
		counts[key]++
		if !p.Many && counts[key] > 1 {
			return failure("link-cardinality", b.Consumer, b.Port, ErrPortAlreadyBound)
		}
	}
	for _, id := range sortedIDs(g) {
		for _, p := range g.nodes[id].definition.Ports {
			if p.Required && counts[struct {
				tree TreeID
				port PortID
			}{id, p.ID}] == 0 {
				return failure("dependencies", id, p.ID, errors.Join(ErrDependencyInUse, ErrMissingCapability))
			}
		}
	}
	return nil
}

func accessible(g *graph, consumer, provider TreeID, c CapabilityDescriptor, local bool) bool {
	if local {
		if ancestor(g, provider, consumer) {
			return c.Visibility&Descendants != 0
		}
		return c.Visibility&Ancestors != 0
	}
	if c.Visibility&Public == 0 {
		return false
	}
	// Every crossed ownership boundary must explicitly export its descendants.
	for n := g.nodes[provider]; n != nil && n.parent != ""; n = g.nodes[n.parent] {
		parent := g.nodes[n.parent]
		if parent == nil {
			return false
		}
		if consumer != "" && (consumer == parent.id || ancestor(g, parent.id, consumer)) {
			break
		}
		if !parent.definition.ExportChildren {
			return false
		}
	}
	return true
}

func sortedIDs(g *graph) []TreeID {
	ids := make([]TreeID, 0, len(g.nodes))
	for id := range g.nodes {
		ids = append(ids, id)
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	return ids
}

// indexRoutes owns its map and slices for this graph alone. Store descriptors,
// not capability objects: resolution must still check this graph's instances.
func (g *graph) indexRoutes() {
	g.routes = make(map[routeKey][]Binding)
	for _, b := range sortedBindings(g) {
		key := routeKey{consumer: b.Consumer, port: b.Port}
		g.routes[key] = append(g.routes[key], b)
	}
}

func sortedBindings(g *graph) []Binding {
	bs := make([]Binding, 0, len(g.bindings))
	for _, b := range g.bindings {
		bs = append(bs, b)
	}
	sort.Slice(bs, func(i, j int) bool { return bs[i].Order < bs[j].Order })
	return bs
}

// Ownership plus explicit lifecycle resources must have a safe release order.
// Runtime call cycles are deliberately excluded from this graph.
func validateResourceOrder(g *graph) error {
	deps := map[TreeID][]TreeID{}
	for id, n := range g.nodes {
		if n.parent != "" {
			deps[id] = append(deps[id], n.parent)
		}
	}
	for _, b := range g.bindings {
		p, _ := portOf(g.nodes[b.Consumer].definition, b.Port)
		if p.Phase != Runtime {
			deps[b.Consumer] = append(deps[b.Consumer], b.Provider)
		}
	}
	state := map[TreeID]uint8{}
	var visit func(TreeID) error
	visit = func(id TreeID) error {
		if state[id] == 1 {
			return failure("resource-order", id, "", errors.Join(ErrPrepareFailed, ErrDependencyInUse))
		}
		if state[id] == 2 {
			return nil
		}
		state[id] = 1
		for _, dep := range deps[id] {
			if err := visit(dep); err != nil {
				return err
			}
		}
		state[id] = 2
		return nil
	}
	for _, id := range sortedIDs(g) {
		if err := visit(id); err != nil {
			return err
		}
	}
	return nil
}

// lifecycleOrder sorts prerequisites first. Runtime bindings never contribute
// edges. Explicit startup edges override the parent's default child-first start.
func lifecycleOrder(g *graph, phase Phase) ([]TreeID, error) {
	deps := map[TreeID]map[TreeID]bool{}
	for id := range g.nodes {
		deps[id] = map[TreeID]bool{}
	}
	for _, b := range g.bindings {
		p, _ := portOf(g.nodes[b.Consumer].definition, b.Port)
		if p.Phase == Construction || (phase == Startup && p.Phase == Startup) {
			deps[b.Consumer][b.Provider] = true
		}
	}
	for id, n := range g.nodes {
		if n.parent == "" {
			continue
		}
		if phase == Construction {
			deps[id][n.parent] = true
		} else {
			// A declared dependency on an ancestor requires that ancestor first.
			needsAncestor := false
			for dep := range deps[id] {
				if dep == n.parent || ancestor(g, dep, id) {
					needsAncestor = true
				}
			}
			if !needsAncestor {
				deps[n.parent][id] = true
			}
		}
	}
	state := map[TreeID]uint8{}
	var result []TreeID
	var visit func(TreeID) error
	visit = func(id TreeID) error {
		if state[id] == 1 {
			return failure("lifecycle-order", id, "", fmt.Errorf("%w: construction/start dependency cycle", ErrPrepareFailed))
		}
		if state[id] == 2 {
			return nil
		}
		state[id] = 1
		keys := make([]TreeID, 0, len(deps[id]))
		for dep := range deps[id] {
			keys = append(keys, dep)
		}
		sort.Slice(keys, func(i, j int) bool { return keys[i] < keys[j] })
		for _, dep := range keys {
			if err := visit(dep); err != nil {
				return err
			}
		}
		state[id] = 2
		result = append(result, id)
		return nil
	}
	for _, id := range sortedIDs(g) {
		if err := visit(id); err != nil {
			return nil, err
		}
	}
	return result, nil
}
