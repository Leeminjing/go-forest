package topology

import (
	"encoding/json"
	"fmt"
	"sort"

	"github.com/Leeminjing/go-forest/forest"
)

// canonicalManifest is owned by a single operation. Its topology, encoded bytes
// and indexes never escape into a cache or a published mutable object. A caller
// may transfer the value/bytes only when this local owner is no longer used.
type canonicalManifest struct {
	value    Manifest
	nodes    map[forest.TreeID]Tree
	bindings map[string]Binding
	keys     map[factIdentity]string
	checked  *Catalog
	encoded  []byte
	digest   string
}
type factIdentity struct {
	Definition       forest.DefinitionRef
	Contract, Digest string
}

func canonicalize(m Manifest) (*canonicalManifest, error) {
	if m.Version != FormatVersion {
		return nil, ErrUnsupportedFormat
	}
	if len(m.Trees) > 10000 || len(m.Bindings) > 50000 {
		return nil, ErrInvalidManifest
	}
	n := Empty()
	n.Trees = append(n.Trees, m.Trees...)
	n.Bindings = append(n.Bindings, m.Bindings...)
	sort.Slice(n.Trees, func(i, j int) bool { return n.Trees[i].ID < n.Trees[j].ID })
	sort.Slice(n.Bindings, func(i, j int) bool { return n.Bindings[i].Ordinal < n.Bindings[j].Ordinal })
	nodes := map[forest.TreeID]Tree{}
	for _, t := range n.Trees {
		if !validID(string(t.ID)) || (t.ParentID != "" && !validID(string(t.ParentID))) || !validID(t.Definition.TypeID) || !validID(t.Definition.Version) || !validID(t.ContractKey) || !validDigest(t.ImplementationDigest) {
			return nil, fmt.Errorf("%w: tree %q", ErrInvalidManifest, t.ID)
		}
		if _, ok := nodes[t.ID]; ok {
			return nil, ErrInvalidManifest
		}
		nodes[t.ID] = t
	}
	// A color walk is iterative so valid deep forests do not consume the Go stack.
	done := map[forest.TreeID]bool{}
	for _, t := range n.Trees {
		path := map[forest.TreeID]bool{}
		for id := t.ID; id != "" && !done[id]; id = nodes[id].ParentID {
			if _, ok := nodes[id]; !ok {
				return nil, fmt.Errorf("%w: missing parent %q", ErrInvalidManifest, id)
			}
			if path[id] {
				return nil, fmt.Errorf("%w: Nest cycle", ErrInvalidManifest)
			}
			path[id] = true
		}
		for id := range path {
			done[id] = true
		}
	}
	ids := map[string]bool{}
	for i, b := range n.Bindings {
		if !validID(b.ID) || !validID(string(b.Consumer)) || !validID(string(b.Provider)) || !validID(string(b.Port)) || !validID(string(b.Capability)) || ids[b.ID] || b.Ordinal != uint64(i+1) || (b.Kind != "link" && b.Kind != "nest") {
			return nil, ErrInvalidManifest
		}
		if _, ok := nodes[b.Consumer]; !ok {
			return nil, ErrInvalidManifest
		}
		if _, ok := nodes[b.Provider]; !ok {
			return nil, ErrInvalidManifest
		}
		ids[b.ID] = true
	}
	data, err := json.Marshal(n)
	if err != nil || len(data) > MaxManifestBytes {
		return nil, ErrInvalidManifest
	}
	return &canonicalManifest{value: n, nodes: nodes, encoded: data}, nil
}
func (m *canonicalManifest) sum() string {
	if m.digest == "" {
		m.digest = hash("forest-topology/manifest/v1", m.encoded)
	}
	return m.digest
}
func (m *canonicalManifest) bindingIndex() map[string]Binding {
	if m.bindings == nil {
		m.bindings = bindingIndex(m.value)
	}
	return m.bindings
}
func (m *canonicalManifest) treeKey(t Tree) string {
	id := factIdentity{t.Definition, t.ContractKey, t.ImplementationDigest}
	if s, ok := m.keys[id]; ok {
		return s
	}
	if m.keys == nil {
		m.keys = map[factIdentity]string{}
	}
	s := factKey(DefinitionFacts{Definition: t.Definition, ContractKey: t.ContractKey, ImplementationDigest: t.ImplementationDigest})
	m.keys[id] = s
	return s
}
