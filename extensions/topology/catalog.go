package topology

import (
	"fmt"
	"reflect"
	"sort"

	"github.com/Leeminjing/go-forest/forest"
)

// Registration is a host assertion about code/configuration, not a serialization
// of Build. Never derive ImplementationDigest from a secret value.
type Registration struct {
	Definition                        forest.Definition
	ContractKey, ImplementationDigest string
}
type Catalog struct {
	entries map[forest.DefinitionRef]Registration
}

func NewCatalog(entries []Registration) (*Catalog, error) {
	c := &Catalog{entries: map[forest.DefinitionRef]Registration{}}
	for _, r := range entries {
		ref := r.Definition.Ref()
		if !validID(ref.TypeID) || !validID(ref.Version) || r.Definition.Build == nil || !validID(r.ContractKey) || !validDigest(r.ImplementationDigest) {
			return nil, ErrDefinitionMismatch
		}
		if _, ok := c.entries[ref]; ok {
			return nil, ErrDefinitionMismatch
		}
		r.Definition.Ports = append([]forest.PortDescriptor(nil), r.Definition.Ports...)
		r.Definition.Capabilities = append([]forest.CapabilityDescriptor(nil), r.Definition.Capabilities...)
		c.entries[ref] = r
	}
	return c, nil
}
func (c *Catalog) check(m Manifest) error {
	if c == nil {
		return ErrDefinitionUnavailable
	}
	for _, t := range m.Trees {
		r, ok := c.entries[t.Definition]
		if !ok {
			return &Error{Stage: "catalog", TreeID: string(t.ID), Cause: ErrDefinitionUnavailable}
		}
		if r.ContractKey != t.ContractKey || r.ImplementationDigest != t.ImplementationDigest {
			return &Error{Stage: "catalog", TreeID: string(t.ID), Cause: ErrDefinitionMismatch}
		}
	}
	return nil
}
func (c *Catalog) register(f *forest.Forest) error {
	refs := make([]forest.DefinitionRef, 0, len(c.entries))
	for ref := range c.entries {
		refs = append(refs, ref)
	}
	sort.Slice(refs, func(i, j int) bool {
		if refs[i].TypeID != refs[j].TypeID {
			return refs[i].TypeID < refs[j].TypeID
		}
		return refs[i].Version < refs[j].Version
	})
	for _, ref := range refs {
		if e := f.Register(c.entries[ref].Definition); e != nil {
			return e
		}
	}
	return nil
}
func Capture(s forest.Snapshot, c *Catalog) (Manifest, error) {
	if c == nil {
		return Manifest{}, ErrDefinitionUnavailable
	}
	m := Empty()
	for id, t := range s.Trees {
		r, ok := c.entries[forest.DefinitionRef{TypeID: t.TypeID, Version: t.Version}]
		if !ok {
			return m, fmt.Errorf("%w: %s", ErrDefinitionUnavailable, id)
		}
		if id != t.ID || !reflect.DeepEqual(t.Ports, r.Definition.Ports) || !reflect.DeepEqual(t.Capabilities, r.Definition.Capabilities) {
			return m, fmt.Errorf("%w: %s", ErrDefinitionMismatch, id)
		}
		m.Trees = append(m.Trees, Tree{ID: id, ParentID: t.ParentID, Definition: r.Definition.Ref(), ContractKey: r.ContractKey, ImplementationDigest: r.ImplementationDigest})
	}
	all := make([]forest.Binding, 0, len(s.Links)+len(s.NestBindings))
	seenOrder := map[uint64]bool{}
	for _, group := range []struct {
		values map[string]forest.LinkInfo
		local  bool
	}{{s.Links, false}, {s.NestBindings, true}} {
		for id, b := range group.values {
			if id != b.ID || b.Local != group.local || b.Order == 0 || seenOrder[b.Order] {
				return m, ErrInvalidManifest
			}
			seenOrder[b.Order] = true
			all = append(all, b.Binding)
		}
	}
	sort.Slice(all, func(i, j int) bool { return all[i].Order < all[j].Order })
	for i, b := range all {
		kind := "link"
		if b.Local {
			kind = "nest"
		}
		m.Bindings = append(m.Bindings, Binding{ID: b.ID, Consumer: b.Consumer, Port: b.Port, Provider: b.Provider, Capability: b.Capability, Kind: kind, Ordinal: uint64(i + 1)})
	}
	return Normalize(m)
}
