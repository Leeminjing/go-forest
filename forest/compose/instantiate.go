package compose

import (
	"fmt"

	"github.com/Leeminjing/go-forest/forest"
)

// Instance describes a proposed placement, not a running physical instance.
// Root identity and all management paths are stable across its subsequent plans.
type Instance struct {
	root       forest.TreeID
	trees      map[string]forest.TreeID
	links      map[string]string
	operations []forest.Operation
}

// Instantiate is pure: it neither registers definitions nor mutates a Forest.
// An empty parent plants an independent root; otherwise the root is nested.
func (b *Blueprint) Instantiate(root, parent forest.TreeID) (*Instance, error) {
	if b == nil || !validDefinition(b.root) {
		return nil, ErrInvalidBlueprint
	}
	if !validName(string(root)) {
		return nil, fmt.Errorf("%w: root %q", ErrInvalidName, root)
	}
	i := &Instance{root: root, trees: map[string]forest.TreeID{}, links: map[string]string{}}
	parents := map[forest.TreeID]forest.TreeID{}
	var bindings []forest.Link
	var expand func(*Blueprint, forest.TreeID, forest.TreeID, string)
	expand = func(template *Blueprint, scope, owner forest.TreeID, path string) {
		i.trees[path] = scope
		parents[scope] = owner
		if owner == "" {
			i.operations = append(i.operations, forest.AddRoot(scope, template.root))
		} else {
			i.operations = append(i.operations, forest.Nest(owner, scope, template.root))
		}
		for _, tree := range template.trees {
			id := forest.TreeID(join(string(scope), string(tree.ID)))
			localPath := join(path, string(tree.ID))
			parentID := scope
			if tree.Parent != "" {
				parentID = forest.TreeID(join(string(scope), string(tree.Parent)))
			}
			if tree.Composite != nil {
				expand(tree.Composite, id, parentID, localPath)
			} else {
				i.trees[localPath] = id
				parents[id] = parentID
				i.operations = append(i.operations, forest.Nest(parentID, id, tree.Definition))
			}
		}
		for _, local := range template.bindings {
			link := local
			link.ID = string(scope) + "/@" + local.ID
			link.Consumer = scope
			if local.Consumer != "" {
				link.Consumer = forest.TreeID(join(string(scope), string(local.Consumer)))
			}
			link.Provider = scope
			if local.Provider != "" {
				link.Provider = forest.TreeID(join(string(scope), string(local.Provider)))
			}
			i.links[join(path, local.ID)] = link.ID
			bindings = append(bindings, link)
		}
	}
	expand(b, root, parent, "")
	for _, link := range bindings {
		if ancestor(parents, link.Consumer, link.Provider) || ancestor(parents, link.Provider, link.Consumer) {
			i.operations = append(i.operations, forest.BindNest(link))
		} else {
			i.operations = append(i.operations, forest.Connect(link))
		}
	}
	return i, nil
}

func join(prefix, name string) string {
	if prefix == "" {
		return name
	}
	return prefix + "/" + name
}
func ancestor(parents map[forest.TreeID]forest.TreeID, parent, child forest.TreeID) bool {
	// External parents are not part of this placement. They may name a proposed
	// internal ID; stop on repeats and let Apply validate that final topology.
	seen := map[forest.TreeID]bool{}
	for next := parents[child]; next != ""; next = parents[next] {
		if next == parent {
			return true
		}
		if seen[next] {
			return false
		}
		seen[next] = true
	}
	return false
}

func (i *Instance) Root() forest.TreeID { return i.root }
func (i *Instance) Tree(path string) (forest.TreeID, error) {
	id, ok := i.trees[path]
	if !ok {
		return "", fmt.Errorf("%w: tree path %q", forest.ErrNotFound, path)
	}
	return id, nil
}
func (i *Instance) Link(path string) (string, error) {
	id, ok := i.links[path]
	if !ok {
		return "", fmt.Errorf("%w: binding path %q", forest.ErrNotFound, path)
	}
	return id, nil
}

// Plan returns a detached operation slice for combining with other plans and
// external connections. Applying it twice without removal is an ID conflict.
func (i *Instance) Plan(baseGeneration uint64) forest.ChangePlan {
	return forest.ChangePlan{BaseGeneration: baseGeneration, Operations: append([]forest.Operation(nil), i.operations...)}
}
