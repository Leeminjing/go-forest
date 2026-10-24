// Package compose expands reusable composite trees into ordinary Forest plans.
// It owns no runtime, registry, lifecycle, or invocation scope.
package compose

import (
	"errors"
	"fmt"
	"strings"
	"unicode/utf8"

	"github.com/Leeminjing/go-forest/forest"
)

var (
	ErrInvalidBlueprint = errors.New("invalid blueprint")
	ErrInvalidName      = errors.New("invalid composite name")
	ErrBoundary         = errors.New("composite boundary violation")
)

// Spec uses names local to one template. An empty endpoint or Parent is its root.
// Definitions must already be registered when the expanded plan is applied.
type Spec struct {
	Root     forest.DefinitionRef
	Trees    []Tree
	Bindings []forest.Link
}

// Tree selects exactly one Definition or previously compiled Composite.
// Parent can name an ordinary local tree, but not a Composite's internals.
type Tree struct {
	ID         forest.TreeID
	Parent     forest.TreeID
	Definition forest.DefinitionRef
	Composite  *Blueprint
}

// Blueprint is immutable after New and safe for concurrent instantiation.
// Its zero value is invalid. Nested templates can only reference earlier,
// immutable Blueprints, so recursive template cycles cannot be constructed.
type Blueprint struct {
	root     forest.DefinitionRef
	trees    []Tree
	bindings []forest.Link
}

func New(spec Spec) (*Blueprint, error) {
	b := snapshot(&Blueprint{root: spec.Root, trees: spec.Trees, bindings: spec.Bindings}, make(map[*Blueprint]*Blueprint))
	if err := b.validate(); err != nil {
		return nil, err
	}
	return b, nil
}

// Capture nested declarations too: replacing a caller-owned Blueprint variable
// after New must not mutate an already compiled enclosing template.
func snapshot(source *Blueprint, copies map[*Blueprint]*Blueprint) *Blueprint {
	if copy, ok := copies[source]; ok {
		return copy
	}
	copy := &Blueprint{root: source.root, trees: append([]Tree(nil), source.trees...), bindings: append([]forest.Link(nil), source.bindings...)}
	copies[source] = copy
	for n := range copy.trees {
		if child := copy.trees[n].Composite; child != nil {
			copy.trees[n].Composite = snapshot(child, copies)
		}
	}
	return copy
}

func validName(name string) bool {
	return name != "" && name != "." && name != ".." && !strings.Contains(name, "/") && utf8.ValidString(name)
}
func validDefinition(ref forest.DefinitionRef) bool { return ref.TypeID != "" && ref.Version != "" }
func invalid(path string, cause error) error {
	return fmt.Errorf("%w: %s: %w", ErrInvalidBlueprint, path, cause)
}

func (b *Blueprint) validate() error {
	if b == nil || !validDefinition(b.root) {
		return invalid("root definition", forest.ErrInvalidDefinition)
	}
	nodes := map[forest.TreeID]Tree{"": {}}
	for _, tree := range b.trees {
		if !validName(string(tree.ID)) {
			return invalid(string(tree.ID), ErrInvalidName)
		}
		if _, exists := nodes[tree.ID]; exists {
			return invalid(string(tree.ID), errors.New("duplicate tree name"))
		}
		hasDefinition := tree.Definition != (forest.DefinitionRef{})
		if hasDefinition == (tree.Composite != nil) {
			return invalid(string(tree.ID), errors.New("choose exactly one definition or composite"))
		}
		if hasDefinition && !validDefinition(tree.Definition) {
			return invalid(string(tree.ID), forest.ErrInvalidDefinition)
		}
		if tree.Composite != nil && !validDefinition(tree.Composite.root) {
			return invalid(string(tree.ID), ErrInvalidBlueprint)
		}
		nodes[tree.ID] = tree
	}
	for _, tree := range b.trees {
		if strings.Contains(string(tree.Parent), "/") {
			return invalid(string(tree.ID)+" parent", ErrBoundary)
		}
		parent, exists := nodes[tree.Parent]
		if !exists {
			return invalid(string(tree.ID)+" parent", forest.ErrNotFound)
		}
		if parent.Composite != nil {
			return invalid(string(tree.ID)+" parent", ErrBoundary)
		}
	}
	state := map[forest.TreeID]uint8{"": 2}
	var visit func(forest.TreeID) error
	visit = func(id forest.TreeID) error {
		if state[id] == 1 {
			return invalid(string(id), forest.ErrNestCycle)
		}
		if state[id] == 2 {
			return nil
		}
		state[id] = 1
		if err := visit(nodes[id].Parent); err != nil {
			return err
		}
		state[id] = 2
		return nil
	}
	for _, tree := range b.trees {
		if err := visit(tree.ID); err != nil {
			return err
		}
	}
	bindings := map[string]bool{}
	for _, link := range b.bindings {
		if !validName(link.ID) {
			return invalid(link.ID, ErrInvalidName)
		}
		if bindings[link.ID] {
			return invalid(link.ID, errors.New("duplicate binding name"))
		}
		bindings[link.ID] = true
		for _, endpoint := range []forest.TreeID{link.Consumer, link.Provider} {
			if strings.Contains(string(endpoint), "/") {
				return invalid(link.ID, ErrBoundary)
			}
			if _, exists := nodes[endpoint]; !exists {
				return invalid(link.ID+" endpoint", forest.ErrNotFound)
			}
		}
		if link.Consumer == link.Provider || link.Port == "" || link.Capability == "" {
			return invalid(link.ID, forest.ErrInvalidLink)
		}
	}
	return nil
}
