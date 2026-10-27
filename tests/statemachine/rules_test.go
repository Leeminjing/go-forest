package statemachine

import (
	"errors"
	"fmt"
	"testing"

	"github.com/Leeminjing/go-forest/forest"
	m "github.com/Leeminjing/go-forest/tests/internal/topologymodel"
)

type ruleStep struct {
	ok    bool
	ops   []m.Op
	cause error
}
type ruleCase struct {
	name  string
	steps []ruleStep
}

func root(id, d string) m.Op { return m.Op{Kind: "add-root", Tree: id, Definition: d} }
func nested(parent, id, d string) m.Op {
	return m.Op{Kind: "nest", Parent: parent, Tree: id, Definition: d}
}
func link(id, c, p, target string, local bool) m.Op {
	kind := "link"
	if local {
		kind = "bind-nest"
	}
	return m.Op{Kind: kind, Binding: m.Binding{ID: id, Consumer: c, Port: p, Provider: target, Capability: "value"}}
}
func replacement(id, d string, keep bool) m.Op {
	return m.Op{Kind: "replace", Tree: id, Definition: d, KeepChildren: keep}
}
func yes(ops ...m.Op) ruleStep           { return ruleStep{ok: true, ops: ops} }
func no(err error, ops ...m.Op) ruleStep { return ruleStep{ops: ops, cause: err} }
func ruleCases() []ruleCase {
	out := []ruleCase{
		{"empty-and-nonempty-noop", []ruleStep{yes(), yes(root("a", "provider-v1")), yes(m.Op{Kind: "unnest", Tree: "a"}), yes(root("temporary", "provider-v1"), m.Op{Kind: "remove", Tree: "temporary"})}},
		{"nest-cycle-and-parent-removal", []ruleStep{yes(root("a", "provider-v1"), nested("a", "b", "provider-v1")), no(forest.ErrNestCycle, m.Op{Kind: "move", Tree: "a", Parent: "b"}), no(forest.ErrDependencyInUse, m.Op{Kind: "remove", Tree: "a"}), yes(m.Op{Kind: "unnest", Tree: "b"}, m.Op{Kind: "remove", Tree: "a"}), yes(m.Op{Kind: "remove-subtree", Tree: "b"})}},
		{"required-cardinality-type", []ruleStep{no(forest.ErrDependencyInUse, root("c", "runtime-required")), yes(root("p", "provider-v1"), root("q", "other"), root("c", "runtime-required"), link("l", "c", "in", "p", false)), no(forest.ErrPortAlreadyBound, link("second", "c", "in", "p", false)), no(forest.ErrContractMismatch, m.Op{Kind: "rebind", Binding: m.Binding{ID: "l", Consumer: "c", Port: "in", Provider: "q", Capability: "value"}}), no(forest.ErrDependencyInUse, m.Op{Kind: "unlink", Binding: m.Binding{ID: "l"}}), yes(m.Op{Kind: "remove", Tree: "c"}, m.Op{Kind: "remove", Tree: "p"})}},
		{"export-and-local-kind", []ruleStep{yes(root("a", "sealed"), nested("a", "p", "provider-v1"), root("c", "runtime-optional")), no(forest.ErrMissingCapability, link("l", "c", "in", "p", false)), yes(m.Op{Kind: "unnest", Tree: "p"}, link("l", "c", "in", "p", false)), no(forest.ErrInvalidLink, m.Op{Kind: "move", Tree: "c", Parent: "p"}), yes(m.Op{Kind: "move", Tree: "c", Parent: "p"}, m.Op{Kind: "unlink", Binding: m.Binding{ID: "l"}}, link("l", "c", "in", "p", true))}},
		{"local-visibility-and-unrelated", []ruleStep{yes(root("p", "private"), nested("p", "c", "runtime-optional")), no(forest.ErrMissingCapability, link("l", "c", "in", "p", true)), yes(replacement("p", "provider-v1", true), link("l", "c", "in", "p", true)), no(forest.ErrInvalidLink, link("self", "c", "in", "c", true))}},
		{"ancestor-direction", []ruleStep{yes(root("c", "runtime-optional"), nested("c", "p", "up"), link("l", "c", "in", "p", true))}},
		{"descendant-direction", []ruleStep{yes(root("p", "down"), nested("p", "c", "runtime-optional"), link("l", "c", "in", "p", true))}},
		{"semantic-contract", []ruleStep{no(forest.ErrContractMismatch, root("p", "provider-v1"), root("c", "semantic"), link("l", "c", "in", "p", false)), yes(root("p", "provider-v2"), root("c", "semantic"), link("l", "c", "in", "p", false)), no(forest.ErrContractMismatch, replacement("p", "provider-v1", false))}},
		{"move-and-keep", []ruleStep{yes(root("a", "provider-v1"), root("b", "provider-v1"), nested("a", "fixed", "fixed")), no(forest.ErrReparentUnsupported, m.Op{Kind: "move", Tree: "fixed", Parent: "b"}), no(forest.ErrUnsupportedHotReplace, replacement("a", "provider-v1", false)), yes(replacement("a", "provider-v2", true)), no(forest.ErrUnsupportedHotReplace, replacement("a", "no-keep", true)), yes(m.Op{Kind: "remove-subtree", Tree: "a"}), yes(root("a", "provider-v1"))}},
		{"exclusive-replacement", []ruleStep{yes(root("p", "exclusive")), no(forest.ErrUnsupportedHotReplace, replacement("p", "provider-v2", false))}},
		{"runtime-cycle", []ruleStep{yes(root("a", "runtime-required"), root("b", "runtime-required"), link("ab", "a", "in", "b", false), link("ba", "b", "in", "a", false))}},
		{"many-runtime-order", []ruleStep{yes(root("a", "provider-v1"), root("b", "provider-v2"), root("c", "runtime-many"), link("first", "c", "in", "a", false), link("second", "c", "in", "b", false)), yes(m.Op{Kind: "rebind", Binding: m.Binding{ID: "first", Consumer: "c", Port: "in", Provider: "b", Capability: "value"}}), yes(m.Op{Kind: "unlink", Binding: m.Binding{ID: "first"}}, link("first", "c", "in", "a", false))}},
	}
	for _, phase := range []string{"construction", "startup"} {
		out = append(out, ruleCase{phase + "-retention", []ruleStep{yes(root("a", "provider-v1"), root("b", "provider-v2"), root("c", phase+"-many"), link("first", "c", "in", "a", false), link("second", "c", "in", "b", false)), no(forest.ErrDependencyInUse, m.Op{Kind: "unlink", Binding: m.Binding{ID: "first"}}, link("first", "c", "in", "a", false)), no(forest.ErrDependencyInUse, replacement("a", "provider-v2", false)), yes(replacement("c", phase+"-many", false), m.Op{Kind: "unlink", Binding: m.Binding{ID: "first"}}, link("first", "c", "in", "a", false), replacement("a", "provider-v2", false))}})
		out = append(out, ruleCase{phase + "-id-reuse", []ruleStep{yes(root("p", "provider-v1"), root("a", phase+"-optional"), root("b", phase+"-optional"), link("reuse", "a", "in", "p", false)), no(forest.ErrDependencyInUse, m.Op{Kind: "remove", Tree: "a"}, link("reuse", "b", "in", "p", false)), yes(m.Op{Kind: "remove", Tree: "a"}, replacement("b", phase+"-optional", false), link("reuse", "b", "in", "p", false))}})
		out = append(out, ruleCase{phase + "-cycle", []ruleStep{no(forest.ErrPrepareFailed, root("a", phase+"-required"), root("b", phase+"-required"), link("ab", "a", "in", "b", false), link("ba", "b", "in", "a", false))}})
		out = append(out, ruleCase{phase + "-local", []ruleStep{yes(root("p", "provider-v1"), nested("p", "c", phase+"-required"), link("l", "c", "in", "p", true))}})
	}
	for _, stage := range []string{"build", "start", "readiness"} {
		out = append(out, ruleCase{"failure-" + stage, []ruleStep{yes(root("old", "provider-v1")), no(forest.ErrPrepareFailed, root("p", "fail-"+stage))}})
	}
	return out
}

func knownTrace(c ruleCase) m.Trace {
	trace := m.Trace{Name: c.name, Format: m.FormatVersion, Generator: m.GeneratorVersion, Catalog: Catalog()}
	model := m.New(trace.Catalog)
	for i, s := range c.steps {
		p := m.Plan{Base: model.State.Generation, Ops: s.ops}
		want := s.ok
		trace.Steps = append(trace.Steps, m.Step{ID: fmt.Sprintf("rule-%d", i), Base: m.BaseIntent{Kind: "current"}, Plan: p, Expect: &want})
		next, _ := model.Next(p)
		model.State = next
	}
	return trace
}
func TestModelKnownRules(t *testing.T) {
	for _, c := range ruleCases() {
		t.Run(c.name, func(t *testing.T) {
			trace := knownTrace(c)
			h := newHarness(t, trace.Catalog, trace.IDs())
			for i, step := range trace.Steps {
				_, issues := h.model.Next(step.Plan)
				if (len(issues) == 0) != *step.Expect {
					t.Fatalf("independent golden rule %d: want=%t issues=%v", i, *step.Expect, issues)
				}
				h.step(step.Plan)
				if want := c.steps[i].cause; want != nil && !errors.Is(h.lastApplyErr, want) {
					t.Fatalf("step %d expected errors.Is(%v), got %v", i, want, h.lastApplyErr)
				}
			}
		})
	}
}
