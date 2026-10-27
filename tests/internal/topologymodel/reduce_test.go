package topologymodel_test

import (
	"context"
	m "github.com/Leeminjing/go-forest/tests/internal/topologymodel"
	"testing"
)

func TestReducerPreservesFailureAndVersionIntent(t *testing.T) {
	trace := m.Trace{Format: m.FormatVersion, Catalog: []m.Definition{{ID: "safe", Hot: true, Move: true}}, Steps: []m.Step{
		{ID: "noise", Base: m.BaseIntent{Kind: "current"}, Plan: m.Plan{Ops: []m.Op{{Kind: "add-root", Tree: "noise", Definition: "safe"}}}},
		{ID: "target", Base: m.BaseIntent{Kind: "current"}, Plan: m.Plan{Base: 1, Ops: []m.Op{{Kind: "add-root", Tree: "kept", Definition: "safe"}, {Kind: "unnest", Tree: "noise"}}}},
	}}
	predicate := func(candidate m.Trace) bool {
		for _, s := range candidate.Steps {
			for _, o := range s.Plan.Ops {
				if s.ID == "target" && o.Kind == "add-root" && o.Tree == "kept" {
					return true
				}
			}
		}
		return false
	}
	result := m.Reduce(context.Background(), trace, 200, predicate)
	if !result.Minimal || len(result.Trace.Steps) != 1 || len(result.Trace.Steps[0].Plan.Ops) != 1 || result.Trace.Steps[0].Plan.Base != 0 {
		t.Fatal(result)
	}
}

func TestReductionBudgetAndMissingVersionAnchorsFailClosed(t *testing.T) {
	trace := m.Trace{Format: m.FormatVersion, Catalog: []m.Definition{{ID: "safe", Version: "1", Hot: true, Move: true}}, Steps: []m.Step{
		{ID: "anchor", Base: m.BaseIntent{Kind: "current"}, Plan: m.Plan{Ops: []m.Op{{Kind: "add-root", Tree: "a", Definition: "safe"}}}},
		{ID: "target", Base: m.BaseIntent{Kind: "step", Step: "anchor"}, Plan: m.Plan{Base: 1, Ops: []m.Op{{Kind: "unnest", Tree: "a"}}}},
	}}
	missing := m.Clone(trace)
	missing.Steps = missing.Steps[1:]
	if _, err := m.Rebase(missing); err == nil {
		t.Fatal("removed successful version anchor silently repaired")
	}
	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	result := m.Reduce(canceled, trace, 100, func(m.Trace) bool { t.Fatal("canceled reducer executed candidate"); return false })
	if result.Minimal || result.Attempts != 0 {
		t.Fatal(result)
	}
	result = m.Reduce(context.Background(), trace, 1, func(m.Trace) bool { return false })
	if result.Minimal || result.Attempts != 1 {
		t.Fatal("budget exhaustion claimed minimal", result)
	}
}

func TestReducerVisitsEveryConcurrentPlanLocation(t *testing.T) {
	for _, race := range []bool{false, true} {
		p := m.Plan{Ops: []m.Op{{Kind: "add-root", Tree: "target", Definition: "safe"}, {Kind: "add-root", Tree: "noise", Definition: "safe"}}}
		action := m.Action{ID: "target-action", Kind: "apply", Plan: &p, Base: m.BaseIntent{Kind: "current"}}
		if race {
			action = m.Action{ID: "target-action", Kind: "prepared-race", Plans: []m.Plan{p, {Ops: []m.Op{{Kind: "add-root", Tree: "other", Definition: "safe"}}}}, Base: m.BaseIntent{Kind: "current"}}
		}
		trace := m.Trace{Format: m.FormatVersion, Catalog: []m.Definition{{ID: "safe", Version: "1", Hot: true, Move: true}}, Actions: []m.Action{action}}
		predicate := func(t m.Trace) bool {
			for _, a := range t.Actions {
				plans := a.Plans
				if a.Plan != nil {
					plans = append(append([]m.Plan(nil), plans...), *a.Plan)
				}
				for _, p := range plans {
					for _, o := range p.Ops {
						if o.Tree == "target" {
							return true
						}
					}
				}
			}
			return false
		}
		result := m.Reduce(context.Background(), trace, 200, predicate)
		if !result.Minimal || len(result.Trace.Actions) != 1 {
			t.Fatal(result)
		}
		got := result.Trace.Actions[0].Plan
		if race {
			got = &result.Trace.Actions[0].Plans[0]
		}
		if len(got.Ops) != 1 || got.Ops[0].Tree != "target" {
			t.Fatal("false concurrent minimality", result)
		}
	}
}

func TestReducerSimplifiesConcurrentOperationParameters(t *testing.T) {
	trace := m.Trace{Format: m.FormatVersion, Catalog: []m.Definition{{ID: "v1", Version: "1", Hot: true}, {ID: "v2", Version: "2", Hot: true}}, Actions: []m.Action{{Kind: "apply", Plan: &m.Plan{Ops: []m.Op{{Kind: "add-root", Tree: "target", Definition: "v2", KeepChildren: true}}}, Base: m.BaseIntent{Kind: "current"}}}}
	result := m.Reduce(context.Background(), trace, 100, func(t m.Trace) bool {
		return len(t.Actions) == 1 && t.Actions[0].Plan != nil && len(t.Actions[0].Plan.Ops) == 1 && t.Actions[0].Plan.Ops[0].Tree == "target"
	})
	op := result.Trace.Actions[0].Plan.Ops[0]
	if !result.Minimal || op.KeepChildren || op.Definition != "v1" {
		t.Fatal(result)
	}
}
