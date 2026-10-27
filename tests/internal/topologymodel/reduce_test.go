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
