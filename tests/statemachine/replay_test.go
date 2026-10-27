package statemachine

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	m "github.com/Leeminjing/go-forest/tests/internal/topologymodel"
)

func faultyTrace() m.Trace {
	c := ruleCase{name: "tool-selftest-not-core-bug", steps: []ruleStep{yes(root("noise", "provider-v1")), yes(root("p", "provider-v1"), root("q", "provider-next"), root("c", "runtime-many")), yes(link("a", "c", "in", "p", false), link("b", "c", "in", "q", false)), yes(m.Op{Kind: "remove", Tree: "noise"})}}
	trace := knownTrace(c)
	trace.TestFault = "swap-resolution"
	trace.FaultStep = "rule-2"
	return trace
}
func TestModelNativeReplayAndReduction(t *testing.T) {
	trace := faultyTrace()
	original := runNative(t, trace)
	if original.Err != nil || len(original.Reports) != 1 || original.Reports[0].Failure == nil || original.Reports[0].Signature.Rule != "resolution" {
		t.Fatal("controlled checker error not detected", original)
	}
	wanted := original.Reports[0].Signature
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	reduced := m.Reduce(ctx, trace, 120, func(candidate m.Trace) bool { return sameNativeFailure(t, candidate, wanted) })
	if !reduced.Minimal || len(reduced.Trace.Steps) >= len(trace.Steps) {
		t.Fatal("did not reduce to rule-relative minimum", reduced)
	}
	for range 3 {
		if !sameNativeFailure(t, reduced.Trace, wanted) {
			t.Fatal("reduced native failure not reproducible")
		}
	}
	saveTrace(t, filepath.Join(original.Directory, "reduced.json"), reduced.Trace)
	t.Logf("artificial checker selftest originalSteps=%d reducedSteps=%d attempts=%d artifact=%s", len(trace.Steps), len(reduced.Trace.Steps), reduced.Attempts, original.Directory)
}

func TestModelReplay(t *testing.T) {
	path := os.Getenv("FOREST_MODEL_REPLAY")
	if path == "" {
		t.Skip("set FOREST_MODEL_REPLAY to an input.json or standalone trace")
	}
	traces := loadTraces(t, path)
	requireNative(t, traces...)
}

func sameNativeFailure(t *testing.T, trace m.Trace, wanted signature) bool {
	t.Helper()
	if err := validateTrace(trace); err != nil {
		return false
	}
	run := runNative(t, trace)
	return run.Err == nil && len(run.Reports) == 1 && run.Reports[0].Failure != nil && run.Reports[0].Signature == wanted
}

func autoReduce(t *testing.T, trace m.Trace, wanted signature, dir string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	result := m.Reduce(ctx, trace, 32, func(candidate m.Trace) bool {
		if validateTrace(candidate) != nil {
			return false
		}
		run := runNativeContext(t, ctx, candidate)
		return run.Err == nil && len(run.Reports) == 1 && run.Reports[0].Failure != nil && run.Reports[0].Signature == wanted
	})
	confirmReduction(t, ctx, &result, wanted)
	saveTrace(t, filepath.Join(dir, "reduced.json"), result.Trace)
	raw, err := json.MarshalIndent(map[string]any{"signature": wanted, "minimized": result.Minimal, "attempts": result.Attempts, "rules": "step/operation/action deletion; keep flag; schema-preserving definition version"}, "", "  ")
	if err != nil {
		t.Error(err)
	} else if err = os.WriteFile(filepath.Join(dir, "reduction.json"), raw, 0600); err != nil {
		t.Error(err)
	}
}
func TestModelReduce(t *testing.T) {
	path := os.Getenv("FOREST_MODEL_REDUCE")
	if path == "" {
		t.Skip("set FOREST_MODEL_REDUCE to the saved native failure")
	}
	traces := loadTraces(t, path)
	for _, trace := range traces {
		run := runNative(t, trace)
		if run.Err != nil || len(run.Reports) != 1 {
			t.Fatalf("original replay incomplete: %v artifact=%s", run.Err, run.Directory)
		}
		if run.Reports[0].Failure == nil {
			t.Log("no failure in seed", trace.Seed)
			continue
		}
		ctx, cancel := context.WithTimeout(context.Background(), 7*time.Minute)
		wanted := run.Reports[0].Signature
		reduced := m.Reduce(ctx, run.Traces[0], 4000, func(candidate m.Trace) bool { return sameNativeFailure(t, candidate, wanted) })
		confirmReduction(t, ctx, &reduced, wanted)
		cancel()
		saveTrace(t, filepath.Join(run.Directory, "reduced.json"), reduced.Trace)
		t.Logf("originalFailure=%+v minimized=%t attempts=%d artifact=%s", wanted, reduced.Minimal, reduced.Attempts, run.Directory)
	}
}

func normalizeTrace(trace m.Trace) m.Trace {
	out := m.Clone(trace)
	for i := range out.Actions {
		if out.Actions[i].ID == "" {
			out.Actions[i].ID = fmt.Sprintf("action-%04d", i)
		}
	}
	return out
}

func confirmReduction(t *testing.T, ctx context.Context, result *m.Reduction, wanted signature) {
	t.Helper()
	if !result.Minimal {
		return
	}
	for range 3 {
		if ctx.Err() != nil {
			result.Minimal = false
			return
		}
		run := runNativeContext(t, ctx, result.Trace)
		if run.Err != nil || len(run.Reports) != 1 || run.Reports[0].Failure == nil || run.Reports[0].Signature != wanted {
			result.Minimal = false
			return
		}
	}
}
func saveTrace(t *testing.T, path string, trace m.Trace) {
	t.Helper()
	raw, err := json.MarshalIndent(trace, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	if err = os.WriteFile(path, raw, 0600); err != nil {
		t.Log(string(raw))
		t.Fatal(err)
	}
}
func loadTraces(t *testing.T, path string) []m.Trace {
	t.Helper()
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	traces, err := decodeTraces(raw)
	if err != nil {
		t.Fatal(err)
	}
	return traces
}

func decodeTraces(raw []byte) ([]m.Trace, error) {
	var input workerInput
	if err := json.Unmarshal(raw, &input); err == nil && len(input.Traces) > 0 {
		for _, trace := range input.Traces {
			if err = validateTrace(trace); err != nil {
				return nil, err
			}
		}
		return input.Traces, nil
	}
	var trace m.Trace
	if err := json.Unmarshal(raw, &trace); err != nil {
		return nil, err
	}
	if err := validateTrace(trace); err != nil {
		return nil, err
	}
	return []m.Trace{trace}, nil
}
