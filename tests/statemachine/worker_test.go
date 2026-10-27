package statemachine

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/Leeminjing/go-forest/forest"
	m "github.com/Leeminjing/go-forest/tests/internal/topologymodel"
)

type signature struct{ Rule, Step, Constraints, Phases, Relations, Role string }
type outcome struct {
	Step             string
	Base, Generation uint64
	Accepted         bool
	Issues           []m.Issue
	Error            string
}
type report struct {
	Race                bool
	Process             int
	ControlledToolFault string
	Name                string
	Events              []nativeEvent
	native              *harness
	Seed                int64
	Go, OS              string
	Outcomes            []outcome
	Failure             *failure
	Signature           signature
	Expected            m.State
	Actual              json.RawMessage
	Accepted, Rejected  int
	Coverage            map[string]int
}
type workerInput struct {
	Traces         []m.Trace
	Result, Cancel string
}
type nativeRun struct {
	Traces    []m.Trace
	Reports   []report
	Err       error
	Directory string
	Killed    bool
}

func jsonFact(v any) json.RawMessage {
	raw, err := json.Marshal(v)
	if err != nil {
		raw, _ = json.Marshal(fmt.Sprintf("%+v", v))
	}
	return raw
}
func failureSignature(rule, id string, issues []m.Issue, state m.State, model *m.Model) signature {
	constraints := map[string]bool{}
	for _, x := range issues {
		constraints[string(x)] = true
	}
	phases, relations := map[string]bool{}, map[string]bool{}
	for _, n := range state.Nodes {
		for _, p := range model.Catalog[n.Definition].Ports {
			phases[string(p.Phase)] = true
		}
	}
	for _, b := range state.Bindings {
		if b.Local {
			relations["nest"] = true
		} else {
			relations["link"] = true
		}
	}
	join := func(values map[string]bool) string {
		var out []string
		for v := range values {
			out = append(out, v)
		}
		sort.Strings(out)
		return strings.Join(out, ",")
	}
	return signature{Rule: rule, Step: id, Constraints: join(constraints), Phases: join(phases), Relations: join(relations)}
}
func executeTrace(t *testing.T, ctx context.Context, trace m.Trace) report {
	r := report{Name: trace.Name, Seed: trace.Seed, Go: runtime.Version(), OS: runtime.GOOS, Race: raceBuild, Process: os.Getpid(), ControlledToolFault: trace.TestFault, Coverage: map[string]int{}}
	h := newHarness(t, trace.Catalog, trace.IDs(), forest.Options{DrainTimeout: time.Duration(trace.DrainMillis) * time.Millisecond})
	r.native = h
	h.ctx = ctx
	h.testFault = trace.TestFault
	h.faultStep = trace.FaultStep
	for _, s := range trace.Steps {
		_ = json.NewEncoder(os.Stdout).Encode(map[string]any{"stage": "step.begin", "seed": trace.Seed, "step": s.ID, "base": s.Plan.Base})
		h.currentStep = s.ID
		expected, issues := h.model.Next(s.Plan)
		var err error
		if s.Expect != nil && *s.Expect != (len(issues) == 0) {
			err = mismatch("golden-model", *s.Expect, issues)
		} else {
			err = h.apply(s.Plan)
		}
		o := outcome{Step: s.ID, Base: s.Plan.Base, Generation: h.f.Snapshot().Generation, Accepted: h.lastApplyErr == nil, Issues: issues}
		if h.lastApplyErr != nil {
			o.Error = h.lastApplyErr.Error()
		}
		r.Outcomes = append(r.Outcomes, o)
		if o.Accepted {
			r.Accepted++
		} else {
			r.Rejected++
		}
		for _, op := range s.Plan.Ops {
			key := op.Kind + "/rejected"
			if o.Accepted {
				key = op.Kind + "/accepted"
			}
			r.Coverage[key]++
		}
		if len(s.Plan.Ops) == 0 {
			r.Coverage["empty/"+fmt.Sprint(o.Accepted)]++
		}
		if err != nil {
			var f *failure
			if !errors.As(err, &f) {
				f = &failure{Rule: "native-error", Detail: err.Error()}
			}
			r.Failure = f
			r.Signature = failureSignature(f.Rule, s.ID, issues, expected, h.model)
			r.Signature.Role = f.Role
			r.Expected = expected
			r.Actual = snapshotFact(h.f.Snapshot())
			_ = json.NewEncoder(os.Stdout).Encode(map[string]any{"stage": "failure", "signature": r.Signature, "detail": f.Detail})
			return r
		}
		_ = json.NewEncoder(os.Stdout).Encode(map[string]any{"stage": "step.end", "seed": trace.Seed, "step": s.ID, "generation": o.Generation})
	}
	if len(trace.Actions) > 0 {
		if err := h.schedule(ctx, trace.Actions); err != nil {
			var f *failure
			if !errors.As(err, &f) {
				f = &failure{Rule: "schedule", Detail: err.Error()}
			}
			r.Failure = f
			r.Signature = failureSignature(f.Rule, h.currentStep, nil, h.model.State, h.model)
			r.Signature.Role = f.Role
			r.Expected = h.model.State
			r.Actual = snapshotFact(h.f.Snapshot())
		}
	}
	return r
}

func TestModelWorker(t *testing.T) {
	path := os.Getenv("FOREST_MODEL_WORKER_INPUT")
	if path == "" {
		t.Skip("supervised executable role")
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var in workerInput
	if err = json.Unmarshal(raw, &in); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	watcherDone := make(chan struct{})
	defer func() { cancel(); <-watcherDone }()
	go func() {
		defer close(watcherDone)
		ticker := time.NewTicker(10 * time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				if _, e := os.Stat(in.Cancel); e == nil {
					cancel()
					return
				}
			}
		}
	}()
	var reports []report
	for i, trace := range in.Traces {
		if trace.Format != m.FormatVersion {
			t.Fatal("unsupported trace format", trace.Format)
		}
		var r report
		t.Run(fmt.Sprintf("case-%d", i), func(t *testing.T) { r = executeTrace(t, ctx, trace) })
		if r.native != nil {
			r.Events = r.native.nativeEvents()
			if err := r.native.cleanupFailure; err != nil && r.Failure == nil {
				var f *failure
				if !errors.As(err, &f) {
					f = &failure{Rule: "cleanup", Detail: err.Error()}
				}
				r.Failure = f
				r.Signature = failureSignature(f.Rule, "cleanup", nil, r.native.model.State, r.native.model)
				r.Signature.Role = f.Role
				r.Expected = r.native.model.State
				r.Actual = snapshotFact(r.native.f.Snapshot())
			}
		}
		if r.Failure != nil {
			r.Failure.Expected = jsonFact(r.Failure.Expected)
			r.Failure.Actual = jsonFact(r.Failure.Actual)
		}
		reports = append(reports, r)
		if r.Failure != nil || t.Failed() {
			break
		}
	}
	data, err := json.MarshalIndent(reports, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	if err = os.WriteFile(in.Result, data, 0600); err != nil {
		t.Fatal(err)
	}
}

func evidenceRoot(t *testing.T) string {
	t.Helper()
	if configured := os.Getenv("FOREST_MODEL_ARTIFACT_DIR"); configured != "" {
		return configured
	}
	root, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	for {
		if _, err = os.Stat(filepath.Join(root, "go.mod")); err == nil {
			break
		}
		parent := filepath.Dir(root)
		if parent == root {
			t.Fatal("module root not found")
		}
		root = parent
	}
	return filepath.Join(root, ".scratch", "core-state-machine", "runs")
}
func runNative(t *testing.T, traces ...m.Trace) nativeRun {
	return runNativeContext(t, context.Background(), traces...)
}
func runNativeContext(t *testing.T, parent context.Context, traces ...m.Trace) nativeRun {
	t.Helper()
	traces = append([]m.Trace(nil), traces...)
	for i := range traces {
		traces[i] = normalizeTrace(traces[i])
	}
	for _, trace := range traces {
		if err := validateTrace(trace); err != nil {
			return nativeRun{Err: fmt.Errorf("invalid trace: %w", err)}
		}
	}
	root := evidenceRoot(t)
	if err := os.MkdirAll(root, 0700); err != nil {
		t.Fatal(err)
	}
	dir, err := os.MkdirTemp(root, "experiment-")
	if err != nil {
		t.Fatal(err)
	}
	out := nativeRun{Directory: dir, Traces: traces}
	in := workerInput{Traces: traces, Result: filepath.Join(dir, "result.json"), Cancel: filepath.Join(dir, "cancel")}
	raw, err := json.MarshalIndent(in, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	input := filepath.Join(dir, "input.json")
	if err = os.WriteFile(input, raw, 0600); err != nil {
		t.Log(string(raw))
		t.Fatal(err)
	}
	logPath := filepath.Join(dir, "worker.log")
	log, err := os.Create(logPath)
	if err != nil {
		t.Fatal(err)
	}
	budget := 20 * time.Second
	if deadline, ok := parent.Deadline(); ok && time.Until(deadline) < budget {
		budget = time.Until(deadline)
	}
	if deadline, ok := t.Deadline(); ok && time.Until(deadline) < budget+3*time.Second {
		budget = time.Until(deadline) - 3*time.Second
	}
	if budget < time.Second {
		log.Close()
		out.Err = errors.New("insufficient worker budget")
		return out
	}
	ctx, cancel := context.WithTimeout(parent, budget)
	defer cancel()
	cmd := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestModelWorker$", "-test.v")
	hideWorker(cmd)
	cmd.Env = append(os.Environ(), "FOREST_MODEL_WORKER_INPUT="+input, "FOREST_MODEL_RESOURCE_ROOT="+filepath.Join(dir, "resources"), "GORACE="+os.Getenv("GORACE")+" log_path=stderr atexit_sleep_ms=0")
	cmd.Stdout = log
	cmd.Stderr = log
	// Ask fixtures to cooperate before CommandContext's eventual OS Kill+Wait.
	done := make(chan struct{})
	requested := make(chan struct{})
	go func() {
		defer close(requested)
		timer := time.NewTimer(budget - time.Second)
		defer timer.Stop()
		select {
		case <-done:
			return
		case <-timer.C:
			_ = os.WriteFile(in.Cancel, []byte("cancel"), 0600)
		}
	}()
	out.Err = cmd.Run()
	close(done)
	<-requested
	if err = log.Close(); err != nil {
		t.Error(err)
	}
	out.Killed = ctx.Err() != nil
	data, readErr := os.ReadFile(in.Result)
	if readErr == nil {
		if err = json.Unmarshal(data, &out.Reports); err != nil {
			out.Err = err
		}
	} else if out.Err == nil {
		out.Err = readErr
	}
	data, err = os.ReadFile(logPath)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(data), "WARNING: DATA RACE") {
		out.Err = errors.New("native child race detector reported a race")
	}
	return out
}
func requireNative(t *testing.T, traces ...m.Trace) []report {
	t.Helper()
	run := runNative(t, traces...)
	for index, r := range run.Reports {
		if r.Failure != nil {
			if index < len(run.Traces) {
				autoReduce(t, run.Traces[index], r.Signature, run.Directory)
			}
			t.Fatalf("seed=%d signature=%+v error=%v evidence=%s replay: FOREST_MODEL_REPLAY=%s go test ./tests/statemachine -run TestModelReplay", r.Seed, r.Signature, r.Failure, run.Directory, filepath.Join(run.Directory, "input.json"))
		}
	}
	if run.Err != nil || len(run.Reports) != len(traces) {
		t.Fatalf("worker exit=%v forced=%t completed=%d/%d evidence=%s", run.Err, run.Killed, len(run.Reports), len(traces), run.Directory)
	}
	return run.Reports
}
