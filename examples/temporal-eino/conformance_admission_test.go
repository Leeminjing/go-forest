package einodemo

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	tf "github.com/Leeminjing/go-forest/adapters/temporal"
	"github.com/Leeminjing/go-forest/examples/temporal-eino/flow"
	"github.com/Leeminjing/go-forest/examples/temporal-eino/protocol"
	"github.com/Leeminjing/go-forest/forest"
	tk "github.com/Leeminjing/go-forest/testkit"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/sdk/client"
	"os"
	"testing"
	"time"
)

type conformancePing interface{ Ping() string }
type conformanceAlive struct{}

func (conformanceAlive) Ping() string { return "old forest alive" }
func topology(f *forest.Forest) tk.Topology {
	snapshot := f.Snapshot()
	type node struct{ Parent, Instance, Type, Version string }
	projection := struct {
		Nodes       map[string]node
		Links, Nest map[string]forest.Binding
	}{Nodes: map[string]node{}, Links: map[string]forest.Binding{}, Nest: map[string]forest.Binding{}}
	for id, n := range snapshot.Trees {
		projection.Nodes[string(id)] = node{string(n.ParentID), n.InstanceID, n.TypeID, n.Version}
	}
	for id, l := range snapshot.Links {
		projection.Links[id] = l.Binding
	}
	for id, l := range snapshot.NestBindings {
		projection.Nest[id] = l.Binding
	}
	bytes, err := json.Marshal(projection)
	if err != nil {
		panic(err)
	}
	hash := sha256.Sum256(bytes)
	return tk.Topology{Forest: "original fixture forest", Generation: snapshot.Generation, Fingerprint: hex.EncodeToString(hash[:])}
}
func TestConformanceCandidateRollbackAndPostCommitAdmission(t *testing.T) {
	if os.Getenv("FOREST_INTEGRATION") != "1" {
		t.Skip("requires real services")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	o := Options{Address: os.Getenv("FOREST_TEMPORAL_ADDRESS"), Queue: fmt.Sprintf("kit-admission-%d", time.Now().UnixNano()), Directory: t.TempDir()}
	if o.Address == "" {
		t.Fatal("explicit integration requires real Temporal")
	}
	f := forest.New(forest.Options{})
	defer func() {
		cleanup, cancel := context.WithTimeout(context.Background(), 20*time.Second)
		defer cancel()
		if err := f.Close(cleanup); err != nil {
			t.Error("original forest cleanup", err)
		}
	}()
	d := forest.Definition{TypeID: "kit.original", Version: "1", Capabilities: []forest.CapabilityDescriptor{forest.Capability[conformancePing]("ping", forest.Public)}, Build: func(_ context.Context, e *forest.BuildEnv) (forest.Lifecycle, error) {
		return forest.Hooks{}, forest.Provide(e, "ping", conformanceAlive{})
	}}
	if err := f.Register(d); err != nil {
		t.Fatal(err)
	}
	if _, err := f.Apply(ctx, forest.ChangePlan{Operations: []forest.Operation{forest.AddRoot("original", d.Ref())}}); err != nil {
		t.Fatal(err)
	}
	before := topology(f)
	j, _ := tk.NewJournal(128)
	record := func(e tk.Event) error { _, err := j.Record(e); return err }
	native, err := client.DialContext(ctx, client.Options{HostPort: o.Address})
	if err != nil {
		t.Fatal(err)
	}
	defer native.Close()
	in := protocol.Input{ExecutionID: o.Queue, OperationID: o.Queue, Text: "oak"}
	run, err := native.ExecuteWorkflow(ctx, client.StartWorkflowOptions{ID: o.Queue, TaskQueue: o.Queue}, flow.Execute, in)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		cleanup, cancel := context.WithTimeout(context.Background(), 20*time.Second)
		defer cancel()
		err := native.TerminateWorkflow(cleanup, run.GetID(), run.GetRunID(), "owned conformance cleanup")
		var missing *serviceerror.NotFound
		if err != nil && !errors.As(err, &missing) {
			t.Error("owned workflow cleanup", err)
		}
	}()
	gate := tk.NewBarrier()
	defer gate.Release()
	var candidate *Runtime
	probe := func() ([]tk.Resource, error) {
		open, closes := candidate.native.Report()
		err := candidate.native.Check()
		if open && err != nil {
			return nil, err
		}
		if !open && !errors.Is(err, os.ErrClosed) {
			return nil, fmt.Errorf("unexpected probe error: %v", err)
		}
		return []tk.Resource{{ID: "candidate-native-file", Ownership: tk.Owned, Usable: open, CloseCount: uint64(closes)}}, nil
	}
	o.Hook = func(_ context.Context, p protocol.Point) error {
		if p.Stage == "model" {
			return record(tk.Event{Stage: "business.enter"})
		}
		return nil
	}
	boom := errors.New("controlled readiness rejection")
	o.Forest = f
	o.Prepared = func(c context.Context, r *Runtime) error {
		candidate = r
		p, err := probe()
		if err != nil {
			return err
		}
		if err = record(tk.Event{Stage: "candidate.prepared", Resources: p}); err != nil {
			return err
		}
		if err = gate.Hold(c); err != nil {
			return err
		}
		return boom
	}
	if err = record(tk.Event{Stage: "candidate.begin"}); err != nil {
		t.Fatal(err)
	}
	done := make(chan struct{})
	var buildErr error
	go func() { _, buildErr = New(ctx, o); close(done) }()
	defer func() {
		gate.Release()
		cleanup, cancel := context.WithTimeout(context.Background(), 20*time.Second)
		defer cancel()
		if err := tk.Await(cleanup, done); err != nil {
			t.Error("candidate did not join", err)
		}
	}()
	if err = tk.Await(ctx, gate.Arrived()); err != nil {
		t.Fatal(err)
	}
	if err = f.Invoke(ctx, func(c context.Context) error {
		return forest.Use[conformancePing](c, f, "original", "ping", func(p conformancePing) error {
			if p.Ping() != "old forest alive" {
				return boom
			}
			return nil
		})
	}); err != nil {
		t.Fatal(err)
	}
	if err = record(tk.Event{Stage: "original.served"}); err != nil {
		t.Fatal(err)
	}
	gate.Release()
	if err = tk.Await(ctx, done); err != nil {
		t.Fatal(err)
	}
	err = buildErr
	if !errors.Is(err, boom) {
		t.Fatal(err)
	}
	if err = f.Wait(ctx); err != nil {
		t.Fatal(err)
	}
	p, err := probe()
	if err != nil {
		t.Fatal(err)
	}
	if err = record(tk.Event{Stage: "candidate.end", Resources: p}); err != nil {
		t.Fatal(err)
	}
	after := topology(f)
	// Registry definitions remain registered after failed construction. The
	// positive control uses a fresh Forest with the same real queued workflow;
	// it does not claim Registry is part of topology rollback.
	o.Forest = nil
	o.Prepared = nil
	o.Directory = t.TempDir()
	r, err := New(ctx, o)
	if r != nil {
		defer func() {
			cleanup, cancel := context.WithTimeout(context.Background(), 20*time.Second)
			defer cancel()
			if err := r.Close(cleanup); err != nil {
				t.Error("positive control cleanup", err)
			}
		}()
	}
	if err != nil {
		t.Fatal(err)
	}
	workerState, err := tf.Inspect(ctx, r.Forest, "app/worker")
	if err != nil || workerState.Phase != "disabled" || workerState.Started != 0 {
		t.Fatal(workerState, err)
	}
	if err = record(tk.Event{Stage: "commit"}); err != nil {
		t.Fatal(err)
	}
	// Mark the invocation of explicit Enable, before that operation can poll.
	if err = record(tk.Event{Stage: "enable"}); err != nil {
		t.Fatal(err)
	}
	if err = r.Enable(ctx); err != nil {
		t.Fatal(err)
	}
	id := Ref{WorkflowID: run.GetID(), RunID: run.GetRunID()}
	var state protocol.WorkflowState
	for {
		state, err = r.Query(ctx, id)
		if err == nil && state.Stage == "paused" {
			break
		}
		select {
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		case <-time.After(5 * time.Millisecond):
		}
	}
	if err = r.Decide(ctx, id, protocol.Decision{ID: "go", ExecutionID: in.ExecutionID, Token: state.Pause.Token, Target: state.Pause.Targets[0], Value: "continue"}); err != nil {
		t.Fatal(err)
	}
	out, err := r.Result(ctx, id)
	if err != nil || out.Text != "finished:v1:oak" {
		t.Fatal(out, err)
	}
	evidence, err := j.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	if err = tk.CheckAdmission(tk.AdmissionEvidence{Label: "Eino/Temporal candidate", Before: before, After: after, Trace: evidence}); err != nil {
		t.Fatal(err)
	}
}
