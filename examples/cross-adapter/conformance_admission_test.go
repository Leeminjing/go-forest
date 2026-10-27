package crossdemo

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	tf "github.com/Leeminjing/go-forest/adapters/temporal"
	"github.com/Leeminjing/go-forest/examples/cross-adapter/flow"
	"github.com/Leeminjing/go-forest/examples/cross-adapter/protocol"
	"github.com/Leeminjing/go-forest/forest"
	tk "github.com/Leeminjing/go-forest/testkit"
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
	o := Options{Address: os.Getenv("FOREST_TEMPORAL_ADDRESS"), DSN: os.Getenv("FOREST_POSTGRES_DSN"), TaskQueue: fmt.Sprintf("kit-admission-%d", time.Now().UnixNano()), Directory: t.TempDir()}
	if o.Address == "" || o.DSN == "" {
		t.Fatal("explicit integration requires real services")
	}
	if err := Setup(ctx, o.DSN); err != nil {
		t.Fatal(err)
	}
	f := forest.New(forest.Options{})
	defer f.Close(ctx)
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
	run, err := native.ExecuteWorkflow(ctx, client.StartWorkflowOptions{ID: o.TaskQueue, TaskQueue: o.TaskQueue}, flow.Command, flow.Input{Request: protocol.Request{OperationID: o.TaskQueue, Target: o.TaskQueue, Payload: "oak"}})
	if err != nil {
		t.Fatal(err)
	}
	defer native.TerminateWorkflow(context.Background(), run.GetID(), run.GetRunID(), "owned conformance cleanup")
	gate := tk.NewBarrier()
	defer gate.Release()
	var candidate *Runtime
	probe := func() ([]tk.Resource, error) {
		closes := candidate.native.Closes()
		err := candidate.native.Check()
		open := err == nil
		if err != nil && closes != 1 {
			return nil, err
		}
		if open && closes != 0 {
			return nil, errors.New("closed original file usable")
		}
		return []tk.Resource{{ID: "candidate-native-file", Ownership: tk.Owned, Usable: open, CloseCount: uint64(closes)}}, nil
	}
	o.BetweenReads = func(context.Context, protocol.Request) error { return record(tk.Event{Stage: "business.enter"}) }
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
	done := make(chan error, 1)
	go func() { _, err := New(ctx, o); done <- err }()
	defer func() {
		gate.Release()
		select {
		case <-done:
		case <-time.After(10 * time.Second):
			t.Error("candidate did not join")
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
	err = <-done
	done <- err
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
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close(ctx)
	state, err := tf.Inspect(ctx, r.Forest, "app/worker")
	if err != nil || state.Phase != "disabled" || state.Started != 0 {
		t.Fatal(state, err)
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
	var out protocol.Result
	if err = run.Get(ctx, &out); err != nil || out.Receipt.Output != "v1:oak" {
		t.Fatal(out, err)
	}
	evidence, err := j.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	if err = tk.CheckAdmission(tk.AdmissionEvidence{Label: "Fx/Temporal/GORM candidate", Before: before, After: after, Trace: evidence}); err != nil {
		t.Fatal(err)
	}
}
