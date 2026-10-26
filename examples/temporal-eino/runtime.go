// Package einodemo composes the published Temporal and Eino adapters.
package einodemo

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"io"
	"log/slog"
	"path/filepath"
	"time"

	ef "github.com/Leeminjing/go-forest/adapters/eino"
	tf "github.com/Leeminjing/go-forest/adapters/temporal"
	"github.com/Leeminjing/go-forest/examples/temporal-eino/flow"
	"github.com/Leeminjing/go-forest/examples/temporal-eino/internal/activitybridge"
	"github.com/Leeminjing/go-forest/examples/temporal-eino/internal/business"
	"github.com/Leeminjing/go-forest/examples/temporal-eino/internal/nativeeino"
	"github.com/Leeminjing/go-forest/examples/temporal-eino/internal/observe"
	"github.com/Leeminjing/go-forest/examples/temporal-eino/internal/state"
	"github.com/Leeminjing/go-forest/examples/temporal-eino/internal/steps"
	"github.com/Leeminjing/go-forest/examples/temporal-eino/protocol"
	"github.com/Leeminjing/go-forest/forest"
	enumspb "go.temporal.io/api/enums/v1"
	historypb "go.temporal.io/api/history/v1"
	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/log"
	"go.temporal.io/sdk/worker"
)

var ErrUnavailable = protocol.ErrUnavailable

type Options struct {
	Address, Queue, Directory, Version string
	Profile                            string                                      // Expected execution schema; empty selects protocol.Profile.
	Forest                             *forest.Forest                              `json:"-"`
	Hook                               func(context.Context, protocol.Point) error `json:"-"`
	Prepared                           func(context.Context, *Runtime) error       `json:"-"`
}
type Ref struct{ WorkflowID, RunID string }
type Runtime struct {
	Forest                  *forest.Forest
	Process                 string
	o                       Options
	meter                   *observe.Meter
	state                   *state.Store
	bridge                  *activitybridge.Bridge
	native                  *observe.Handle
	business                [2]*observe.Handle
	owns, removed, switched bool
	businessID              forest.TreeID
	management              chan struct{}
	requests                requestGate
}

func New(ctx context.Context, o Options) (r *Runtime, err error) {
	if o.Address == "" || o.Queue == "" || o.Directory == "" {
		return nil, protocol.ErrConflict
	}
	o.Directory, err = filepath.Abs(o.Directory)
	if err != nil {
		return nil, err
	}
	if o.Version == "" {
		o.Version = "v1"
	}
	if o.Profile == "" {
		o.Profile = protocol.Profile
	}
	if len(o.Profile) > 256 {
		return nil, protocol.ErrConflict
	}
	if o.Version != "v1" && o.Version != "v2" {
		return nil, protocol.ErrConflict
	}
	b := make([]byte, 12)
	if _, err = rand.Read(b); err != nil {
		return nil, err
	}
	f := o.Forest
	owns := f == nil
	if owns {
		f = forest.New(forest.Options{DrainTimeout: time.Second})
	}
	r = &Runtime{Forest: f, Process: "te-" + hex.EncodeToString(b), o: o, owns: owns, native: &observe.Handle{}, business: [2]*observe.Handle{{}, {}}, management: make(chan struct{}, 1), businessID: "app/business"}
	r.management <- struct{}{}
	r.requests.init()
	r.meter = &observe.Meter{Process: r.Process, Forest: f, Hook: o.Hook}
	committed := false
	defer func() {
		if err != nil && owns && !committed {
			c, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			_ = f.Close(c)
		}
	}()
	owner, err := ef.NewOwner(f, 32)
	if err != nil {
		return nil, err
	}
	data := state.Definition(o.Directory, r.Process, o.Profile, func(s *state.Store) { r.state = s })
	agent, err := nativeeino.ADK(owner, r.meter, filepath.Join(o.Directory, "handles"), r.native)
	if err != nil {
		return nil, err
	}
	graph, err := nativeeino.Graph(owner, r.meter)
	if err != nil {
		return nil, err
	}
	step := steps.Definition(r.meter)
	cd, err := tf.ClientDefinition(f, tf.ClientOptions{TypeID: "te.client", Version: "1", Native: client.Options{HostPort: o.Address}})
	if err != nil {
		return nil, err
	}
	wd, err := tf.WorkerDefinition(f, tf.WorkerOptions{TypeID: "te.worker", Version: "1", TreeID: "app/worker", TaskQueue: o.Queue, Client: client.Options{HostPort: o.Address}, Native: worker.Options{WorkerStopTimeout: 150 * time.Millisecond}, Ports: []forest.PortDescriptor{forest.Port[activitybridge.Steps]("steps", forest.Required()), forest.Port[activitybridge.Graph]("graph", forest.Required())}, BuildRegistration: func(e *forest.BuildEnv) (tf.Register, error) {
		s, err := forest.Reference[activitybridge.Steps](e, "steps")
		if err != nil {
			return nil, err
		}
		g, err := forest.Reference[activitybridge.Graph](e, "graph")
		if err != nil {
			return nil, err
		}
		r.bridge = &activitybridge.Bridge{Steps: s, Graph: g, Meter: r.meter}
		return func(reg tf.Registry) error {
			reg.RegisterWorkflow(flow.Execute)
			reg.RegisterActivityWithOptions(r.bridge.Step, activity.RegisterOptions{Name: "te.step"})
			reg.RegisterActivityWithOptions(r.bridge.ReadGraph, activity.RegisterOptions{Name: "te.graph"})
			return nil
		}, nil
	}})
	if err != nil {
		return nil, err
	}
	app := forest.Definition{TypeID: "te.app", Version: "1", ExportChildren: true, Build: func(context.Context, *forest.BuildEnv) (forest.Lifecycle, error) {
		return forest.Hooks{OnReady: func(c context.Context) error {
			if o.Prepared != nil {
				return o.Prepared(c, r)
			}
			return nil
		}}, nil
	}}
	v1 := business.Definition(f, r.meter, "v1", filepath.Join(o.Directory, "handles"), r.business[0])
	v2 := business.Definition(f, r.meter, "v2", filepath.Join(o.Directory, "handles"), r.business[1])
	selected := v1
	if o.Version == "v2" {
		selected = v2
	}
	for _, d := range []forest.Definition{app, data, agent, graph, step, cd, wd, v1, v2} {
		if err = f.Register(d); err != nil {
			return nil, err
		}
	}
	connect := func(id string, c forest.TreeID, p forest.PortID, provider forest.TreeID, cap forest.CapabilityID) forest.Operation {
		return forest.Connect(forest.Link{ID: id, Consumer: c, Port: p, Provider: provider, Capability: cap})
	}
	ops := []forest.Operation{forest.AddRoot("state", data.Ref()), forest.AddRoot("app", app.Ref()), forest.Nest("app", "app/client", cd.Ref()), forest.Nest("app", "app/worker", wd.Ref()), forest.Nest("app", "app/steps", step.Ref()), forest.Nest("app", "app/adk", agent.Ref()), forest.Nest("app", "app/graph", graph.Ref()), forest.Nest("app", "app/business", selected.Ref()), connect("worker.steps", "app/worker", "steps", "app/steps", "steps"), connect("worker.graph", "app/worker", "graph", "app/graph", "graph"), connect("steps.agent", "app/steps", "agent", "app/adk", "agent"), connect("steps.state", "app/steps", "state", "state", "state"), connect("adk.state", "app/adk", "state", "state", "state"), connect("adk.business", "app/adk", "business", "app/business", "business"), connect("graph.business", "app/graph", "business", "app/business", "business"), connect("business.state", "app/business", "state", "state", "state")}
	if _, err = f.Apply(ctx, forest.ChangePlan{BaseGeneration: f.Snapshot().Generation, Operations: ops}); err != nil {
		return nil, err
	}
	committed = true
	finalize, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	status, err := tf.Inspect(finalize, f, "app/worker")
	if err != nil {
		return r, err
	}
	r.bridge.WorkerIdentity = status.Identity
	return r, nil
}
func (r *Runtime) lock(ctx context.Context) (func(), error) {
	if _, e := r.Forest.Generation(ctx); e == nil {
		return nil, tf.ErrNestedScope
	}
	if ctx.Value(requestKey{}) != nil {
		return nil, tf.ErrNestedScope
	}
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-r.management:
		return func() { r.management <- struct{}{} }, nil
	}
}
func (r *Runtime) Enable(ctx context.Context) error {
	u, e := r.lock(ctx)
	if e != nil {
		return e
	}
	defer u()
	if r.requests.isClosed() {
		return ErrUnavailable
	}
	return tf.Enable(ctx, r.Forest, "app/worker")
}
func (r *Runtime) client(ctx context.Context, fn func(context.Context, client.Client) error) error {
	c, leave, e := r.requests.enter(ctx)
	if e != nil {
		return e
	}
	defer leave()
	return r.Forest.Invoke(c, func(call context.Context) error {
		return forest.Use[tf.ClientAccess](call, r.Forest, "app/client", "client", func(a tf.ClientAccess) error { return a.Use(call, func(n client.Client) error { return fn(call, n) }) })
	})
}
func (r *Runtime) Start(ctx context.Context, id string, in protocol.Input) (out Ref, err error) {
	if !protocol.Key(id) || !protocol.Valid(in) {
		return out, protocol.ErrConflict
	}
	err = r.client(ctx, func(c context.Context, n client.Client) error {
		run, e := n.ExecuteWorkflow(c, client.StartWorkflowOptions{ID: id, TaskQueue: r.o.Queue, WorkflowIDReusePolicy: enumspb.WORKFLOW_ID_REUSE_POLICY_REJECT_DUPLICATE}, flow.Execute, in)
		if e == nil {
			out = Ref{run.GetID(), run.GetRunID()}
		}
		return e
	})
	return
}
func (r *Runtime) Result(ctx context.Context, id Ref) (out protocol.Result, err error) {
	err = r.client(ctx, func(c context.Context, n client.Client) error {
		return n.GetWorkflow(c, id.WorkflowID, id.RunID).Get(c, &out)
	})
	return
}
func (r *Runtime) Query(ctx context.Context, id Ref) (out protocol.WorkflowState, err error) {
	err = r.client(ctx, func(c context.Context, n client.Client) error {
		v, e := n.QueryWorkflow(c, id.WorkflowID, id.RunID, "state")
		if e != nil {
			return e
		}
		return v.Get(&out)
	})
	return
}
func (r *Runtime) Decide(ctx context.Context, id Ref, d protocol.Decision) error {
	if !protocol.Key(d.ID) || !protocol.Key(d.ExecutionID) || len(d.Token) > 128 || len(d.Target) > 256 || len(d.Value) > 32 {
		return protocol.ErrConflict
	}
	return r.client(ctx, func(c context.Context, n client.Client) error {
		return n.SignalWorkflow(c, id.WorkflowID, id.RunID, "decision", d)
	})
}
func (r *Runtime) Cancel(ctx context.Context, id Ref) error {
	return r.client(ctx, func(c context.Context, n client.Client) error { return n.CancelWorkflow(c, id.WorkflowID, id.RunID) })
}
func (r *Runtime) Counts() protocol.Counts { return r.meter.Counts() }

type HostStatus struct {
	Active  int
	Closing bool
}

func (r *Runtime) HostStatus() HostStatus {
	r.requests.mu.Lock()
	defer r.requests.mu.Unlock()
	return HostStatus{Active: len(r.requests.calls), Closing: r.requests.closed}
}
func (r *Runtime) Inspect(ctx context.Context, in protocol.Input) (out protocol.Evidence, err error) {
	if !protocol.Valid(in) {
		return out, protocol.ErrConflict
	}
	c, leave, err := r.requests.enter(ctx)
	if err != nil {
		return out, err
	}
	defer leave()
	err = r.Forest.Invoke(c, func(call context.Context) error {
		return forest.Use[interface {
			Inspect(context.Context, protocol.Input) (protocol.Evidence, error)
		}](call, r.Forest, "state", "state", func(s interface {
			Inspect(context.Context, protocol.Input) (protocol.Evidence, error)
		}) error {
			var e error
			out, e = s.Inspect(call, in)
			return e
		})
	})
	return
}
func (r *Runtime) Replay(ctx context.Context, id Ref) error {
	return r.client(ctx, func(c context.Context, n client.Client) error {
		it := n.GetWorkflowHistory(c, id.WorkflowID, id.RunID, false, 0)
		h := &historypb.History{}
		for it.HasNext() {
			v, e := it.Next()
			if e != nil {
				return e
			}
			h.Events = append(h.Events, v)
		}
		replayer := worker.NewWorkflowReplayer()
		replayer.RegisterWorkflow(flow.Execute)
		return replayer.ReplayWorkflowHistory(log.NewStructuredLogger(slog.New(slog.NewTextHandler(io.Discard, nil))), h)
	})
}
func (r *Runtime) Switch(ctx context.Context, mode string) error {
	u, e := r.lock(ctx)
	if e != nil {
		return e
	}
	defer u()
	if r.requests.isClosed() || r.switched || r.o.Version != "v1" {
		return ErrUnavailable
	}
	d := forest.DefinitionRef{TypeID: "te.business", Version: "v2"}
	var ops []forest.Operation
	if mode == "replace" {
		ops = []forest.Operation{forest.Replace(r.businessID, d, false)}
	} else if mode == "rebind" {
		next := forest.TreeID("app/business-v2")
		ops = []forest.Operation{forest.Nest("app", next, d), forest.Unlink("business.state"), forest.Connect(forest.Link{ID: "new.state", Consumer: next, Port: "state", Provider: "state", Capability: "state"}), forest.Rebind(forest.Link{ID: "adk.business", Consumer: "app/adk", Port: "business", Provider: next, Capability: "business"}), forest.Rebind(forest.Link{ID: "graph.business", Consumer: "app/graph", Port: "business", Provider: next, Capability: "business"}), forest.Remove(r.businessID)}
	} else {
		return protocol.ErrConflict
	}
	_, e = r.Forest.Apply(ctx, forest.ChangePlan{BaseGeneration: r.Forest.Snapshot().Generation, Operations: ops})
	if e == nil {
		r.switched = true
		if mode == "rebind" {
			r.businessID = "app/business-v2"
		}
	}
	return e
}
func (r *Runtime) Resources() protocol.ResourceReport {
	n, nc := r.native.Report()
	a, ac := r.business[0].Report()
	b, bc := r.business[1].Report()
	claim := r.state != nil && r.state.Claimed()
	return protocol.ResourceReport{Claim: claim, NativeOpen: n, NativeCloses: nc, BusinessOpen: a || b, BusinessCloses: ac + bc}
}
func (r *Runtime) Close(ctx context.Context) error {
	u, e := r.lock(ctx)
	if e != nil {
		return e
	}
	defer u()
	r.requests.seal()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-r.requests.done:
	}
	if !r.removed {
		if e = tf.Quiesce(ctx, r.Forest, "app/worker"); e != nil {
			return e
		}
		if _, e = r.Forest.Apply(ctx, forest.ChangePlan{BaseGeneration: r.Forest.Snapshot().Generation, Operations: []forest.Operation{forest.RemoveSubtree("app"), forest.Remove("state")}}); e != nil {
			return e
		}
		r.removed = true
	}
	if e = r.Forest.Wait(ctx); e != nil {
		return e
	}
	if r.owns {
		return r.Forest.Close(ctx)
	}
	return nil
}
