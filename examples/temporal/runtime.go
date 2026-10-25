// Package temporaldemo composes a native durable workflow with business trees.
package temporaldemo

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"path/filepath"
	"time"

	"github.com/Leeminjing/go-forest-agent/agent"
	"github.com/Leeminjing/go-forest-agent/trees/events"
	"github.com/Leeminjing/go-forest-agent/trees/filecheckpoint"
	"github.com/Leeminjing/go-forest-agent/trees/manualapproval"
	gf "github.com/Leeminjing/go-forest/adapters/gorm"
	tf "github.com/Leeminjing/go-forest/adapters/temporal"
	"github.com/Leeminjing/go-forest/examples/temporal/flow"
	"github.com/Leeminjing/go-forest/examples/temporal/internal/jobservice"
	"github.com/Leeminjing/go-forest/forest"
	enumspb "go.temporal.io/api/enums/v1"
	historypb "go.temporal.io/api/history/v1"
	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/log"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/worker"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

type Options struct {
	Model                              agent.Model
	Address, DSN, Directory, TaskQueue string
	BeforeApply                        func(context.Context, flow.Planned) error
	AfterCommit                        func(context.Context, flow.Receipt) error
}
type Runtime struct {
	Forest  *forest.Forest
	options Options
	next    forest.DefinitionRef
}
type Ref struct{ WorkflowID, RunID string }
type jobs interface {
	Plan(context.Context, flow.Input) (flow.Planned, error)
	Apply(context.Context, flow.Planned) (flow.Receipt, error)
	Read(context.Context, string) (flow.Result, error)
}

func open(dsn string) (*gorm.DB, error) {
	return gorm.Open(postgres.Open(dsn), &gorm.Config{DisableAutomaticPing: true, Logger: logger.Default.LogMode(logger.Silent)})
}

// Setup is an explicit administration step, never called by a tree lifecycle.
func Setup(ctx context.Context, dsn string) error {
	db, err := open(dsn)
	if err != nil {
		return err
	}
	pool, err := db.DB()
	if err != nil {
		return err
	}
	defer pool.Close()
	return db.WithContext(ctx).AutoMigrate(&jobservice.Record{})
}
func New(ctx context.Context, o Options) (r *Runtime, err error) {
	if o.Address == "" || o.DSN == "" || o.Directory == "" || o.TaskQueue == "" {
		return nil, errors.New("address, DSN, directory and queue required")
	}
	f := forest.New(forest.Options{})
	r = &Runtime{Forest: f, options: o}
	defer func() {
		if err != nil {
			c, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			f.Close(c)
		}
	}()
	db, err := gf.Definition(f, gf.Options{TypeID: "temporal-example.db", Version: "1", Ownership: gf.Owned, Open: func(context.Context) (*gorm.DB, error) { return open(o.DSN) }})
	if err != nil {
		return nil, err
	}
	cd, err := tf.ClientDefinition(f, tf.ClientOptions{TypeID: "temporal-example.client", Version: "1", Native: client.Options{HostPort: o.Address}})
	if err != nil {
		return nil, err
	}
	wd, err := tf.WorkerDefinition(f, tf.WorkerOptions{TypeID: "temporal-example.worker", Version: "1", TreeID: "jobs/worker", TaskQueue: o.TaskQueue, Client: client.Options{HostPort: o.Address}, Ports: []forest.PortDescriptor{forest.Port[jobs]("jobs", forest.Required())}, BuildRegistration: func(e *forest.BuildEnv) (tf.Register, error) {
		ref, err := forest.Reference[jobs](e, "jobs")
		if err != nil {
			return nil, err
		}
		return func(r tf.Registry) error {
			r.RegisterWorkflow(flow.Job)
			r.RegisterActivityWithOptions(func(c context.Context, in flow.Input) (out flow.Planned, err error) {
				err = ref.Use(c, func(j jobs) error { out, err = j.Plan(c, in); return err })
				return out, translate(err)
			}, activity.RegisterOptions{Name: "job.plan"})
			r.RegisterActivityWithOptions(func(c context.Context, in flow.Planned) (out flow.Receipt, err error) {
				err = ref.Use(c, func(j jobs) error { out, err = j.Apply(c, in); return err })
				return out, translate(err)
			}, activity.RegisterOptions{Name: "job.apply"})
			r.RegisterActivityWithOptions(func(c context.Context, id string) (out flow.Result, err error) {
				err = ref.Use(c, func(j jobs) error { out, err = j.Read(c, id); return err })
				return out, translate(err)
			}, activity.RegisterOptions{Name: "job.read"})
			return nil
		}, nil
	}})
	if err != nil {
		return nil, err
	}
	first := jobservice.Definition(f, jobservice.Options{Version: "1", BeforeApply: o.BeforeApply, AfterCommit: o.AfterCommit})
	next := jobservice.Definition(f, jobservice.Options{Version: "2"})
	r.next = next.Ref()
	defs := map[forest.TreeID]forest.Definition{"jobs": first, "jobs/worker": wd, "database": db, "client": cd, "agent": agent.Definition(agent.Options{}), "model": modelDefinition(o.Model), "checkpoint": filecheckpoint.Definition(filepath.Join(o.Directory, "agent")), "events": events.Definition(events.New(1024)), "approval": manualapproval.Definition()}
	for _, d := range defs {
		if err = f.Register(d); err != nil {
			return nil, err
		}
	}
	if err = f.Register(next); err != nil {
		return nil, err
	}
	ops := []forest.Operation{}
	for id, d := range defs {
		if id == "jobs/worker" {
			ops = append(ops, forest.Nest("jobs", id, d.Ref()))
		} else {
			ops = append(ops, forest.AddRoot(id, d.Ref()))
		}
	}
	ops = append(ops, forest.BindNest(forest.Link{ID: "worker.jobs", Consumer: "jobs/worker", Port: "jobs", Provider: "jobs", Capability: "jobs"}))
	ops = append(ops, forest.Connect(forest.Link{ID: "agent.approval", Consumer: "agent", Port: "approval", Provider: "approval", Capability: "approval"}))
	for _, l := range []forest.Link{{ID: "jobs.db", Consumer: "jobs", Port: "database", Provider: "database", Capability: "database"}, {ID: "jobs.agent", Consumer: "jobs", Port: "agent", Provider: "agent", Capability: "executor"}, {ID: "agent.model", Consumer: "agent", Port: "model", Provider: "model", Capability: "model"}, {ID: "agent.checkpoint", Consumer: "agent", Port: "checkpoint", Provider: "checkpoint", Capability: "checkpoint"}, {ID: "agent.events", Consumer: "agent", Port: "events", Provider: "events", Capability: "events"}} {
		ops = append(ops, forest.Connect(l))
	}
	if _, err = f.Apply(ctx, forest.ChangePlan{Operations: ops}); err != nil {
		return nil, err
	}
	if err = tf.Enable(ctx, f, "jobs/worker"); err != nil {
		return nil, err
	}
	return r, nil
}
func translate(err error) error {
	var pending *jobservice.Continuation
	if errors.As(err, &pending) {
		return temporal.NewNonRetryableApplicationError("native Agent continuation required", "AgentContinuation", nil, pending.RunID, string(pending.Status))
	}
	if errors.Is(err, jobservice.ErrConflict) {
		return temporal.NewNonRetryableApplicationError("input identity conflict", "InputConflict", nil)
	}
	if errors.Is(err, jobservice.ErrUnknown) {
		return temporal.NewNonRetryableApplicationError("reconcile business operation before retry", "OutcomeUnknown", nil)
	}
	return err
}
func (r *Runtime) use(ctx context.Context, fn func(client.Client) error) error {
	return r.Forest.Invoke(ctx, func(c context.Context) error {
		return forest.Use[tf.ClientAccess](c, r.Forest, "client", "client", func(a tf.ClientAccess) error { return a.Use(c, fn) })
	})
}
func (r *Runtime) StartRun(ctx context.Context, in flow.Input) (out Ref, err error) {
	err = r.use(ctx, func(c client.Client) error {
		run, err := c.ExecuteWorkflow(ctx, client.StartWorkflowOptions{ID: in.ID, TaskQueue: r.options.TaskQueue, WorkflowIDReusePolicy: enumspb.WORKFLOW_ID_REUSE_POLICY_REJECT_DUPLICATE}, flow.Job, in)
		if err == nil {
			out = Ref{WorkflowID: run.GetID(), RunID: run.GetRunID()}
		}
		return err
	})
	return
}
func (r *Runtime) Query(ctx context.Context, id Ref) (out flow.State, err error) {
	err = r.use(ctx, func(c client.Client) error {
		v, err := c.QueryWorkflow(ctx, id.WorkflowID, id.RunID, "state")
		if err != nil {
			return err
		}
		return v.Get(&out)
	})
	return
}
func (r *Runtime) Signal(ctx context.Context, id Ref) error {
	return r.use(ctx, func(c client.Client) error { return c.SignalWorkflow(ctx, id.WorkflowID, id.RunID, "continue", true) })
}
func (r *Runtime) Cancel(ctx context.Context, id Ref) error {
	return r.use(ctx, func(c client.Client) error { return c.CancelWorkflow(ctx, id.WorkflowID, id.RunID) })
}
func (r *Runtime) Result(ctx context.Context, id Ref) (out flow.Result, err error) {
	err = r.use(ctx, func(c client.Client) error { return c.GetWorkflow(ctx, id.WorkflowID, id.RunID).Get(ctx, &out) })
	return
}
func (r *Runtime) Replay(ctx context.Context, id Ref) error {
	return r.use(ctx, func(c client.Client) error {
		iterator := c.GetWorkflowHistory(ctx, id.WorkflowID, id.RunID, false, 0)
		history := &historypb.History{}
		for iterator.HasNext() {
			e, err := iterator.Next()
			if err != nil {
				return err
			}
			history.Events = append(history.Events, e)
		}
		replayer := worker.NewWorkflowReplayer()
		replayer.RegisterWorkflow(flow.Job)
		return replayer.ReplayWorkflowHistory(log.NewStructuredLogger(slog.New(slog.NewTextHandler(io.Discard, nil))), history)
	})
}
func (r *Runtime) ReplaceBusiness(ctx context.Context) error {
	_, err := r.Forest.Apply(ctx, forest.ChangePlan{BaseGeneration: r.Forest.Snapshot().Generation, Operations: []forest.Operation{forest.Replace("jobs", r.next, true)}})
	return err
}
func (r *Runtime) Close(ctx context.Context) error {
	if err := tf.Quiesce(ctx, r.Forest, "jobs/worker"); err != nil {
		return err
	}
	return r.Forest.Close(ctx)
}

type model struct{}

func (model) Generate(ctx context.Context, request agent.ModelRequest, emit func(string) error) (agent.ModelResult, error) {
	if err := ctx.Err(); err != nil {
		return agent.ModelResult{}, err
	}
	if len(request.Messages) == 0 {
		return agent.ModelResult{}, agent.ErrInvalid
	}
	out := "plan:" + request.Messages[0].Content
	if err := emit(out); err != nil {
		return agent.ModelResult{}, err
	}
	return agent.ModelResult{Content: out}, nil
}
func modelDefinition(value agent.Model) forest.Definition {
	if value == nil {
		value = model{}
	}
	return forest.Definition{TypeID: "temporal-example.model", Version: "1", Capabilities: []forest.CapabilityDescriptor{forest.Capability[agent.Model]("model", forest.Public)}, Build: func(_ context.Context, e *forest.BuildEnv) (forest.Lifecycle, error) {
		return forest.Hooks{}, forest.Provide(e, "model", value)
	}}
}
