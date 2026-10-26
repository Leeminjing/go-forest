// Package crossdemo composes released adapters without changing their runtimes.
package crossdemo

import (
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	ff "github.com/Leeminjing/go-forest/adapters/fx"
	gf "github.com/Leeminjing/go-forest/adapters/gorm"
	tf "github.com/Leeminjing/go-forest/adapters/temporal"
	"github.com/Leeminjing/go-forest/examples/cross-adapter/flow"
	"github.com/Leeminjing/go-forest/examples/cross-adapter/internal/activitybridge"
	"github.com/Leeminjing/go-forest/examples/cross-adapter/internal/business"
	"github.com/Leeminjing/go-forest/examples/cross-adapter/internal/fxmodule"
	"github.com/Leeminjing/go-forest/examples/cross-adapter/protocol"
	"github.com/Leeminjing/go-forest/forest"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/stdlib"
	enumspb "go.temporal.io/api/enums/v1"
	historypb "go.temporal.io/api/history/v1"
	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/log"
	"go.temporal.io/sdk/worker"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

var ErrUnavailable = errors.New("cross-adapter application closing or unavailable")

type Options struct {
	Address, DSN, TaskQueue, Directory, Version string
	Namespace                                   string
	// Forest may contain unrelated existing trees. The runtime removes only its
	// own app/database and never closes a supplied forest.
	Forest          *forest.Forest
	BetweenReads    func(context.Context, protocol.Request) error
	BeforeCommit    func(context.Context, protocol.Request) error
	AfterCommit     func(context.Context, protocol.Receipt) error
	Child           func(context.Context) error
	Prepared        func(context.Context, *Runtime) error
	BusinessStopped func(string)
}
type Ref struct{ WorkflowID, RunID string }
type ResourceReport struct {
	FileOpen       bool
	FileCloses     int
	PoolResponding bool
}
type Runtime struct {
	Forest            *forest.Forest
	Process           string
	options           Options
	native            *fxmodule.Resource
	bridge            *activitybridge.Bridge
	poolMu            sync.Mutex
	pool              *sql.DB
	management        chan struct{}
	requests          requestGate
	removed, switched bool
	businessID        forest.TreeID
	ownsForest        bool
	businessCalls     atomic.Int64
}

func open(dsn, name string) (*gorm.DB, *sql.DB, error) {
	cfg, err := pgx.ParseConfig(dsn)
	if err != nil {
		return nil, nil, err
	}
	cfg.ConnectTimeout = 3 * time.Second
	cfg.RuntimeParams["application_name"] = name
	pool := stdlib.OpenDB(*cfg)
	db, err := gorm.Open(postgres.New(postgres.Config{Conn: pool}), &gorm.Config{DisableAutomaticPing: true, Logger: logger.Default.LogMode(logger.Silent)})
	if err != nil {
		pool.Close()
		return nil, nil, err
	}
	return db, pool, nil
}

// Setup is explicit administration, outside Forest topology atomicity.
func Setup(ctx context.Context, dsn string) error {
	db, pool, err := open(dsn, "forest-cross-setup")
	if err != nil {
		return err
	}
	defer pool.Close()
	return db.WithContext(ctx).AutoMigrate(&business.Record{}, &business.Effect{})
}

// New prepares and commits the composition. Once committed, identity inspection
// is independent of caller cancellation. A non-nil Runtime with an error means
// publication succeeded but final inspection failed; the caller retains Close.
func New(ctx context.Context, o Options) (r *Runtime, err error) {
	if o.Address == "" || o.DSN == "" || o.TaskQueue == "" || o.Directory == "" {
		return nil, errors.New("address, DSN, queue and resource directory required")
	}
	if o.Version == "" {
		o.Version = "v1"
	}
	if o.Version != "v1" && o.Version != "v2" {
		return nil, protocol.ErrConflict
	}
	identity := make([]byte, 12)
	if _, err = rand.Read(identity); err != nil {
		return nil, err
	}
	f := o.Forest
	owns := f == nil
	if owns {
		f = forest.New(forest.Options{DrainTimeout: time.Second})
	}
	r = &Runtime{Forest: f, Process: "cross-" + hex.EncodeToString(identity), options: o, native: &fxmodule.Resource{}, management: make(chan struct{}, 1), businessID: "app/business", ownsForest: owns}
	r.management <- struct{}{}
	r.requests.init()
	committed := false
	defer func() {
		if err != nil && owns && !committed {
			c, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			f.Close(c)
		}
	}()
	db, err := gf.Definition(f, gf.Options{TypeID: "cross.database", Version: "1", Ownership: gf.Owned, Open: func(context.Context) (*gorm.DB, error) {
		d, p, e := open(o.DSN, r.Process)
		r.poolMu.Lock()
		r.pool = p
		r.poolMu.Unlock()
		return d, e
	}})
	if err != nil {
		return nil, err
	}
	fxdef, err := fxmodule.Definition(f, fxmodule.Options{Process: r.Process, Directory: o.Directory, Resource: r.native, BetweenReads: o.BetweenReads, Child: o.Child})
	if err != nil {
		return nil, err
	}
	cd, err := tf.ClientDefinition(f, tf.ClientOptions{TypeID: "cross.client", Version: "1", Native: client.Options{HostPort: o.Address, Namespace: o.Namespace}})
	if err != nil {
		return nil, err
	}
	wd, err := tf.WorkerDefinition(f, tf.WorkerOptions{TypeID: "cross.worker", Version: "1", TreeID: "app/worker", TaskQueue: o.TaskQueue, Client: client.Options{HostPort: o.Address, Namespace: o.Namespace}, Native: worker.Options{WorkerStopTimeout: 150 * time.Millisecond}, Ports: []forest.PortDescriptor{forest.Port[activitybridge.Borrower]("processor", forest.Required())}, BuildRegistration: func(e *forest.BuildEnv) (tf.Register, error) {
		ref, err := forest.Reference[activitybridge.Borrower](e, "processor")
		if err != nil {
			return nil, err
		}
		r.bridge = &activitybridge.Bridge{Forest: f, Service: ref, Process: r.Process}
		return func(reg tf.Registry) error {
			reg.RegisterWorkflow(flow.Command)
			reg.RegisterActivityWithOptions(r.bridge.Apply, activity.RegisterOptions{Name: "cross.apply"})
			reg.RegisterActivityWithOptions(r.bridge.Inspect, activity.RegisterOptions{Name: "cross.inspect"})
			return nil
		}, nil
	}})
	if err != nil {
		return nil, err
	}
	owner := forest.Definition{TypeID: "cross.app", Version: "1", ExportChildren: true, Build: func(context.Context, *forest.BuildEnv) (forest.Lifecycle, error) {
		return forest.Hooks{OnReady: func(c context.Context) error {
			if o.Prepared != nil {
				return o.Prepared(c, r)
			}
			return nil
		}}, nil
	}}
	businessDef := func(v string) forest.Definition {
		return business.Definition(f, business.Options{Version: v, Process: r.Process, BeforeCommit: o.BeforeCommit, AfterCommit: o.AfterCommit, Stopped: o.BusinessStopped, Called: func() { r.businessCalls.Add(1) }})
	}
	v1, v2 := businessDef("v1"), businessDef("v2")
	selected := v1
	if o.Version == "v2" {
		selected = v2
	}
	for _, d := range []forest.Definition{owner, db, fxdef, cd, wd, v1, v2} {
		if err = f.Register(d); err != nil {
			return nil, err
		}
	}
	_, err = f.Apply(ctx, forest.ChangePlan{BaseGeneration: f.Snapshot().Generation, Operations: []forest.Operation{
		forest.AddRoot("app", owner.Ref()), forest.AddRoot("database", db.Ref()), forest.Nest("app", "app/client", cd.Ref()), forest.Nest("app", "app/worker", wd.Ref()), forest.Nest("app", "app/fx", fxdef.Ref()), forest.Nest("app", "app/business", selected.Ref()),
		forest.Connect(forest.Link{ID: "worker.fx", Consumer: "app/worker", Port: "processor", Provider: "app/fx", Capability: "service"}),
		forest.Connect(forest.Link{ID: "fx.business", Consumer: "app/fx", Port: "business", Provider: "app/business", Capability: "commands"}),
		forest.Connect(forest.Link{ID: "business.db", Consumer: "app/business", Port: "database", Provider: "database", Capability: "database"}),
	}})
	if err != nil {
		return nil, err
	}
	committed = true
	finalize, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	w, err := tf.Inspect(finalize, f, "app/worker")
	if err != nil {
		return r, fmt.Errorf("inspect committed worker: %w", err)
	}
	x, err := ff.Inspect(finalize, f, "app/fx")
	if err != nil {
		return r, fmt.Errorf("inspect committed Fx app: %w", err)
	}
	r.bridge.WorkerIdentity = w.Identity
	r.bridge.FxIdentity = x.Identity
	return r, nil
}
func (r *Runtime) lock(ctx context.Context) (func(), error) {
	if _, err := r.Forest.Generation(ctx); err == nil {
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
	unlock, err := r.lock(ctx)
	if err != nil {
		return err
	}
	defer unlock()
	if r.requests.isClosed() {
		return ErrUnavailable
	}
	if err = ff.Enable(ctx, r.Forest, "app/fx"); err != nil {
		return err
	}
	return tf.Enable(ctx, r.Forest, "app/worker")
}
func (r *Runtime) useClient(ctx context.Context, fn func(context.Context, client.Client) error) error {
	c, leave, err := r.requests.enter(ctx)
	if err != nil {
		return err
	}
	defer leave()
	return r.Forest.Invoke(c, func(call context.Context) error {
		return forest.Use[tf.ClientAccess](call, r.Forest, "app/client", "client", func(a tf.ClientAccess) error { return a.Use(call, func(c client.Client) error { return fn(call, c) }) })
	})
}
func (r *Runtime) Start(ctx context.Context, workflowID string, in protocol.Request, wait bool) (out Ref, err error) {
	if workflowID == "" || !business.Valid(in) {
		return out, protocol.ErrConflict
	}
	err = r.useClient(ctx, func(call context.Context, c client.Client) error {
		run, e := c.ExecuteWorkflow(call, client.StartWorkflowOptions{ID: workflowID, TaskQueue: r.options.TaskQueue, WorkflowIDReusePolicy: enumspb.WORKFLOW_ID_REUSE_POLICY_REJECT_DUPLICATE}, flow.Command, flow.Input{Request: in, Wait: wait})
		if e == nil {
			out = Ref{WorkflowID: run.GetID(), RunID: run.GetRunID()}
		}
		return e
	})
	return
}
func (r *Runtime) Result(ctx context.Context, id Ref) (out protocol.Result, err error) {
	err = r.useClient(ctx, func(c context.Context, n client.Client) error {
		return n.GetWorkflow(c, id.WorkflowID, id.RunID).Get(c, &out)
	})
	return
}
func (r *Runtime) Cancel(ctx context.Context, id Ref) error {
	return r.useClient(ctx, func(c context.Context, n client.Client) error { return n.CancelWorkflow(c, id.WorkflowID, id.RunID) })
}
func (r *Runtime) Signal(ctx context.Context, id Ref) error {
	return r.useClient(ctx, func(c context.Context, n client.Client) error {
		return n.SignalWorkflow(c, id.WorkflowID, id.RunID, "continue", true)
	})
}

func (r *Runtime) Query(ctx context.Context, id Ref) (out flow.State, err error) {
	err = r.useClient(ctx, func(c context.Context, n client.Client) error {
		v, e := n.QueryWorkflow(c, id.WorkflowID, id.RunID, "state")
		if e != nil {
			return e
		}
		return v.Get(&out)
	})
	return
}

// BusinessCalls counts actual application method entries, not Scope references.
func (r *Runtime) BusinessCalls() int64 { return r.businessCalls.Load() }

type HostStatus struct {
	Active  int
	Closing bool
}

func (r *Runtime) HostStatus() HostStatus {
	r.requests.mu.Lock()
	defer r.requests.mu.Unlock()
	return HostStatus{Active: len(r.requests.calls), Closing: r.requests.closed}
}
func (r *Runtime) Inspect(ctx context.Context, in protocol.Request) (out protocol.Observation, err error) {
	c, leave, err := r.requests.enter(ctx)
	if err != nil {
		return out, err
	}
	defer leave()
	err = r.Forest.Invoke(c, func(call context.Context) error {
		return forest.Use[ff.Access[activitybridge.Processor]](call, r.Forest, "app/fx", "service", func(a ff.Access[activitybridge.Processor]) error {
			return a.Use(call, func(call context.Context, p activitybridge.Processor) error {
				var e error
				out, e = p.Inspect(call, in)
				return e
			})
		})
	})
	return
}
func (r *Runtime) Switch(ctx context.Context, mode string) error {
	unlock, err := r.lock(ctx)
	if err != nil {
		return err
	}
	defer unlock()
	if r.requests.isClosed() || r.switched || r.options.Version != "v1" {
		return ErrUnavailable
	}
	d := forest.DefinitionRef{TypeID: "cross.business", Version: "v2"}
	ops := []forest.Operation{}
	if mode == "replace" {
		ops = append(ops, forest.Replace(r.businessID, d, false))
	} else if mode == "rebind" {
		next := forest.TreeID("app/business-v2")
		ops = append(ops, forest.Nest("app", next, d), forest.Unlink("business.db"), forest.Connect(forest.Link{ID: "new.db", Consumer: next, Port: "database", Provider: "database", Capability: "database"}), forest.Rebind(forest.Link{ID: "fx.business", Consumer: "app/fx", Port: "business", Provider: next, Capability: "commands"}), forest.Remove(r.businessID))
	} else {
		return protocol.ErrConflict
	}
	_, err = r.Forest.Apply(ctx, forest.ChangePlan{BaseGeneration: r.Forest.Snapshot().Generation, Operations: ops})
	if err == nil {
		r.switched = true
		if mode == "rebind" {
			r.businessID = "app/business-v2"
		}
	}
	return err
}
func (r *Runtime) Replay(ctx context.Context, id Ref) error {
	return r.useClient(ctx, func(c context.Context, n client.Client) error {
		it := n.GetWorkflowHistory(c, id.WorkflowID, id.RunID, false, 0)
		history := &historypb.History{}
		for it.HasNext() {
			event, e := it.Next()
			if e != nil {
				return e
			}
			history.Events = append(history.Events, event)
		}
		replayer := worker.NewWorkflowReplayer()
		replayer.RegisterWorkflow(flow.Command)
		return replayer.ReplayWorkflowHistory(log.NewStructuredLogger(slog.New(slog.NewTextHandler(io.Discard, nil))), history)
	})
}
func (r *Runtime) Resources(ctx context.Context) ResourceReport {
	r.poolMu.Lock()
	pool := r.pool
	r.poolMu.Unlock()
	return ResourceReport{FileOpen: r.native.Check() == nil, FileCloses: r.native.Closes(), PoolResponding: pool != nil && pool.PingContext(ctx) == nil}
}
func (r *Runtime) Close(ctx context.Context) error {
	unlock, err := r.lock(ctx)
	if err != nil {
		return err
	}
	defer unlock()
	r.requests.seal()
	select {
	case <-r.requests.done:
	case <-ctx.Done():
		return ctx.Err()
	}
	if !r.removed {
		if err = tf.Quiesce(ctx, r.Forest, "app/worker"); err != nil {
			return err
		}
		if err = ff.Quiesce(ctx, r.Forest, "app/fx"); err != nil {
			return err
		}
		if _, err = r.Forest.Apply(ctx, forest.ChangePlan{BaseGeneration: r.Forest.Snapshot().Generation, Operations: []forest.Operation{forest.RemoveSubtree("app"), forest.Remove("database")}}); err != nil {
			return err
		}
		r.removed = true
	}
	if err = r.Forest.Wait(ctx); err != nil {
		return err
	}
	if r.ownsForest {
		return r.Forest.Close(ctx)
	}
	return nil
}
