// Package ecosystem is an application example, not a generic persistence or
// distributed transaction framework. Run exactly one driver per namespace.
package ecosystem

import (
	"context"
	"errors"
	"regexp"
	"sync"
	"time"

	"github.com/Leeminjing/go-forest-agent/agent"
	"github.com/Leeminjing/go-forest-agent/trees/calculator"
	"github.com/Leeminjing/go-forest-agent/trees/events"
	"github.com/Leeminjing/go-forest-agent/trees/filecheckpoint"
	"github.com/Leeminjing/go-forest-agent/trees/manualapproval"
	"github.com/Leeminjing/go-forest-agent/trees/scriptedmodel"
	ginforest "github.com/Leeminjing/go-forest/adapters/gin"
	dbtree "github.com/Leeminjing/go-forest/adapters/gorm"
	grpcforest "github.com/Leeminjing/go-forest/adapters/grpc"
	nt "github.com/Leeminjing/go-forest/adapters/nats"
	"github.com/Leeminjing/go-forest/extensions/observatory"
	"github.com/Leeminjing/go-forest/extensions/telemetry"
	"github.com/Leeminjing/go-forest/forest"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"go.opentelemetry.io/otel/propagation"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

type Options struct {
	DSN, NATSURL, Namespace, CheckpointDir, Token string
	HTTPAddress, GRPCAddress, ObservatoryAddress  string
	BeforeSubmit                                  func(context.Context, string) error `json:"-"`
	AfterPublish, AfterReceive, AfterAgentCreate  func(context.Context) error         `json:"-"`
	Telemetry                                     telemetry.Options                   `json:"-"`
}
type Runtime struct {
	Forest                              *forest.Forest
	Telemetry                           *telemetry.Recorder
	HTTPURL, GRPCTarget, ObservatoryURL string
	options                             Options
	tickMu                              sync.Mutex
	manageMu                            sync.Mutex
	cancel                              context.CancelFunc
	done                                chan struct{}
	mu                                  sync.Mutex
	driverErrors                        uint64
}

var validNamespace = regexp.MustCompile(`^[A-Za-z][A-Za-z0-9_]{0,47}$`)

func openDB(dsn string) (*gorm.DB, error) {
	return gorm.Open(postgres.Open(dsn), &gorm.Config{DisableAutomaticPing: true, Logger: logger.Default.LogMode(logger.Silent)})
}
func connect(url string) func(context.Context) (*nats.Conn, error) {
	return func(ctx context.Context) (*nats.Conn, error) {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		deadline := 3 * time.Second
		if d, ok := ctx.Deadline(); ok {
			deadline = time.Until(d)
			if deadline <= 0 {
				return nil, ctx.Err()
			}
		}
		return nats.Connect(url, nats.Timeout(deadline), nats.MaxReconnects(-1))
	}
}

// Initialize is a separate, explicitly privileged operation. It is never called
// by a candidate Factory or Start and is not a Forest rollbackable transaction.
func Initialize(ctx context.Context, o Options) error {
	if !validNamespace.MatchString(o.Namespace) || o.DSN == "" || o.NATSURL == "" {
		return ErrInvalid
	}
	db, err := openDB(o.DSN)
	if err != nil {
		return err
	}
	pool, err := db.DB()
	if err != nil {
		return err
	}
	defer pool.Close()
	if err = db.WithContext(ctx).AutoMigrate(&Task{}, &outbox{}, &inbox{}); err != nil {
		return err
	}
	nc, err := connect(o.NATSURL)(ctx)
	if err != nil {
		return err
	}
	defer nc.Close()
	js, err := jetstream.New(nc)
	if err != nil {
		return err
	}
	if _, err = js.CreateStream(ctx, jetstream.StreamConfig{Name: o.Namespace, Subjects: []string{o.Namespace}, Storage: jetstream.FileStorage, Duplicates: time.Second}); err != nil {
		return err
	}
	_, err = js.CreateConsumer(ctx, o.Namespace, jetstream.ConsumerConfig{Durable: "worker", FilterSubject: o.Namespace, AckPolicy: jetstream.AckExplicitPolicy, AckWait: 15 * time.Second, MaxDeliver: 5, MaxAckPending: 2})
	return err
}
func New(ctx context.Context, o Options) (r *Runtime, err error) {
	if !validNamespace.MatchString(o.Namespace) || o.CheckpointDir == "" || o.DSN == "" || o.NATSURL == "" {
		return nil, ErrInvalid
	}
	f := forest.New(forest.Options{})
	r = &Runtime{Forest: f, options: o}
	defer func() {
		if err != nil {
			c, x := context.WithTimeout(context.Background(), 5*time.Second)
			defer x()
			f.Close(c)
		}
	}()
	telemetryOptions := o.Telemetry
	telemetryOptions.Operations = []string{"http.submit", "http.get", "rpc.submit", "publish", "receive", "agent.step"}
	r.Telemetry, err = telemetry.New(f, telemetryOptions)
	if err != nil {
		return nil, err
	}
	db, err := dbtree.Definition(f, dbtree.Options{TypeID: "ecosystem.gorm", Version: "1", Ownership: dbtree.Owned, Open: func(context.Context) (*gorm.DB, error) { return openDB(o.DSN) }})
	if err != nil {
		return nil, err
	}
	connection := nt.Connection{Ownership: nt.Owned, Connect: connect(o.NATSURL)}
	pub, err := nt.PublisherDefinition(f, nt.PublisherOptions{TypeID: "ecosystem.publisher", Version: "1", Connection: connection})
	if err != nil {
		return nil, err
	}
	cons, err := nt.ConsumerDefinition(f, nt.ConsumerOptions{TypeID: "ecosystem.consumer", Version: "1", Stream: o.Namespace, Consumer: "worker", Connection: connection, Concurrency: 2, ProcessTimeout: 10 * time.Second})
	if err != nil {
		return nil, err
	}
	http, err := httpDefinition(f, o)
	if err != nil {
		return nil, err
	}
	rpc, err := rpcDefinition(f, o)
	if err != nil {
		return nil, err
	}
	bundle, err := observatory.New(f, observatory.Options{Token: o.Token, Address: o.ObservatoryAddress})
	if err != nil {
		return nil, err
	}
	root := forest.Definition{TypeID: "ecosystem.root", Version: "1", ExportChildren: true, Build: func(context.Context, *forest.BuildEnv) (forest.Lifecycle, error) { return forest.Hooks{}, nil }}
	defs := map[forest.TreeID]forest.Definition{"app": root, "app/db": db, "app/publisher": pub, "app/consumer": cons, "app/repository": repositoryDefinition(o.Namespace), "app/service": serviceDefinition("v1", o.BeforeSubmit), "app/handler": handlerDefinition(o.AfterReceive), "app/http": http, "app/grpc": rpc, "app/telemetry": r.Telemetry.Definition("ecosystem.telemetry"), "app/agent": agent.Definition(agent.Options{}), "app/checkpoint": filecheckpoint.Definition(o.CheckpointDir), "app/model": scriptedmodel.Definition("v1"), "app/tool": calculator.Definition(false), "app/events": events.Definition(events.New(4096)), "app/approval": manualapproval.Definition()}
	for _, d := range defs {
		if err = f.Register(d); err != nil {
			return nil, err
		}
	}
	if err = f.Register(serviceDefinition("v2", nil)); err != nil {
		return nil, err
	}
	for _, d := range bundle.Definitions {
		if err = f.Register(d); err != nil {
			return nil, err
		}
	}
	placement, err := bundle.Blueprint.Instantiate("observatory", "")
	if err != nil {
		return nil, err
	}
	plan := placement.Plan(0)
	plan.Operations = append(plan.Operations, forest.AddRoot("app", root.Ref()))
	for id, d := range defs {
		if id == "app" {
			continue
		}
		plan.Operations = append(plan.Operations, forest.Nest("app", id, d.Ref()))
	}
	// Operations may reference a later node; validation sees the complete candidate.
	for _, l := range []forest.Link{
		{ID: "db", Consumer: "app/repository", Port: "database", Provider: "app/db", Capability: "database"},
		{ID: "service.repo", Consumer: "app/service", Port: "repository", Provider: "app/repository", Capability: "repository"},
		{ID: "handler.repo", Consumer: "app/handler", Port: "repository", Provider: "app/repository", Capability: "repository"},
		{ID: "consumer.handler", Consumer: "app/consumer", Port: "handler", Provider: "app/handler", Capability: "handle"},
		{ID: "http.service", Consumer: "app/http", Port: "tasks", Provider: "app/service", Capability: "tasks"},
		{ID: "rpc.service", Consumer: "app/grpc", Port: "tasks", Provider: "app/service", Capability: "tasks"},
		{ID: "http.telemetry", Consumer: "app/http", Port: "telemetry", Provider: "app/telemetry", Capability: "runtime"},
		{ID: "rpc.telemetry", Consumer: "app/grpc", Port: "telemetry", Provider: "app/telemetry", Capability: "runtime"},
		{ID: "handler.telemetry", Consumer: "app/handler", Port: "telemetry", Provider: "app/telemetry", Capability: "runtime"},
		{ID: "observatory.runtime", Consumer: "observatory", Port: "runtime", Provider: "app/telemetry", Capability: "runtime"},
	} {
		plan.Operations = append(plan.Operations, forest.Connect(l))
	}
	for port, provider := range map[forest.PortID]struct {
		id  forest.TreeID
		cap forest.CapabilityID
	}{"checkpoint": {"app/checkpoint", "checkpoint"}, "model": {"app/model", "model"}, "tools": {"app/tool", "tool"}, "events": {"app/events", "events"}, "approval": {"app/approval", "approval"}} {
		plan.Operations = append(plan.Operations, forest.Connect(forest.Link{ID: "agent." + string(port), Consumer: "app/agent", Port: port, Provider: provider.id, Capability: provider.cap}))
	}
	if _, err = f.Apply(ctx, plan); err != nil {
		return nil, err
	}
	err = f.Invoke(ctx, func(c context.Context) error {
		if err := forest.Use[ginforest.Endpoint](c, f, "app/http", "http", func(e ginforest.Endpoint) error { r.HTTPURL = e.URL(); return nil }); err != nil {
			return err
		}
		if err := forest.Use[ginforest.Endpoint](c, f, "observatory", "http", func(e ginforest.Endpoint) error { r.ObservatoryURL = e.URL(); return nil }); err != nil {
			return err
		}
		return forest.Use[grpcforest.Endpoint](c, f, "app/grpc", "rpc", func(e grpcforest.Endpoint) error { r.GRPCTarget = e.Target(); return nil })
	})
	return r, err
}
func (r *Runtime) repository(ctx context.Context, fn func(context.Context, repositoryAPI) error) error {
	return r.Forest.Invoke(ctx, func(c context.Context) error {
		return forest.Use[repositoryAPI](c, r.Forest, "app/repository", "repository", func(repo repositoryAPI) error { return fn(c, repo) })
	})
}
func (r *Runtime) Enable(ctx context.Context) error {
	return r.Forest.Invoke(ctx, func(c context.Context) error {
		return forest.Use[nt.Control](c, r.Forest, "app/consumer", "control", func(w nt.Control) error { return w.Enable(c) })
	})
}

// Tick performs bounded relay and driver work outside a long-lived Scope. Hosts
// serialize Tick and ensure no other process drives this namespace/checkpoint.
func (r *Runtime) Tick(ctx context.Context) error {
	r.tickMu.Lock()
	defer r.tickMu.Unlock()
	var pending []outbox
	if err := r.repository(ctx, func(c context.Context, p repositoryAPI) error { var err error; pending, err = p.Pending(c); return err }); err != nil {
		return err
	}
	for _, row := range pending {
		err := r.Forest.Invoke(ctx, func(c context.Context) error {
			return r.Telemetry.Measure(c, "publish", func(c context.Context) error {
				return forest.Use[nt.Publisher](c, r.Forest, "app/publisher", "publisher", func(p nt.Publisher) error {
					header := nats.Header{jetstream.MsgIDHeader: []string{r.options.Namespace + ":" + row.ID}}
					if row.Traceparent != "" {
						header.Set("traceparent", row.Traceparent)
					}
					if row.Tracestate != "" {
						header.Set("tracestate", row.Tracestate)
					}
					_, err := p.Publish(c, &nats.Msg{Subject: r.options.Namespace, Data: row.Data, Header: header})
					return err
				})
			})
		})
		if err != nil {
			return err
		}
		if r.options.AfterPublish != nil {
			if err = r.options.AfterPublish(ctx); err != nil {
				return err
			}
		}
		if err = r.repository(ctx, func(c context.Context, p repositoryAPI) error { return p.MarkSent(c, row.ID) }); err != nil {
			return err
		}
	}
	var tasks []Task
	if err := r.repository(ctx, func(c context.Context, p repositoryAPI) error { var err error; tasks, err = p.Runnable(c); return err }); err != nil {
		return err
	}
	for _, task := range tasks {
		if err := r.drive(ctx, task); err != nil {
			return err
		}
	}
	return nil
}

// Trace context is metadata only; it never transfers a Forest Scope.
var tracePropagation = propagation.TraceContext{}

func (r *Runtime) drive(ctx context.Context, task Task) error {
	id := r.options.Namespace + ":" + task.ID
	var run agent.Run
	for steps := 0; steps < 16; steps++ {
		err := r.Forest.Invoke(ctx, func(c context.Context) error {
			return r.Telemetry.Measure(c, "agent.step", func(c context.Context) error {
				return forest.Use[agent.Executor](c, r.Forest, "app/agent", "executor", func(e agent.Executor) error {
					var err error
					run, err = e.Read(c, id)
					if errors.Is(err, agent.ErrNotFound) {
						run, err = e.Create(c, id, task.Input)
						if err == nil && r.options.AfterAgentCreate != nil {
							err = r.options.AfterAgentCreate(c)
						}
						return err
					}
					if err != nil {
						return err
					}
					if run.Status == agent.ModelRunning || run.Status == agent.ToolRunning {
						run, err = e.Recover(c, id)
						return err
					}
					switch run.Status {
					case agent.Completed, agent.Failed, agent.Canceled, agent.AwaitingApproval, agent.OutcomeUnknown:
						return nil
					}
					run, err = e.Advance(c, id)
					return err
				})
			})
		})
		if err != nil {
			return err
		}
		switch run.Status {
		case agent.Completed, agent.Failed, agent.Canceled, agent.AwaitingApproval, agent.OutcomeUnknown:
			result := ""
			if len(run.Messages) > 0 {
				result = run.Messages[len(run.Messages)-1].Content
			}
			return r.repository(ctx, func(c context.Context, p repositoryAPI) error {
				return p.Finish(c, task.ID, string(run.Status), result)
			})
		}
	}
	return nil
}
func (r *Runtime) Start(ctx context.Context) error {
	r.manageMu.Lock()
	defer r.manageMu.Unlock()
	if r.cancel != nil {
		return ErrConflict
	}
	if err := r.Enable(ctx); err != nil {
		return err
	}
	life, cancel := context.WithCancel(context.Background())
	r.cancel = cancel
	r.done = make(chan struct{})
	go func() {
		defer close(r.done)
		ticker := time.NewTicker(100 * time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case <-life.Done():
				return
			case <-ticker.C:
				if err := r.Tick(life); err != nil {
					r.mu.Lock()
					r.driverErrors++
					r.mu.Unlock()
				}
			}
		}
	}()
	return nil
}
func (r *Runtime) DriverErrors() uint64 { r.mu.Lock(); defer r.mu.Unlock(); return r.driverErrors }
func (r *Runtime) Replace(ctx context.Context) (uint64, error) {
	return r.Forest.Apply(ctx, forest.ChangePlan{BaseGeneration: r.Forest.Snapshot().Generation, Operations: []forest.Operation{forest.Replace("app/service", forest.DefinitionRef{TypeID: "ecosystem.service", Version: "v2"}, false)}})
}
func (r *Runtime) Close(ctx context.Context) error {
	r.manageMu.Lock()
	defer r.manageMu.Unlock()
	if r.cancel != nil {
		r.cancel()
		select {
		case <-r.done:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	err := r.Forest.Invoke(ctx, func(c context.Context) error {
		return forest.Use[nt.Control](c, r.Forest, "app/consumer", "control", func(w nt.Control) error { return w.Quiesce(c) })
	})
	if err != nil && !errors.Is(err, forest.ErrClosed) {
		return err
	}
	return r.Forest.Close(ctx)
}
