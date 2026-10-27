package ecosystem

import (
	"context"
	"errors"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	dbtree "github.com/Leeminjing/go-forest/adapters/gorm"
	nt "github.com/Leeminjing/go-forest/adapters/nats"
	"github.com/Leeminjing/go-forest/forest"
	tk "github.com/Leeminjing/go-forest/testkit"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"
	"gorm.io/gorm"
)

type confHook func(context.Context, string, confCall, confReceipt) error
type confFixture struct {
	parentTrace                   string
	entryTraces                   map[string][]string
	ctx                           context.Context
	o                             Options
	r                             *Runtime
	nc                            *nats.Conn
	js                            jetstream.JetStream
	native                        jetstream.Consumer
	domain, streamKey, durableKey string
	evidence                      confEvidence
	bundle                        *confBundle
	hook                          confHook
	borrowCloses                  atomic.Uint64
	entriesMu                     sync.Mutex
	entries                       map[string][]tk.Execution
}
type confBundle struct {
	f          *confFixture
	pool       confPool
	conn       *nats.Conn
	connCloses atomic.Uint64
	handlersMu sync.Mutex
	handlers   map[string]*confHandler
	defs       map[string]forest.Definition
	ready      func(context.Context, *confBundle) error
}
type confHandler struct {
	b        *confBundle
	store    forest.Ref[confStoreAPI]
	next     forest.Ref[nt.Handler]
	file     confFile
	version  string
	identity atomic.Value
}

func (b *confBundle) switchVersion(mode string) error {
	f := b.f
	tree := "app/proof/handler"
	ops := []forest.Operation{forest.Remove("app/proof/gate"), forest.Replace(forest.TreeID(tree), b.defs["v2"].Ref(), false)}
	if mode == "rebind" {
		tree = "app/proof/handler-v2"
		ops = []forest.Operation{forest.Remove("app/proof/gate"), forest.Nest("app/proof", forest.TreeID(tree), b.defs["v2"].Ref()), forest.Connect(forest.Link{ID: "proof.v2.store", Consumer: forest.TreeID(tree), Port: "store", Provider: "app/proof/store", Capability: "store"}), forest.Connect(forest.Link{ID: "proof.v2.next", Consumer: forest.TreeID(tree), Port: "next", Provider: "app/handler", Capability: "handle"}), forest.Rebind(forest.Link{ID: "proof.consumer", Consumer: "app/proof/consumer", Port: "handler", Provider: forest.TreeID(tree), Capability: "handle"}), forest.Remove("app/proof/handler")}
	} else if mode != "replace" {
		return ErrInvalid
	}
	if _, err := f.r.Forest.Apply(f.ctx, forest.ChangePlan{BaseGeneration: f.generation(), Operations: ops}); err != nil {
		return err
	}
	return b.anchor("v2", tree)
}

func (h *confHandler) Handle(ctx context.Context, m nt.Message) (err error) {
	defer func() {
		if err != nil {
			h.b.f.evidence.reject(m.StreamSequence, err)
		}
	}()
	instance, _ := h.identity.Load().(string)
	if instance == "" {
		return errorsNewEvidence()
	}
	carrier := propagation.MapCarrier{}
	for _, key := range []string{"traceparent", "tracestate"} {
		carrier[key] = m.Header.Get(key)
	}
	ctx = tracePropagation.Extract(ctx, carrier)
	g, err := h.b.f.r.Forest.Generation(ctx)
	if err != nil {
		return err
	}
	call := confMessageCall(h.b.f.domain, h.b.f.streamKey, h.b.f.durableKey, instance, h.version, m, g)
	ctx = context.WithValue(ctx, confCallKey{}, call)
	at := func(stage string, receipt confReceipt) error {
		if err := h.b.f.evidence.at(ctx, h.b.f.r.Forest, stage); err != nil {
			return err
		}
		if h.b.f.hook != nil {
			return h.b.f.hook(ctx, stage, call, receipt)
		}
		return nil
	}
	if err = at("handler.enter", confReceipt{}); err != nil {
		return
	}
	if err = h.store.Use(ctx, func(s confStoreAPI) error { return s.Observe(ctx) }); err != nil {
		return
	}
	if err = at("handler.between", confReceipt{}); err != nil {
		return
	}
	var receipt confReceipt
	if err = h.store.Use(ctx, func(s confStoreAPI) error { var e error; receipt, e = s.Apply(ctx); return e }); err != nil {
		return
	}
	if err = h.next.Use(ctx, func(next nt.Handler) error { return next.Handle(ctx, m) }); err != nil {
		return
	}
	if err = at("handler.committed", receipt); err != nil {
		return
	}
	if err = at("handler.return", receipt); err != nil {
		return
	}
	return h.b.f.evidence.finish(ctx, receipt)
}
func (b *confBundle) handlerDefinition(version string) forest.Definition {
	return forest.Definition{TypeID: b.f.o.Namespace + ".proof.handler", Version: version, HotSwappable: true, Ports: []forest.PortDescriptor{forest.Port[confStoreAPI]("store", forest.Required()), forest.Port[nt.Handler]("next", forest.Required())}, Capabilities: []forest.CapabilityDescriptor{forest.Capability[nt.Handler]("handle", forest.Public)}, Build: func(_ context.Context, e *forest.BuildEnv) (forest.Lifecycle, error) {
		s, err := forest.Reference[confStoreAPI](e, "store")
		if err != nil {
			return nil, err
		}
		n, err := forest.Reference[nt.Handler](e, "next")
		if err != nil {
			return nil, err
		}
		h := &confHandler{b: b, store: s, next: n, version: version}
		b.handlersMu.Lock()
		b.handlers[version] = h
		b.handlersMu.Unlock()
		return forest.Hooks{OnStart: func(context.Context) error { return h.file.open(b.f.o.CheckpointDir) }, OnStop: func(context.Context) error { return h.file.close() }}, forest.Provide(e, "handle", h)
	}}
}
func (b *confBundle) definitions() error {
	f := b.f
	root := forest.Definition{TypeID: f.o.Namespace + ".proof.root", Version: "1", ExportChildren: true, Build: func(context.Context, *forest.BuildEnv) (forest.Lifecycle, error) { return forest.Hooks{}, nil }}
	db, err := dbtree.Definition(f.r.Forest, dbtree.Options{TypeID: f.o.Namespace + ".proof.db", Version: "1", Ownership: dbtree.Owned, Open: func(context.Context) (*gorm.DB, error) { return b.pool.open(f.o.DSN) }})
	if err != nil {
		return err
	}
	consumer, err := nt.ConsumerDefinition(f.r.Forest, nt.ConsumerOptions{TypeID: f.o.Namespace + ".proof.consumer", Version: "1", Stream: f.o.Namespace, Consumer: "conformance", Concurrency: 2, ProcessTimeout: 10 * time.Second, Connection: nt.Connection{Ownership: nt.Owned, Connect: func(c context.Context) (*nats.Conn, error) {
		nc, e := connect(f.o.NATSURL)(c)
		if nc != nil {
			b.conn = nc
			nc.SetClosedHandler(func(*nats.Conn) { b.connCloses.Add(1) })
		}
		return nc, e
	}}})
	if err != nil {
		return err
	}
	borrowed, err := nt.PublisherDefinition(f.r.Forest, nt.PublisherOptions{TypeID: f.o.Namespace + ".proof.borrowed", Version: "1", Connection: nt.Connection{Ownership: nt.Borrowed, Connect: func(context.Context) (*nats.Conn, error) { return f.nc, nil }}})
	if err != nil {
		return err
	}
	gate := forest.Definition{TypeID: f.o.Namespace + ".proof.gate", Version: "1", Ports: []forest.PortDescriptor{forest.Port[nt.Control]("consumer", forest.Required(), forest.During(forest.Startup)), forest.Port[dbtree.Database]("database", forest.Required(), forest.During(forest.Startup)), forest.Port[nt.Handler]("handler", forest.Required(), forest.During(forest.Startup))}, Build: func(_ context.Context, e *forest.BuildEnv) (forest.Lifecycle, error) {
		readyHandler, err := forest.Reference[nt.Handler](e, "handler")
		if err != nil {
			return nil, err
		}
		control, err := forest.Reference[nt.Control](e, "consumer")
		if err != nil {
			return nil, err
		}
		data, err := forest.Reference[dbtree.Database](e, "database")
		if err != nil {
			return nil, err
		}
		return forest.Hooks{OnStart: func(c context.Context) error {
			if err := readyHandler.Use(c, func(nt.Handler) error { return nil }); err != nil {
				return err
			}
			if err := control.Use(c, func(w nt.Control) error {
				s := w.Status()
				if s.Enabled || s.Inflight != 0 {
					return errors.New("candidate already consuming")
				}
				return nil
			}); err != nil {
				return err
			}
			if err := data.Use(c, func(d dbtree.Database) error {
				return d.WithDB(c, func(db *gorm.DB) error { var n int; return db.Raw("SELECT 1").Scan(&n).Error })
			}); err != nil {
				return err
			}
			if b.ready != nil {
				return b.ready(c, b)
			}
			return nil
		}}, nil
	}}
	b.defs = map[string]forest.Definition{"root": root, "db": db, "consumer": consumer, "borrowed": borrowed, "store": confStoreDefinition(f.r.Forest, &f.evidence, f.o.Namespace, f.o.Namespace+".proof.store"), "v1": b.handlerDefinition("v1"), "v2": b.handlerDefinition("v2"), "gate": gate}
	for _, def := range b.defs {
		if err = f.r.Forest.Register(def); err != nil {
			return err
		}
	}
	return nil
}
func (b *confBundle) plan() forest.ChangePlan {
	ops := []forest.Operation{forest.Nest("app", "app/proof", b.defs["root"].Ref())}
	for _, name := range []string{"db", "consumer", "borrowed", "store", "gate"} {
		ops = append(ops, forest.Nest("app/proof", forest.TreeID("app/proof/"+name), b.defs[name].Ref()))
	}
	ops = append(ops, forest.Nest("app/proof", "app/proof/handler", b.defs["v1"].Ref()))
	links := []forest.Link{{ID: "proof.gate.handler", Consumer: "app/proof/gate", Port: "handler", Provider: "app/proof/handler", Capability: "handle"}, {ID: "proof.consumer", Consumer: "app/proof/consumer", Port: "handler", Provider: "app/proof/handler", Capability: "handle"}, {ID: "proof.store.db", Consumer: "app/proof/store", Port: "database", Provider: "app/proof/db", Capability: "database"}, {ID: "proof.handler.store", Consumer: "app/proof/handler", Port: "store", Provider: "app/proof/store", Capability: "store"}, {ID: "proof.handler.next", Consumer: "app/proof/handler", Port: "next", Provider: "app/handler", Capability: "handle"}, {ID: "proof.gate.consumer", Consumer: "app/proof/gate", Port: "consumer", Provider: "app/proof/consumer", Capability: "control"}, {ID: "proof.gate.db", Consumer: "app/proof/gate", Port: "database", Provider: "app/proof/db", Capability: "database"}}
	for _, link := range links {
		ops = append(ops, forest.Connect(link))
	}
	ops = append(ops, forest.Rebind(forest.Link{ID: "db", Consumer: "app/repository", Port: "database", Provider: "app/proof/db", Capability: "database"}))
	return forest.ChangePlan{BaseGeneration: b.f.r.Forest.Snapshot().Generation, Operations: ops}
}
func (b *confBundle) anchor(version, tree string) error {
	b.handlersMu.Lock()
	h := b.handlers[version]
	b.handlersMu.Unlock()
	node, err := b.f.r.Forest.InspectTree(forest.TreeID(tree))
	if err != nil {
		return err
	}
	if h == nil {
		return errorsNewEvidence()
	}
	h.identity.Store(b.f.domain + "/" + node.InstanceID)
	return nil
}

func confContext(t *testing.T) context.Context {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	t.Cleanup(cancel)
	return ctx
}
func confOptions(t *testing.T) Options {
	t.Helper()
	if os.Getenv("FOREST_INTEGRATION") != "1" {
		t.Skip("real PostgreSQL/JetStream required")
	}
	o := Options{DSN: os.Getenv("FOREST_POSTGRES_DSN"), NATSURL: os.Getenv("FOREST_NATS_URL"), Namespace: "Proof_" + confNonce(), CheckpointDir: t.TempDir(), Token: "ecosystem-conformance-token"}
	if o.DSN == "" || o.NATSURL == "" {
		t.Fatal("explicit integration requires PostgreSQL/JetStream")
	}
	return o
}
func confSetup(ctx context.Context, o Options) error {
	if err := Initialize(ctx, o); err != nil {
		return err
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
	if err = db.WithContext(ctx).AutoMigrate(&confReceipt{}, &confEffect{}); err != nil {
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
	_, err = js.CreateConsumer(ctx, o.Namespace, jetstream.ConsumerConfig{Durable: "conformance", FilterSubject: o.Namespace, AckPolicy: jetstream.AckExplicitPolicy, AckWait: 15 * time.Second, MaxDeliver: 5, MaxAckPending: 2})
	return err
}
func confOpen(ctx context.Context, o Options) (f *confFixture, err error) {
	f = &confFixture{ctx: ctx, o: o, domain: confProcess + "/forest/" + confNonce(), entries: map[string][]tk.Execution{}, entryTraces: map[string][]string{}}
	o.BeforeSubmit = func(c context.Context, id string) error {
		g, e := f.r.Forest.Generation(c)
		if e != nil {
			return e
		}
		x := tk.Execution{Scope: tk.Scope{Forest: f.domain, Call: "entry/" + confNonce(), Generation: g}, Task: id, Step: "entry", Attempt: "1", Process: confProcess, Instance: f.r.Forest.Snapshot().Trees["app/service"].InstanceID}
		f.entriesMu.Lock()
		f.entries[id] = append(f.entries[id], x)
		f.entryTraces[id] = append(f.entryTraces[id], trace.SpanContextFromContext(c).TraceID().String())
		f.entriesMu.Unlock()
		return nil
	}
	f.r, err = New(ctx, o)
	if err != nil {
		return f, err
	}
	f.nc, err = connect(o.NATSURL)(ctx)
	if err != nil {
		return f, err
	}
	f.nc.SetClosedHandler(func(*nats.Conn) { f.borrowCloses.Add(1) })
	f.js, err = jetstream.New(f.nc)
	if err != nil {
		return f, err
	}
	f.native, err = f.js.Consumer(ctx, o.Namespace, "conformance")
	if err != nil {
		return f, err
	}
	stream, err := f.js.Stream(ctx, o.Namespace)
	if err != nil {
		return f, err
	}
	si, err := stream.Info(ctx)
	if err != nil {
		return f, err
	}
	ci, err := f.native.Info(ctx)
	if err != nil {
		return f, err
	}
	f.streamKey = confDigest(struct{ Name, Created string }{si.Config.Name, si.Created.UTC().Format(time.RFC3339Nano)})
	f.durableKey = confDigest(struct{ Name, Created string }{ci.Name, ci.Created.UTC().Format(time.RFC3339Nano)})
	f.bundle = &confBundle{f: f, handlers: map[string]*confHandler{}}
	err = f.bundle.definitions()
	return f, err
}
func newConf(t *testing.T) *confFixture {
	t.Helper()
	ctx := confContext(t)
	o := confOptions(t)
	if err := confSetup(ctx, o); err != nil {
		t.Fatal(err)
	}
	f, err := confOpen(ctx, o)
	if f != nil {
		t.Cleanup(func() { f.close(t) })
	}
	if err != nil {
		t.Fatal(err)
	}
	if _, err = f.r.Forest.Apply(ctx, f.bundle.plan()); err != nil {
		t.Fatal(err)
	}
	if err = f.bundle.anchor("v1", "app/proof/handler"); err != nil {
		t.Fatal(err)
	}
	return f
}
func (f *confFixture) close(t *testing.T) {
	t.Helper()
	c, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	if f.r != nil {
		if f.bundle != nil {
			if err := f.quiesce(c); err != nil && !errors.Is(err, forest.ErrClosed) && !errors.Is(err, forest.ErrNotFound) {
				t.Error(err)
			}
		}
		if err := f.r.Close(c); err != nil {
			t.Error(err)
		}
	}
	if f.nc != nil {
		f.nc.Close()
	}
}
func (f *confFixture) control(ctx context.Context, fn func(nt.Control) error) error {
	return f.r.Forest.Invoke(ctx, func(c context.Context) error {
		return forest.Use[nt.Control](c, f.r.Forest, "app/proof/consumer", "control", fn)
	})
}
func (f *confFixture) quiesce(ctx context.Context) error {
	return f.r.Forest.Invoke(ctx, func(c context.Context) error {
		return forest.Use[nt.Control](c, f.r.Forest, "app/proof/consumer", "control", func(w nt.Control) error { return w.Quiesce(c) })
	})
}
func (f *confFixture) enable(t *testing.T) {
	t.Helper()
	if err := f.r.Forest.Invoke(f.ctx, func(c context.Context) error {
		return forest.Use[nt.Control](c, f.r.Forest, "app/proof/consumer", "control", func(w nt.Control) error { return w.Enable(c) })
	}); err != nil {
		t.Fatal(err)
	}
}
func (f *confFixture) generation() uint64 { return f.r.Forest.Snapshot().Generation }
func confUntil(t *testing.T, ctx context.Context, fn func() bool) {
	t.Helper()
	for !fn() {
		select {
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		case <-time.After(5 * time.Millisecond):
		}
	}
}
func (f *confFixture) await(t *testing.T, seq uint64) confObservation {
	t.Helper()
	var out confObservation
	confUntil(t, f.ctx, func() bool { var ok bool; out, ok = f.evidence.result(seq); return ok })
	return out
}
func (f *confFixture) acked(t *testing.T, seq uint64) {
	t.Helper()
	confUntil(t, f.ctx, func() bool {
		s, err := f.native.Info(f.ctx)
		if err != nil {
			t.Fatal(err)
		}
		return s.AckFloor.Stream >= seq && s.NumAckPending == 0
	})
}
func (f *confFixture) pinned(t *testing.T, seq uint64, out confObservation, g uint64) tk.PinnedEvidence {
	t.Helper()
	stream, err := f.js.Stream(f.ctx, f.o.Namespace)
	if err != nil {
		t.Fatal(err)
	}
	stored, err := stream.GetMsg(f.ctx, seq)
	if err != nil {
		t.Fatal(err)
	}
	if stored.Sequence != out.Message.StreamSequence || stored.Header.Get(jetstream.MsgIDHeader) != out.Message.ID || string(stored.Data) != string(out.Message.Data) {
		t.Fatal("broker message and callback disagree")
	}
	return tk.PinnedEvidence{Label: out.Message.ID, Scope: tk.Scope{Forest: f.domain, Call: out.Execution.Scope.Call, Generation: g}, Required: []string{"handler.enter", "store.observe", "store.apply", "handler.return"}, Trace: out.Trace}
}
func (f *confFixture) relay(t *testing.T, id string) uint64 {
	t.Helper()
	var rows []outbox
	if err := f.r.repository(f.ctx, func(c context.Context, r repositoryAPI) error { var err error; rows, err = r.Pending(c); return err }); err != nil {
		t.Fatal(err)
	}
	for _, row := range rows {
		if row.ID != id {
			continue
		}
		var ack *jetstream.PubAck
		err := f.r.Forest.Invoke(f.ctx, func(c context.Context) error {
			return forest.Use[nt.Publisher](c, f.r.Forest, "app/publisher", "publisher", func(p nt.Publisher) error {
				header := nats.Header{jetstream.MsgIDHeader: []string{f.o.Namespace + ":" + row.ID}}
				if row.Traceparent != "" {
					header.Set("traceparent", row.Traceparent)
				}
				if row.Tracestate != "" {
					header.Set("tracestate", row.Tracestate)
				}
				var err error
				ack, err = p.Publish(c, &nats.Msg{Subject: f.o.Namespace, Data: row.Data, Header: header})
				return err
			})
		})
		if err != nil {
			t.Fatal(err)
		}
		if err = f.r.repository(f.ctx, func(c context.Context, r repositoryAPI) error { return r.MarkSent(c, row.ID) }); err != nil {
			t.Fatal(err)
		}
		return ack.Sequence
	}
	t.Fatal("no pending outbox", id)
	return 0
}
func (f *confFixture) complete(t *testing.T, id string) Task {
	t.Helper()
	var out Task
	confUntil(t, f.ctx, func() bool {
		if err := f.r.Tick(f.ctx); err != nil {
			t.Fatal(err)
		}
		if err := f.r.repository(f.ctx, func(c context.Context, r repositoryAPI) error { var err error; out, err = r.Get(c, id); return err }); err != nil {
			t.Fatal(err)
		}
		return out.State == "completed"
	})
	return out
}
