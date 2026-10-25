// Package natsforest adapts native NATS JetStream without changing Forest Core.
package natsforest

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/Leeminjing/go-forest/forest"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

var (
	ErrInvalidOptions = errors.New("invalid NATS tree options")
	ErrUnavailable    = errors.New("NATS tree unavailable")
	ErrQuiescing      = errors.New("NATS consumer is quiescing")
	ErrPublishUnknown = errors.New("publish outcome unknown; reconcile by event ID")
	ErrHandlerPanic   = errors.New("message handler panicked")
)

type Ownership uint8

const (
	Owned Ownership = iota + 1
	Borrowed
)

type Connection struct {
	Ownership Ownership
	// Connect must honor its timeout and clean up partial errors. A successful
	// returned connection has exactly the explicitly declared owner.
	Connect func(context.Context) (*nats.Conn, error)
	Timeout time.Duration
}

func (c Connection) valid() bool {
	return c.Connect != nil && (c.Ownership == Owned || c.Ownership == Borrowed) && c.Timeout >= 0
}
func (c Connection) open(ctx context.Context) (*nats.Conn, jetstream.JetStream, error) {
	timeout := c.Timeout
	if timeout == 0 {
		timeout = 5 * time.Second
	}
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	nc, err := c.Connect(ctx)
	if err != nil {
		if nc != nil && c.Ownership == Owned {
			nc.Close()
		}
		return nil, nil, err
	}
	if nc == nil {
		return nil, nil, ErrInvalidOptions
	}
	js, err := jetstream.New(nc)
	if err == nil {
		err = nc.FlushWithContext(ctx)
	}
	if err != nil {
		if c.Ownership == Owned {
			nc.Close()
		}
		return nil, nil, err
	}
	return nc, js, nil
}

type Publisher interface {
	Publish(context.Context, *nats.Msg) (*jetstream.PubAck, error)
}
type PublisherOptions struct {
	TypeID, Version string
	Connection      Connection
}

func PublisherDefinition(f *forest.Forest, o PublisherOptions) (forest.Definition, error) {
	if f == nil || o.TypeID == "" || o.Version == "" || !o.Connection.valid() {
		return forest.Definition{}, ErrInvalidOptions
	}
	return forest.Definition{TypeID: o.TypeID, Version: o.Version, Capabilities: []forest.CapabilityDescriptor{forest.Capability[Publisher]("publisher", forest.Public)}, Build: func(_ context.Context, e *forest.BuildEnv) (forest.Lifecycle, error) {
		p := &publisher{f: f, options: o}
		return p, forest.Provide(e, "publisher", p)
	}}, nil
}

type publisher struct {
	f       *forest.Forest
	options PublisherOptions
	mu      sync.RWMutex
	nc      *nats.Conn
	js      jetstream.JetStream
	closed  bool
}

func (p *publisher) Start(ctx context.Context) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	var err error
	p.nc, p.js, err = p.options.Connection.open(ctx)
	return err
}
func (p *publisher) Publish(ctx context.Context, m *nats.Msg) (*jetstream.PubAck, error) {
	if m == nil || m.Subject == "" || m.Header.Get(jetstream.MsgIDHeader) == "" {
		return nil, ErrInvalidOptions
	}
	if _, err := p.f.Generation(ctx); err != nil {
		return nil, err
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	p.mu.RLock()
	defer p.mu.RUnlock()
	if p.closed || p.js == nil {
		return nil, ErrUnavailable
	}
	ack, err := p.js.PublishMsg(ctx, m)
	if err != nil {
		return nil, errors.Join(ErrPublishUnknown, err)
	}
	return ack, nil
}
func (p *publisher) Stop(context.Context) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.closed = true
	if p.nc != nil && p.options.Connection.Ownership == Owned {
		p.nc.Close()
	}
	return nil
}

// Handler is defined by this consumer. It cannot acknowledge a message early.
// Return nil only after the application's durable success condition is met.
type Handler interface {
	Handle(context.Context, Message) error
}
type Message struct {
	Subject, ID              string
	Data                     []byte
	Header                   nats.Header
	Delivery, StreamSequence uint64
}
type Status struct {
	Enabled, Quiescing                       bool
	Inflight                                 int
	Handled, Failed, AckUnknown, FetchErrors uint64
	LastStage, LastEventID                   string
	Terminal                                 bool
}
type Control interface {
	Enable(context.Context) error
	Quiesce(context.Context) error
	Status() Status
}
type ConsumerOptions struct {
	TypeID, Version, Stream, Consumer string
	Connection                        Connection
	Concurrency                       int
	ProcessTimeout, RetryDelay        time.Duration
	// Handler port is named "handler". Broker resources must already exist;
	// preparation never creates, updates, or deletes server resources.
}

func ConsumerDefinition(f *forest.Forest, o ConsumerOptions) (forest.Definition, error) {
	if f == nil || o.TypeID == "" || o.Version == "" || o.Stream == "" || o.Consumer == "" || !o.Connection.valid() || o.Concurrency < 0 || o.Concurrency > 256 || o.ProcessTimeout < 0 || o.RetryDelay < 0 {
		return forest.Definition{}, ErrInvalidOptions
	}
	if o.Concurrency == 0 {
		o.Concurrency = 1
	}
	if o.ProcessTimeout == 0 {
		o.ProcessTimeout = 10 * time.Second
	}
	if o.RetryDelay == 0 {
		o.RetryDelay = time.Second
	}
	return forest.Definition{TypeID: o.TypeID, Version: o.Version, Ports: []forest.PortDescriptor{forest.Port[Handler]("handler", forest.Required())}, Capabilities: []forest.CapabilityDescriptor{forest.Capability[Control]("control", forest.Public)}, Build: func(_ context.Context, e *forest.BuildEnv) (forest.Lifecycle, error) {
		ref, err := forest.Reference[Handler](e, "handler")
		if err != nil {
			return nil, err
		}
		life, cancel := context.WithCancel(context.Background())
		c := &consumer{f: f, options: o, handler: ref, life: life, cancel: cancel}
		return c, forest.Provide(e, "control", c)
	}}, nil
}

type session struct {
	cancel  context.CancelFunc
	done    chan struct{}
	enabled bool
}
type consumer struct {
	f          *forest.Forest
	options    ConsumerOptions
	handler    forest.Ref[Handler]
	life       context.Context
	cancel     context.CancelFunc
	mu         sync.Mutex
	nc         *nats.Conn
	native     jetstream.Consumer
	current    *session
	closed     bool
	status     Status
	maxDeliver int
}

func (c *consumer) Start(ctx context.Context) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	nc, js, err := c.options.Connection.open(ctx)
	if err != nil {
		return err
	}
	c.nc = nc
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	c.native, err = js.Consumer(ctx, c.options.Stream, c.options.Consumer)
	if err != nil {
		return err
	}
	info, err := c.native.Info(ctx)
	if err != nil {
		return err
	}
	cfg := info.Config
	// Explicit Ack and finite MaxDeliver are required. AckWait exceeds the
	// managed handler deadline; duplicate delivery is still possible and expected.
	if cfg.Durable == "" || cfg.AckPolicy != jetstream.AckExplicitPolicy || cfg.MaxDeliver <= 0 || cfg.AckWait <= c.options.ProcessTimeout || len(cfg.BackOff) != 0 || cfg.MaxAckPending < c.options.Concurrency {
		return ErrInvalidOptions
	}
	c.maxDeliver = cfg.MaxDeliver
	return nil
}
func (c *consumer) Enable(ctx context.Context) error {
	if _, err := c.f.Generation(ctx); err != nil {
		return err
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed || c.native == nil {
		return ErrUnavailable
	}
	if c.current != nil {
		select {
		case <-c.current.done:
			c.current = nil
		default:
			if c.current.enabled {
				return nil
			}
			return ErrQuiescing
		}
	}
	// Iterators are created only after the tree has been committed and explicitly enabled.
	iters := make([]jetstream.MessagesContext, 0, c.options.Concurrency)
	for i := 0; i < c.options.Concurrency; i++ {
		it, err := c.native.Messages(jetstream.PullMaxMessages(1))
		if err != nil {
			for _, old := range iters {
				old.Stop()
			}
			return err
		}
		iters = append(iters, it)
	}
	fetchCtx, cancel := context.WithCancel(c.life)
	s := &session{cancel: cancel, done: make(chan struct{}), enabled: true}
	c.current = s
	c.status.Enabled = true
	c.status.Quiescing = false
	var wg sync.WaitGroup
	wg.Add(len(iters))
	for _, it := range iters {
		go func(it jetstream.MessagesContext) { defer wg.Done(); defer it.Stop(); c.work(fetchCtx, s, it) }(it)
	}
	go func() {
		wg.Wait()
		c.mu.Lock()
		s.enabled = false
		if c.current == s {
			c.status.Enabled = false
			c.status.Quiescing = false
		}
		close(s.done)
		c.mu.Unlock()
	}()
	return nil
}
func (c *consumer) Quiesce(ctx context.Context) error {
	if _, err := c.f.Generation(ctx); err != nil {
		return err
	}
	return c.quiesce(ctx)
}
func (c *consumer) quiesce(ctx context.Context) error {
	c.mu.Lock()
	s := c.current
	if s == nil {
		c.mu.Unlock()
		return nil
	}
	s.enabled = false
	s.cancel()
	c.status.Enabled = false
	c.status.Quiescing = true
	c.mu.Unlock()
	select {
	case <-s.done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
func (c *consumer) Status() Status { c.mu.Lock(); defer c.mu.Unlock(); return c.status }
func (c *consumer) work(fetchCtx context.Context, s *session, it jetstream.MessagesContext) {
	for {
		msg, err := it.Next(jetstream.NextContext(fetchCtx))
		if err != nil {
			if fetchCtx.Err() != nil {
				return
			}
			c.mu.Lock()
			c.status.FetchErrors++
			c.status.LastStage = "fetch"
			c.mu.Unlock()
			select {
			case <-fetchCtx.Done():
				return
			case <-time.After(c.options.RetryDelay):
			}
			continue
		}
		c.mu.Lock()
		if !s.enabled {
			c.mu.Unlock()
			return
		}
		c.status.Inflight++
		c.mu.Unlock()
		c.process(msg)
		c.mu.Lock()
		c.status.Inflight--
		c.mu.Unlock()
	}
}
func (c *consumer) process(msg jetstream.Msg) {
	meta, err := msg.Metadata()
	if err != nil {
		c.record("metadata", "", false, false)
		return
	}
	id := msg.Headers().Get(jetstream.MsgIDHeader)
	if len(id) > 256 {
		id = id[:256]
	}
	if id == "" {
		id = fmt.Sprintf("%s:%d", c.options.Stream, meta.Sequence.Stream)
	}
	header := nats.Header{}
	for key, values := range msg.Headers() {
		header[key] = append([]string(nil), values...)
	}
	m := Message{Subject: msg.Subject(), ID: id, Data: append([]byte(nil), msg.Data()...), Header: header, Delivery: meta.NumDelivered, StreamSequence: meta.Sequence.Stream}
	ctx, cancel := context.WithTimeout(c.life, c.options.ProcessTimeout)
	defer cancel()
	err = c.f.Invoke(ctx, func(callCtx context.Context) error {
		return c.handler.Use(callCtx, func(h Handler) error { return handle(callCtx, h, m) })
	})
	if err != nil {
		terminal := int(meta.NumDelivered) >= c.maxDeliver
		c.record("handler", id, terminal, false)
		if !terminal {
			if err = msg.NakWithDelay(c.options.RetryDelay); err != nil {
				c.record("nak-unknown", id, false, false)
			}
		}
		return
	}
	if err = msg.DoubleAck(ctx); err != nil {
		c.record("ack-unknown", id, false, true)
		return
	}
	c.mu.Lock()
	c.status.Handled++
	c.mu.Unlock()
}
func handle(ctx context.Context, h Handler, m Message) (err error) {
	defer func() {
		if recover() != nil {
			err = ErrHandlerPanic
		}
	}()
	return h.Handle(ctx, m)
}
func (c *consumer) record(stage, id string, terminal, ack bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.status.Failed++
	if ack {
		c.status.AckUnknown++
	}
	c.status.LastStage = stage
	c.status.LastEventID = id
	c.status.Terminal = terminal
}
func (c *consumer) Stop(context.Context) error {
	c.mu.Lock()
	c.closed = true
	c.cancel()
	s := c.current
	if s != nil {
		s.enabled = false
		s.cancel()
	}
	c.status.Enabled = false
	c.mu.Unlock()
	// Forest reports drain timeout while a noncooperative handler is still alive.
	// Do not return or release its resources before it actually finishes.
	if s != nil {
		<-s.done
	}
	if c.nc != nil && c.options.Connection.Ownership == Owned {
		c.nc.Close()
	}
	return nil
}
