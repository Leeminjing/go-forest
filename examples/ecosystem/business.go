package ecosystem

import (
	"context"
	"encoding/json"

	nt "github.com/Leeminjing/go-forest/adapters/nats"
	"github.com/Leeminjing/go-forest/forest"
	"go.opentelemetry.io/otel/propagation"
)

// These contracts are owned by their consumers, not the database or broker.
type taskStore interface {
	Submit(context.Context, string, string, string) (Task, error)
	Get(context.Context, string) (Task, error)
}
type receiver interface {
	Receive(context.Context, event) error
}
type Service interface {
	Submit(context.Context, string, string) (Task, error)
	Get(context.Context, string) (Task, error)
}
type taskService struct {
	store   forest.Ref[taskStore]
	version string
	before  func(context.Context, string) error
}

func (s *taskService) Submit(ctx context.Context, id, input string) (task Task, err error) {
	if s.before != nil {
		if err = s.before(ctx, id); err != nil {
			return
		}
	}
	err = s.store.Use(ctx, func(r taskStore) error { task, err = r.Submit(ctx, id, input, s.version); return err })
	return
}
func (s *taskService) Get(ctx context.Context, id string) (task Task, err error) {
	err = s.store.Use(ctx, func(r taskStore) error { task, err = r.Get(ctx, id); return err })
	return
}
func serviceDefinition(version string, before func(context.Context, string) error) forest.Definition {
	return forest.Definition{TypeID: "ecosystem.service", Version: version, HotSwappable: true, Ports: []forest.PortDescriptor{forest.Port[taskStore]("repository", forest.Required())}, Capabilities: []forest.CapabilityDescriptor{forest.Capability[Service]("tasks", forest.Public)}, Build: func(_ context.Context, e *forest.BuildEnv) (forest.Lifecycle, error) {
		ref, err := forest.Reference[taskStore](e, "repository")
		if err != nil {
			return nil, err
		}
		return forest.Hooks{}, forest.Provide(e, "tasks", &taskService{store: ref, version: version, before: before})
	}}
}

type handler struct {
	store   forest.Ref[receiver]
	metrics forest.Ref[measure]
	after   func(context.Context) error
}

func (h *handler) Handle(ctx context.Context, m nt.Message) error {
	carrier := propagation.MapCarrier{}
	// NATS headers are case-sensitive; HTTP HeaderCarrier canonicalizes keys.
	// Preserve the protocol's lower-case W3C field names explicitly instead.
	for _, key := range []string{"traceparent", "tracestate"} {
		carrier[key] = m.Header.Get(key)
	}
	ctx = tracePropagation.Extract(ctx, carrier)
	return h.metrics.Use(ctx, func(r measure) error {
		return r.Measure(ctx, "receive", func(ctx context.Context) error { return h.handle(ctx, m) })
	})
}
func (h *handler) handle(ctx context.Context, m nt.Message) error {
	var e event
	if len(m.Data) > 8192 || json.Unmarshal(m.Data, &e) != nil || e.ID == "" {
		return ErrInvalid
	}
	if err := h.store.Use(ctx, func(r receiver) error { return r.Receive(ctx, e) }); err != nil {
		return err
	}
	if h.after != nil {
		return h.after(ctx)
	}
	return nil
}
func handlerDefinition(after func(context.Context) error) forest.Definition {
	return forest.Definition{TypeID: "ecosystem.handler", Version: "1", HotSwappable: true, Ports: []forest.PortDescriptor{forest.Port[receiver]("repository", forest.Required()), forest.Port[measure]("telemetry", forest.Required())}, Capabilities: []forest.CapabilityDescriptor{forest.Capability[nt.Handler]("handle", forest.Public)}, Build: func(_ context.Context, e *forest.BuildEnv) (forest.Lifecycle, error) {
		ref, err := forest.Reference[receiver](e, "repository")
		if err != nil {
			return nil, err
		}
		metrics, err := forest.Reference[measure](e, "telemetry")
		if err != nil {
			return nil, err
		}
		return forest.Hooks{}, forest.Provide(e, "handle", &handler{store: ref, metrics: metrics, after: after})
	}}
}
