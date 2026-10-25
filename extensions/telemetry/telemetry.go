// Package telemetry instruments explicitly selected boundaries. It does not
// intercept arbitrary Ref calls or infer provider identities from topology.
package telemetry

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"sort"
	"sync"
	"time"

	"github.com/Leeminjing/go-forest/forest"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/metric"
	metricnoop "go.opentelemetry.io/otel/metric/noop"
	"go.opentelemetry.io/otel/trace"
	tracenoop "go.opentelemetry.io/otel/trace/noop"
)

var ErrInvalid = errors.New("invalid telemetry configuration or operation")

type Options struct {
	// Operations is a finite allowlist of boundary names, not request identifiers.
	Operations     []string
	Capacity       int
	FreshFor       time.Duration
	TracerProvider trace.TracerProvider
	MeterProvider  metric.MeterProvider
}
type Operation struct {
	Name     string    `json:"name"`
	Calls    uint64    `json:"calls,string"`
	Errors   uint64    `json:"errors,string"`
	Inflight uint64    `json:"inflight,string"`
	Seconds  float64   `json:"seconds"`
	LastAt   time.Time `json:"last_at"`
	Fresh    bool      `json:"fresh"`
}
type Detail struct {
	Operation  string    `json:"operation"`
	Generation uint64    `json:"generation,string"`
	Outcome    string    `json:"outcome"`
	TraceID    string    `json:"trace_id,omitempty"`
	Sampled    bool      `json:"trace_sampled"`
	At         time.Time `json:"at"`
}
type Frame struct {
	Coverage       string      `json:"coverage"`
	SessionID      string      `json:"session_id"`
	SampledAt      time.Time   `json:"sampled_at"`
	Since          time.Time   `json:"since"`
	Operations     []Operation `json:"operations"`
	Details        []Detail    `json:"details"`
	DroppedDetails uint64      `json:"dropped_details,string"`
}
type Recorder struct {
	forest     *forest.Forest
	options    Options
	session    string
	since      time.Time
	mu         sync.Mutex
	operations map[string]*Operation
	details    []Detail
	dropped    uint64
	tracer     trace.Tracer
	count      metric.Int64Counter
	failures   metric.Int64Counter
	active     metric.Int64UpDownCounter
	duration   metric.Float64Histogram
}

func New(f *forest.Forest, o Options) (*Recorder, error) {
	if f == nil || len(o.Operations) == 0 || len(o.Operations) > 128 || o.Capacity < 0 || o.Capacity > 256 || o.FreshFor < 0 {
		return nil, ErrInvalid
	}
	if o.Capacity == 0 {
		o.Capacity = 64
	}
	if o.FreshFor == 0 {
		o.FreshFor = 30 * time.Second
	}
	if o.TracerProvider == nil {
		o.TracerProvider = tracenoop.NewTracerProvider()
	}
	if o.MeterProvider == nil {
		o.MeterProvider = metricnoop.NewMeterProvider()
	}
	id := make([]byte, 16)
	if _, err := rand.Read(id); err != nil {
		return nil, err
	}
	r := &Recorder{forest: f, options: o, session: hex.EncodeToString(id), since: time.Now().UTC(), operations: map[string]*Operation{}, tracer: o.TracerProvider.Tracer("go-forest.explicit")}
	for _, key := range o.Operations {
		if key == "" || len(key) > 96 || r.operations[key] != nil {
			return nil, ErrInvalid
		}
		r.operations[key] = &Operation{Name: key}
	}
	m := o.MeterProvider.Meter("go-forest.explicit")
	var err error
	r.count, err = m.Int64Counter("forest.boundary.calls")
	if err != nil {
		return nil, err
	}
	r.failures, err = m.Int64Counter("forest.boundary.errors")
	if err != nil {
		return nil, err
	}
	r.active, err = m.Int64UpDownCounter("forest.boundary.inflight")
	if err != nil {
		return nil, err
	}
	r.duration, err = m.Float64Histogram("forest.boundary.duration", metric.WithUnit("s"))
	if err != nil {
		return nil, err
	}
	return r, nil
}

// Measure requires an existing Scope. Attributes contain no inputs or raw error
// messages. Providers/exporters are host-owned and must use bounded async export.
func (r *Recorder) Measure(ctx context.Context, name string, fn func(context.Context) error) (err error) {
	if fn == nil {
		return ErrInvalid
	}
	g, err := r.forest.Generation(ctx)
	if err != nil {
		return err
	}
	r.mu.Lock()
	op := r.operations[name]
	if op == nil {
		r.mu.Unlock()
		return ErrInvalid
	}
	op.Calls++
	op.Inflight++
	r.mu.Unlock()
	attrs := []attribute.KeyValue{attribute.String("forest.operation", name)}
	spanCtx, span := r.tracer.Start(ctx, name, trace.WithAttributes(append(attrs, attribute.Int64("forest.generation", int64(g)), attribute.String("forest.instrumentation_session", r.session))...))
	r.count.Add(spanCtx, 1, metric.WithAttributes(attrs...))
	r.active.Add(spanCtx, 1, metric.WithAttributes(attrs...))
	start := time.Now()
	returned := false
	defer func() {
		elapsed := time.Since(start).Seconds()
		outcome := "ok"
		if !returned {
			outcome = "panic"
		} else if err != nil {
			outcome = "error"
		}
		if outcome != "ok" {
			r.failures.Add(spanCtx, 1, metric.WithAttributes(attrs...))
			span.SetStatus(codes.Error, outcome)
		}
		r.active.Add(spanCtx, -1, metric.WithAttributes(attrs...))
		r.duration.Record(spanCtx, elapsed, metric.WithAttributes(attrs...))
		sc := span.SpanContext()
		d := Detail{Operation: name, Generation: g, Outcome: outcome, Sampled: sc.IsSampled(), At: time.Now().UTC()}
		if sc.IsValid() {
			d.TraceID = sc.TraceID().String()
		}
		span.End()
		r.mu.Lock()
		defer r.mu.Unlock()
		op.Inflight--
		op.Seconds += elapsed
		op.LastAt = d.At
		if outcome != "ok" {
			op.Errors++
		}
		if len(r.details) == r.options.Capacity {
			copy(r.details, r.details[1:])
			r.details = r.details[:len(r.details)-1]
			r.dropped++
		}
		r.details = append(r.details, d)
	}()
	err = fn(spanCtx)
	returned = true
	return err
}
func (r *Recorder) Snapshot() Frame {
	r.mu.Lock()
	defer r.mu.Unlock()
	now := time.Now().UTC()
	frame := Frame{Coverage: "explicit boundaries only; totals since session start; no inferred Link traffic; session ID is not Core InstanceID", SessionID: r.session, SampledAt: now, Since: r.since, DroppedDetails: r.dropped, Details: append([]Detail{}, r.details...)}
	for _, op := range r.operations {
		v := *op
		v.Fresh = !v.LastAt.IsZero() && now.Sub(v.LastAt) <= r.options.FreshFor
		frame.Operations = append(frame.Operations, v)
	}
	sort.Slice(frame.Operations, func(i, j int) bool { return frame.Operations[i].Name < frame.Operations[j].Name })
	return frame
}

// ReadRuntime is a small, consumer-compatible, detached JSON boundary for a
// read-only observation gateway. It does not create or hold a Forest Scope.
func (r *Recorder) ReadRuntime(ctx context.Context) (json.RawMessage, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	b, err := json.Marshal(r.Snapshot())
	return b, err
}
func (r *Recorder) Definition(typeID string) forest.Definition {
	return forest.Definition{TypeID: typeID, Version: "1", Capabilities: []forest.CapabilityDescriptor{forest.Capability[*Recorder]("runtime", forest.Public)}, Build: func(_ context.Context, e *forest.BuildEnv) (forest.Lifecycle, error) {
		return forest.Hooks{}, forest.Provide(e, "runtime", r)
	}}
}
