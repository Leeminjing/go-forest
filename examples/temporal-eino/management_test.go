package einodemo_test

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"testing"

	ef "github.com/Leeminjing/go-forest/adapters/eino"
	tf "github.com/Leeminjing/go-forest/adapters/temporal"
	demo "github.com/Leeminjing/go-forest/examples/temporal-eino"
	"github.com/Leeminjing/go-forest/examples/temporal-eino/protocol"
	"github.com/Leeminjing/go-forest/forest"
	"github.com/cloudwego/eino/schema"
	"go.temporal.io/sdk/temporal"
)

func TestHostCloseCancelsLocalGetWithoutWaitingForDurableWorkflow(t *testing.T) {
	ctx, o := config(t)
	r, e := demo.New(ctx, o)
	if e != nil {
		t.Fatal(e)
	}
	defer r.Close(ctx)
	if e = r.Enable(ctx); e != nil {
		t.Fatal(e)
	}
	id, e := r.Start(ctx, o.Queue, input(o))
	if e != nil {
		t.Fatal(e)
	}
	paused(t, ctx, r, id)
	done := make(chan error, 1)
	go func() { _, e := r.Result(ctx, id); done <- e }()
	until(t, ctx, func() bool { return r.HostStatus().Active == 1 })
	if e = r.Close(ctx); e != nil {
		t.Fatal(e)
	}
	select {
	case e = <-done:
		if e == nil {
			t.Fatal("waiting workflow became completed")
		}
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	if s := r.HostStatus(); !s.Closing || s.Active != 0 {
		t.Fatal(s)
	}
}
func TestActivityCannotSynchronouslyManageItsOwnResources(t *testing.T) {
	ctx, o := config(t)
	var r *demo.Runtime
	o.Hook = func(c context.Context, p protocol.Point) error {
		if p.Stage == "model" {
			if e := r.Close(c); !errors.Is(e, tf.ErrNestedScope) {
				return errors.New("nested close admitted")
			}
			if e := r.Switch(c, "replace"); !errors.Is(e, tf.ErrNestedScope) {
				return errors.New("nested switch admitted")
			}
		}
		return nil
	}
	var e error
	r, e = demo.New(ctx, o)
	if e != nil {
		t.Fatal(e)
	}
	defer r.Close(ctx)
	if e = r.Enable(ctx); e != nil {
		t.Fatal(e)
	}
	id, e := r.Start(ctx, o.Queue, input(o))
	if e != nil {
		t.Fatal(e)
	}
	continueRun(t, ctx, r, id, input(o))
}
func TestNativeErrorClassificationSurvivesTheActivityBoundary(t *testing.T) {
	ctx, o := config(t)
	o.Hook = func(c context.Context, p protocol.Point) error {
		if p.Stage == "step.admission" {
			return temporal.NewNonRetryableApplicationError("known pre-engine refusal", "NativeContract", nil)
		}
		return nil
	}
	r, e := demo.New(ctx, o)
	if e != nil {
		t.Fatal(e)
	}
	defer r.Close(ctx)
	if e = r.Enable(ctx); e != nil {
		t.Fatal(e)
	}
	id, e := r.Start(ctx, o.Queue, input(o))
	if e != nil {
		t.Fatal(e)
	}
	_, e = r.Result(ctx, id)
	var app *temporal.ApplicationError
	if !errors.As(e, &app) || app.Type() != "NativeContract" || !app.NonRetryable() {
		t.Fatal(e)
	}
	if r.Counts().Query != 0 {
		t.Fatal("refusal entered Eino")
	}
}

type grapher interface {
	Stream(context.Context, string, ef.GraphCall, func(*schema.StreamReader[string]) error) error
	Invoke(context.Context, string, ef.GraphCall) (string, error)
	Status() ef.Status
}

func TestEarlyConsumerErrorJoinsProducerAndRetainsOldStreamGeneration(t *testing.T) {
	ctx, o := config(t)
	entered, release := make(chan struct{}), make(chan struct{})
	var once, free sync.Once
	var mu sync.Mutex
	var points []protocol.Point
	o.Hook = func(c context.Context, p protocol.Point) error {
		if p.Stage == "stream.producer" {
			once.Do(func() { close(entered) })
			<-release
		}
		if p.Stage == "business.observe" {
			mu.Lock()
			points = append(points, p)
			mu.Unlock()
		}
		return nil
	}
	r, e := demo.New(ctx, o)
	if e != nil {
		t.Fatal(e)
	}
	defer r.Close(ctx)
	defer free.Do(func() { close(release) })
	raw, _ := json.Marshal(input(o))
	consumerError := errors.New("consumer failed")
	done := make(chan error, 1)
	go func() {
		done <- r.Forest.Invoke(ctx, func(c context.Context) error {
			return forest.Use[grapher](c, r.Forest, "app/graph", "graph", func(g grapher) error {
				return g.Stream(c, string(raw), ef.GraphCall{}, func(reader *schema.StreamReader[string]) error {
					first, e := reader.Recv()
					if e != nil {
						return e
					}
					if first != "v1/" {
						return protocol.ErrConflict
					}
					return consumerError
				})
			})
		})
	}()
	select {
	case <-entered:
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	if e = r.Switch(ctx, "rebind"); e != nil {
		t.Fatal(e)
	}
	select {
	case e = <-done:
		t.Fatal("consumer return released producer", e)
	default:
	}
	if e = r.Forest.Invoke(ctx, func(c context.Context) error {
		return forest.Use[grapher](c, r.Forest, "app/graph", "graph", func(g grapher) error {
			if g.Status().Joining == 0 {
				return errors.New("producer not joining")
			}
			out, e := g.Invoke(c, string(raw), ef.GraphCall{})
			if e == nil && out != "v2/v2" {
				return protocol.ErrConflict
			}
			return e
		})
	}); e != nil {
		t.Fatal(e)
	}
	if p := r.Resources(); p.BusinessCloses != 0 {
		t.Fatal("old stream resource stopped", p)
	}
	free.Do(func() { close(release) })
	select {
	case e = <-done:
		if !errors.Is(e, consumerError) {
			t.Fatal(e)
		}
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	mu.Lock()
	defer mu.Unlock()
	seenOld, seenNew := 0, 0
	for _, p := range points {
		if p.Version == "v1" {
			seenOld++
			if p.Generation != 1 {
				t.Fatal(p)
			}
		} else {
			seenNew++
			if p.Version != "v2" || p.Generation != 2 {
				t.Fatal(p)
			}
		}
	}
	if seenOld != 2 || seenNew != 2 {
		t.Fatal(points)
	}
}
