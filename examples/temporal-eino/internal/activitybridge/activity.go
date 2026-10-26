package activitybridge

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"time"

	ef "github.com/Leeminjing/go-forest/adapters/eino"
	tf "github.com/Leeminjing/go-forest/adapters/temporal"
	"github.com/Leeminjing/go-forest/examples/temporal-eino/internal/observe"
	"github.com/Leeminjing/go-forest/examples/temporal-eino/protocol"
	"github.com/Leeminjing/go-forest/forest"
	"github.com/cloudwego/eino/schema"
	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/temporal"
)

type Steps interface {
	Step(context.Context, protocol.Request) (protocol.Result, error)
}
type Graph interface {
	Invoke(context.Context, string, ef.GraphCall) (string, error)
	Stream(context.Context, string, ef.GraphCall, func(*schema.StreamReader[string]) error) error
}
type Bridge struct {
	Steps          forest.Ref[Steps]
	Graph          forest.Ref[Graph]
	Meter          *observe.Meter
	WorkerIdentity string
}

func nativeError(e error) error {
	if e == nil {
		return nil
	}
	if errors.Is(e, protocol.ErrUnknown) {
		return temporal.NewNonRetryableApplicationError("reconcile native execution and effect", "OutcomeUnknown", nil)
	}
	if errors.Is(e, protocol.ErrConflict) {
		return temporal.NewNonRetryableApplicationError("input identity conflict", "InputConflict", nil)
	}
	if errors.Is(e, protocol.ErrCorrupt) || errors.Is(e, ef.ErrCorrupt) || errors.Is(e, ef.ErrProfile) || errors.Is(e, protocol.ErrMissing) {
		return temporal.NewNonRetryableApplicationError("required state unavailable or incompatible", "CheckpointInvalid", nil)
	}
	if errors.Is(e, protocol.ErrBusy) || errors.Is(e, ef.ErrBusy) || errors.Is(e, protocol.ErrTransient) {
		return temporal.NewApplicationError("execution not admitted", "AdmissionRetry", nil)
	}
	var app *temporal.ApplicationError
	if errors.As(e, &app) {
		return app
	}
	var canceled *temporal.CanceledError
	if errors.As(e, &canceled) {
		return canceled
	}
	var timeout *temporal.TimeoutError
	if errors.As(e, &timeout) {
		return timeout
	}
	if errors.Is(e, context.Canceled) {
		return temporal.NewCanceledError()
	}
	return temporal.NewNonRetryableApplicationError("native execution did not finish", "ExecutionFailed", nil)
}
func (b *Bridge) Step(ctx context.Context, in protocol.Request) (out protocol.Result, err error) {
	ctx = observe.Begin(ctx)
	if err = b.Meter.At(ctx, "activity", "", in.Input, in.Step, nil); err != nil {
		return out, nativeError(err)
	}
	if err = tf.Go(ctx, func(c context.Context) error {
		ticker := time.NewTicker(100 * time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case <-c.Done():
				return nil
			case <-ticker.C:
				activity.RecordHeartbeat(c)
			}
		}
	}); err != nil {
		return out, err
	}
	err = b.Steps.Use(ctx, func(s Steps) error { var e error; out, e = s.Step(ctx, in); return e })
	g, _ := b.Meter.Forest.Generation(ctx)
	out.Delivery = protocol.Delivery{Process: b.Meter.Process, Worker: b.WorkerIdentity, Generation: g, Attempt: activity.GetInfo(ctx).Attempt}
	return out, nativeError(err)
}
func (b *Bridge) ReadGraph(ctx context.Context, in protocol.Input) (out string, err error) {
	ctx = observe.Begin(ctx)
	if err = tf.Go(ctx, func(c context.Context) error {
		ticker := time.NewTicker(100 * time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case <-c.Done():
				return nil
			case <-ticker.C:
				activity.RecordHeartbeat(c)
			}
		}
	}); err != nil {
		return out, err
	}
	data, _ := json.Marshal(in)
	err = b.Graph.Use(ctx, func(g Graph) error {
		first, e := g.Invoke(ctx, string(data), ef.GraphCall{})
		if e != nil {
			return e
		}
		var streamed string
		e = g.Stream(ctx, string(data), ef.GraphCall{}, func(r *schema.StreamReader[string]) error {
			for {
				v, e := r.Recv()
				if errors.Is(e, io.EOF) {
					break
				}
				if e != nil {
					return e
				}
				streamed += v
				if len(streamed) > 1<<20 {
					return protocol.ErrPartial
				}
			}
			return nil
		})
		if e != nil {
			return e
		}
		if first != streamed {
			return protocol.ErrConflict
		}
		out = first
		return nil
	})
	return out, nativeError(err)
}
