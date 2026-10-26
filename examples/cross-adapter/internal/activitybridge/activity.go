// Package activitybridge owns the narrow contract used by Temporal activities.
package activitybridge

import (
	"context"
	"time"

	tf "github.com/Leeminjing/go-forest/adapters/temporal"
	"github.com/Leeminjing/go-forest/examples/cross-adapter/internal/trace"
	"github.com/Leeminjing/go-forest/examples/cross-adapter/protocol"
	"github.com/Leeminjing/go-forest/forest"
	"go.temporal.io/sdk/activity"
)

type Processor interface {
	Process(context.Context, protocol.Request) (protocol.Result, error)
	Inspect(context.Context, protocol.Request) (protocol.Observation, error)
}
type Borrower interface {
	Use(context.Context, func(context.Context, Processor) error) error
}
type Bridge struct {
	Forest                              *forest.Forest
	Service                             forest.Ref[Borrower]
	Process, WorkerIdentity, FxIdentity string
}

func (b *Bridge) Apply(ctx context.Context, in protocol.Request) (out protocol.Result, err error) {
	ctx, evidence := trace.Attach(ctx)
	g, err := b.Forest.Generation(ctx)
	if err != nil {
		return out, err
	}
	trace.Add(ctx, protocol.Point{Stage: "activity", Process: b.Process, Generation: g})
	if err = tf.Go(ctx, func(c context.Context) error {
		ticker := time.NewTicker(100 * time.Millisecond)
		defer ticker.Stop()
		for {
			activity.RecordHeartbeat(c)
			select {
			case <-c.Done():
				return nil
			case <-ticker.C:
			}
		}
	}); err != nil {
		return out, err
	}
	err = b.Service.Use(ctx, func(a Borrower) error {
		return a.Use(ctx, func(call context.Context, p Processor) error { var e error; out, e = p.Process(call, in); return e })
	})
	out.Trace = evidence.Points()
	out.Attempt = activity.GetInfo(ctx).Attempt
	out.WorkerIdentity = b.WorkerIdentity
	out.FxIdentity = b.FxIdentity
	return out, Translate(err, in.OperationID)
}
func (b *Bridge) Inspect(ctx context.Context, in protocol.Request) (out protocol.Observation, err error) {
	err = b.Service.Use(ctx, func(a Borrower) error {
		return a.Use(ctx, func(call context.Context, p Processor) error { var e error; out, e = p.Inspect(call, in); return e })
	})
	return out, Translate(err, in.OperationID)
}
