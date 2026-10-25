package telemetry_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/Leeminjing/go-forest/extensions/telemetry"
	"github.com/Leeminjing/go-forest/forest"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
)

func TestCountersSurviveTraceSamplingAndBoundedDetails(t *testing.T) {
	f := forest.New(forest.Options{})
	defer f.Close(context.Background())
	mr := sdkmetric.NewManualReader()
	mp := sdkmetric.NewMeterProvider(sdkmetric.WithReader(mr))
	defer mp.Shutdown(context.Background())
	tp := sdktrace.NewTracerProvider(sdktrace.WithSampler(sdktrace.NeverSample()))
	defer tp.Shutdown(context.Background())
	r, err := telemetry.New(f, telemetry.Options{Operations: []string{"handler"}, Capacity: 1, FreshFor: time.Nanosecond, MeterProvider: mp, TracerProvider: tp})
	if err != nil {
		t.Fatal(err)
	}
	sentinel := errors.New("sensitive error must not be captured")
	err = f.Invoke(context.Background(), func(ctx context.Context) error {
		if err := r.Measure(ctx, "handler", func(context.Context) error { return nil }); err != nil {
			return err
		}
		return r.Measure(ctx, "handler", func(context.Context) error { return sentinel })
	})
	if !errors.Is(err, sentinel) {
		t.Fatal(err)
	}
	time.Sleep(time.Millisecond) // Windows clocks may coalesce consecutive timestamps.
	frame := r.Snapshot()
	op := frame.Operations[0]
	if op.Calls != 2 || op.Errors != 1 || op.Inflight != 0 || op.Fresh || frame.DroppedDetails != 1 || len(frame.Details) != 1 || frame.Details[0].Sampled {
		t.Fatalf("incorrect coverage: %+v", frame)
	}
	var got metricdata.ResourceMetrics
	if err = mr.Collect(context.Background(), &got); err != nil {
		t.Fatal(err)
	}
	counts := map[string]int64{}
	for _, sm := range got.ScopeMetrics {
		for _, m := range sm.Metrics {
			if sum, ok := m.Data.(metricdata.Sum[int64]); ok {
				for _, point := range sum.DataPoints {
					counts[m.Name] += point.Value
				}
			}
		}
	}
	if counts["forest.boundary.calls"] != 2 || counts["forest.boundary.errors"] != 1 || counts["forest.boundary.inflight"] != 0 {
		t.Fatal(counts)
	}
	if err = r.Measure(context.Background(), "handler", func(context.Context) error { t.Fatal("unscoped business entered"); return nil }); !errors.Is(err, forest.ErrScopeClosed) {
		t.Fatal(err)
	}
}
