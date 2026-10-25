package temporaldemo_test

import (
	"context"
	"os"
	"testing"
	"time"

	demo "github.com/Leeminjing/go-forest/examples/temporal"
	"github.com/Leeminjing/go-forest/examples/temporal/flow"
)

func config(t *testing.T) demo.Options {
	t.Helper()
	address, dsn := os.Getenv("FOREST_TEMPORAL_ADDRESS"), os.Getenv("FOREST_POSTGRES_DSN")
	if address == "" || dsn == "" {
		if os.Getenv("FOREST_INTEGRATION") == "1" {
			t.Fatal("real Temporal and PostgreSQL configuration required")
		}
		t.Skip("real service integration")
	}
	return demo.Options{Address: address, DSN: dsn, Directory: t.TempDir(), TaskQueue: "demo-" + time.Now().Format("150405.000000000")}
}
func TestRealAgentDatabaseWorkflowAndReplay(t *testing.T) {
	o := config(t)
	ctx, cancel := context.WithTimeout(context.Background(), 35*time.Second)
	defer cancel()
	if err := demo.Setup(ctx, o.DSN); err != nil {
		t.Fatal(err)
	}
	r, err := demo.New(ctx, o)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close(ctx)
	id, err := r.StartRun(ctx, flow.Input{ID: o.TaskQueue, Text: "oak", Wait: true})
	if err != nil {
		t.Fatal(err)
	}
	for {
		state, err := r.Query(ctx, id)
		if err != nil {
			t.Fatal(err)
		}
		if state.Stage == "waiting" {
			break
		}
		select {
		case <-time.After(10 * time.Millisecond):
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		}
	}
	if err = r.ReplaceBusiness(ctx); err != nil {
		t.Fatal(err)
	}
	if err = r.Signal(ctx, id); err != nil {
		t.Fatal(err)
	}
	out, err := r.Result(ctx, id)
	if err != nil {
		t.Fatal(err)
	}
	if out.Receipt.Plan != "plan:oak" || out.Receipt.Changes != 1 || out.Receipt.Version != "1" || out.ReadVersion != "2" {
		t.Fatalf("result=%+v", out)
	}
	if err = r.Replay(ctx, id); err != nil {
		t.Fatal(err)
	}
	if len(r.Forest.Snapshot().Trees) < 8 {
		t.Fatal("real composition missing")
	}
}
