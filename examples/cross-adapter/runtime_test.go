package crossdemo_test

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	demo "github.com/Leeminjing/go-forest/examples/cross-adapter"
	"github.com/Leeminjing/go-forest/examples/cross-adapter/protocol"
)

func config(t *testing.T) (context.Context, demo.Options) {
	t.Helper()
	if os.Getenv("FOREST_INTEGRATION") != "1" {
		t.Skip("real Temporal/PostgreSQL acceptance: FOREST_INTEGRATION=1")
	}
	address, dsn := os.Getenv("FOREST_TEMPORAL_ADDRESS"), os.Getenv("FOREST_POSTGRES_DSN")
	if address == "" || dsn == "" {
		t.Fatal("explicit integration requires actual services")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	t.Cleanup(cancel)
	return ctx, demo.Options{Address: address, DSN: dsn, TaskQueue: fmt.Sprintf("cross-%d", time.Now().UnixNano()), Directory: t.TempDir()}
}
func input(o demo.Options) protocol.Request {
	return protocol.Request{OperationID: o.TaskQueue, Target: o.TaskQueue, Payload: "oak"}
}

func TestRealWorkflowCrossesFxAndDatabaseInOneGeneration(t *testing.T) {
	ctx, o := config(t)
	if err := demo.Setup(ctx, o.DSN); err != nil {
		t.Fatal(err)
	}
	r, err := demo.New(ctx, o)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close(ctx)
	if err = r.Enable(ctx); err != nil {
		t.Fatal(err)
	}
	id, err := r.Start(ctx, o.TaskQueue, input(o), false)
	if err != nil {
		t.Fatal(err)
	}
	out, err := r.Result(ctx, id)
	if err != nil {
		t.Fatal(err)
	}
	if out.VersionBefore != "v1" || out.VersionAfter != "v1" || out.Receipt.EffectVersion != "v1" || out.Reused {
		t.Fatalf("result=%+v", out)
	}
	if out.Attempt != 1 || out.WorkerIdentity == "" || out.FxIdentity == "" {
		t.Fatal("missing actual execution identity", out)
	}
	seen := map[string]bool{}
	for _, p := range out.Trace {
		if p.Generation != 1 {
			t.Fatalf("mixed generation: %+v", p)
		}
		seen[p.Stage] = true
	}
	for _, stage := range []string{"activity", "fx", "business.observe", "business.apply", "database"} {
		if !seen[stage] {
			t.Fatal("missing boundary", stage)
		}
	}
	obs, err := r.Inspect(ctx, input(o))
	if err != nil || !obs.Found || obs.EffectCount != 1 || obs.Receipt != out.Receipt {
		t.Fatalf("receipt=%+v %v", obs, err)
	}
	if err = r.Close(ctx); err != nil {
		t.Fatal(err)
	}
	resources := r.Resources(ctx)
	if resources.FileOpen || resources.FileCloses != 1 || resources.PoolResponding {
		t.Fatalf("resources=%+v", resources)
	}
}

func TestReplayDoesNotInvokeBusinessAgain(t *testing.T) {
	ctx, o := config(t)
	if e := demo.Setup(ctx, o.DSN); e != nil {
		t.Fatal(e)
	}
	r, e := demo.New(ctx, o)
	if e != nil {
		t.Fatal(e)
	}
	defer r.Close(ctx)
	if e = r.Enable(ctx); e != nil {
		t.Fatal(e)
	}
	id, e := r.Start(ctx, o.TaskQueue, input(o), false)
	if e != nil {
		t.Fatal(e)
	}
	if _, e = r.Result(ctx, id); e != nil {
		t.Fatal(e)
	}
	before := r.BusinessCalls()
	if e = r.Replay(ctx, id); e != nil {
		t.Fatal(e)
	}
	if r.BusinessCalls() != before {
		t.Fatal("replay executed business outside Activity history")
	}
}
