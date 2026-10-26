package einodemo_test

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	demo "github.com/Leeminjing/go-forest/examples/temporal-eino"
	"github.com/Leeminjing/go-forest/examples/temporal-eino/protocol"
)

func config(t *testing.T) (context.Context, demo.Options) {
	t.Helper()
	if os.Getenv("FOREST_INTEGRATION") != "1" {
		t.Skip("real Temporal acceptance requires FOREST_INTEGRATION=1")
	}
	address := os.Getenv("FOREST_TEMPORAL_ADDRESS")
	if address == "" {
		t.Fatal("explicit integration requires Temporal address")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	t.Cleanup(cancel)
	return ctx, demo.Options{Address: address, Queue: fmt.Sprintf("te-%d", time.Now().UnixNano()), Directory: t.TempDir()}
}
func input(o demo.Options) protocol.Input {
	return protocol.Input{ExecutionID: o.Queue, OperationID: o.Queue, Text: "oak"}
}
func until(t *testing.T, ctx context.Context, fn func() bool) {
	t.Helper()
	for !fn() {
		select {
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		case <-time.After(5 * time.Millisecond):
		}
	}
}
func TestRealWorkflowUsesNativeGraphAndAgentThenSignalResumesCheckpoint(t *testing.T) {
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
	var state protocol.WorkflowState
	until(t, ctx, func() bool { state, e = r.Query(ctx, id); return e == nil && state.Stage == "paused" })
	if state.Pause.Token == "" || len(state.Pause.Targets) != 1 {
		t.Fatal("missing native interrupt", state)
	}
	if e = r.Decide(ctx, id, protocol.Decision{ID: "continue-1", ExecutionID: o.Queue, Token: state.Pause.Token, Target: state.Pause.Targets[0], Value: "continue"}); e != nil {
		t.Fatal(e)
	}
	out, e := r.Result(ctx, id)
	if e != nil {
		t.Fatal(e)
	}
	if out.State != "completed" || out.Text != "finished:v1:oak" || out.Receipt.Content != "v1:oak" {
		t.Fatal(out)
	}
	engine := r.Process + "/" + r.Forest.Snapshot().Trees["app/adk"].InstanceID
	if out.Engine != engine {
		t.Fatal("missing actual Eino identity", out.Engine, engine)
	}
	diagnostic, e := r.Query(ctx, id)
	if e != nil {
		t.Fatal(e)
	}
	graphEngine := r.Process + "/" + r.Forest.Snapshot().Trees["app/graph"].InstanceID
	if diagnostic.Stage != "completed" || diagnostic.Graph.Instance != graphEngine {
		t.Fatal("graph execution evidence missing", diagnostic, e)
	}
	for _, p := range out.Trace {
		if p.Stage == "model" || p.Stage == "checkpoint.get" || p.Stage == "business.inspect" {
			if p.Instance != engine {
				t.Fatal("native boundary lost physical identity", p)
			}
		}
	}
	evidence, e := r.Inspect(ctx, input(o))
	if e != nil {
		t.Fatal(e)
	}
	if evidence.Artifacts != 1 || evidence.Receipt != out.Receipt {
		t.Fatal(evidence)
	}
	counts := r.Counts()
	if counts.Apply != 1 || counts.Resume != 1 || counts.Graph < 2 {
		t.Fatal(counts)
	}
	if e = r.Replay(ctx, id); e != nil {
		t.Fatal(e)
	}
	if r.Counts() != counts {
		t.Fatal("replay invoked business engines")
	}
}
