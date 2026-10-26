package einodemo_test

import (
	"errors"
	"testing"

	demo "github.com/Leeminjing/go-forest/examples/temporal-eino"
	"github.com/Leeminjing/go-forest/examples/temporal-eino/protocol"
	"go.temporal.io/sdk/temporal"
)

func TestIndependentNativeSessionsReconcileOneArtifactAndRejectChangedInput(t *testing.T) {
	ctx, o := config(t)
	r, e := demo.New(ctx, o)
	if e != nil {
		t.Fatal(e)
	}
	defer r.Close(ctx)
	if e = r.Enable(ctx); e != nil {
		t.Fatal(e)
	}
	inputs := []protocol.Input{input(o), input(o)}
	inputs[1].ExecutionID += "-second"
	ids := make([]demo.Ref, 2)
	for i, in := range inputs {
		ids[i], e = r.Start(ctx, in.ExecutionID, in)
		if e != nil {
			t.Fatal(e)
		}
	}
	for i, id := range ids {
		paused(t, ctx, r, id)
		evidence, e := r.Inspect(ctx, inputs[i])
		if e != nil || evidence.Artifacts != 1 {
			t.Fatal(evidence, e)
		}
	}
	a := continueRun(t, ctx, r, ids[0], inputs[0])
	b := continueRun(t, ctx, r, ids[1], inputs[1])
	if a.Receipt != b.Receipt || r.Counts().Apply != 2 || r.Counts().Resume != 2 {
		t.Fatal(a, b, r.Counts())
	}
	changed := input(o)
	changed.ExecutionID += "-conflict"
	changed.Text = "different"
	id, e := r.Start(ctx, changed.ExecutionID, changed)
	if e != nil {
		t.Fatal(e)
	}
	_, e = r.Result(ctx, id)
	var app *temporal.ApplicationError
	if !errors.As(e, &app) || app.Type() != "OutcomeUnknown" {
		t.Fatal("native tool error lost execution uncertainty", e)
	}
	evidence, e := r.Inspect(ctx, input(o))
	if e != nil || evidence.Receipt != a.Receipt || evidence.Artifacts != 1 {
		t.Fatal(evidence, e)
	}
	if _, e = r.Inspect(ctx, changed); !errors.Is(e, protocol.ErrConflict) {
		t.Fatal("conflicting operation was not distinguishable", e)
	}
}
