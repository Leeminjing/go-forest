package einodemo_test

import (
	demo "github.com/Leeminjing/go-forest/examples/temporal-eino"
	"github.com/Leeminjing/go-forest/examples/temporal-eino/protocol"
	"testing"
)

func TestStaleConflictAndDuplicateSignalsDoNotAdvanceNativeExecution(t *testing.T) {
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
	p := paused(t, ctx, r, id).Pause
	d := protocol.Decision{ID: "bad", ExecutionID: o.Queue, Token: "stale", Target: p.Targets[0], Value: "continue"}
	if e = r.Decide(ctx, id, d); e != nil {
		t.Fatal(e)
	}
	until(t, ctx, func() bool {
		s, e := r.Query(ctx, id)
		return e == nil && s.Decisions[d.ID] == "rejected:stale-or-invalid"
	})
	if e = r.Decide(ctx, id, d); e != nil {
		t.Fatal(e)
	}
	until(t, ctx, func() bool { s, e := r.Query(ctx, id); return e == nil && s.Decisions[d.ID] == "duplicate" })
	d.Token = p.Token
	if e = r.Decide(ctx, id, d); e != nil {
		t.Fatal(e)
	}
	until(t, ctx, func() bool { s, e := r.Query(ctx, id); return e == nil && s.Decisions[d.ID] == "rejected:conflict" })
	if r.Counts().Resume != 0 {
		t.Fatal("invalid signals resumed native state")
	}
	continueRun(t, ctx, r, id, input(o))
	if r.Counts().Apply != 1 || r.Counts().Resume != 1 {
		t.Fatal(r.Counts())
	}
}
