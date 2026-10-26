package einodemo_test

import (
	"context"
	"errors"
	demo "github.com/Leeminjing/go-forest/examples/temporal-eino"
	"github.com/Leeminjing/go-forest/examples/temporal-eino/protocol"
	"sync"
	"testing"
	"time"
)

func TestSignalsDuringResumeAreClassifiedAndCompletionWaitsForFinalGraph(t *testing.T) {
	ctx, o := config(t)
	resumeEntered, resumeRelease, graphEntered, graphRelease := make(chan struct{}), make(chan struct{}), make(chan struct{}), make(chan struct{})
	var resumeOnce, graphOnce, freeResume, freeGraph sync.Once
	var mu sync.Mutex
	resumed := false
	o.Hook = func(c context.Context, p protocol.Point) error {
		if p.Stage == "step.sealed" && p.Result != nil && p.Result.State == "completed" {
			mu.Lock()
			resumed = true
			mu.Unlock()
			resumeOnce.Do(func() { close(resumeEntered) })
			<-resumeRelease
		}
		mu.Lock()
		done := resumed
		mu.Unlock()
		if p.Stage == "graph.between" && done {
			graphOnce.Do(func() { close(graphEntered) })
			<-graphRelease
			return errors.New("final graph verification failed")
		}
		return nil
	}
	r, e := demo.New(ctx, o)
	if e != nil {
		t.Fatal(e)
	}
	defer r.Close(ctx)
	defer freeResume.Do(func() { close(resumeRelease) })
	defer freeGraph.Do(func() { close(graphRelease) })
	if e = r.Enable(ctx); e != nil {
		t.Fatal(e)
	}
	id, e := r.Start(ctx, o.Queue, input(o))
	if e != nil {
		t.Fatal(e)
	}
	s := paused(t, ctx, r, id)
	d := protocol.Decision{ID: "accepted", ExecutionID: o.Queue, Token: s.Pause.Token, Target: s.Pause.Targets[0], Value: "continue"}
	if e = r.Decide(ctx, id, d); e != nil {
		t.Fatal(e)
	}
	select {
	case <-resumeEntered:
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	if e = r.Decide(ctx, id, d); e != nil {
		t.Fatal(e)
	}
	probe, end := context.WithTimeout(ctx, 3*time.Second)
	defer end()
	until(t, probe, func() bool { s, e := r.Query(ctx, id); return e == nil && s.Decisions[d.ID] == "duplicate" })
	d.Value = "stop"
	if e = r.Decide(ctx, id, d); e != nil {
		t.Fatal(e)
	}
	until(t, ctx, func() bool { s, e := r.Query(ctx, id); return e == nil && s.Decisions[d.ID] == "rejected:conflict" })
	freeResume.Do(func() { close(resumeRelease) })
	select {
	case <-graphEntered:
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	s, e = r.Query(ctx, id)
	if e != nil || s.Stage != "verifying" {
		t.Fatal("premature completion", s, e)
	}
	freeGraph.Do(func() { close(graphRelease) })
	if _, e = r.Result(ctx, id); e == nil {
		t.Fatal("verification failure completed workflow")
	}
	s, e = r.Query(ctx, id)
	if e != nil || s.Stage != "failed" {
		t.Fatal("failed workflow diagnostic stale", s, e)
	}
	if r.Counts().Resume != 1 || r.Counts().Apply != 1 {
		t.Fatal("signals reentered native engine", r.Counts())
	}
}

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
