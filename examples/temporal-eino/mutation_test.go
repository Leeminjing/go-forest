package einodemo_test

import (
	"context"
	"errors"
	tk "github.com/Leeminjing/go-forest/testkit"
	"sync"
	"testing"

	demo "github.com/Leeminjing/go-forest/examples/temporal-eino"
	"github.com/Leeminjing/go-forest/examples/temporal-eino/protocol"
	"github.com/Leeminjing/go-forest/forest"
)

func paused(t *testing.T, ctx context.Context, r *demo.Runtime, id demo.Ref) protocol.WorkflowState {
	t.Helper()
	var s protocol.WorkflowState
	until(t, ctx, func() bool { var e error; s, e = r.Query(ctx, id); return e == nil && s.Stage == "paused" })
	return s
}
func continueRun(t *testing.T, ctx context.Context, r *demo.Runtime, id demo.Ref, in protocol.Input) protocol.Result {
	t.Helper()
	s := paused(t, ctx, r, id)
	e := r.Decide(ctx, id, protocol.Decision{ID: "continue", ExecutionID: in.ExecutionID, Token: s.Pause.Token, Target: s.Pause.Targets[0], Value: "continue"})
	if e != nil {
		t.Fatal(e)
	}
	out, e := r.Result(ctx, id)
	if e != nil {
		t.Fatal(e)
	}
	return out
}
func TestOldNativeAgentKeepsGenerationAcrossReplaceAndRebind(t *testing.T) {
	for _, mode := range []string{"replace", "rebind"} {
		t.Run(mode, func(t *testing.T) {
			ctx, o := config(t)
			old := input(o)
			next := old
			next.ExecutionID += "-next"
			next.OperationID += "-next"
			barrier := tk.NewBarrier()
			o.Hook = func(c context.Context, p protocol.Point) error {
				if p.Stage == "tool.between" && p.Input == old {
					return barrier.Hold(c)
				}
				return nil
			}
			r, e := demo.New(ctx, o)
			if e != nil {
				t.Fatal(e)
			}
			defer r.Close(ctx)
			defer barrier.Release()
			if e = r.Enable(ctx); e != nil {
				t.Fatal(e)
			}
			before := r.Forest.Snapshot()
			id, e := r.Start(ctx, o.Queue, old)
			if e != nil {
				t.Fatal(e)
			}
			if e = tk.Await(ctx, barrier.Arrived()); e != nil {
				t.Fatal(e)
			}
			if e = r.Switch(ctx, mode); e != nil {
				t.Fatal(e)
			}
			after := r.Forest.Snapshot()
			for _, tree := range []forest.TreeID{"app/adk", "app/graph", "app/worker", "state"} {
				if before.Trees[tree].InstanceID != after.Trees[tree].InstanceID {
					t.Fatal("stable resource rebuilt", tree)
				}
			}
			newID, e := r.Start(ctx, o.Queue+"-next", next)
			if e != nil {
				t.Fatal(e)
			}
			newState := paused(t, ctx, r, newID)
			if p := r.Resources(); !p.BusinessOpen || p.BusinessCloses != 0 {
				t.Fatal("old resource prematurely released", p)
			}
			barrier.Release()
			oldState := paused(t, ctx, r, id)
			for _, pair := range []struct {
				s protocol.WorkflowState
				g uint64
				v string
			}{{oldState, 1, "v1"}, {newState, 2, "v2"}} {
				if pair.s.Last.Generation != pair.g || pair.s.Last.Receipt.Version != pair.v {
					t.Fatal(pair.s)
				}
				j, err := tk.NewJournal(128)
				if err != nil {
					t.Fatal(err)
				}
				call := pair.s.Last.Engine + "/step-0"
				for _, point := range pair.s.Last.Trace {
					if _, err = j.Record(tk.Event{Stage: point.Stage, Execution: &tk.Execution{Scope: tk.Scope{Forest: point.Process, Call: call, Generation: point.Generation}}}); err != nil {
						t.Fatal(err)
					}
				}
				evidence, err := j.Snapshot()
				if err != nil {
					t.Fatal(err)
				}
				if err = tk.CheckPinned(tk.PinnedEvidence{Label: call, Scope: tk.Scope{Forest: pair.s.Last.Process, Call: call, Generation: pair.g}, Required: []string{"tool.between", "business.apply"}, Trace: evidence}); err != nil {
					t.Fatal(err)
				}
			}
			out := continueRun(t, ctx, r, id, old)
			if out.Generation != 2 || out.Receipt.Version != "v1" || out.Text != "finished:v1:oak" {
				t.Fatal("resume mixed effect identity with route", out)
			}
			_ = continueRun(t, ctx, r, newID, next)
			until(t, ctx, func() bool { return r.Resources().BusinessCloses == 1 })
		})
	}
}
func TestRetryBeforeNativeAdmissionUsesNewGeneration(t *testing.T) {
	ctx, o := config(t)
	entered, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	var free sync.Once
	o.Hook = func(c context.Context, p protocol.Point) error {
		transient := false
		if p.Stage == "step.admission" && p.Step == 0 {
			once.Do(func() { transient = true; close(entered) })
		}
		if transient {
			<-release
			return protocol.ErrTransient
		}
		return nil
	}
	r, e := demo.New(ctx, o)
	if e != nil {
		t.Fatal(e)
	}
	defer r.Close(ctx)
	defer free.Do(func() { close(release) })
	if e = r.Enable(ctx); e != nil {
		t.Fatal(e)
	}
	id, e := r.Start(ctx, o.Queue, input(o))
	if e != nil {
		t.Fatal(e)
	}
	select {
	case <-entered:
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	if e = r.Switch(ctx, "replace"); e != nil {
		t.Fatal(e)
	}
	free.Do(func() { close(release) })
	s := paused(t, ctx, r, id)
	if s.Last.Delivery.Attempt != 2 || s.Last.Generation != 2 || s.Last.Receipt.Version != "v2" || r.Counts().Query != 1 {
		t.Fatal(s.Last, r.Counts())
	}
	continueRun(t, ctx, r, id, input(o))
}

type stepper interface {
	Step(context.Context, protocol.Request) (protocol.Result, error)
}

func deliver(ctx context.Context, r *demo.Runtime, q protocol.Request) (out protocol.Result, err error) {
	err = r.Forest.Invoke(ctx, func(c context.Context) error {
		return forest.Use[stepper](c, r.Forest, "app/steps", "steps", func(s stepper) error { var e error; out, e = s.Step(c, q); return e })
	})
	return
}
func TestSealedStepRetriesDoNotResumeStaleNativeCheckpoint(t *testing.T) {
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
	original := continueRun(t, ctx, r, id, input(o))
	ev, e := r.Inspect(ctx, input(o))
	if e != nil {
		t.Fatal(e)
	}
	counts := r.Counts()
	for _, record := range ev.Records {
		out, e := deliver(ctx, r, record.Request)
		if e != nil || !out.Reused || out.State != record.Result.State {
			t.Fatal(out, e)
		}
	}
	if r.Counts() != counts || original.Receipt != ev.Receipt {
		t.Fatal("retries executed native code")
	}
	q := ev.Records[1].Request
	q.Decision.Value = "stop"
	if _, e = deliver(ctx, r, q); !errors.Is(e, protocol.ErrConflict) {
		t.Fatal("changed step accepted", e)
	}
}
