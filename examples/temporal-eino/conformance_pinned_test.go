package einodemo_test

import (
	"context"
	demo "github.com/Leeminjing/go-forest/examples/temporal-eino"
	tk "github.com/Leeminjing/go-forest/testkit"
	"go.temporal.io/sdk/activity"
	"strconv"
	"sync"
	"testing"
)

// Each point retains identifiers read in the actual native callback. No result
// projection overwrites every point with an expected invocation label.
type pinCapture struct {
	mu    sync.Mutex
	calls map[string]*tk.Journal
}

func taskKey(ref demo.Ref) string { return ref.WorkflowID + "/" + ref.RunID }
func (p *pinCapture) record(ctx context.Context, r *demo.Runtime, stage string) error {
	info := activity.GetInfo(ctx)
	key := info.WorkflowExecution.ID + "/" + info.WorkflowExecution.RunID
	g, err := r.Forest.Generation(ctx)
	if err != nil {
		return err
	}
	attempt := strconv.Itoa(int(info.Attempt))
	x := tk.Execution{Scope: tk.Scope{Forest: r.Process, Call: key + "/" + info.ActivityID + "/" + attempt, Generation: g}, Task: key, Step: info.ActivityID, Attempt: attempt, Process: r.Process, Instance: r.Forest.Snapshot().Trees["app/worker"].InstanceID}
	p.mu.Lock()
	if p.calls == nil {
		p.calls = map[string]*tk.Journal{}
	}
	j := p.calls[key]
	if j == nil {
		j, err = tk.NewJournal(128)
		p.calls[key] = j
	}
	p.mu.Unlock()
	if err != nil {
		return err
	}
	_, err = j.Record(tk.Event{Stage: stage, Execution: &x})
	return err
}
func (p *pinCapture) proof(t *testing.T, domain string, expected, source demo.Ref, g uint64) tk.PinnedEvidence {
	t.Helper()
	p.mu.Lock()
	j := p.calls[taskKey(source)]
	p.mu.Unlock()
	if j == nil {
		t.Fatal("native invocation was never witnessed", source)
	}
	snapshot, err := j.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	entries := snapshot.Entries()
	if len(entries) == 0 || entries[0].Execution == nil {
		t.Fatal("native invocation has no points")
	}
	first := entries[0].Execution
	// The native step/attempt belongs to this trace; the expected logical task
	// comes independently from Start's WorkflowID/RunID, never from that trace.
	expectedCall := taskKey(expected) + "/" + first.Step + "/" + first.Attempt
	return tk.PinnedEvidence{Label: taskKey(expected), Scope: tk.Scope{Forest: domain, Call: expectedCall, Generation: g}, Required: []string{"tool.between", "business.apply"}, Trace: snapshot}
}
