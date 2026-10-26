// Package flow contains deterministic native orchestration, no ordinary I/O.
package flow

import (
	"github.com/Leeminjing/go-forest/examples/temporal-eino/protocol"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"
	"time"
)

func Execute(ctx workflow.Context, in protocol.Input) (out protocol.Result, err error) {
	state := protocol.WorkflowState{Stage: "starting", Decisions: map[string]string{}}
	seen := map[string]string{}
	if err = workflow.SetQueryHandler(ctx, "state", func() (protocol.WorkflowState, error) { return state, nil }); err != nil {
		return
	}
	timeout, heartbeat := 20*time.Second, 3*time.Second
	if in.AttemptMillis != 0 {
		timeout = time.Duration(in.AttemptMillis) * time.Millisecond
		if heartbeat > timeout/2 {
			heartbeat = timeout / 2
		}
	}
	ctx = workflow.WithActivityOptions(ctx, workflow.ActivityOptions{StartToCloseTimeout: timeout, ScheduleToCloseTimeout: 2 * time.Minute, HeartbeatTimeout: heartbeat, WaitForCancellation: true, RetryPolicy: &temporal.RetryPolicy{InitialInterval: 100 * time.Millisecond, MaximumInterval: time.Second, MaximumAttempts: 4}})
	var graph string
	if err = workflow.ExecuteActivity(ctx, "te.graph", in).Get(ctx, &graph); err != nil {
		return
	}
	info := workflow.GetInfo(ctx)
	request := protocol.Request{Input: in, WorkflowID: info.WorkflowExecution.ID, RunID: info.WorkflowExecution.RunID, Action: "run"}
	for request.Step < 8 {
		state.Stage = "running"
		if err = workflow.ExecuteActivity(ctx, "te.step", request).Get(ctx, &out); err != nil {
			return
		}
		state.Last = out
		if out.State != "paused" {
			state.Stage = out.State
			break
		}
		state.Stage = "paused"
		state.Pause = out.Pause
		for {
			var d protocol.Decision
			if !workflow.GetSignalChannel(ctx, "decision").Receive(ctx, &d) {
				return out, temporal.NewCanceledError()
			}
			h := protocol.Hash(d)
			if old, ok := seen[d.ID]; ok {
				if old == h {
					state.Decisions[d.ID] = "duplicate"
				} else {
					state.Decisions[d.ID] = "rejected:conflict"
				}
				continue
			}
			if len(seen) >= 64 {
				return out, temporal.NewNonRetryableApplicationError("decision budget", "Budget", nil)
			}
			seen[d.ID] = h
			if !protocol.Key(d.ID) || d.ExecutionID != in.ExecutionID || d.Token != out.Pause.Token || len(out.Pause.Targets) != 1 || d.Target != out.Pause.Targets[0] || (d.Value != "continue" && d.Value != "stop") {
				state.Decisions[d.ID] = "rejected:stale-or-invalid"
				continue
			}
			state.Decisions[d.ID] = "accepted"
			request.Step++
			request.Action = "resume"
			request.Decision = d
			break
		}
	}
	if out.State != "completed" && out.State != "stopped" {
		return out, temporal.NewNonRetryableApplicationError("step budget", "Budget", nil)
	}
	err = workflow.ExecuteActivity(ctx, "te.graph", in).Get(ctx, &graph)
	return
}
