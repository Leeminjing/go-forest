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
	var pending []protocol.Decision
	var selected *protocol.Decision
	var signalErr error
	ready, closing := false, false
	defer func() {
		closing = true
		if err != nil {
			state.Stage = "failed"
			if temporal.IsCanceledError(err) || ctx.Err() != nil {
				state.Stage = "canceled"
			}
		}
	}()
	if err = workflow.SetQueryHandler(ctx, "state", func() (protocol.WorkflowState, error) { return state, nil }); err != nil {
		return
	}
	choose := func() {
		for ready && selected == nil && len(pending) > 0 {
			d := pending[0]
			pending = pending[1:]
			if d.ExecutionID != in.ExecutionID || d.Token != state.Pause.Token || len(state.Pause.Targets) != 1 || d.Target != state.Pause.Targets[0] || (d.Value != "continue" && d.Value != "stop") {
				state.Decisions[d.ID] = "rejected:stale-or-invalid"
				continue
			}
			state.Decisions[d.ID] = "accepted"
			selected = &d
			ready = false
			state.Stage = "resuming"
		}
	}
	workflow.Go(ctx, func(receive workflow.Context) {
		for {
			var d protocol.Decision
			if !workflow.GetSignalChannel(receive, "decision").Receive(receive, &d) {
				return
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
				signalErr = temporal.NewNonRetryableApplicationError("decision budget", "Budget", nil)
				return
			}
			seen[d.ID] = h
			if !protocol.Key(d.ID) || d.ExecutionID != in.ExecutionID || len(d.Token) > 128 || len(d.Target) > 256 || (d.Value != "continue" && d.Value != "stop") {
				state.Decisions[d.ID] = "rejected:stale-or-invalid"
				continue
			}
			if closing {
				state.Decisions[d.ID] = "rejected:closed"
				continue
			}
			if !ready && state.Pause.Token != "" && d.Token == state.Pause.Token {
				state.Decisions[d.ID] = "rejected:stale-or-invalid"
				continue
			}
			state.Decisions[d.ID] = "pending"
			pending = append(pending, d)
			choose()
		}
	})
	timeout, heartbeat := 20*time.Second, 3*time.Second
	if in.AttemptMillis != 0 {
		timeout = time.Duration(in.AttemptMillis) * time.Millisecond
		if heartbeat > timeout/2 {
			heartbeat = timeout / 2
		}
	}
	ctx = workflow.WithActivityOptions(ctx, workflow.ActivityOptions{StartToCloseTimeout: timeout, ScheduleToCloseTimeout: 2 * time.Minute, HeartbeatTimeout: heartbeat, WaitForCancellation: true, RetryPolicy: &temporal.RetryPolicy{InitialInterval: 100 * time.Millisecond, MaximumInterval: time.Second, MaximumAttempts: 4}})
	var graph protocol.GraphResult
	if err = workflow.ExecuteActivity(ctx, "te.graph", in).Get(ctx, &graph); err != nil {
		return
	}
	state.Graph = graph
	info := workflow.GetInfo(ctx)
	request := protocol.Request{Input: in, WorkflowID: info.WorkflowExecution.ID, RunID: info.WorkflowExecution.RunID, Action: "run"}
	for request.Step < 8 {
		if signalErr != nil {
			return out, signalErr
		}
		state.Stage = "running"
		if err = workflow.ExecuteActivity(ctx, "te.step", request).Get(ctx, &out); err != nil {
			return
		}
		state.Last = out
		if out.State != "paused" {
			break
		}
		state.Stage = "paused"
		state.Pause = out.Pause
		ready = true
		selected = nil
		choose()
		if err = workflow.Await(ctx, func() bool { return selected != nil || signalErr != nil }); err != nil {
			return
		}
		if signalErr != nil {
			return out, signalErr
		}
		request.Step++
		request.Action = "resume"
		request.Decision = *selected
	}
	if out.State != "completed" && out.State != "stopped" {
		return out, temporal.NewNonRetryableApplicationError("step budget", "Budget", nil)
	}
	closing = true
	ready = false
	for _, d := range pending {
		state.Decisions[d.ID] = "rejected:closed"
	}
	pending = nil
	state.Stage = "verifying"
	if err = workflow.ExecuteActivity(ctx, "te.graph", in).Get(ctx, &graph); err != nil {
		return
	}
	state.Graph = graph
	if signalErr != nil {
		return out, signalErr
	}
	state.Stage = out.State
	return
}
