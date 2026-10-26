// Package flow contains native deterministic Workflow orchestration only.
package flow

import (
	"time"

	"github.com/Leeminjing/go-forest/examples/cross-adapter/protocol"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"
)

type Input struct {
	Request protocol.Request
	Wait    bool
}
type State struct{ Stage string }

func Command(ctx workflow.Context, in Input) (out protocol.Result, err error) {
	state := State{Stage: "applying"}
	if err = workflow.SetQueryHandler(ctx, "state", func() (State, error) { return state, nil }); err != nil {
		return
	}
	ctx = workflow.WithActivityOptions(ctx, workflow.ActivityOptions{StartToCloseTimeout: 20 * time.Second, ScheduleToCloseTimeout: 2 * time.Minute, HeartbeatTimeout: 3 * time.Second, WaitForCancellation: true, RetryPolicy: &temporal.RetryPolicy{InitialInterval: 100 * time.Millisecond, MaximumInterval: time.Second, MaximumAttempts: 4}})
	if err = workflow.ExecuteActivity(ctx, "cross.apply", in.Request).Get(ctx, &out); err != nil {
		return
	}
	if in.Wait {
		state.Stage = "waiting"
		var signal bool
		workflow.GetSignalChannel(ctx, "continue").Receive(ctx, &signal)
	}
	var observation protocol.Observation
	err = workflow.ExecuteActivity(ctx, "cross.inspect", in.Request).Get(ctx, &observation)
	state.Stage = "completed"
	return
}
