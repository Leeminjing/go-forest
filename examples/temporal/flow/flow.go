// Package flow is deterministic Temporal orchestration and pure data contracts.
// It imports no Forest, database, client or Agent runtime.
package flow

import (
	"time"

	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"
)

type Input struct {
	ID, Text string
	Wait     bool
}
type Planned struct{ ID, Digest, Plan string }
type Receipt struct {
	ID, Digest, Plan, Version string
	Generation                uint64
	Changes                   int
}
type Result struct {
	Receipt        Receipt
	ReadVersion    string
	ReadGeneration uint64
}
type State struct {
	Stage   string
	Receipt Receipt
}

func Job(ctx workflow.Context, in Input) (out Result, err error) {
	state := State{Stage: "planning"}
	if err = workflow.SetQueryHandler(ctx, "state", func() (State, error) { return state, nil }); err != nil {
		return
	}
	ctx = workflow.WithActivityOptions(ctx, workflow.ActivityOptions{StartToCloseTimeout: 5 * time.Second, ScheduleToCloseTimeout: time.Minute, RetryPolicy: &temporal.RetryPolicy{InitialInterval: 100 * time.Millisecond, MaximumInterval: time.Second, MaximumAttempts: 5}})
	var plan Planned
	if err = workflow.ExecuteActivity(ctx, "job.plan", in).Get(ctx, &plan); err != nil {
		return
	}
	state.Stage = "applying"
	if err = workflow.ExecuteActivity(ctx, "job.apply", plan).Get(ctx, &state.Receipt); err != nil {
		return
	}
	if in.Wait {
		state.Stage = "waiting"
		var signal bool
		workflow.GetSignalChannel(ctx, "continue").Receive(ctx, &signal)
	}
	state.Stage = "reading"
	err = workflow.ExecuteActivity(ctx, "job.read", in.ID).Get(ctx, &out)
	state.Stage = "completed"
	return
}
