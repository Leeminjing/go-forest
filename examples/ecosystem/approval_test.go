package ecosystem_test

import (
	"context"
	"testing"
	"time"

	"github.com/Leeminjing/go-forest-agent/agent"
	"github.com/Leeminjing/go-forest/forest"
)

func TestApprovalAndExplicitUnknownResolutionResumeSettlement(t *testing.T) {
	for _, mode := range []string{"approval", "unknown"} {
		t.Run(mode, func(t *testing.T) {
			o := options(t)
			o.RequireApproval = true
			r := runtime(t, o)
			ctx := context.Background()
			if err := r.Enable(ctx); err != nil {
				t.Fatal(err)
			}
			if _, err := request(r.HTTPURL+"/tasks", "POST", `{"id":"paused","input":"await authorization"}`); err != nil {
				t.Fatal(err)
			}
			until := func(state string) {
				t.Helper()
				deadline := time.Now().Add(5 * time.Second)
				for {
					if err := r.Tick(ctx); err != nil {
						t.Fatal(err)
					}
					task, err := request(r.HTTPURL+"/tasks/paused", "GET", "")
					if err != nil {
						t.Fatal(err)
					}
					if task.State == state {
						return
					}
					if time.Now().After(deadline) {
						t.Fatalf("state=%s want=%s", task.State, state)
					}
					time.Sleep(10 * time.Millisecond)
				}
			}
			until("awaiting-approval")
			id := o.Namespace + ":paused"
			manage := func(fn func(context.Context, agent.Executor) error) {
				t.Helper()
				if err := r.Forest.Invoke(ctx, func(c context.Context) error {
					return forest.Use[agent.Executor](c, r.Forest, "app/agent", "executor", func(e agent.Executor) error { return fn(c, e) })
				}); err != nil {
					t.Fatal(err)
				}
			}
			manage(func(c context.Context, e agent.Executor) error {
				run, err := e.Read(c, id)
				if err != nil {
					return err
				}
				_, err = e.Decide(c, id, run.Revision, run.Calls[run.Current].Call.ID, agent.Granted)
				return err
			})
			want := "model v1: 5"
			if mode == "unknown" {
				// A public checkpoint fixture represents a previous process that persisted
				// ToolRunning but never persisted an outcome. No executor is running here.
				err := r.Forest.Invoke(ctx, func(c context.Context) error {
					return forest.Use[agent.Checkpoint](c, r.Forest, "app/checkpoint", "checkpoint", func(cp agent.Checkpoint) error {
						run, err := cp.Load(c, id)
						if err != nil {
							return err
						}
						previous := run.Revision
						run.Revision++
						run.Status = agent.ToolRunning
						return cp.CompareAndSwap(c, id, previous, run)
					})
				})
				if err != nil {
					t.Fatal(err)
				}
				until("outcome-unknown")
				for i := 0; i < 3; i++ {
					if err = r.Tick(ctx); err != nil {
						t.Fatal(err)
					}
				}
				manage(func(c context.Context, e agent.Executor) error {
					run, err := e.Read(c, id)
					if err != nil {
						return err
					}
					if run.Status != agent.OutcomeUnknown {
						t.Fatal("unknown tool was automatically replayed")
					}
					_, err = e.Resolve(c, id, run.Calls[run.Current].Call.ID, "host-resolved-value")
					return err
				})
				want = "model v1: host-resolved-value"
			}
			until("completed")
			task, err := request(r.HTTPURL+"/tasks/paused", "GET", "")
			if err != nil || task.Result != want {
				t.Fatalf("did not settle resumed run: %+v %v", task, err)
			}
		})
	}
}
