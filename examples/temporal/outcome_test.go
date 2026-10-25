package temporaldemo_test

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Leeminjing/go-forest-agent/agent"
	demo "github.com/Leeminjing/go-forest/examples/temporal"
	"github.com/Leeminjing/go-forest/examples/temporal/flow"
	"github.com/Leeminjing/go-forest/examples/temporal/internal/jobservice"
	"github.com/Leeminjing/go-forest/forest"
	"go.temporal.io/sdk/temporal"
)

type service interface {
	Plan(context.Context, flow.Input) (flow.Planned, error)
	Apply(context.Context, flow.Planned) (flow.Receipt, error)
	Read(context.Context, string) (flow.Result, error)
}

func useService(ctx context.Context, r *demo.Runtime, fn func(context.Context, service) error) error {
	return r.Forest.Invoke(ctx, func(c context.Context) error {
		return forest.Use[service](c, r.Forest, "jobs", "jobs", func(s service) error { return fn(c, s) })
	})
}
func TestBusinessReceiptDeduplicatesAndRejectsChangedInput(t *testing.T) {
	o := config(t)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	if err := demo.Setup(ctx, o.DSN); err != nil {
		t.Fatal(err)
	}
	r, err := demo.New(ctx, o)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close(ctx)
	var planned flow.Planned
	err = useService(ctx, r, func(c context.Context, s service) error {
		planned, err = s.Plan(c, flow.Input{ID: o.TaskQueue, Text: "pine"})
		return err
	})
	if err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	results := make(chan flow.Receipt, 8)
	failures := make(chan error, 8)
	for n := 0; n < 8; n++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			err := useService(ctx, r, func(c context.Context, s service) error {
				v, err := s.Apply(c, planned)
				if err == nil {
					results <- v
				}
				return err
			})
			if err != nil {
				failures <- err
			}
		}()
	}
	wg.Wait()
	close(results)
	close(failures)
	for err := range failures {
		t.Fatal(err)
	}
	for r := range results {
		if r.Changes != 1 || r.Plan != "plan:pine" {
			t.Fatalf("non-idempotent: %+v", r)
		}
	}
	err = useService(ctx, r, func(c context.Context, s service) error {
		_, err := s.Plan(c, flow.Input{ID: o.TaskQueue, Text: "different"})
		return err
	})
	if !errors.Is(err, jobservice.ErrConflict) {
		t.Fatalf("input conflict ignored: %v", err)
	}
}
func TestUnknownOutcomeIsNotBlindlyRetried(t *testing.T) {
	o := config(t)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	var calls atomic.Int32
	o.AfterCommit = func(context.Context, flow.Receipt) error { calls.Add(1); return jobservice.ErrUnknown }
	if err := demo.Setup(ctx, o.DSN); err != nil {
		t.Fatal(err)
	}
	r, err := demo.New(ctx, o)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close(ctx)
	id, err := r.StartRun(ctx, flow.Input{ID: o.TaskQueue, Text: "maple"})
	if err != nil {
		t.Fatal(err)
	}
	_, err = r.Result(ctx, id)
	var app *temporal.ApplicationError
	if !errors.As(err, &app) || app.Type() != "OutcomeUnknown" || calls.Load() != 1 {
		t.Fatalf("unknown falsely completed/replayed: %v calls=%d", err, calls.Load())
	}
	err = useService(ctx, r, func(c context.Context, s service) error {
		out, err := s.Read(c, o.TaskQueue)
		if err == nil && out.Receipt.Changes != 1 {
			t.Error("business reconciliation lost receipt")
		}
		return err
	})
	if err != nil {
		t.Fatal(err)
	}
}

type modelFunc func(context.Context, agent.ModelRequest, func(string) error) (agent.ModelResult, error)

func (m modelFunc) Generate(c context.Context, in agent.ModelRequest, emit func(string) error) (agent.ModelResult, error) {
	return m(c, in, emit)
}
func TestInProgressAgentKeepsNativeIdentityWithoutReplay(t *testing.T) {
	o := config(t)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	entered, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	var calls atomic.Int32
	o.Model = modelFunc(func(context.Context, agent.ModelRequest, func(string) error) (agent.ModelResult, error) {
		calls.Add(1)
		close(entered)
		<-release
		return agent.ModelResult{Content: "plan:ash"}, nil
	})
	if err := demo.Setup(ctx, o.DSN); err != nil {
		t.Fatal(err)
	}
	r, err := demo.New(ctx, o)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close(ctx)
	defer once.Do(func() { close(release) })
	done := make(chan error, 1)
	plan := func() error {
		return useService(ctx, r, func(c context.Context, s service) error {
			_, err := s.Plan(c, flow.Input{ID: o.TaskQueue, Text: "ash"})
			return err
		})
	}
	go func() { done <- plan() }()
	select {
	case <-entered:
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	err = plan()
	var pending *jobservice.Continuation
	if !errors.As(err, &pending) || pending.RunID == "" || pending.Status != agent.ModelRunning {
		t.Fatalf("native running state hidden: %v", err)
	}
	once.Do(func() { close(release) })
	if err = <-done; err != nil {
		t.Fatal(err)
	}
	if calls.Load() != 1 {
		t.Fatal("in-progress native model replayed")
	}
}
