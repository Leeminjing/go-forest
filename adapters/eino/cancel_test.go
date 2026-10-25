package einoforest_test

import (
	"context"
	"errors"
	"testing"
	"time"

	ef "github.com/Leeminjing/go-forest/adapters/eino"
	"github.com/Leeminjing/go-forest/forest"
	"github.com/cloudwego/eino/adk"
)

func TestNativeADKCancelKeepsLeaseUntilRealJoin(t *testing.T) {
	f := forest.New(forest.Options{})
	defer f.Close(context.Background())
	owner, _ := ef.NewOwner(f, 8)
	entered, release := make(chan struct{}), make(chan struct{})
	defer func() {
		select {
		case <-release:
		default:
			close(release)
		}
	}()
	d, err := nativeDefinition(ef.Config{TypeID: "native", Version: "1", Namespace: "native", Owner: owner}, t.TempDir(), nativeTool{entered: entered, release: release, label: "oak"})
	if err != nil {
		t.Fatal(err)
	}
	mount(t, f, d)
	cancels := make(chan adk.AgentCancelFunc, 1)
	done := make(chan error, 1)
	go func() {
		done <- invokeADK(f, func(ctx context.Context, a ef.ADK) error {
			_, err := a.Query(ctx, "block", ef.RunOptions{CheckpointID: "shared", OnCancelReady: func(c adk.AgentCancelFunc) { cancels <- c }}, func(e *adk.AgentEvent) error { _, err := ef.ReadMessage(e); return err })
			return err
		})
	}()
	waitSignal(t, entered)
	cancel := <-cancels
	handle, _ := cancel(adk.WithAgentCancelMode(adk.CancelImmediate))
	canceled := make(chan struct{})
	go func() { handle.Wait(); close(canceled) }()
	waitSignal(t, canceled)
	err = invokeADK(f, func(ctx context.Context, a ef.ADK) error {
		_, err := a.Resume(ctx, nil, ef.RunOptions{CheckpointID: "shared"}, func(*adk.AgentEvent) error { return nil })
		return err
	})
	if !errors.Is(err, ef.ErrBusy) {
		t.Fatalf("early lease release: %v", err)
	}
	select {
	case err := <-done:
		t.Fatalf("returned before tool exit: %v", err)
	default:
	}
	close(release)
	select {
	case err := <-done:
		var nativeCancel *adk.CancelError
		if !errors.As(err, &nativeCancel) {
			t.Fatalf("native cancel lost: %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("cancel Join did not finish")
	}
}
func TestContextCancellationIsNotSuccessfulResult(t *testing.T) {
	f := forest.New(forest.Options{})
	defer f.Close(context.Background())
	owner, _ := ef.NewOwner(f, 8)
	d, err := nativeDefinition(ef.Config{TypeID: "native", Version: "1", Namespace: "native", Owner: owner}, t.TempDir(), nativeTool{label: "oak"})
	if err != nil {
		t.Fatal(err)
	}
	mount(t, f, d)
	ctx, cancel := context.WithCancel(context.Background())
	err = f.Invoke(ctx, func(c context.Context) error {
		return forest.Use[ef.ADK](c, f, "agent", "agent", func(a ef.ADK) error {
			_, err := a.Query(c, "cancel", ef.RunOptions{CheckpointID: "canceled"}, func(e *adk.AgentEvent) error { cancel(); return nil })
			return err
		})
	})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("cancellation hidden: %v", err)
	}
}
