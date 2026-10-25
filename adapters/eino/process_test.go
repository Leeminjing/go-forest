package einoforest_test

import (
	"bufio"
	"context"
	"encoding/json"
	"os"
	"os/exec"
	"testing"
	"time"

	ef "github.com/Leeminjing/go-forest/adapters/eino"
	"github.com/Leeminjing/go-forest/forest"
	"github.com/cloudwego/eino/adk"
)

func TestCheckpointProcessHelper(t *testing.T) {
	dir := os.Getenv("FOREST_EINO_CHILD")
	if dir == "" {
		t.Skip("child only")
	}
	f := forest.New(forest.Options{})
	owner, _ := ef.NewOwner(f, 4)
	d, err := nativeDefinition(ef.Config{TypeID: "native", Version: "1", Namespace: "persisted", Owner: owner}, dir, nativeTool{interrupt: true, label: "oak"})
	if err != nil {
		t.Fatal(err)
	}
	mount(t, f, d)
	var address string
	err = invokeADK(f, func(ctx context.Context, a ef.ADK) error {
		_, err := a.Query(ctx, "approve", ef.RunOptions{CheckpointID: "durable"}, func(e *adk.AgentEvent) error {
			_, err := ef.ReadMessage(e)
			if e.Action != nil && e.Action.Interrupted != nil {
				address = e.Action.Interrupted.InterruptContexts[0].ID
			}
			return err
		})
		return err
	})
	if err != nil || address == "" {
		t.Fatalf("no native checkpoint: %v", err)
	}
	if err = json.NewEncoder(os.Stdout).Encode(map[string]string{"address": address}); err != nil {
		t.Fatal(err)
	}
	for {
		time.Sleep(time.Hour)
	} // Keep the old process alive until parent Kill.
}
func TestActualProcessExitAndNativeResume(t *testing.T) {
	dir := t.TempDir()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestCheckpointProcessHelper$")
	cmd.Env = append(os.Environ(), "FOREST_EINO_CHILD="+dir)
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	cmd.Stderr = os.Stderr
	if err = cmd.Start(); err != nil {
		t.Fatal(err)
	}
	defer func() {
		if cmd.ProcessState == nil {
			cmd.Process.Kill()
			cmd.Wait()
		}
	}()
	var ready map[string]string
	line := make(chan []byte, 1)
	go func() {
		s := bufio.NewScanner(stdout)
		if s.Scan() {
			line <- append([]byte(nil), s.Bytes()...)
		} else {
			line <- nil
		}
	}()
	select {
	case payload := <-line:
		if json.Unmarshal(payload, &ready) != nil || ready["address"] == "" {
			t.Fatal("child failed to publish native interrupt")
		}
	case <-ctx.Done():
		t.Fatal("child readiness deadline")
	}
	if err = cmd.Process.Kill(); err != nil {
		t.Fatal(err)
	}
	_ = cmd.Wait()
	f := forest.New(forest.Options{})
	defer f.Close(context.Background())
	owner, _ := ef.NewOwner(f, 4)
	d, err := nativeDefinition(ef.Config{TypeID: "native", Version: "1", Namespace: "persisted", Owner: owner}, dir, nativeTool{interrupt: true, label: "new"})
	if err != nil {
		t.Fatal(err)
	}
	mount(t, f, d)
	var answer string
	err = invokeADK(f, func(ctx context.Context, a ef.ADK) error {
		_, err := a.Resume(ctx, &adk.ResumeParams{Targets: map[string]any{ready["address"]: "restored"}}, ef.RunOptions{CheckpointID: "durable"}, func(e *adk.AgentEvent) error {
			m, err := ef.ReadMessage(e)
			if m != nil && m.Content != "" {
				answer = m.Content
			}
			return err
		})
		return err
	})
	if err != nil || answer != "answer:restored" {
		t.Fatalf("native resume=%s err=%v", answer, err)
	}
}
