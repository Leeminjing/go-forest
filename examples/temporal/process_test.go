package temporaldemo_test

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"sync"
	"testing"
	"time"

	demo "github.com/Leeminjing/go-forest/examples/temporal"
	"github.com/Leeminjing/go-forest/examples/temporal/flow"
	"go.temporal.io/sdk/client"
)

func TestChildWorker(t *testing.T) {
	if os.Getenv("FOREST_TEMPORAL_CHILD") != "1" {
		t.Skip("child entry")
	}
	o := demo.Options{Address: os.Getenv("FOREST_TEMPORAL_ADDRESS"), DSN: os.Getenv("FOREST_POSTGRES_DSN"), Directory: os.Getenv("FOREST_TEMPORAL_CHILD_DIR"), TaskQueue: os.Getenv("FOREST_TEMPORAL_CHILD_QUEUE")}
	if marker := os.Getenv("FOREST_TEMPORAL_CRASH_MARKER"); marker != "" {
		o.AfterCommit = func(context.Context, flow.Receipt) error {
			if err := os.WriteFile(marker, []byte("committed"), 0600); err != nil {
				return err
			}
			for {
				time.Sleep(time.Hour)
			}
		}
	}
	r, err := demo.New(context.Background(), o)
	if err != nil {
		t.Fatal(err)
	}
	_ = r
	fmt.Println(`{"ready":true}`)
	for {
		time.Sleep(time.Hour)
	}
}

type process struct {
	cmd  *exec.Cmd
	once sync.Once
}

func (p *process) stop() {
	p.once.Do(func() {
		if p.cmd.Process != nil {
			_ = p.cmd.Process.Kill()
		}
		_ = p.cmd.Wait()
	})
}
func child(t *testing.T, o demo.Options, marker string) *process {
	t.Helper()
	executable, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	cmd := exec.Command(executable, "-test.run=^TestChildWorker$")
	cmd.Env = append(os.Environ(), "FOREST_TEMPORAL_CHILD=1", "FOREST_TEMPORAL_CHILD_DIR="+o.Directory, "FOREST_TEMPORAL_CHILD_QUEUE="+o.TaskQueue, "FOREST_TEMPORAL_CRASH_MARKER="+marker)
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	log, err := os.Create(filepath.Join(t.TempDir(), "worker.log"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { log.Close() })
	cmd.Stderr = log
	if err = cmd.Start(); err != nil {
		t.Fatal(err)
	}
	p := &process{cmd: cmd}
	t.Cleanup(p.stop)
	ready := make(chan bool, 1)
	go func() {
		scanner := bufio.NewScanner(stdout)
		for scanner.Scan() {
			var value struct{ Ready bool }
			if json.Unmarshal(scanner.Bytes(), &value) == nil && value.Ready {
				ready <- true
				return
			}
		}
		ready <- false
	}()
	select {
	case ok := <-ready:
		if !ok {
			t.Fatal("worker exited before readiness; see worker.log")
		}
	case <-time.After(15 * time.Second):
		t.Fatal("worker readiness timeout")
	}
	return p
}
func TestWorkerKillAfterCommitReconcilesSameWorkflow(t *testing.T) {
	o := config(t)
	ctx, cancel := context.WithTimeout(context.Background(), 40*time.Second)
	defer cancel()
	if err := demo.Setup(ctx, o.DSN); err != nil {
		t.Fatal(err)
	}
	c, err := client.DialContext(ctx, client.Options{HostPort: o.Address})
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	marker := filepath.Join(t.TempDir(), "committed")
	first := child(t, o, marker)
	run, err := c.ExecuteWorkflow(ctx, client.StartWorkflowOptions{ID: o.TaskQueue, TaskQueue: o.TaskQueue}, flow.Job, flow.Input{ID: o.TaskQueue, Text: "birch"})
	if err != nil {
		t.Fatal(err)
	}
	for {
		if _, err := os.Stat(marker); err == nil {
			break
		}
		select {
		case <-time.After(10 * time.Millisecond):
		case <-ctx.Done():
			t.Fatal("commit marker missing")
		}
	}
	first.stop() // Kill AND Wait before taking over the same checkpoint directory.
	child(t, o, "")
	var result flow.Result
	if err = run.Get(ctx, &result); err != nil {
		t.Fatal(err)
	}
	if result.Receipt.Changes != 1 || result.Receipt.Plan != "plan:birch" {
		t.Fatalf("duplicate/changed commit: %+v", result)
	}
	iterator := c.GetWorkflowHistory(ctx, run.GetID(), run.GetRunID(), false, 0)
	completedPlan, completedApply := 0, 0
	for iterator.HasNext() {
		event, err := iterator.Next()
		if err != nil {
			t.Fatal(err)
		}
		if event.GetActivityTaskCompletedEventAttributes() != nil {
			completedApply++
		}
		if scheduled := event.GetActivityTaskScheduledEventAttributes(); scheduled != nil && scheduled.ActivityType.Name == "job.plan" {
			completedPlan++
		}
	}
	if completedPlan != 1 || completedApply != 3 {
		t.Fatalf("history re-created steps: plan=%d completed=%d", completedPlan, completedApply)
	}
}
func server(t *testing.T, address, database string) *process {
	t.Helper()
	binary := os.Getenv("FOREST_TEMPORAL_BINARY")
	if binary == "" {
		if os.Getenv("FOREST_INTEGRATION") == "1" {
			t.Fatal("FOREST_TEMPORAL_BINARY required for actual server restart")
		}
		t.Skip("actual server restart")
	}
	_, port, err := net.SplitHostPort(address)
	if err != nil {
		t.Fatal(err)
	}
	cmd := exec.Command(binary, "server", "start-dev", "--ip", "127.0.0.1", "--port", port, "--headless", "--db-filename", database)
	log, err := os.OpenFile(database+".log", os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0600)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { log.Close() })
	cmd.Stdout = log
	cmd.Stderr = log
	if err = cmd.Start(); err != nil {
		t.Fatal(err)
	}
	p := &process{cmd: cmd}
	t.Cleanup(p.stop)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	for {
		c, err := client.DialContext(ctx, client.Options{HostPort: address})
		if err == nil {
			_, err = c.CheckHealth(ctx, &client.CheckHealthRequest{})
			c.Close()
			if err == nil {
				return p
			}
		}
		select {
		case <-time.After(100 * time.Millisecond):
		case <-ctx.Done():
			t.Fatal("owned server readiness timeout")
		}
	}
}
func TestActualPersistentServerRestartKeepsHistory(t *testing.T) {
	o := config(t)
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	o.Address = listener.Addr().String()
	listener.Close()
	database := filepath.Join(t.TempDir(), "server.sqlite")
	first := server(t, o.Address, database)
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()
	if err = demo.Setup(ctx, o.DSN); err != nil {
		t.Fatal(err)
	}
	r, err := demo.New(ctx, o)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close(ctx)
	id, err := r.StartRun(ctx, flow.Input{ID: o.TaskQueue, Text: "elm", Wait: true})
	if err != nil {
		t.Fatal(err)
	}
	for {
		state, err := r.Query(ctx, id)
		if err != nil {
			t.Fatal(err)
		}
		if state.Stage == "waiting" {
			break
		}
		select {
		case <-time.After(10 * time.Millisecond):
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		}
	}
	first.stop()
	short, end := context.WithTimeout(ctx, 150*time.Millisecond)
	_, err = r.Query(short, id)
	end()
	if err == nil {
		t.Fatal("disconnected query falsely succeeded")
	}
	server(t, o.Address, database)
	if err = r.Signal(ctx, id); err != nil {
		t.Fatal(err)
	}
	result, err := r.Result(ctx, id)
	if err != nil {
		t.Fatal(err)
	}
	if result.Receipt.ID != id.WorkflowID || result.Receipt.Changes != 1 {
		t.Fatalf("identity/history lost: %+v", result)
	}
	if err = r.Replay(ctx, id); err != nil {
		t.Fatal(err)
	}
}
