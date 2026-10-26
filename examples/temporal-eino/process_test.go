package einodemo_test

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	demo "github.com/Leeminjing/go-forest/examples/temporal-eino"
	"github.com/Leeminjing/go-forest/examples/temporal-eino/protocol"
	"go.temporal.io/sdk/temporal"
)

type childConfig struct {
	Options demo.Options
	Files   demo.WorkerFiles
}

func TestWorkerChild(t *testing.T) {
	path := os.Getenv("FOREST_TEMPORAL_EINO_CHILD")
	if path == "" {
		t.Skip("executable role only")
	}
	data, e := os.ReadFile(path)
	if e != nil {
		t.Fatal(e)
	}
	var c childConfig
	if e = json.Unmarshal(data, &c); e != nil {
		t.Fatal(e)
	}
	if e = demo.RunWorker(context.Background(), c.Options, c.Files); e != nil {
		t.Fatal(e)
	}
}

type child struct {
	cmd   *exec.Cmd
	log   *os.File
	ready string
}

func startChild(t *testing.T, ctx context.Context, o demo.Options, mode, label string) *child {
	t.Helper()
	dir := t.TempDir()
	files := demo.WorkerFiles{Ready: filepath.Join(dir, "ready.json"), Stop: filepath.Join(dir, "stop"), Barrier: filepath.Join(dir, "barrier.json"), Mode: mode}
	raw, e := json.Marshal(childConfig{Options: o, Files: files})
	if e != nil {
		t.Fatal(e)
	}
	path := filepath.Join(dir, "configuration.json")
	if e = os.WriteFile(path, raw, 0600); e != nil {
		t.Fatal(e)
	}
	log, e := os.Create(filepath.Join(dir, label+".log"))
	if e != nil {
		t.Fatal(e)
	}
	cmd := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestWorkerChild$")
	cmd.Env = append(os.Environ(), "FOREST_TEMPORAL_EINO_CHILD="+path)
	cmd.Stdout = log
	cmd.Stderr = log
	if e = cmd.Start(); e != nil {
		t.Fatal(e)
	}
	c := &child{cmd: cmd, log: log, ready: files.Ready}
	t.Cleanup(func() {
		if cmd.ProcessState == nil {
			_ = cmd.Process.Kill()
			_ = cmd.Wait()
		}
		_ = log.Close()
		data, _ := os.ReadFile(log.Name())
		if strings.Contains(string(data), "WARNING: DATA RACE") {
			t.Error("child race", string(data))
		}
	})
	readReport(t, ctx, files.Ready)
	return c
}
func readReport(t *testing.T, ctx context.Context, path string) demo.WorkerReport {
	t.Helper()
	var r demo.WorkerReport
	until(t, ctx, func() bool { data, e := os.ReadFile(path); return e == nil && json.Unmarshal(data, &r) == nil })
	return r
}
func TestKilledWorkerRestoresNativeCheckpointAndOriginalWorkflow(t *testing.T) {
	for _, mode := range []string{"paused", "completed", "unknown"} {
		t.Run(mode, func(t *testing.T) {
			ctx, o := config(t)
			controllerOptions := o
			controllerOptions.Directory = t.TempDir()
			control, e := demo.New(ctx, controllerOptions)
			if e != nil {
				t.Fatal(e)
			}
			defer control.Close(ctx)
			a := startChild(t, ctx, o, mode, "A")
			aReady := readReport(t, ctx, a.ready)
			id, e := control.Start(ctx, o.Queue, input(o))
			if e != nil {
				t.Fatal(e)
			}
			if mode == "completed" {
				p := paused(t, ctx, control, id).Pause
				if e = control.Decide(ctx, id, protocol.Decision{ID: "go", ExecutionID: o.Queue, Token: p.Token, Target: p.Targets[0], Value: "continue"}); e != nil {
					t.Fatal(e)
				}
			}
			barrier := readReport(t, ctx, filepath.Join(filepath.Dir(a.ready), "barrier.json"))
			if barrier.Evidence.Artifacts != 1 {
				t.Fatal(barrier)
			}
			if e = a.cmd.Process.Kill(); e != nil {
				t.Fatal(e)
			}
			if e = a.cmd.Wait(); e == nil {
				t.Fatal("worker was not killed")
			}
			if _, e = os.Stat(a.ready + ".stopped"); !errors.Is(e, os.ErrNotExist) {
				t.Fatal("killed worker claimed graceful stop", e)
			}
			if e = demo.HandoverAfterExit(o.Directory, aReady.Process); e != nil {
				t.Fatal(e)
			}
			o.Version = "v2"
			b := startChild(t, ctx, o, "", "B")
			bReady := readReport(t, ctx, b.ready)
			if bReady.Process == aReady.Process || bReady.Worker == aReady.Worker || bReady.Agent == aReady.Agent {
				t.Fatal("identities not renewed", aReady, bReady)
			}
			if mode == "paused" {
				s := paused(t, ctx, control, id)
				if !s.Last.Reused {
					t.Fatal("pause step not redelivered", s)
				}
				if e = control.Decide(ctx, id, protocol.Decision{ID: "go", ExecutionID: o.Queue, Token: s.Pause.Token, Target: s.Pause.Targets[0], Value: "continue"}); e != nil {
					t.Fatal(e)
				}
			}
			out, e := control.Result(ctx, id)
			if mode == "unknown" {
				var app *temporal.ApplicationError
				if !errors.As(e, &app) || app.Type() != "OutcomeUnknown" {
					t.Fatal("unsafe crash retried", out, e)
				}
			} else if e != nil || out.State != "completed" || out.Receipt != barrier.Evidence.Receipt || out.Text != "finished:v1:oak" {
				t.Fatal(out, e)
			}
			if e = control.Replay(ctx, id); e != nil {
				t.Fatal(e)
			}
			if e = os.WriteFile(filepath.Join(filepath.Dir(b.ready), "stop"), []byte("stop"), 0600); e != nil {
				t.Fatal(e)
			}
			if e = b.cmd.Wait(); e != nil {
				t.Fatal(e)
			}
			stopped := readReport(t, ctx, b.ready+".stopped")
			if stopped.Resources.Claim || stopped.Resources.NativeOpen || stopped.Resources.BusinessOpen || stopped.Resources.NativeCloses != 1 || stopped.Resources.BusinessCloses != 1 {
				t.Fatal("recovered worker did not release", stopped)
			}
			if stopped.Counts.Apply != 0 {
				t.Fatal("completed tool reentered", stopped.Counts)
			}
			if mode == "paused" && stopped.Counts.Resume != 1 {
				t.Fatal("native resume did not occur", stopped.Counts)
			}
			if mode != "paused" && (stopped.Counts.Model != 0 || stopped.Counts.Resume != 0 || stopped.Counts.Query != 0) {
				t.Fatal("native engine reran sealed or unknown step", stopped.Counts)
			}
			audit, e := demo.Audit(o.Directory, input(o))
			if e != nil || audit.Artifacts != 1 || audit.Receipt != barrier.Evidence.Receipt {
				t.Fatal(audit, e)
			}
			if mode == "unknown" && audit.Records[0].State != "unknown" {
				t.Fatal("unknown not persisted", audit)
			}
		})
	}
}

var _ = time.Second
