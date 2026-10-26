package crossdemo_test

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	ff "github.com/Leeminjing/go-forest/adapters/fx"
	demo "github.com/Leeminjing/go-forest/examples/cross-adapter"
	"github.com/Leeminjing/go-forest/examples/cross-adapter/protocol"
	"github.com/jackc/pgx/v5"
)

type childConfig struct {
	Address, DSN, Queue, Directory, Version string
	Files                                   demo.WorkerFiles
}

func TestWorkerSubprocess(t *testing.T) {
	path := os.Getenv("FOREST_CROSS_CHILD")
	if path == "" {
		t.Skip("subprocess entry point")
	}
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var cfg childConfig
	if err = json.Unmarshal(data, &cfg); err != nil {
		t.Fatal(err)
	}
	if err = demo.RunWorker(context.Background(), demo.Options{Address: cfg.Address, DSN: cfg.DSN, TaskQueue: cfg.Queue, Directory: cfg.Directory, Version: cfg.Version}, cfg.Files); err != nil {
		t.Fatal(err)
	}
}

type process struct {
	cmd     *exec.Cmd
	done    chan struct{}
	err     error
	logPath string
}

func startWorker(t *testing.T, ctx context.Context, o demo.Options, version, commit string) (*process, demo.WorkerFiles, demo.WorkerReport) {
	t.Helper()
	directory := t.TempDir()
	files := demo.WorkerFiles{Ready: filepath.Join(directory, "ready.json"), Stop: filepath.Join(directory, "stop"), Commit: commit}
	data, err := json.Marshal(childConfig{Address: o.Address, DSN: o.DSN, Queue: o.TaskQueue, Directory: directory, Version: version, Files: files})
	if err != nil {
		t.Fatal(err)
	}
	config := filepath.Join(directory, "configuration.json")
	if err = os.WriteFile(config, data, 0600); err != nil {
		t.Fatal(err)
	}
	cmd := exec.Command(os.Args[0], "-test.run=^TestWorkerSubprocess$", "-test.timeout=120s")
	cmd.Env = append(os.Environ(), "FOREST_CROSS_CHILD="+config)
	log, err := os.Create(filepath.Join(directory, "worker.log"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { log.Close() })
	cmd.Stdout = log
	cmd.Stderr = log
	if err = cmd.Start(); err != nil {
		t.Fatal(err)
	}
	p := &process{cmd: cmd, done: make(chan struct{}), logPath: log.Name()}
	go func() { p.err = cmd.Wait(); close(p.done) }()
	t.Cleanup(func() {
		select {
		case <-p.done:
		default:
			cmd.Process.Kill()
			<-p.done
		}
	})
	eventually(t, ctx, func() bool {
		select {
		case <-p.done:
			t.Fatalf("worker exited: %v; inspect %s", p.err, log.Name())
		default:
		}
		_, e := os.Stat(files.Ready)
		return e == nil
	})
	var report demo.WorkerReport
	readJSON(t, files.Ready, &report)
	return p, files, report
}
func readJSON(t *testing.T, path string, to any) {
	t.Helper()
	data, e := os.ReadFile(path)
	if e == nil {
		e = json.Unmarshal(data, to)
	}
	if e != nil {
		t.Fatal(e)
	}
}

func TestKillAfterCommitRecoversOriginalWorkflowWithoutRepeatingEffect(t *testing.T) {
	ctx, o := config(t)
	if e := demo.Setup(ctx, o.DSN); e != nil {
		t.Fatal(e)
	}
	// The control forest has the same queue but never enables its own worker.
	control, e := demo.New(ctx, o)
	if e != nil {
		t.Fatal(e)
	}
	defer control.Close(ctx)
	if e = ff.Enable(ctx, control.Forest, "app/fx"); e != nil {
		t.Fatal(e)
	}
	marker := filepath.Join(t.TempDir(), "committed.json")
	first, firstFiles, a := startWorker(t, ctx, o, "v1", marker)
	id, e := control.Start(ctx, o.TaskQueue, input(o), false)
	if e != nil {
		t.Fatal(e)
	}
	eventually(t, ctx, func() bool { _, e := os.Stat(marker); return e == nil })
	var committed protocol.Receipt
	readJSON(t, marker, &committed)
	obs, e := control.Inspect(ctx, input(o))
	if e != nil || !obs.Found || obs.EffectCount != 1 || obs.Receipt != committed {
		t.Fatal("commit not confirmed", obs, e)
	}
	if _, e = os.Stat(marker + ".returned"); !errors.Is(e, os.ErrNotExist) {
		t.Fatal("Activity returned before kill", e)
	}
	if e = first.cmd.Process.Kill(); e != nil {
		t.Fatal(e)
	}
	wait(t, ctx, first.done)
	if first.err == nil {
		t.Fatal("worker was not forcibly terminated")
	}
	if _, e = os.Stat(firstFiles.Ready + ".stopped"); !errors.Is(e, os.ErrNotExist) {
		t.Fatal("killed worker fabricated graceful shutdown")
	}
	crashLog, e := os.ReadFile(first.logPath)
	if e != nil {
		t.Fatal(e)
	}
	if strings.Contains(string(crashLog), "WARNING: DATA RACE") {
		t.Fatal("killed worker reported a data race")
	}
	admin, e := pgx.Connect(ctx, o.DSN)
	if e != nil {
		t.Fatal(e)
	}
	defer admin.Close(ctx)
	eventually(t, ctx, func() bool {
		var count int
		e := admin.QueryRow(ctx, "SELECT count(*) FROM pg_stat_activity WHERE application_name = $1", a.Process).Scan(&count)
		if e != nil {
			t.Fatal(e)
		}
		return count == 0
	})
	second, files, b := startWorker(t, ctx, o, "v2", "")
	out, e := control.Result(ctx, id)
	if e != nil {
		t.Fatal(e)
	}
	if !out.Reused || out.Attempt < 2 || out.VersionBefore != "v2" || out.VersionAfter != "v2" || out.Receipt != committed {
		t.Fatalf("recovery=%+v", out)
	}
	if a.Process == b.Process || a.WorkerIdentity == b.WorkerIdentity || a.FxIdentity == b.FxIdentity || a.Generation != 1 || b.Generation != 1 {
		t.Fatal("physical/global generation identity confused", a, b)
	}
	if out.Receipt.EffectProcess != a.Process {
		t.Fatal("receipt origin overwritten")
	}
	checkGeneration(t, out, 1)
	obs, e = control.Inspect(ctx, input(o))
	if e != nil || obs.EffectCount != 1 || obs.Receipt != committed {
		t.Fatal(obs, e)
	}
	if e = control.Replay(ctx, id); e != nil {
		t.Fatal(e)
	}
	again, e := control.Inspect(ctx, input(o))
	if e != nil || again != obs {
		t.Fatal("history replay changed effects", again, e)
	}
	if e = os.WriteFile(files.Stop, []byte("stop"), 0600); e != nil {
		t.Fatal(e)
	}
	wait(t, ctx, second.done)
	if second.err != nil {
		t.Fatal("recovered worker did not stop cleanly", second.err)
	}
	var stopped demo.WorkerReport
	readJSON(t, files.Ready+".stopped", &stopped)
	if !stopped.Stopped || stopped.Resources.FileOpen || stopped.Resources.PoolResponding || stopped.Resources.FileCloses != 1 {
		t.Fatal("recovery did not drain resources", stopped)
	}
}
