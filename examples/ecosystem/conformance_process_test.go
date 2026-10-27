package ecosystem

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

	tk "github.com/Leeminjing/go-forest/testkit"
)

func TestConformanceKilledProcessRedeliveryKeepsOriginalSQLEffect(t *testing.T) {
	ctx := confContext(t)
	o := confOptions(t)
	if err := confSetup(ctx, o); err != nil {
		t.Fatal(err)
	}
	admin, err := confOpen(ctx, o)
	if admin != nil {
		defer admin.close(t)
	}
	if err != nil {
		t.Fatal(err)
	}
	a := confStartChild(t, ctx, o, "hold")
	first := confReadReport(t, ctx, a.config.Barrier)
	published := confReadReport(t, ctx, a.config.Ready+".published")
	if first.Observation.Message.StreamSequence != published.Sequence || first.Observation.Message.Delivery == 0 {
		t.Fatal(first, published)
	}
	receipt, effectCount, inboxCount := confReadDatabase(t, ctx, o, "crash")
	if receipt != first.Observation.Receipt || effectCount != 1 || inboxCount != 1 {
		t.Fatal(receipt, effectCount, inboxCount, first)
	}
	broker, err := admin.native.Info(ctx)
	if err != nil || broker.NumAckPending < 1 || broker.AckFloor.Stream >= published.Sequence {
		t.Fatal("Ack occurred before forced interruption", broker, err)
	}
	if err = a.cmd.Process.Kill(); err != nil {
		t.Fatal(err)
	}
	if err = a.cmd.Wait(); err == nil {
		t.Fatal("first process was not killed")
	}
	if _, err = os.Stat(a.config.Ready + ".stopped"); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("killed process claimed graceful exit", err)
	}
	b := confStartChild(t, ctx, o, "resume")
	delivery := confReadReport(t, ctx, b.config.Result)
	if delivery.Observation.Message.StreamSequence != first.Observation.Message.StreamSequence || delivery.Observation.Message.ID != first.Observation.Message.ID || delivery.Observation.Message.Delivery <= first.Observation.Message.Delivery {
		t.Fatal("fresh publish substituted for native redelivery", first, delivery)
	}
	if err = tk.CheckIdentity(tk.IdentityEvidence{Label: "real NATS process exit and original-message redelivery", Original: first.Observation.Execution, Delivery: delivery.Observation.Execution, OriginalEffect: receipt.effect(), DeliveredEffect: delivery.Observation.Receipt.effect(), Changed: []tk.IdentityField{tk.Call, tk.Attempt, tk.Process, tk.Instance}}); err != nil {
		t.Fatal(err)
	}
	actual := tk.IdentityEvidence{Original: first.Observation.Execution, Delivery: delivery.Observation.Execution, OriginalEffect: receipt.effect(), DeliveredEffect: delivery.Observation.Receipt.effect(), Changed: []tk.IdentityField{tk.Call, tk.Attempt, tk.Process, tk.Instance}}
	for _, field := range []string{"origin", "attempt", "task"} {
		bad := actual
		switch field {
		case "origin":
			bad.DeliveredEffect.OriginProcess = bad.Delivery.Process
		case "attempt":
			bad.Delivery.Attempt = bad.Original.Attempt
		case "task":
			bad.Delivery.Task = first.Observation.Message.ID // transport ID is not the stream-incarnation/sequence task
		}
		if err = tk.CheckIdentity(bad); !errors.Is(err, tk.ErrViolation) {
			t.Fatal("corrupted native identity accepted", field, err)
		}
	}
	receiptAfter, n, rows := confReadDatabase(t, ctx, o, "crash")
	if receiptAfter != receipt || n != 1 || rows != 1 {
		t.Fatal("SQL effect repeated", receiptAfter, n, rows)
	}
	settled := confReadReport(t, ctx, b.config.Ready+".settled")
	if settled.Task.Result != "model v1: 5" {
		t.Fatal(settled)
	}
	info, err := admin.native.Info(ctx)
	if err != nil || info.NumAckPending != 0 || info.AckFloor.Stream < published.Sequence {
		t.Fatal("native Ack not confirmed", info, err)
	}
	if err = os.WriteFile(b.config.Stop, []byte("stop"), 0600); err != nil {
		t.Fatal(err)
	}
	stopped := confReadReport(t, ctx, b.config.Ready+".stopped")
	if err = b.cmd.Wait(); err != nil {
		t.Fatal(err)
	}
	for _, r := range stopped.Resources {
		if r.Ownership == tk.Owned && (r.Usable || r.CloseCount != 1) {
			t.Fatal(r)
		}
		if r.Ownership == tk.Borrowed && (!r.Usable || r.CloseCount != 0) {
			t.Fatal(r)
		}
	}
}

type confChildConfig struct {
	Options                            Options
	Ready, Barrier, Result, Stop, Mode string
}
type confChildReport struct {
	Observation confObservation
	Sequence    uint64
	Resources   []tk.Resource
	Task        Task
}

func TestConformanceWorkerChild(t *testing.T) {
	path := os.Getenv("FOREST_CONFORMANCE_CHILD")
	if path == "" {
		t.Skip("actual executable role only")
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var cfg confChildConfig
	if err = json.Unmarshal(raw, &cfg); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()
	f, err := confOpen(ctx, cfg.Options)
	if f != nil {
		defer f.close(t)
	}
	if err != nil {
		t.Fatal(err)
	}
	if _, err = f.r.Forest.Apply(ctx, f.bundle.plan()); err != nil {
		t.Fatal(err)
	}
	if err = f.bundle.anchor("v1", "app/proof/handler"); err != nil {
		t.Fatal(err)
	}
	f.hook = func(c context.Context, stage string, call confCall, receipt confReceipt) error {
		if (cfg.Mode == "hold" && stage != "handler.committed") || (cfg.Mode == "resume" && stage != "handler.return") {
			return nil
		}
		if cfg.Mode == "hold" || cfg.Mode == "resume" {
			g, err := f.r.Forest.Generation(c)
			if err != nil {
				return err
			}
			call.Execution.Scope.Generation = g
			frame := confChildReport{Observation: confObservation{Execution: call.Execution, Message: call.Message, Receipt: receipt}}
			target := cfg.Barrier
			if cfg.Mode == "resume" {
				target = cfg.Result
			}
			if err = confWriteReport(target, frame); err != nil {
				return err
			}
			if cfg.Mode == "hold" {
				select {}
			}
		}
		return nil
	}
	f.enable(t)
	if err = confWriteReport(cfg.Ready, confChildReport{}); err != nil {
		t.Fatal(err)
	}
	if cfg.Mode == "hold" {
		f.submit(t, "http", "crash")
		sequence := f.relay(t, "crash")
		if err = confWriteReport(cfg.Ready+".published", confChildReport{Sequence: sequence}); err != nil {
			t.Fatal(err)
		}
	} else {
		report := confReadReport(t, ctx, cfg.Result)
		f.acked(t, report.Observation.Message.StreamSequence)
		task := f.complete(t, "crash")
		if err = confWriteReport(cfg.Ready+".settled", confChildReport{Task: task}); err != nil {
			t.Fatal(err)
		}
	}
	confUntil(t, ctx, func() bool { _, err := os.Stat(cfg.Stop); return err == nil })
	if err = f.quiesce(ctx); err != nil {
		t.Fatal(err)
	}
	if err = f.r.Close(ctx); err != nil {
		t.Fatal(err)
	}
	samples, err := f.bundle.samples(ctx, "v1")
	if err != nil {
		t.Fatal(err)
	}
	if err = confWriteReport(cfg.Ready+".stopped", confChildReport{Resources: samples}); err != nil {
		t.Fatal(err)
	}
}

type confChild struct {
	cmd    *exec.Cmd
	config confChildConfig
}

func confStartChild(t *testing.T, ctx context.Context, o Options, mode string) *confChild {
	t.Helper()
	dir := t.TempDir()
	c := &confChild{config: confChildConfig{Options: o, Mode: mode, Ready: filepath.Join(dir, "ready"), Barrier: filepath.Join(dir, "barrier"), Result: filepath.Join(dir, "result"), Stop: filepath.Join(dir, "stop")}}
	path := filepath.Join(dir, "config.json")
	raw, err := json.Marshal(c.config)
	if err != nil {
		t.Fatal(err)
	}
	if err = os.WriteFile(path, raw, 0600); err != nil {
		t.Fatal(err)
	}
	logPath := filepath.Join(dir, "child.log")
	log, err := os.Create(logPath)
	if err != nil {
		t.Fatal(err)
	}
	c.cmd = exec.CommandContext(ctx, os.Args[0], "-test.run=^TestConformanceWorkerChild$", "-test.timeout=125s", "-test.v")
	confHide(c.cmd)
	c.cmd.Env = append(os.Environ(), "FOREST_CONFORMANCE_CHILD="+path, "GIN_MODE=release")
	c.cmd.Stdout = log
	c.cmd.Stderr = log
	if err = c.cmd.Start(); err != nil {
		log.Close()
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if c.cmd.ProcessState == nil {
			_ = c.cmd.Process.Kill()
			_ = c.cmd.Wait()
		}
		if err := log.Close(); err != nil {
			t.Error(err)
		}
		data, err := os.ReadFile(logPath)
		if err != nil {
			t.Error(err)
			return
		}
		if strings.Contains(string(data), "WARNING: DATA RACE") {
			t.Errorf("child race detector: %s", data)
		}
		if t.Failed() {
			t.Logf("child %s: %s", mode, data)
		}
	})
	confReadReport(t, ctx, c.config.Ready)
	return c
}

func confWriteReport(path string, report confChildReport) error {
	raw, err := json.Marshal(report)
	if err != nil {
		return err
	}
	file, err := os.CreateTemp(filepath.Dir(path), "report-")
	if err != nil {
		return err
	}
	name := file.Name()
	defer os.Remove(name)
	if _, err = file.Write(raw); err != nil {
		file.Close()
		return err
	}
	if err = file.Sync(); err != nil {
		file.Close()
		return err
	}
	if err = file.Close(); err != nil {
		return err
	}
	return os.Rename(name, path)
}
func confReadReport(t *testing.T, ctx context.Context, path string) (out confChildReport) {
	t.Helper()
	confUntil(t, ctx, func() bool {
		raw, err := os.ReadFile(path)
		if errors.Is(err, os.ErrNotExist) || confReportBusy(err) {
			return false
		}
		if err != nil {
			t.Fatal(err)
		}
		if err = json.Unmarshal(raw, &out); err != nil {
			t.Fatal(err)
		}
		return true
	})
	return
}
func confReadDatabase(t *testing.T, ctx context.Context, o Options, id string) (out confReceipt, effects, inboxes int64) {
	t.Helper()
	db, err := openDB(o.DSN)
	if err != nil {
		t.Fatal(err)
	}
	pool, err := db.DB()
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	query := db.WithContext(ctx).Where("namespace = ? AND id = ?", o.Namespace, id)
	if err = query.First(&out).Error; err != nil {
		t.Fatal(err)
	}
	var effect confEffect
	if err = db.WithContext(ctx).Where("namespace = ? AND id = ?", o.Namespace, id).First(&effect).Error; err != nil {
		t.Fatal(err)
	}
	if effect.Content != out.Content || effect.Count != 1 {
		t.Fatal("effect and receipt disagree", effect, out)
	}
	if err = db.WithContext(ctx).Model(&confEffect{}).Where("namespace = ? AND id = ?", o.Namespace, id).Count(&effects).Error; err != nil {
		t.Fatal(err)
	}
	if err = db.WithContext(ctx).Model(&inbox{}).Where("namespace = ? AND id = ?", o.Namespace, id).Count(&inboxes).Error; err != nil {
		t.Fatal(err)
	}
	return
}
