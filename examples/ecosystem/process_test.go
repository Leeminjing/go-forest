package ecosystem_test

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"testing"
	"time"

	"github.com/Leeminjing/go-forest/examples/ecosystem"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

func TestProcessHelper(t *testing.T) {
	mode := os.Getenv("FOREST_CHILD_CASE")
	if mode == "" {
		t.Skip("child only")
	}
	var o ecosystem.Options
	if err := json.Unmarshal([]byte(os.Getenv("FOREST_CHILD_OPTIONS")), &o); err != nil {
		t.Fatal(err)
	}
	barrier := func(context.Context) error { fmt.Println("FOREST_BARRIER"); select {} }
	switch mode {
	case "publish":
		o.AfterPublish = barrier
	case "receive":
		o.AfterReceive = barrier
	case "agent":
		o.AfterAgentCreate = barrier
	default:
		t.Fatal("invalid child mode")
	}
	r, err := ecosystem.New(context.Background(), o)
	if err != nil {
		t.Fatal(err)
	}
	if mode != "publish" {
		if err = r.Enable(context.Background()); err != nil {
			t.Fatal(err)
		}
	}
	if _, err = request(r.HTTPURL+"/tasks", "POST", `{"id":"crash","input":"recover this task"}`); err != nil {
		t.Fatal(err)
	}
	for {
		if err = r.Tick(context.Background()); err != nil {
			t.Fatal(err)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

func TestRealProcessCrashRecoveryAndInboxDedup(t *testing.T) {
	for _, mode := range []string{"publish", "receive", "agent"} {
		t.Run(mode, func(t *testing.T) {
			o := options(t)
			payload, err := json.Marshal(o)
			if err != nil {
				t.Fatal(err)
			}
			c, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()
			cmd := exec.CommandContext(c, os.Args[0], "-test.run=^TestProcessHelper$", "-test.v")
			cmd.Env = append(os.Environ(), "FOREST_CHILD_CASE="+mode, "FOREST_CHILD_OPTIONS="+string(payload), "GIN_MODE=release")
			stdout, err := cmd.StdoutPipe()
			if err != nil {
				t.Fatal(err)
			}
			cmd.Stderr = os.Stderr
			if err = cmd.Start(); err != nil {
				t.Fatal(err)
			}
			barrier := make(chan bool, 1)
			go func() {
				scanner := bufio.NewScanner(stdout)
				for scanner.Scan() {
					if scanner.Text() == "FOREST_BARRIER" {
						barrier <- true
						return
					}
				}
				barrier <- false
			}()
			select {
			case ready := <-barrier:
				if !ready {
					cmd.Wait()
					t.Fatal("child exited before fault point")
				}
			case <-c.Done():
				cmd.Process.Kill()
				cmd.Wait()
				t.Fatal("no crash barrier")
			}
			if err = cmd.Process.Kill(); err != nil {
				t.Fatal(err)
			}
			_ = cmd.Wait()
			// Ensure republishing is outside JetStream's one-second dedup window.
			if mode == "publish" {
				time.Sleep(1100 * time.Millisecond)
			}
			r := runtime(t, o)
			if err = r.Start(context.Background()); err != nil {
				t.Fatal(err)
			}
			deadline := time.Now().Add(20 * time.Second)
			for {
				task, err := request(r.HTTPURL+"/tasks/crash", "GET", "")
				if err != nil {
					t.Fatal(err)
				}
				if task.State == "completed" {
					if task.Result != "model v1: 5" {
						t.Fatal(task)
					}
					break
				}
				if time.Now().After(deadline) {
					t.Fatalf("recovery failed: %+v driver errors=%d", task, r.DriverErrors())
				}
				time.Sleep(20 * time.Millisecond)
			}
			// A fresh duplicate after success cannot repeat the database effect.
			nc, err := nats.Connect(o.NATSURL)
			if err != nil {
				t.Fatal(err)
			}
			defer nc.Close()
			js, _ := jetstream.New(nc)
			if _, err = js.Publish(context.Background(), o.Namespace, []byte(`{"id":"crash","input":"recover this task"}`), jetstream.WithMsgID("distinct-transport-id")); err != nil {
				t.Fatal(err)
			}
			native, _ := js.Consumer(context.Background(), o.Namespace, "worker")
			for {
				info, err := native.Info(context.Background())
				if err != nil {
					t.Fatal(err)
				}
				if info.NumAckPending == 0 && info.NumPending == 0 {
					break
				}
				if time.Now().After(deadline) {
					t.Fatal("duplicate did not drain")
				}
				time.Sleep(20 * time.Millisecond)
			}
			db, err := gorm.Open(postgres.Open(o.DSN), &gorm.Config{Logger: logger.Default.LogMode(logger.Silent)})
			if err != nil {
				t.Fatal(err)
			}
			pool, _ := db.DB()
			defer pool.Close()
			var count int64
			if err = db.Table("forest_ecosystem_inbox").Where("namespace = ? AND id = ?", o.Namespace, "crash").Count(&count).Error; err != nil || count != 1 {
				t.Fatalf("inbox effect duplicated: %d %v", count, err)
			}
		})
	}
}
