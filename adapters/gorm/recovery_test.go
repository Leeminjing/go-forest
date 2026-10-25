package gormforest_test

import (
	"context"
	"os"
	"os/exec"
	"testing"
	"time"

	dbtree "github.com/Leeminjing/go-forest/adapters/gorm"
	"github.com/Leeminjing/go-forest/forest"
	"gorm.io/gorm"
)

func TestDatabaseRestartKeepsPoolAndForestIdentity(t *testing.T) {
	container := os.Getenv("FOREST_POSTGRES_RESTART_CONTAINER")
	if container == "" {
		t.Skip("requires an explicitly owned restartable database container")
	}
	db := database(t)
	f := forest.New(forest.Options{})
	defer f.Close(context.Background())
	ctx := context.Background()
	d, err := dbtree.Definition(f, dbtree.Options{TypeID: "db", Version: "1", Ownership: dbtree.Borrowed, Open: func(context.Context) (*gorm.DB, error) { return db, nil }})
	if err != nil {
		t.Fatal(err)
	}
	if err = f.Register(d); err != nil {
		t.Fatal(err)
	}
	if _, err = f.Apply(ctx, forest.ChangePlan{Operations: []forest.Operation{forest.AddRoot("db", d.Ref())}}); err != nil {
		t.Fatal(err)
	}
	commandCtx, cancel := context.WithTimeout(ctx, 20*time.Second)
	defer cancel()
	if output, err := exec.CommandContext(commandCtx, "docker", "restart", container).CombinedOutput(); err != nil {
		t.Fatalf("restart failed: %v %s", err, output)
	}
	deadline := time.Now().Add(10 * time.Second)
	for {
		err = f.Invoke(commandCtx, func(c context.Context) error {
			return forest.Use[dbtree.Database](c, f, "db", "database", func(r dbtree.Database) error {
				return r.WithDB(c, func(d *gorm.DB) error { return d.Exec("SELECT 1").Error })
			})
		})
		if err == nil {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("database pool did not reconnect")
		}
		time.Sleep(20 * time.Millisecond)
	}
	if f.Snapshot().Generation != 1 {
		t.Fatal("reconnect rebuilt forest")
	}
}
