package gormforest_test

import (
	"context"
	"errors"
	"testing"
	"time"

	dbtree "github.com/Leeminjing/go-forest/adapters/gorm"
	"github.com/Leeminjing/go-forest/forest"
	"gorm.io/gorm"
)

func TestOwnedPoolWaitsForPinnedWorkAndRollbackCleans(t *testing.T) {
	db := database(t)
	pool, _ := db.DB()
	ctx := context.Background()
	f := forest.New(forest.Options{DrainTimeout: 20 * time.Millisecond})
	defer f.Close(ctx)
	d, err := dbtree.Definition(f, dbtree.Options{TypeID: "owned", Version: "1", Ownership: dbtree.Owned, Open: func(context.Context) (*gorm.DB, error) { return db, nil }})
	if err != nil {
		t.Fatal(err)
	}
	if err = f.Register(d); err != nil {
		t.Fatal(err)
	}
	if _, err = f.Apply(ctx, forest.ChangePlan{Operations: []forest.Operation{forest.AddRoot("db", d.Ref())}}); err != nil {
		t.Fatal(err)
	}
	entered, release, done := make(chan struct{}), make(chan struct{}), make(chan error, 1)
	go func() {
		done <- f.Invoke(ctx, func(c context.Context) error {
			return forest.Use[dbtree.Database](c, f, "db", "database", func(r dbtree.Database) error {
				return r.WithDB(c, func(d *gorm.DB) error { close(entered); <-release; return d.Exec("SELECT 1").Error })
			})
		})
	}()
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("no call")
	}
	if _, err = f.Apply(ctx, forest.ChangePlan{BaseGeneration: 1, Operations: []forest.Operation{forest.Remove("db")}}); err != nil {
		t.Fatal(err)
	}
	if err = pool.PingContext(ctx); err != nil {
		t.Fatalf("closed active pool: %v", err)
	}
	close(release)
	if err = <-done; err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(time.Second)
	for {
		if pool.PingContext(ctx) != nil {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("owned pool not released")
		}
		time.Sleep(time.Millisecond)
	}
	failedDB := database(t)
	failedPool, _ := failedDB.DB()
	fail, err := dbtree.Definition(f, dbtree.Options{TypeID: "failed", Version: "1", Ownership: dbtree.Owned, Open: func(context.Context) (*gorm.DB, error) { return failedDB, errors.New("open failed") }})
	if err != nil {
		t.Fatal(err)
	}
	if err = f.Register(fail); err != nil {
		t.Fatal(err)
	}
	if _, err = f.Apply(ctx, forest.ChangePlan{BaseGeneration: 2, Operations: []forest.Operation{forest.AddRoot("failed", fail.Ref())}}); !errors.Is(err, forest.ErrPrepareFailed) {
		t.Fatal(err)
	}
	if f.Snapshot().Generation != 2 {
		t.Fatal("failed change advanced version")
	}
	deadline = time.Now().Add(time.Second)
	for {
		if failedPool.PingContext(ctx) != nil {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("rollback leaked owned pool")
		}
		time.Sleep(time.Millisecond)
	}
}
