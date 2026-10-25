package gormforest_test

import (
	"context"
	"errors"
	"fmt"
	"os"
	"testing"
	"time"

	dbtree "github.com/Leeminjing/go-forest/adapters/gorm"
	"github.com/Leeminjing/go-forest/forest"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

func database(t *testing.T) *gorm.DB {
	t.Helper()
	if os.Getenv("FOREST_INTEGRATION") != "1" {
		t.Skip("set FOREST_INTEGRATION=1 and FOREST_POSTGRES_DSN for real PostgreSQL")
	}
	dsn := os.Getenv("FOREST_POSTGRES_DSN")
	if dsn == "" {
		t.Fatal("FOREST_POSTGRES_DSN is required")
	}
	db, err := gorm.Open(postgres.Open(dsn), &gorm.Config{Logger: logger.Default.LogMode(logger.Silent)})
	if err != nil {
		t.Fatal(err)
	}
	pool, err := db.DB()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { pool.Close() })
	return db
}

func TestNativeTransactionRollbackAndBorrowedOwnership(t *testing.T) {
	db := database(t)
	ctx := context.Background()
	f := forest.New(forest.Options{})
	def, err := dbtree.Definition(f, dbtree.Options{TypeID: "db", Version: "1", Ownership: dbtree.Borrowed, Open: func(context.Context) (*gorm.DB, error) { return db, nil }})
	if err != nil {
		t.Fatal(err)
	}
	if err = f.Register(def); err != nil {
		t.Fatal(err)
	}
	if _, err = f.Apply(ctx, forest.ChangePlan{Operations: []forest.Operation{forest.AddRoot("db", def.Ref())}}); err != nil {
		t.Fatal(err)
	}
	use := func(fn func(*gorm.DB) error) error {
		return f.Invoke(ctx, func(c context.Context) error {
			return forest.Use[dbtree.Database](c, f, "db", "database", func(d dbtree.Database) error { return d.WithDB(c, fn) })
		})
	}
	sentinel := errors.New("rollback")
	table := fmt.Sprintf("forest_tx_%d", time.Now().UnixNano())
	if err = use(func(d *gorm.DB) error { return d.Exec("CREATE TABLE " + table + " (value integer)").Error }); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Exec("DROP TABLE " + table) })
	transaction := func(ctx context.Context, fn func(*gorm.DB) error) error {
		return f.Invoke(ctx, func(c context.Context) error {
			return forest.Use[dbtree.Database](c, f, "db", "database", func(d dbtree.Database) error { return d.Transaction(c, fn) })
		})
	}
	err = transaction(ctx, func(tx *gorm.DB) error {
		if err := tx.Exec("INSERT INTO " + table + " VALUES (7), (9)").Error; err != nil {
			return err
		}
		return sentinel
	})
	if !errors.Is(err, sentinel) {
		t.Fatalf("rollback: %v", err)
	}
	count := func(want int64) {
		t.Helper()
		var got int64
		if err := use(func(d *gorm.DB) error { return d.Table(table).Count(&got).Error }); err != nil || got != want {
			t.Fatalf("rows=%d want=%d err=%v", got, want, err)
		}
	}
	count(0)
	if err = transaction(ctx, func(tx *gorm.DB) error { return tx.Exec("INSERT INTO " + table + " VALUES (7), (9)").Error }); err != nil {
		t.Fatal(err)
	}
	count(2)
	deadline, stop := context.WithTimeout(ctx, 30*time.Millisecond)
	err = transaction(deadline, func(tx *gorm.DB) error {
		if err := tx.Exec("INSERT INTO " + table + " VALUES (11)").Error; err != nil {
			return err
		}
		return tx.Exec("SELECT pg_sleep(3)").Error
	})
	stop()
	if err == nil {
		t.Fatal("cancellation succeeded unexpectedly")
	}
	count(2)
	c, cancel := context.WithCancel(ctx)
	cancel()
	err = f.Invoke(c, func(context.Context) error { t.Fatal("canceled call entered"); return nil })
	if !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	closeCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	if err = f.Close(closeCtx); err != nil {
		t.Fatal(err)
	}
	pool, _ := db.DB()
	if err = pool.PingContext(ctx); err != nil {
		t.Fatalf("borrowed pool closed: %v", err)
	}
}
