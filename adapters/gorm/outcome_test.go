package gormforest_test

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"sync"
	"sync/atomic"
	"testing"

	dbtree "github.com/Leeminjing/go-forest/adapters/gorm"
	"github.com/Leeminjing/go-forest/forest"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
)

var registerDriver sync.Once
var commits atomic.Int32
var lostAck = errors.New("lost commit acknowledgment")

type outcomeDriver struct{}
type outcomeConn struct{}
type outcomeTx struct{}

func (outcomeDriver) Open(string) (driver.Conn, error) { return outcomeConn{}, nil }
func (outcomeConn) Prepare(string) (driver.Stmt, error) {
	return nil, errors.New("no statements in outcome classifier test")
}
func (outcomeConn) Close() error               { return nil }
func (outcomeConn) Begin() (driver.Tx, error)  { return outcomeTx{}, nil }
func (outcomeConn) Ping(context.Context) error { return nil }
func (outcomeTx) Commit() error                { commits.Add(1); return lostAck }
func (outcomeTx) Rollback() error              { return nil }

// This unit fault seam verifies classification only. Real transaction/cancel,
// ownership and recovery acceptance use PostgreSQL, never this driver.
func TestUnknownCommitIsTaggedWithoutRetry(t *testing.T) {
	registerDriver.Do(func() { sql.Register("forest_commit_fault", outcomeDriver{}) })
	commits.Store(0)
	pool, err := sql.Open("forest_commit_fault", "")
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	native, err := gorm.Open(postgres.New(postgres.Config{Conn: pool}), &gorm.Config{DisableAutomaticPing: true})
	if err != nil {
		t.Fatal(err)
	}
	f := forest.New(forest.Options{})
	defer f.Close(context.Background())
	d, err := dbtree.Definition(f, dbtree.Options{TypeID: "db", Version: "1", Ownership: dbtree.Borrowed, Open: func(context.Context) (*gorm.DB, error) { return native, nil }})
	if err != nil {
		t.Fatal(err)
	}
	if err = f.Register(d); err != nil {
		t.Fatal(err)
	}
	if _, err = f.Apply(context.Background(), forest.ChangePlan{Operations: []forest.Operation{forest.AddRoot("db", d.Ref())}}); err != nil {
		t.Fatal(err)
	}
	calls := 0
	err = f.Invoke(context.Background(), func(ctx context.Context) error {
		return forest.Use[dbtree.Database](ctx, f, "db", "database", func(d dbtree.Database) error { return d.Transaction(ctx, func(*gorm.DB) error { calls++; return nil }) })
	})
	if !errors.Is(err, dbtree.ErrCommitUnknown) || !errors.Is(err, lostAck) || calls != 1 || commits.Load() != 1 {
		t.Fatalf("unknown commit was lost or replayed: %v calls=%d commits=%d", err, calls, commits.Load())
	}
}
