// Package gormforest owns or explicitly borrows native GORM connection pools.
package gormforest

import (
	"context"
	"database/sql"
	"errors"
	"sync"
	"time"

	"github.com/Leeminjing/go-forest/forest"
	"gorm.io/gorm"
)

var ErrInvalidOptions = errors.New("invalid GORM tree options")
var ErrUnavailable = errors.New("GORM tree unavailable")
var ErrCommitUnknown = errors.New("database commit outcome unknown; reconcile by business ID")

type Ownership uint8

const (
	Owned Ownership = iota + 1
	Borrowed
)

// Database exposes native GORM in a bounded callback. Do not retain db, Rows,
// transactions, or derived handles after the callback. Finish transactions here.
type Database interface {
	WithDB(context.Context, func(*gorm.DB) error) error
	Transaction(context.Context, func(*gorm.DB) error, ...*sql.TxOptions) error
	Stats() sql.DBStats
}
type Options struct {
	TypeID, Version string
	Ownership       Ownership
	// Open must honor ctx, use a bounded dial timeout, and clean up partially
	// opened resources on error. The returned successful pool follows Ownership.
	// Disable GORM's automatic Ping and use the bounded Ping performed by Start.
	Open           func(context.Context) (*gorm.DB, error)
	ConnectTimeout time.Duration
}

func Definition(f *forest.Forest, o Options) (forest.Definition, error) {
	if f == nil || o.TypeID == "" || o.Version == "" || o.Open == nil || (o.Ownership != Owned && o.Ownership != Borrowed) || o.ConnectTimeout < 0 {
		return forest.Definition{}, ErrInvalidOptions
	}
	if o.ConnectTimeout == 0 {
		o.ConnectTimeout = 5 * time.Second
	}
	return forest.Definition{TypeID: o.TypeID, Version: o.Version, Capabilities: []forest.CapabilityDescriptor{forest.Capability[Database]("database", forest.Public)}, Build: func(_ context.Context, e *forest.BuildEnv) (forest.Lifecycle, error) {
		r := &resource{forest: f, options: o}
		return r, forest.Provide(e, "database", r)
	}}, nil
}

type resource struct {
	forest  *forest.Forest
	options Options
	mu      sync.RWMutex
	db      *gorm.DB
	pool    *sql.DB
	closed  bool
}

func (r *resource) Start(ctx context.Context) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closed || r.db != nil {
		return ErrUnavailable
	}
	ctx, cancel := context.WithTimeout(ctx, r.options.ConnectTimeout)
	defer cancel()
	db, err := r.options.Open(ctx)
	// Retain a returned handle even on error so rollback Stop can release it.
	if db != nil {
		r.db = db
		r.pool, _ = db.DB()
	}
	if err != nil {
		return err
	}
	if r.pool == nil {
		return ErrInvalidOptions
	}
	return r.pool.PingContext(ctx)
}
func (r *resource) WithDB(ctx context.Context, fn func(*gorm.DB) error) error {
	if fn == nil {
		return ErrInvalidOptions
	}
	if _, err := r.forest.Generation(ctx); err != nil {
		return err
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	r.mu.RLock()
	defer r.mu.RUnlock()
	if r.closed || r.db == nil {
		return ErrUnavailable
	}
	return fn(r.db.WithContext(ctx))
}
func (r *resource) Stats() sql.DBStats {
	r.mu.RLock()
	defer r.mu.RUnlock()
	if r.pool == nil {
		return sql.DBStats{}
	}
	return r.pool.Stats()
}

// Transaction tags an unsuccessful Commit conservatively as outcome unknown.
// No automatic retries are performed. Native db.Transaction remains available
// through WithDB; its raw errors require the caller's own stage classification.
func (r *resource) Transaction(ctx context.Context, fn func(*gorm.DB) error, options ...*sql.TxOptions) error {
	if fn == nil || len(options) > 1 {
		return ErrInvalidOptions
	}
	return r.WithDB(ctx, func(db *gorm.DB) error {
		tx := db.Begin(options...)
		if tx.Error != nil {
			return tx.Error
		}
		finished := false
		defer func() {
			if !finished {
				_ = tx.Rollback().Error
			}
		}()
		if err := fn(tx); err != nil {
			rollback := tx.Rollback().Error
			finished = true
			return errors.Join(err, rollback)
		}
		err := tx.Commit().Error
		finished = true
		if err != nil {
			return errors.Join(ErrCommitUnknown, err)
		}
		return nil
	})
}
func (r *resource) Stop(context.Context) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.closed = true
	if r.pool != nil && r.options.Ownership == Owned {
		return r.pool.Close()
	}
	return nil
}
