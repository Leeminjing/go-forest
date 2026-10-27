package ecosystem

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"os"
	"sync/atomic"
	"time"

	tk "github.com/Leeminjing/go-forest/testkit"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/stdlib"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

func (b *confBundle) samples(ctx context.Context, version string) ([]tk.Resource, error) {
	pool, err := b.pool.sample(ctx)
	if err != nil {
		return nil, err
	}
	b.handlersMu.Lock()
	h := b.handlers[version]
	b.handlersMu.Unlock()
	if h == nil {
		return nil, errorsNewEvidence()
	}
	file, err := h.file.sample()
	if err != nil {
		return nil, err
	}
	if b.conn == nil {
		return nil, errors.New("native connection never opened")
	}
	if b.conn.IsClosed() {
		for b.connCloses.Load() == 0 {
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			case <-time.After(time.Millisecond):
			}
		}
	} else if err = b.conn.FlushWithContext(ctx); err != nil {
		return nil, err
	}
	if err = b.f.nc.FlushWithContext(ctx); err != nil {
		return nil, err
	}
	return []tk.Resource{pool, file, {ID: "original-owned-nats", Ownership: tk.Owned, Usable: !b.conn.IsClosed(), CloseCount: b.connCloses.Load()}, {ID: "native-borrowed-publisher-connection", Ownership: tk.Borrowed, Usable: !b.f.nc.IsClosed(), CloseCount: b.f.borrowCloses.Load()}}, nil
}

// Only physical Close is observed; embedding the concrete native connection
// retains pgx's optional SQL driver interfaces and original query/cancel logic.
type confConn struct {
	*stdlib.Conn
	owner *confPool
}

func (c *confConn) Close() error {
	err := c.Conn.Close()
	if err == nil {
		c.owner.closes.Add(1)
	}
	return err
}

type confConnector struct {
	driver.Connector
	owner *confPool
}

func (c confConnector) Connect(ctx context.Context) (driver.Conn, error) {
	native, err := c.Connector.Connect(ctx)
	if err != nil {
		return nil, err
	}
	pg, ok := native.(*stdlib.Conn)
	if !ok {
		_ = native.Close()
		return nil, errors.New("unexpected native pgx connection")
	}
	c.owner.opens.Add(1)
	return &confConn{Conn: pg, owner: c.owner}, nil
}

type confPool struct {
	db            *sql.DB
	opens, closes atomic.Uint64
}

func (p *confPool) open(dsn string) (*gorm.DB, error) {
	config, err := pgx.ParseConfig(dsn)
	if err != nil {
		return nil, err
	}
	p.db = sql.OpenDB(confConnector{Connector: stdlib.GetConnector(*config), owner: p})
	p.db.SetMaxOpenConns(1)
	p.db.SetMaxIdleConns(1)
	return gorm.Open(postgres.New(postgres.Config{Conn: p.db}), &gorm.Config{DisableAutomaticPing: true, Logger: logger.Default.LogMode(logger.Silent)})
}
func (p *confPool) sample(ctx context.Context) (tk.Resource, error) {
	if p.db == nil {
		return tk.Resource{}, errors.New("pool never opened")
	}
	if p.opens.Load() != 1 {
		return tk.Resource{}, fmt.Errorf("single original connection prerequisite failed: opened=%d", p.opens.Load())
	}
	err := p.db.PingContext(ctx)
	n := p.closes.Load()
	if err != nil && !(n == 1 && p.db.Stats().OpenConnections == 0) {
		return tk.Resource{}, err
	}
	if err == nil && n != 0 {
		return tk.Resource{}, errors.New("original closed connection replaced")
	}
	return tk.Resource{ID: "original-pg-connection", Ownership: tk.Owned, Usable: err == nil, CloseCount: n}, nil
}

type confFile struct {
	file   *os.File
	closes atomic.Uint64
}

func (p *confFile) open(directory string) error {
	var err error
	p.file, err = os.CreateTemp(directory, "proof-handler-")
	return err
}
func (p *confFile) close() error {
	if p.file == nil {
		return nil
	}
	if err := p.file.Close(); err != nil {
		return err
	}
	p.closes.Add(1)
	return nil
}
func (p *confFile) sample() (tk.Resource, error) {
	if p.file == nil {
		return tk.Resource{}, errors.New("file never opened")
	}
	_, err := p.file.ReadAt(make([]byte, 1), 0)
	closed := errors.Is(err, os.ErrClosed)
	if !closed {
		if _, err = p.file.Stat(); err != nil {
			return tk.Resource{}, err
		}
	}
	return tk.Resource{ID: "original-handler-file", Ownership: tk.Owned, Usable: !closed, CloseCount: p.closes.Load()}, nil
}
