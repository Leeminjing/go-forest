// Package business owns its database contract and durable command protocol.
package business

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"strings"
	"unicode/utf8"

	gf "github.com/Leeminjing/go-forest/adapters/gorm"
	"github.com/Leeminjing/go-forest/examples/cross-adapter/internal/trace"
	"github.com/Leeminjing/go-forest/examples/cross-adapter/protocol"
	"github.com/Leeminjing/go-forest/forest"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

type database interface {
	WithDB(context.Context, func(*gorm.DB) error) error
	Transaction(context.Context, func(*gorm.DB) error, ...*sql.TxOptions) error
}
type Record struct {
	OperationID                                          string `gorm:"primaryKey;size:128"`
	Digest, Target, Output, EffectVersion, EffectProcess string
	EffectGeneration                                     uint64
	Completed                                            bool
}

func (Record) TableName() string { return "forest_cross_receipts" }

type Effect struct {
	Target string `gorm:"primaryKey;size:128"`
	Count  int64
}

func (Effect) TableName() string { return "forest_cross_effects" }

type Options struct {
	Version, Process string
	BeforeCommit     func(context.Context, protocol.Request) error
	AfterCommit      func(context.Context, protocol.Receipt) error
	Stopped          func(string)
	Called           func()
}
type Service struct {
	f       *forest.Forest
	db      forest.Ref[database]
	options Options
}

func Definition(f *forest.Forest, o Options) forest.Definition {
	return forest.Definition{TypeID: "cross.business", Version: o.Version, HotSwappable: true, Ports: []forest.PortDescriptor{forest.Port[database]("database", forest.Required(), forest.During(forest.Construction))}, Capabilities: []forest.CapabilityDescriptor{forest.Capability[*Service]("commands", forest.Public)}, Build: func(_ context.Context, e *forest.BuildEnv) (forest.Lifecycle, error) {
		r, err := forest.Reference[database](e, "database")
		if err != nil {
			return nil, err
		}
		return forest.Hooks{OnStop: func(context.Context) error {
			if o.Stopped != nil {
				o.Stopped(o.Version)
			}
			return nil
		}}, forest.Provide(e, "commands", &Service{f: f, db: r, options: o})
	}}
}
func Valid(r protocol.Request) bool {
	return r.OperationID != "" && len(r.OperationID) <= 128 && r.Target != "" && len(r.Target) <= 128 && len(r.Payload) <= 4096 && utf8.ValidString(r.OperationID) && utf8.ValidString(r.Target) && utf8.ValidString(r.Payload) && !strings.ContainsRune(r.OperationID, 0) && !strings.ContainsRune(r.Target, 0) && !strings.ContainsRune(r.Payload, 0)
}
func digest(r protocol.Request) string {
	data, _ := json.Marshal(struct{ Schema, Target, Payload string }{"command/v1", r.Target, r.Payload})
	h := sha256.Sum256(data)
	return hex.EncodeToString(h[:])
}
func receipt(r Record) protocol.Receipt {
	return protocol.Receipt{OperationID: r.OperationID, Digest: r.Digest, Target: r.Target, Output: r.Output, EffectVersion: r.EffectVersion, EffectProcess: r.EffectProcess, EffectGeneration: r.EffectGeneration}
}
func (s *Service) point(ctx context.Context, stage string) (uint64, error) {
	g, err := s.f.Generation(ctx)
	if err == nil {
		trace.Add(ctx, protocol.Point{Stage: stage, Process: s.options.Process, Version: s.options.Version, Generation: g})
	}
	return g, err
}
func (s *Service) Observe(ctx context.Context) (string, error) {
	if s.options.Called != nil {
		s.options.Called()
	}
	_, err := s.point(ctx, "business.observe")
	return s.options.Version, err
}
func (s *Service) Apply(ctx context.Context, in protocol.Request) (out protocol.Result, err error) {
	if s.options.Called != nil {
		s.options.Called()
	}
	if !Valid(in) {
		return out, protocol.ErrConflict
	}
	gen, err := s.point(ctx, "business.apply")
	if err != nil {
		return out, err
	}
	out.VersionAfter = s.options.Version
	mutated := false
	err = s.db.Use(ctx, func(d database) error {
		return d.Transaction(ctx, func(tx *gorm.DB) error {
			if _, err := s.point(tx.Statement.Context, "database"); err != nil {
				return err
			}
			row := Record{OperationID: in.OperationID, Digest: digest(in), Target: in.Target}
			if err := tx.Clauses(clause.OnConflict{Columns: []clause.Column{{Name: "operation_id"}}, DoNothing: true}).Create(&row).Error; err != nil {
				return err
			}
			if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).First(&row, "operation_id = ?", in.OperationID).Error; err != nil {
				return err
			}
			if row.Digest != digest(in) {
				return protocol.ErrConflict
			}
			if row.Completed {
				out.Receipt = receipt(row)
				out.Reused = true
				return nil
			}
			effect := Effect{Target: in.Target, Count: 1}
			if err := tx.Clauses(clause.OnConflict{Columns: []clause.Column{{Name: "target"}}, DoUpdates: clause.Assignments(map[string]any{"count": gorm.Expr("forest_cross_effects.count + 1")})}).Create(&effect).Error; err != nil {
				return err
			}
			row.Completed = true
			row.Output = s.options.Version + ":" + in.Payload
			row.EffectVersion = s.options.Version
			row.EffectProcess = s.options.Process
			row.EffectGeneration = gen
			if err := tx.Save(&row).Error; err != nil {
				return err
			}
			if s.options.BeforeCommit != nil {
				if err := s.options.BeforeCommit(ctx, in); err != nil {
					return err
				}
			}
			if err := ctx.Err(); err != nil {
				return err
			}
			out.Receipt = receipt(row)
			mutated = true
			return nil
		}, &sql.TxOptions{Isolation: sql.LevelReadCommitted})
	})
	if errors.Is(err, gf.ErrCommitUnknown) {
		return protocol.Result{}, errors.Join(protocol.ErrUnknown, err)
	}
	if err != nil {
		return protocol.Result{}, err
	}
	if mutated && s.options.AfterCommit != nil {
		err = s.options.AfterCommit(ctx, out.Receipt)
		// A failure after confirmed Commit cannot be classified as ordinary
		// pre-commit work. Cancellation remains native but never implies rollback.
		if err != nil && !errors.Is(err, context.Canceled) {
			err = errors.Join(protocol.ErrUnknown, err)
		}
	}
	return
}
func (s *Service) Inspect(ctx context.Context, in protocol.Request) (out protocol.Observation, err error) {
	if s.options.Called != nil {
		s.options.Called()
	}
	if !Valid(in) {
		return out, protocol.ErrConflict
	}
	err = s.db.Use(ctx, func(d database) error {
		return d.WithDB(ctx, func(tx *gorm.DB) error {
			var row Record
			e := tx.First(&row, "operation_id = ?", in.OperationID).Error
			if e != nil && !errors.Is(e, gorm.ErrRecordNotFound) {
				return e
			}
			if e == nil {
				if row.Digest != digest(in) {
					return protocol.ErrConflict
				}
				if !row.Completed {
					return protocol.ErrUnknown
				}
				out.Found = true
				out.Receipt = receipt(row)
			}
			var effect Effect
			e = tx.First(&effect, "target = ?", in.Target).Error
			if errors.Is(e, gorm.ErrRecordNotFound) {
				return nil
			}
			if e != nil {
				return e
			}
			out.EffectCount = effect.Count
			return nil
		})
	})
	return
}
