// Package jobservice implements business idempotency independently of Temporal.
package jobservice

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"errors"
	"unicode/utf8"

	"github.com/Leeminjing/go-forest-agent/agent"
	gf "github.com/Leeminjing/go-forest/adapters/gorm"
	"github.com/Leeminjing/go-forest/examples/temporal/flow"
	"github.com/Leeminjing/go-forest/forest"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

var ErrConflict = errors.New("business operation input conflict")
var ErrUnknown = errors.New("business outcome requires reconciliation")

type Continuation struct {
	RunID  string
	Status agent.Status
}

func (c *Continuation) Error() string { return "native Agent continuation required" }

type database interface {
	WithDB(context.Context, func(*gorm.DB) error) error
	Transaction(context.Context, func(*gorm.DB) error, ...*sql.TxOptions) error
}
type executor interface {
	Create(context.Context, string, string) (agent.Run, error)
	Read(context.Context, string) (agent.Run, error)
	Advance(context.Context, string) (agent.Run, error)
}
type Record struct {
	ID         string `gorm:"primaryKey;size:128"`
	Digest     string `gorm:"size:64"`
	Plan       string
	Version    string
	Generation uint64
	Changes    int
	Completed  bool
}

func (Record) TableName() string { return "forest_temporal_jobs" }

type Options struct {
	Version     string
	BeforeApply func(context.Context, flow.Planned) error
	AfterCommit func(context.Context, flow.Receipt) error
}
type Service struct {
	f       *forest.Forest
	db      forest.Ref[database]
	agent   forest.Ref[executor]
	options Options
}

func Definition(f *forest.Forest, o Options) forest.Definition {
	return forest.Definition{TypeID: "temporal-example.jobs", Version: o.Version, HotSwappable: true, PreserveChildren: true, ExportChildren: true, Ports: []forest.PortDescriptor{forest.Port[database]("database", forest.Required()), forest.Port[executor]("agent", forest.Required())}, Capabilities: []forest.CapabilityDescriptor{forest.Capability[*Service]("jobs", forest.Public|forest.Descendants)}, Build: func(_ context.Context, e *forest.BuildEnv) (forest.Lifecycle, error) {
		db, err := forest.Reference[database](e, "database")
		if err != nil {
			return nil, err
		}
		a, err := forest.Reference[executor](e, "agent")
		if err != nil {
			return nil, err
		}
		return forest.Hooks{}, forest.Provide(e, "jobs", &Service{f: f, db: db, agent: a, options: o})
	}}
}
func valid(in flow.Input) bool {
	return in.ID != "" && len(in.ID) <= 128 && in.Text != "" && len(in.Text) <= 4096 && utf8.ValidString(in.ID) && utf8.ValidString(in.Text)
}
func digest(text string) string {
	sum := sha256.Sum256([]byte(text))
	return hex.EncodeToString(sum[:])
}
func receipt(r Record) flow.Receipt {
	return flow.Receipt{ID: r.ID, Digest: r.Digest, Plan: r.Plan, Version: r.Version, Generation: r.Generation, Changes: r.Changes}
}
func (s *Service) Plan(ctx context.Context, in flow.Input) (out flow.Planned, err error) {
	if !valid(in) {
		return out, ErrConflict
	}
	var row Record
	err = s.db.Use(ctx, func(d database) error {
		return d.WithDB(ctx, func(db *gorm.DB) error {
			row = Record{ID: in.ID, Digest: digest(in.Text)}
			if err := db.Clauses(clause.OnConflict{DoNothing: true}).Create(&row).Error; err != nil {
				return err
			}
			if err := db.First(&row, "id = ?", in.ID).Error; err != nil {
				return err
			}
			if row.Digest != digest(in.Text) {
				return ErrConflict
			}
			return nil
		})
	})
	if err != nil {
		return
	}
	if row.Plan == "" {
		err = s.agent.Use(ctx, func(a executor) error {
			id := "temporal-" + digest(in.ID+":"+row.Digest)
			r, err := a.Read(ctx, id)
			if errors.Is(err, agent.ErrNotFound) {
				r, err = a.Create(ctx, id, in.Text)
				if errors.Is(err, agent.ErrConflict) {
					r, err = a.Read(ctx, id)
				}
			}
			if err != nil {
				return err
			}
			for step := 0; step < 8; step++ {
				switch r.Status {
				case agent.Completed:
					if len(r.Messages) == 0 {
						return ErrUnknown
					}
					row.Plan = r.Messages[len(r.Messages)-1].Content
					return nil
				case agent.ModelReady, agent.ToolsReady:
					r, err = a.Advance(ctx, id)
					if err != nil {
						return err
					}
				default:
					return &Continuation{RunID: id, Status: r.Status}
				}
			}
			return &Continuation{RunID: id, Status: r.Status}
		})
		if err != nil {
			return
		}
		err = s.db.Use(ctx, func(d database) error {
			return d.WithDB(ctx, func(db *gorm.DB) error {
				if err := db.Model(&Record{}).Where("id = ? AND digest = ? AND plan = ''", in.ID, row.Digest).Update("plan", row.Plan).Error; err != nil {
					return err
				}
				return db.First(&row, "id = ?", in.ID).Error
			})
		})
		if err != nil {
			return
		}
	}
	out = flow.Planned{ID: row.ID, Digest: row.Digest, Plan: row.Plan}
	return
}
func (s *Service) Apply(ctx context.Context, in flow.Planned) (out flow.Receipt, err error) {
	if s.options.BeforeApply != nil {
		if err = s.options.BeforeApply(ctx, in); err != nil {
			return
		}
	}
	gen, err := s.f.Generation(ctx)
	if err != nil {
		return out, err
	}
	err = s.db.Use(ctx, func(d database) error {
		return d.Transaction(ctx, func(db *gorm.DB) error {
			var row Record
			if err := db.Clauses(clause.Locking{Strength: "UPDATE"}).First(&row, "id = ?", in.ID).Error; err != nil {
				return err
			}
			if row.Digest != in.Digest || row.Plan != in.Plan || row.Plan == "" {
				return ErrConflict
			}
			if !row.Completed {
				row.Completed = true
				row.Changes++
				row.Version = s.options.Version
				row.Generation = gen
				if err := db.Save(&row).Error; err != nil {
					return err
				}
			}
			out = receipt(row)
			return nil
		})
	})
	if errors.Is(err, gf.ErrCommitUnknown) {
		return flow.Receipt{}, ErrUnknown
	}
	if err == nil && s.options.AfterCommit != nil {
		err = s.options.AfterCommit(ctx, out)
	}
	return
}
func (s *Service) Read(ctx context.Context, id string) (out flow.Result, err error) {
	err = s.db.Use(ctx, func(d database) error {
		return d.WithDB(ctx, func(db *gorm.DB) error {
			var row Record
			if err := db.First(&row, "id = ?", id).Error; err != nil {
				return err
			}
			if !row.Completed {
				return ErrUnknown
			}
			out.Receipt = receipt(row)
			out.ReadVersion = s.options.Version
			return nil
		})
	})
	if err == nil {
		out.ReadGeneration, err = s.f.Generation(ctx)
	}
	return
}
