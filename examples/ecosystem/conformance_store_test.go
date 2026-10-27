package ecosystem

import (
	"context"
	"encoding/json"

	dbtree "github.com/Leeminjing/go-forest/adapters/gorm"
	"github.com/Leeminjing/go-forest/forest"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

// These are explicit acceptance-fixture effects, not a new production inbox
// schema or a shared transaction with the application's separate Receive.
type confReceipt struct {
	Namespace                         string `gorm:"primaryKey"`
	ID                                string `gorm:"primaryKey"`
	Digest, Content, Process, Version string
	Generation, Count                 uint64
}

func (confReceipt) TableName() string { return "forest_conformance_receipts" }

type confEffect struct {
	Namespace string `gorm:"primaryKey"`
	ID        string `gorm:"primaryKey"`
	Content   string
	Count     uint64
}

func (confEffect) TableName() string { return "forest_conformance_effects" }

type confStoreAPI interface {
	Observe(context.Context) error
	Apply(context.Context) (confReceipt, error)
}
type confStore struct {
	f         *forest.Forest
	db        forest.Ref[dbtree.Database]
	e         *confEvidence
	namespace string
}

func (s *confStore) Observe(ctx context.Context) error {
	if err := s.e.at(ctx, s.f, "store.observe"); err != nil {
		return err
	}
	return s.db.Use(ctx, func(d dbtree.Database) error {
		return d.WithDB(ctx, func(db *gorm.DB) error { var n int; return db.Raw("SELECT 1").Scan(&n).Error })
	})
}
func (s *confStore) Apply(ctx context.Context) (out confReceipt, err error) {
	if err = s.e.at(ctx, s.f, "store.apply"); err != nil {
		return
	}
	call := ctx.Value(confCallKey{}).(confCall)
	var in event
	if err = json.Unmarshal(call.Message.Data, &in); err != nil {
		return
	}
	if in.ID == "" {
		return out, ErrInvalid
	}
	g, err := s.f.Generation(ctx)
	if err != nil {
		return
	}
	out = confReceipt{Namespace: s.namespace, ID: in.ID, Digest: confDigest(in), Content: "received:" + in.Input, Process: call.Execution.Process, Version: call.Version, Generation: g, Count: 1}
	err = s.db.Use(ctx, func(d dbtree.Database) error {
		return d.Transaction(ctx, func(tx *gorm.DB) error {
			inserted := tx.Clauses(clause.OnConflict{DoNothing: true}).Create(&out)
			if inserted.Error != nil {
				return inserted.Error
			}
			if inserted.RowsAffected == 0 {
				var old confReceipt
				if e := tx.Where("namespace = ? AND id = ?", s.namespace, in.ID).First(&old).Error; e != nil {
					return e
				}
				if old.Digest != out.Digest {
					return ErrConflict
				}
				out = old
				return nil
			}
			return tx.Create(&confEffect{Namespace: s.namespace, ID: in.ID, Content: out.Content, Count: 1}).Error
		})
	})
	return
}
func confStoreDefinition(f *forest.Forest, evidence *confEvidence, ns, typeID string) forest.Definition {
	return forest.Definition{TypeID: typeID, Version: "1", Ports: []forest.PortDescriptor{forest.Port[dbtree.Database]("database", forest.Required())}, Capabilities: []forest.CapabilityDescriptor{forest.Capability[confStoreAPI]("store", forest.Public)}, Build: func(_ context.Context, e *forest.BuildEnv) (forest.Lifecycle, error) {
		r, err := forest.Reference[dbtree.Database](e, "database")
		if err != nil {
			return nil, err
		}
		return forest.Hooks{}, forest.Provide(e, "store", &confStore{f: f, db: r, e: evidence, namespace: ns})
	}}
}
