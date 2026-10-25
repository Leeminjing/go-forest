package ecosystem

import (
	"context"
	"encoding/json"
	"errors"

	dbtree "github.com/Leeminjing/go-forest/adapters/gorm"
	"github.com/Leeminjing/go-forest/forest"
	"go.opentelemetry.io/otel/propagation"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

var ErrInvalid = errors.New("invalid task")
var ErrConflict = errors.New("task ID already has a different input")

type Task struct {
	Namespace string `gorm:"primaryKey" json:"-"`
	ID        string `gorm:"primaryKey" json:"id"`
	Input     string `json:"input"`
	State     string `json:"state"`
	Version   string `json:"version"`
	Result    string `json:"result"`
}

func (Task) TableName() string { return "forest_ecosystem_tasks" }

type outbox struct {
	Namespace               string `gorm:"primaryKey"`
	ID                      string `gorm:"primaryKey"`
	Data                    []byte
	Sent                    bool
	Traceparent, Tracestate string
}

func (outbox) TableName() string { return "forest_ecosystem_outbox" }

type inbox struct {
	Namespace string `gorm:"primaryKey"`
	ID        string `gorm:"primaryKey"`
}

func (inbox) TableName() string { return "forest_ecosystem_inbox" }

type event struct {
	ID    string `json:"id"`
	Input string `json:"input"`
}

type repositoryAPI interface {
	Submit(context.Context, string, string, string) (Task, error)
	Get(context.Context, string) (Task, error)
	Pending(context.Context) ([]outbox, error)
	MarkSent(context.Context, string) error
	Receive(context.Context, event) error
	Candidates(context.Context, string) ([]Task, error)
	Finish(context.Context, string, string, string) error
}
type repository struct {
	db        forest.Ref[dbtree.Database]
	namespace string
}

func repositoryDefinition(namespace string) forest.Definition {
	return forest.Definition{TypeID: "ecosystem.repository", Version: "1", Ports: []forest.PortDescriptor{forest.Port[dbtree.Database]("database", forest.Required())}, Capabilities: []forest.CapabilityDescriptor{forest.Capability[repositoryAPI]("repository", forest.Public)}, Build: func(_ context.Context, e *forest.BuildEnv) (forest.Lifecycle, error) {
		ref, err := forest.Reference[dbtree.Database](e, "database")
		if err != nil {
			return nil, err
		}
		return forest.Hooks{}, forest.Provide(e, "repository", &repository{db: ref, namespace: namespace})
	}}
}
func (r *repository) with(ctx context.Context, fn func(*gorm.DB) error) error {
	return r.db.Use(ctx, func(d dbtree.Database) error { return d.WithDB(ctx, fn) })
}
func (r *repository) transaction(ctx context.Context, fn func(*gorm.DB) error) error {
	return r.db.Use(ctx, func(d dbtree.Database) error { return d.Transaction(ctx, fn) })
}
func (r *repository) Submit(ctx context.Context, id, input, version string) (task Task, err error) {
	if len(id) == 0 || len(id) > 128 || len(input) == 0 || len(input) > 4096 {
		return task, ErrInvalid
	}
	err = r.transaction(ctx, func(tx *gorm.DB) error {
		task = Task{Namespace: r.namespace, ID: id, Input: input, State: "pending", Version: version}
		result := tx.Clauses(clause.OnConflict{DoNothing: true}).Create(&task)
		if result.Error != nil {
			return result.Error
		}
		if result.RowsAffected == 0 {
			if err := tx.Where("namespace = ? AND id = ?", r.namespace, id).First(&task).Error; err != nil {
				return err
			}
			if task.Input != input {
				return ErrConflict
			}
			return nil
		}
		data, err := json.Marshal(event{ID: id, Input: input})
		if err != nil {
			return err
		}
		carrier := propagation.MapCarrier{}
		tracePropagation.Inject(ctx, carrier)
		return tx.Create(&outbox{Namespace: r.namespace, ID: id, Data: data, Traceparent: carrier["traceparent"], Tracestate: carrier["tracestate"]}).Error
	})
	return
}
func (r *repository) Get(ctx context.Context, id string) (task Task, err error) {
	err = r.with(ctx, func(d *gorm.DB) error { return d.Where("namespace = ? AND id = ?", r.namespace, id).First(&task).Error })
	return
}
func (r *repository) Pending(ctx context.Context) (rows []outbox, err error) {
	err = r.with(ctx, func(d *gorm.DB) error {
		return d.Where("namespace = ? AND sent = ?", r.namespace, false).Order("id").Limit(32).Find(&rows).Error
	})
	return
}
func (r *repository) MarkSent(ctx context.Context, id string) error {
	return r.with(ctx, func(d *gorm.DB) error {
		return d.Model(&outbox{}).Where("namespace = ? AND id = ?", r.namespace, id).Update("sent", true).Error
	})
}
func (r *repository) Receive(ctx context.Context, e event) error {
	return r.transaction(ctx, func(tx *gorm.DB) error {
		var task Task
		if err := tx.Where("namespace = ? AND id = ?", r.namespace, e.ID).First(&task).Error; err != nil {
			return err
		}
		if task.Input != e.Input {
			return ErrConflict
		}
		result := tx.Clauses(clause.OnConflict{DoNothing: true}).Create(&inbox{Namespace: r.namespace, ID: e.ID})
		if result.Error != nil {
			return result.Error
		}
		if result.RowsAffected == 0 {
			return nil
		}
		return tx.Model(&Task{}).Where("namespace = ? AND id = ? AND state = ?", r.namespace, e.ID, "pending").Update("state", "runnable").Error
	})
}
func (r *repository) Candidates(ctx context.Context, after string) (rows []Task, err error) {
	err = r.with(ctx, func(d *gorm.DB) error {
		return d.Where("namespace = ? AND state IN ? AND id > ?", r.namespace, []string{"runnable", "awaiting-approval", "outcome-unknown"}, after).Order("id").Limit(32).Find(&rows).Error
	})
	return
}
func (r *repository) Finish(ctx context.Context, id, state, result string) error {
	return r.with(ctx, func(d *gorm.DB) error {
		return d.Model(&Task{}).Where("namespace = ? AND id = ? AND state IN ?", r.namespace, id, []string{"runnable", "awaiting-approval", "outcome-unknown"}).Updates(map[string]any{"state": state, "result": result}).Error
	})
}
