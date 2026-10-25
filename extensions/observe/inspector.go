package observe

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"time"

	"github.com/Leeminjing/go-forest/forest"
)

type Reader interface {
	Read(context.Context, *Cursor) (Frame, error)
}

// Definition exposes read only to owners through explicit BindNest assembly.
// Each built instance has its own observation session identity.
func Definition(f *forest.Forest) forest.Definition {
	return forest.Definition{TypeID: "forest.observe.inspector", Version: "1",
		Capabilities: []forest.CapabilityDescriptor{forest.Capability[Reader]("read", forest.Ancestors)},
		Build: func(ctx context.Context, env *forest.BuildEnv) (forest.Lifecycle, error) {
			if f == nil {
				return nil, forest.ErrInvalidDefinition
			}
			var id [16]byte
			if _, err := rand.Read(id[:]); err != nil {
				return nil, err
			}
			i := &inspector{source: f, boot: hex.EncodeToString(id[:])}
			return forest.Hooks{}, forest.Provide(env, "read", i)
		},
	}
}

// source is private so tests can reproduce legal cross-sample windows without
// adding scheduling hooks to Core. Production always reads a real Forest.
type source interface {
	Events() []forest.Event
	Snapshot() forest.Snapshot
}
type inspector struct {
	source source
	boot   string
}

func (i *inspector) Read(ctx context.Context, after *Cursor) (Frame, error) {
	if err := ctx.Err(); err != nil {
		return Frame{}, err
	}
	history := i.source.Events()
	var high uint64
	if len(history) > 0 {
		high = history[len(history)-1].Sequence
	}
	frame := Frame{SchemaVersion: 1, BootID: i.boot, Events: []Change{}, Cursor: (Cursor{i.boot, high}).String()}
	if after != nil {
		switch {
		case after.BootID != i.boot:
			frame.ResetReason = "runtime_changed"
		case after.Sequence > high:
			return Frame{}, ErrInvalidCursor
		case len(history) > 0 && after.Sequence < history[0].Sequence-1:
			frame.ResetReason = "history_gap"
		}
	}
	for _, e := range history {
		if after == nil || frame.ResetReason != "" || e.Sequence > after.Sequence {
			frame.Events = append(frame.Events, changeDTO(e))
		}
	}
	frame.Snapshot = snapshotDTO(i.source.Snapshot())
	frame.SampledAt = time.Now().UTC()
	if err := ctx.Err(); err != nil {
		return Frame{}, err
	}
	return frame, nil
}
