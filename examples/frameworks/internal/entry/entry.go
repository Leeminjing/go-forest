// Package entry owns the consumer contract shared by its two transport adapters.
package entry

import (
	"context"
	"strconv"

	"github.com/Leeminjing/go-forest/forest"
)

type worker interface {
	Work(context.Context, bool) (string, error)
}
type Reply struct {
	Value      string `json:"value"`
	Again      string `json:"again"`
	Generation string `json:"generation"`
}

func ports() []forest.PortDescriptor {
	return []forest.PortDescriptor{forest.Port[worker]("business", forest.Required(), forest.RequiresContract("frameworks.work", 1, 1))}
}
func read(ctx context.Context, f *forest.Forest, ref forest.Ref[worker], hold bool) (Reply, error) {
	var result Reply
	err := ref.Use(ctx, func(w worker) error { var err error; result.Value, err = w.Work(ctx, hold); return err })
	if err == nil {
		err = ref.Use(ctx, func(w worker) error { var err error; result.Again, err = w.Work(ctx, false); return err })
	}
	if err != nil {
		return Reply{}, err
	}
	g, err := f.Generation(ctx)
	result.Generation = strconv.FormatUint(g, 10)
	return result, err
}
