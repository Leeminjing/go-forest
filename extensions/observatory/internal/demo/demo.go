// Package demo assembles the same application used by the command and tests.
package demo

import (
	"context"
	"strconv"
	"sync"
	"time"

	ginforest "github.com/Leeminjing/go-forest/adapters/gin"
	"github.com/Leeminjing/go-forest/extensions/observatory"
	"github.com/Leeminjing/go-forest/extensions/observatory/internal/demo/business"
	"github.com/Leeminjing/go-forest/forest"
	"github.com/gin-gonic/gin"
)

type worker interface {
	Work(context.Context, bool) (string, error)
}
type Options struct {
	Token, AppAddress, ObservatoryAddress string
	SampleInterval                        time.Duration
}
type Runtime struct {
	Forest                 *forest.Forest
	AppURL, ObservatoryURL string
	Entered                <-chan struct{}
	release                chan struct{}
	once                   sync.Once
	next                   forest.DefinitionRef
}

func New(ctx context.Context, options Options) (runtime *Runtime, err error) {
	f := forest.New(forest.Options{DrainTimeout: 5 * time.Second, EventCapacity: 16})
	r := &Runtime{Forest: f, release: make(chan struct{})}
	defer func() {
		if err != nil {
			closeCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			_ = f.Close(closeCtx)
		}
	}()
	old := &business.Service{Label: "v1", Entered: make(chan struct{}), Release: r.release}
	r.Entered = old.Entered
	v1 := business.Definition("1", old)
	v2 := business.Definition("2", &business.Service{Label: "v2"})
	r.next = v2.Ref()
	app, err := ginforest.Definition(f, ginforest.Options{TypeID: "demo.application", Version: "1", Address: options.AppAddress,
		Ports: []forest.PortDescriptor{forest.Port[worker]("business", forest.Required(), forest.RequiresContract("demo.work", 1, 1))},
		BuildRouter: func(ctx context.Context, env *forest.BuildEnv) (*gin.Engine, error) {
			ref, err := forest.Reference[worker](env, "business")
			if err != nil {
				return nil, err
			}
			router := gin.New()
			router.Use(ginforest.Scope(f))
			router.GET("/work", func(c *gin.Context) {
				var first, again string
				err := ref.Use(c.Request.Context(), func(w worker) error {
					var err error
					first, err = w.Work(c.Request.Context(), c.Query("hold") == "1")
					return err
				})
				if err == nil {
					err = ref.Use(c.Request.Context(), func(w worker) error { var err error; again, err = w.Work(c.Request.Context(), false); return err })
				}
				if err != nil {
					c.JSON(503, gin.H{"error": "unavailable"})
					return
				}
				generation, _ := f.Generation(c.Request.Context())
				c.JSON(200, gin.H{"value": first, "again": again, "generation": strconv.FormatUint(generation, 10)})
			})
			return router, nil
		},
	})
	if err != nil {
		return nil, err
	}
	bundle, err := observatory.New(f, observatory.Options{Token: options.Token, Address: options.ObservatoryAddress, SampleInterval: options.SampleInterval})
	if err != nil {
		return nil, err
	}
	for _, d := range append([]forest.Definition{v1, v2, app}, bundle.Definitions...) {
		if err = f.Register(d); err != nil {
			return nil, err
		}
	}
	placement, err := bundle.Blueprint.Instantiate("observatory", "")
	if err != nil {
		return nil, err
	}
	plan := placement.Plan(0)
	plan.Operations = append(plan.Operations, forest.AddRoot("business", v1.Ref()), forest.AddRoot("application", app.Ref()), forest.Connect(forest.Link{ID: "work", Consumer: "application", Port: "business", Provider: "business", Capability: "work"}))
	if _, err = f.Apply(ctx, plan); err != nil {
		return nil, err
	}
	for _, item := range []struct {
		id     forest.TreeID
		target *string
	}{{"application", &r.AppURL}, {"observatory", &r.ObservatoryURL}} {
		err = f.Invoke(ctx, func(ctx context.Context) error {
			return forest.Use[ginforest.Endpoint](ctx, f, item.id, "http", func(e ginforest.Endpoint) error { *item.target = e.URL(); return e.ServeError() })
		})
		if err != nil {
			return nil, err
		}
	}
	return r, nil
}
func (r *Runtime) Replace(ctx context.Context) (uint64, error) {
	return r.Forest.Apply(ctx, forest.ChangePlan{BaseGeneration: r.Forest.Snapshot().Generation, Operations: []forest.Operation{forest.Replace("business", r.next, false)}})
}
func (r *Runtime) Release()                        { r.once.Do(func() { close(r.release) }) }
func (r *Runtime) Close(ctx context.Context) error { r.Release(); return r.Forest.Close(ctx) }
