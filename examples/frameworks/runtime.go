// Package frameworks composes HTTP, gRPC and Observatory around one business tree.
package frameworks

import (
	"context"
	"sync"
	"time"

	ginforest "github.com/Leeminjing/go-forest/adapters/gin"
	grpcforest "github.com/Leeminjing/go-forest/adapters/grpc"
	"github.com/Leeminjing/go-forest/examples/frameworks/internal/business"
	"github.com/Leeminjing/go-forest/examples/frameworks/internal/entry"
	"github.com/Leeminjing/go-forest/extensions/observatory"
	"github.com/Leeminjing/go-forest/forest"
)

type Options struct {
	Token, HTTPAddress, GRPCAddress, ObservatoryAddress string
	SampleInterval                                      time.Duration
}
type Runtime struct {
	Forest                              *forest.Forest
	HTTPURL, GRPCTarget, ObservatoryURL string
	Entered                             <-chan struct{}
	release                             chan struct{}
	once                                sync.Once
	next                                forest.DefinitionRef
}

func New(ctx context.Context, options Options) (runtime *Runtime, err error) {
	f := forest.New(forest.Options{})
	r := &Runtime{Forest: f, release: make(chan struct{})}
	defer func() {
		if err != nil {
			closeCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			_ = r.Close(closeCtx)
		}
	}()
	old := &business.Service{Label: "v1", Entered: make(chan struct{}), Release: r.release}
	r.Entered = old.Entered
	v1, v2 := business.Definition("1", old), business.Definition("2", &business.Service{Label: "v2"})
	r.next = v2.Ref()
	http, err := entry.HTTP(f, options.HTTPAddress)
	if err != nil {
		return nil, err
	}
	rpc, err := entry.GRPC(f, options.GRPCAddress)
	if err != nil {
		return nil, err
	}
	bundle, err := observatory.New(f, observatory.Options{Token: options.Token, Address: options.ObservatoryAddress, SampleInterval: options.SampleInterval})
	if err != nil {
		return nil, err
	}
	for _, d := range append([]forest.Definition{v1, v2, http, rpc}, bundle.Definitions...) {
		if err = f.Register(d); err != nil {
			return nil, err
		}
	}
	placement, err := bundle.Blueprint.Instantiate("observatory", "")
	if err != nil {
		return nil, err
	}
	plan := placement.Plan(0)
	plan.Operations = append(plan.Operations, forest.AddRoot("business", v1.Ref()), forest.AddRoot("http", http.Ref()), forest.AddRoot("grpc", rpc.Ref()), forest.Connect(forest.Link{ID: "http.work", Consumer: "http", Port: "business", Provider: "business", Capability: "work"}), forest.Connect(forest.Link{ID: "grpc.work", Consumer: "grpc", Port: "business", Provider: "business", Capability: "work"}))
	if _, err = f.Apply(ctx, plan); err != nil {
		return nil, err
	}
	err = f.Invoke(ctx, func(ctx context.Context) error {
		for _, endpoint := range []struct {
			id     forest.TreeID
			target *string
		}{{"http", &r.HTTPURL}, {"observatory", &r.ObservatoryURL}} {
			if err := forest.Use[ginforest.Endpoint](ctx, f, endpoint.id, "http", func(e ginforest.Endpoint) error { *endpoint.target = e.URL(); return e.ServeError() }); err != nil {
				return err
			}
		}
		return forest.Use[grpcforest.Endpoint](ctx, f, "grpc", "rpc", func(e grpcforest.Endpoint) error { r.GRPCTarget = e.Target(); return e.ServeError() })
	})
	if err != nil {
		return nil, err
	}
	return r, nil
}
func (r *Runtime) Replace(ctx context.Context) (uint64, error) {
	return r.Forest.Apply(ctx, forest.ChangePlan{BaseGeneration: r.Forest.Snapshot().Generation, Operations: []forest.Operation{forest.Replace("business", r.next, false)}})
}
func (r *Runtime) Release()                        { r.once.Do(func() { close(r.release) }) }
func (r *Runtime) Close(ctx context.Context) error { r.Release(); return r.Forest.Close(ctx) }
