// Package einodemo composes two native execution engines, not one universal agent.
package einodemo

import (
	"context"
	"errors"
	"path/filepath"
	"time"

	"github.com/Leeminjing/go-forest-agent/agent"
	"github.com/Leeminjing/go-forest-agent/trees/events"
	"github.com/Leeminjing/go-forest-agent/trees/filecheckpoint"
	"github.com/Leeminjing/go-forest-agent/trees/manualapproval"
	ef "github.com/Leeminjing/go-forest/adapters/eino"
	"github.com/Leeminjing/go-forest/examples/eino/internal/catalog"
	"github.com/Leeminjing/go-forest/forest"
)

type Options struct {
	Directory             string
	BeforeLookup          func(context.Context, string) error
	RequireForestApproval bool
}
type Runtime struct {
	Forest                  *forest.Forest
	nextCatalog, nextNative forest.DefinitionRef
}

func New(ctx context.Context, o Options) (r *Runtime, err error) {
	if o.Directory == "" {
		return nil, errors.New("checkpoint directory required")
	}
	f := forest.New(forest.Options{})
	r = &Runtime{Forest: f}
	defer func() {
		if err != nil {
			c, x := context.WithTimeout(context.Background(), 3*time.Second)
			defer x()
			f.Close(c)
		}
	}()
	owner, err := ef.NewOwner(f, 64)
	if err != nil {
		return nil, err
	}
	native, err := nativeDefinition(owner, filepath.Join(o.Directory, "eino"), "1")
	if err != nil {
		return nil, err
	}
	nextNative, err := nativeDefinition(owner, filepath.Join(o.Directory, "eino"), "2")
	if err != nil {
		return nil, err
	}
	graph, err := graphDefinition(owner)
	if err != nil {
		return nil, err
	}
	cat := catalog.Definition("v1", o.BeforeLookup)
	nextCat := catalog.Definition("v2", nil)
	r.nextCatalog, r.nextNative = nextCat.Ref(), nextNative.Ref()
	defs := map[forest.TreeID]forest.Definition{"catalog": cat, "catalog/eino": native, "catalog/graph": graph, "application": applicationDefinition(), "eino-text": einoBridgeDefinition(), "forest-text": forestBridgeDefinition(), "forest-agent": agent.Definition(agent.Options{}), "forest-model": forestModelDefinition(), "forest-tool": forestToolDefinition(o.RequireForestApproval), "forest-checkpoint": filecheckpoint.Definition(filepath.Join(o.Directory, "forest-agent")), "forest-events": events.Definition(events.New(4096)), "forest-approval": manualapproval.Definition()}
	for _, d := range defs {
		if err = f.Register(d); err != nil {
			return nil, err
		}
	}
	for _, d := range []forest.Definition{nextCat, nextNative} {
		if err = f.Register(d); err != nil {
			return nil, err
		}
	}
	ops := []forest.Operation{}
	for id, d := range defs {
		if id == "catalog/eino" || id == "catalog/graph" {
			ops = append(ops, forest.Nest("catalog", id, d.Ref()))
		} else {
			ops = append(ops, forest.AddRoot(id, d.Ref()))
		}
	}
	for _, l := range []forest.Link{{ID: "eino.lookup", Consumer: "catalog/eino", Port: "lookup", Provider: "catalog", Capability: "lookup"}, {ID: "graph.lookup", Consumer: "catalog/graph", Port: "lookup", Provider: "catalog", Capability: "lookup"}} {
		ops = append(ops, forest.BindNest(l))
	}
	for _, l := range []forest.Link{
		{ID: "application.engine", Consumer: "application", Port: "engine", Provider: "eino-text", Capability: "answer"},
		{ID: "eino-text.agent", Consumer: "eino-text", Port: "agent", Provider: "catalog/eino", Capability: "agent"},
		{ID: "forest-text.executor", Consumer: "forest-text", Port: "executor", Provider: "forest-agent", Capability: "executor"},
		{ID: "forest-tool.lookup", Consumer: "forest-tool", Port: "lookup", Provider: "catalog", Capability: "lookup"},
		{ID: "agent.model", Consumer: "forest-agent", Port: "model", Provider: "forest-model", Capability: "model"},
		{ID: "agent.tools", Consumer: "forest-agent", Port: "tools", Provider: "forest-tool", Capability: "tool"},
		{ID: "agent.checkpoint", Consumer: "forest-agent", Port: "checkpoint", Provider: "forest-checkpoint", Capability: "checkpoint"},
		{ID: "agent.events", Consumer: "forest-agent", Port: "events", Provider: "forest-events", Capability: "events"},
		{ID: "agent.approval", Consumer: "forest-agent", Port: "approval", Provider: "forest-approval", Capability: "approval"},
	} {
		ops = append(ops, forest.Connect(l))
	}
	if _, err = f.Apply(ctx, forest.ChangePlan{Operations: ops}); err != nil {
		return nil, err
	}
	return r, nil
}
func (r *Runtime) Ask(ctx context.Context, q string) (out string, err error) {
	err = r.Forest.Invoke(ctx, func(c context.Context) error {
		return forest.Use[Application](c, r.Forest, "application", "application", func(a Application) error { out, err = a.Ask(c, q); return err })
	})
	return
}
func (r *Runtime) AskEngine(ctx context.Context, engine, q string) (out string, err error) {
	id := forest.TreeID(engine + "-text")
	err = r.Forest.Invoke(ctx, func(c context.Context) error {
		return forest.Use[answerer](c, r.Forest, id, "answer", func(a answerer) error { out, err = a.Answer(c, q); return err })
	})
	return
}
func (r *Runtime) Rebind(ctx context.Context, engine string) (uint64, error) {
	if engine != "eino" && engine != "forest" {
		return r.Forest.Snapshot().Generation, errors.New("unknown engine")
	}
	return r.Forest.Apply(ctx, forest.ChangePlan{BaseGeneration: r.Forest.Snapshot().Generation, Operations: []forest.Operation{forest.Rebind(forest.Link{ID: "application.engine", Consumer: "application", Port: "engine", Provider: forest.TreeID(engine + "-text"), Capability: "answer"})}})
}
func (r *Runtime) ReplaceCatalogAndEino(ctx context.Context) (uint64, error) {
	return r.Forest.Apply(ctx, forest.ChangePlan{BaseGeneration: r.Forest.Snapshot().Generation, Operations: []forest.Operation{forest.Replace("catalog", r.nextCatalog, true), forest.Replace("catalog/eino", r.nextNative, false)}})
}
func (r *Runtime) Close(ctx context.Context) error { return r.Forest.Close(ctx) }
