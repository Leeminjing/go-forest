// Command demo captures a small nested application, restores it, reconfigures
// and rolls it back, then reconstructs it with a new Forest and session.
package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"

	topology "github.com/Leeminjing/go-forest/extensions/topology"
	"github.com/Leeminjing/go-forest/forest"
)

// The composing tree owns its dependency; the provider uses Go's implicit
// interface satisfaction. Forest has no knowledge of this application contract.
type message interface {
	Fetch(context.Context) (string, error)
}
type text string

func (v text) Fetch(context.Context) (string, error) { return string(v), nil }

type forwarding struct{ ref forest.Ref[message] }

func (v forwarding) Fetch(ctx context.Context) (string, error) {
	var out string
	e := v.ref.Use(ctx, func(m message) error { var err error; out, err = m.Fetch(ctx); return err })
	return out, e
}
func identity(value string) string {
	sum := sha256.Sum256([]byte(value))
	return hex.EncodeToString(sum[:])
}

type evidence struct {
	Before, After, Rollback, Cold string
	Generations                   []uint64
	ColdGeneration                uint64
	NewSession                    bool
}

func run() (evidence, error) {
	ctx := context.Background()
	var result evidence
	entries := []topology.Registration{}
	root := forest.Definition{TypeID: "bundle", Version: "1", HotSwappable: true, PreserveChildren: true, Ports: []forest.PortDescriptor{forest.Port[message]("message", forest.Required())}, Capabilities: []forest.CapabilityDescriptor{forest.Capability[message]("message", forest.Public)}, Build: func(_ context.Context, env *forest.BuildEnv) (forest.Lifecycle, error) {
		ref, e := forest.Reference[message](env, "message")
		if e != nil {
			return nil, e
		}
		return forest.Hooks{}, forest.Provide(env, "message", forwarding{ref})
	}}
	entries = append(entries, topology.Registration{Definition: root, ContractKey: "bundle/1", ImplementationDigest: identity("bundle/1")})
	for _, version := range []string{"1", "2"} {
		version := version
		d := forest.Definition{TypeID: "message", Version: version, HotSwappable: true, Capabilities: []forest.CapabilityDescriptor{forest.Capability[message]("message", forest.Ancestors)}, Build: func(_ context.Context, env *forest.BuildEnv) (forest.Lifecycle, error) {
			return forest.Hooks{}, forest.Provide(env, "message", text("v"+version))
		}}
		entries = append(entries, topology.Registration{Definition: d, ContractKey: "message/1", ImplementationDigest: identity("message/" + version)})
	}
	catalog, e := topology.NewCatalog(entries)
	if e != nil {
		return result, e
	}
	source := forest.New(forest.Options{})
	defer source.Close(ctx)
	for _, r := range entries {
		if e = source.Register(r.Definition); e != nil {
			return result, e
		}
	}
	_, e = source.Apply(ctx, forest.ChangePlan{Operations: []forest.Operation{forest.AddRoot("app", root.Ref()), forest.Nest("app", "worker", entries[1].Definition.Ref()), forest.BindNest(forest.Link{ID: "internal-message", Consumer: "app", Port: "message", Provider: "worker", Capability: "message"})}})
	if e != nil {
		return result, e
	}
	manifest, e := topology.Capture(source.Snapshot(), catalog)
	if e != nil {
		return result, e
	}
	if e = source.Close(ctx); e != nil {
		return result, e
	}
	directory, e := os.MkdirTemp("", "forest-topology-demo-")
	if e != nil {
		return result, e
	}
	defer os.RemoveAll(directory)
	store, e := topology.Open(directory, topology.Options{})
	if e != nil {
		return result, e
	}
	defer store.Close()
	seed, e := store.Seed(ctx, topology.Request{ID: "seed", Expected: store.Inspect().Position, Approval: "captured offline application"}, manifest)
	if e != nil {
		return result, e
	}
	f := forest.New(forest.Options{})
	controller, e := topology.NewController(f, store, catalog)
	if e != nil {
		return result, e
	}
	defer controller.Close(ctx)
	first, e := controller.Restore(ctx, topology.Request{ID: "boot-1", Expected: store.Inspect().Position, Approval: "allow new objects; original application exited"})
	if e != nil {
		return result, e
	}
	result.Generations = append(result.Generations, first.Generation)
	fetch := func(c *topology.Controller, f *forest.Forest) (string, error) {
		if e := c.OpenAdmission(ctx); e != nil {
			return "", e
		}
		var value string
		e := c.Invoke(ctx, func(call context.Context) error {
			return forest.Use[message](call, f, "app", "message", func(m message) error { var err error; value, err = m.Fetch(call); return err })
		})
		return value, e
	}
	result.Before, e = fetch(controller, f)
	if e != nil {
		return result, e
	}
	encoded, e := topology.Encode(manifest)
	if e != nil {
		return result, e
	}
	target, e := topology.Decode(encoded)
	if e != nil {
		return result, e
	}
	for i := range target.Trees {
		if target.Trees[i].ID == "worker" {
			target.Trees[i].Definition = entries[2].Definition.Ref()
			target.Trees[i].ImplementationDigest = entries[2].ImplementationDigest
		}
	}
	proposal, e := controller.Preview(target)
	if e != nil {
		return result, e
	}
	second, e := controller.Reconfigure(ctx, topology.Request{ID: "upgrade", Expected: proposal.Expected, Approval: "full rebuild of target application"}, proposal)
	if e != nil {
		return result, e
	}
	result.Generations = append(result.Generations, second.Generation)
	result.After, e = fetch(controller, f)
	if e != nil {
		return result, e
	}
	proposal, e = controller.PreviewRollback(seed.Revision)
	if e != nil {
		return result, e
	}
	third, e := controller.Rollback(ctx, topology.Request{ID: "rollback", Expected: proposal.Expected, Approval: "topology only; business effects remain"}, proposal)
	if e != nil {
		return result, e
	}
	result.Generations = append(result.Generations, third.Generation)
	result.Rollback, e = fetch(controller, f)
	if e != nil {
		return result, e
	}
	if e = controller.Close(ctx); e != nil {
		return result, e
	}
	store, e = topology.Open(directory, topology.Options{})
	if e != nil {
		return result, e
	}
	defer store.Close()
	fresh := forest.New(forest.Options{})
	restarted, e := topology.NewController(fresh, store, catalog)
	if e != nil {
		return result, e
	}
	defer restarted.Close(ctx)
	cold, e := restarted.Restore(ctx, topology.Request{ID: "boot-2", Expected: store.Inspect().Position, Approval: "prior session joined; allow recreation"})
	if e != nil {
		return result, e
	}
	result.ColdGeneration = cold.Generation
	result.NewSession = cold.SessionID != first.SessionID
	result.Cold, e = fetch(restarted, fresh)
	return result, errors.Join(e, restarted.Close(ctx))
}
func main() {
	result, e := run()
	if e != nil {
		fmt.Fprintln(os.Stderr, e)
		os.Exit(1)
	}
	if e = json.NewEncoder(os.Stdout).Encode(result); e != nil {
		fmt.Fprintln(os.Stderr, e)
		os.Exit(1)
	}
}
