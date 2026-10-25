package observe

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"sync"
	"testing"

	"github.com/Leeminjing/go-forest/forest"
)

func mount(t *testing.T, f *forest.Forest) func(*Cursor) Frame {
	t.Helper()
	d := Definition(f)
	var ref forest.Ref[Reader]
	root := forest.Definition{TypeID: "test.reader", Version: "1", Ports: []forest.PortDescriptor{forest.Port[Reader]("reader", forest.Required())}, Build: func(ctx context.Context, env *forest.BuildEnv) (forest.Lifecycle, error) {
		var err error
		ref, err = forest.Reference[Reader](env, "reader")
		return forest.Hooks{}, err
	}}
	for _, def := range []forest.Definition{d, root} {
		if err := f.Register(def); err != nil {
			t.Fatal(err)
		}
	}
	apply(t, f, forest.AddRoot("root", root.Ref()), forest.Nest("root", "inspector", d.Ref()), forest.BindNest(forest.Link{ID: "reader", Consumer: "root", Port: "reader", Provider: "inspector", Capability: "read"}))
	return func(cursor *Cursor) Frame {
		t.Helper()
		var frame Frame
		err := f.Invoke(context.Background(), func(ctx context.Context) error {
			return ref.Use(ctx, func(r Reader) error { var err error; frame, err = r.Read(ctx, cursor); return err })
		})
		if err != nil {
			t.Fatal(err)
		}
		return frame
	}
}

func apply(t *testing.T, f *forest.Forest, ops ...forest.Operation) {
	t.Helper()
	if _, err := f.Apply(context.Background(), forest.ChangePlan{BaseGeneration: f.Snapshot().Generation, Operations: ops}); err != nil {
		t.Fatal(err)
	}
}

func TestDefinitionTopologyCursorAndDetachedData(t *testing.T) {
	f := forest.New(forest.Options{})
	t.Cleanup(func() { _ = f.Close(context.Background()) })
	read := mount(t, f)
	frame := read(nil)
	if frame.Snapshot.Counts.Trees != 2 || frame.Snapshot.Counts.NestEdges != 1 || frame.Snapshot.Counts.NestBindings != 1 || frame.Snapshot.Bindings[0].Kind != "nest_binding" {
		t.Fatal(frame)
	}
	if frame.Snapshot.Trees[0].Capabilities[0].Visibility[0] != "ancestors" {
		t.Fatal(frame.Snapshot.Trees)
	}
	frame.Snapshot.Trees[0].ID = "mutated"
	frame.Events[0].Trees[0] = "mutated"
	cursor, err := ParseCursor(frame.Cursor)
	if err != nil {
		t.Fatal(err)
	}
	next := read(cursor)
	if len(next.Events) != 0 || next.Snapshot.Trees[0].ID == "mutated" {
		t.Fatal(next)
	}
	_, _ = f.Apply(context.Background(), forest.ChangePlan{BaseGeneration: 0})
	apply(t, f)
	next = read(cursor)
	if len(next.Events) != 2 || next.Events[0].Result != "conflict" || next.Events[1].Generation != 1 || next.Events[1].Sequence != 3 {
		t.Fatal(next.Events)
	}
	data, _ := json.Marshal(next)
	if !strings.Contains(string(data), `"generation":"1"`) || strings.Contains(string(data), `"Type"`) {
		t.Fatal(string(data))
	}
}

func TestGapRestartFailuresAndConcurrentRead(t *testing.T) {
	f := forest.New(forest.Options{EventCapacity: 2})
	t.Cleanup(func() { _ = f.Close(context.Background()) })
	read := mount(t, f)
	first := read(nil)
	cursor, _ := ParseCursor(first.Cursor)
	bad := forest.Definition{TypeID: "failure", Version: "1", Build: func(context.Context, *forest.BuildEnv) (forest.Lifecycle, error) {
		return nil, errors.New("secret-password")
	}}
	if err := f.Register(bad); err != nil {
		t.Fatal(err)
	}
	_, _ = f.Apply(context.Background(), forest.ChangePlan{BaseGeneration: 1, Operations: []forest.Operation{forest.AddRoot("bad", bad.Ref())}})
	apply(t, f)
	apply(t, f)
	frame := read(cursor)
	if frame.ResetReason != "history_gap" || frame.Snapshot.Generation != 1 {
		t.Fatal(frame)
	}
	other := *cursor
	other.BootID = strings.Repeat("0", 32)
	if read(&other).ResetReason != "runtime_changed" {
		t.Fatal("missing restart reset")
	}
	var wg sync.WaitGroup
	for n := 0; n < 4; n++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 20; j++ {
				read(nil)
			}
		}()
	}
	for n := 0; n < 20; n++ {
		apply(t, f)
	}
	wg.Wait()
	// A retained failure includes structured context, never the error text.
	_, _ = f.Apply(context.Background(), forest.ChangePlan{BaseGeneration: 1, Operations: []forest.Operation{forest.AddRoot("bad", bad.Ref())}})
	frame = read(nil)
	data, _ := json.Marshal(frame)
	if strings.Contains(string(data), "secret-password") || frame.Events[len(frame.Events)-1].Diagnostic.Stage != "build" {
		t.Fatal(string(data))
	}
}

type sampledSource struct {
	history  []forest.Event
	snapshot forest.Snapshot
}

func (s *sampledSource) Events() []forest.Event    { return s.history }
func (s *sampledSource) Snapshot() forest.Snapshot { return s.snapshot }

func TestPublishedTopologyDoesNotAdvanceDeliveredCursor(t *testing.T) {
	s := &sampledSource{history: []forest.Event{{Sequence: 1, Generation: 1}}, snapshot: forest.Snapshot{Generation: 2, EventSequence: 2}}
	i := &inspector{source: s, boot: strings.Repeat("a", 32)}
	frame, err := i.Read(context.Background(), nil)
	if err != nil {
		t.Fatal(err)
	}
	cursor, _ := ParseCursor(frame.Cursor)
	if cursor.Sequence != 1 || frame.Snapshot.Generation != 2 || frame.Snapshot.ObservedEventSequence != 2 {
		t.Fatal(frame)
	}
	s.history = append(s.history, forest.Event{Sequence: 2, Generation: 2})
	frame, err = i.Read(context.Background(), cursor)
	if err != nil || len(frame.Events) != 1 || frame.Events[0].Sequence != 2 {
		t.Fatal(frame, err)
	}
	cursor.Sequence = 3
	if _, err = i.Read(context.Background(), cursor); !errors.Is(err, ErrInvalidCursor) {
		t.Fatal(err)
	}
}

func TestCursorAndDiagnosticPrecision(t *testing.T) {
	boot := strings.Repeat("a", 32)
	for _, value := range []string{"", boot + ":-1", boot + ":+1", boot + ":1:2", boot + ":18446744073709551616", "bad:1"} {
		if _, err := ParseCursor(value); !errors.Is(err, ErrInvalidCursor) {
			t.Fatal(value, err)
		}
	}
	c, err := ParseCursor(boot + ":18446744073709551615")
	if err != nil || c.Sequence != ^uint64(0) {
		t.Fatal(c, err)
	}
	s := &sampledSource{snapshot: forest.Snapshot{Generation: 9007199254740993, Instances: []forest.InstanceInfo{{Error: &forest.Error{Stage: "stop", Tree: "tree", Cause: errors.New("raw-secret")}}}}}
	i := &inspector{source: s, boot: boot}
	frame, err := i.Read(context.Background(), nil)
	if err != nil {
		t.Fatal(err)
	}
	data, _ := json.Marshal(frame)
	if !strings.Contains(string(data), `"generation":"9007199254740993"`) || strings.Contains(string(data), "raw-secret") {
		t.Fatal(string(data))
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err = i.Read(ctx, nil); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
}

type clock interface{ Tick() int }
type tick int

func (t tick) Tick() int { return int(t) }

func TestCrossBranchManyBindingsPreserveOrderAndContracts(t *testing.T) {
	f := forest.New(forest.Options{})
	t.Cleanup(func() { _ = f.Close(context.Background()) })
	read := mount(t, f)
	owner := forest.Definition{TypeID: "owner", Version: "1", ExportChildren: true, Build: func(context.Context, *forest.BuildEnv) (forest.Lifecycle, error) { return forest.Hooks{}, nil }}
	provider := forest.Definition{TypeID: "clock", Version: "1", Capabilities: []forest.CapabilityDescriptor{forest.Capability[clock]("tick", forest.Public, forest.Contract{ID: "time", Version: 2})}, Build: func(ctx context.Context, env *forest.BuildEnv) (forest.Lifecycle, error) {
		return forest.Hooks{}, forest.Provide(env, "tick", tick(1))
	}}
	consumer := forest.Definition{TypeID: "consumer", Version: "1", Ports: []forest.PortDescriptor{forest.Port[clock]("clocks", forest.Required(), forest.Many(), forest.RequiresContract("time", 1, 2)), forest.Port[clock]("startup", forest.During(forest.Startup))}, Build: func(context.Context, *forest.BuildEnv) (forest.Lifecycle, error) { return forest.Hooks{}, nil }}
	for _, d := range []forest.Definition{owner, provider, consumer} {
		if err := f.Register(d); err != nil {
			t.Fatal(err)
		}
	}
	apply(t, f, forest.AddRoot("group", owner.Ref()), forest.Nest("group", "a", provider.Ref()), forest.Nest("group", "b", provider.Ref()), forest.AddRoot("consumer", consumer.Ref()), forest.Connect(forest.Link{ID: "z-first", Consumer: "consumer", Port: "clocks", Provider: "b", Capability: "tick"}), forest.Connect(forest.Link{ID: "a-second", Consumer: "consumer", Port: "clocks", Provider: "a", Capability: "tick"}))
	frame := read(nil)
	if frame.Snapshot.Counts.Trees != 6 || frame.Snapshot.Counts.NestEdges != 3 || frame.Snapshot.Counts.Links != 2 || frame.Snapshot.Bindings[1].ID != "z-first" || frame.Snapshot.Bindings[2].ID != "a-second" {
		t.Fatal(frame.Snapshot)
	}
	for _, tree := range frame.Snapshot.Trees {
		if tree.ID == "consumer" {
			if !tree.Ports[0].Many || tree.Ports[0].ContractID != "time" || tree.Ports[0].MaxVersion != 2 || tree.Ports[1].Phase != "startup" {
				t.Fatal(tree)
			}
		}
	}
}
