package consumer_test

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/Leeminjing/go-forest/forest"
)

type number interface{ Number() int }
type value int

func (v value) Number() int { return int(v) }

type reader interface {
	Read(context.Context) (int, error)
}
type consumer struct{ input forest.Ref[number] }

func (c *consumer) Read(ctx context.Context) (out int, err error) {
	err = c.input.Use(ctx, func(v number) error { out = v.Number(); return nil })
	return
}
func fixture(t testing.TB, extra int, stopped chan struct{}) *forest.Forest {
	t.Helper()
	f := forest.New(forest.Options{})
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if e := f.Close(ctx); e != nil {
			t.Error(e)
		}
	})
	for i := 1; i <= 3; i++ {
		i := i
		d := forest.Definition{TypeID: "number", Version: fmt.Sprint(i), HotSwappable: true, Capabilities: []forest.CapabilityDescriptor{forest.Capability[number]("value", forest.Public)}, Build: func(_ context.Context, e *forest.BuildEnv) (forest.Lifecycle, error) {
			return forest.Hooks{OnStop: func(context.Context) error {
				if i == 1 && stopped != nil {
					close(stopped)
				}
				return nil
			}}, forest.Provide(e, "value", value(i))
		}}
		if e := f.Register(d); e != nil {
			t.Fatal(e)
		}
	}
	d := forest.Definition{TypeID: "consumer", Version: "1", Ports: []forest.PortDescriptor{forest.Port[number]("input", forest.Required())}, Capabilities: []forest.CapabilityDescriptor{forest.Capability[reader]("read", forest.Public)}, Build: func(_ context.Context, e *forest.BuildEnv) (forest.Lifecycle, error) {
		r, err := forest.Reference[number](e, "input")
		if err != nil {
			return nil, err
		}
		return forest.Hooks{}, forest.Provide(e, "read", &consumer{r})
	}}
	u := forest.Definition{TypeID: "unrelated", Version: "1", Ports: []forest.PortDescriptor{forest.Port[number]("many", forest.Many())}, Build: func(context.Context, *forest.BuildEnv) (forest.Lifecycle, error) { return forest.Hooks{}, nil }}
	for _, def := range []forest.Definition{d, u} {
		if e := f.Register(def); e != nil {
			t.Fatal(e)
		}
	}
	ops := []forest.Operation{forest.AddRoot("provider", forest.DefinitionRef{TypeID: "number", Version: "1"}), forest.AddRoot("other", forest.DefinitionRef{TypeID: "number", Version: "3"}), forest.AddRoot("consumer", d.Ref()), forest.AddRoot("unrelated", u.Ref()), forest.Connect(forest.Link{ID: "input", Consumer: "consumer", Port: "input", Provider: "provider", Capability: "value"})}
	for i := 0; i < extra; i++ {
		ops = append(ops, forest.Connect(forest.Link{ID: fmt.Sprint("extra-", i), Consumer: "unrelated", Port: "many", Provider: "other", Capability: "value"}))
	}
	if _, e := f.Apply(context.Background(), forest.ChangePlan{Operations: ops}); e != nil {
		t.Fatal(e)
	}
	return f
}
func read(ctx context.Context, f *forest.Forest) (out int, err error) {
	err = forest.Use[reader](ctx, f, "consumer", "read", func(r reader) error { out, err = r.Read(ctx); return err })
	return
}
func TestPublishedCorePinsOldCallsAndDrainsAfterActualReturn(t *testing.T) {
	stopped := make(chan struct{})
	f := fixture(t, 32, stopped)
	entered, release := make(chan struct{}), make(chan struct{})
	done := make(chan error, 1)
	var once sync.Once
	defer once.Do(func() { close(release) })
	go func() {
		done <- f.Invoke(context.Background(), func(ctx context.Context) error {
			first, e := read(ctx, f)
			if e != nil {
				return e
			}
			g, e := f.Generation(ctx)
			if e != nil {
				return e
			}
			close(entered)
			<-release
			second, e := read(ctx, f)
			if e != nil {
				return e
			}
			after, e := f.Generation(ctx)
			if e != nil {
				return e
			}
			if first != 1 || second != 1 || g != 1 || after != 1 {
				return fmt.Errorf("old call mixed %d/%d/%d/%d", first, second, g, after)
			}
			return nil
		})
	}()
	select {
	case <-entered:
	case e := <-done:
		t.Fatalf("old call failed: %v", e)
	case <-time.After(5 * time.Second):
		t.Fatal("no entry")
	}
	if _, e := f.Apply(context.Background(), forest.ChangePlan{BaseGeneration: 1, Operations: []forest.Operation{forest.Replace("provider", forest.DefinitionRef{TypeID: "number", Version: "2"}, false)}}); e != nil {
		t.Fatal(e)
	}
	check := func(expected int) {
		t.Helper()
		e := f.Invoke(context.Background(), func(ctx context.Context) error {
			v, e := read(ctx, f)
			if e != nil {
				return e
			}
			if v != expected {
				return fmt.Errorf("got %d want %d", v, expected)
			}
			return nil
		})
		if e != nil {
			t.Fatal(e)
		}
	}
	check(2)
	if _, e := f.Apply(context.Background(), forest.ChangePlan{BaseGeneration: 2, Operations: []forest.Operation{forest.Rebind(forest.Link{ID: "input", Consumer: "consumer", Port: "input", Provider: "other", Capability: "value"})}}); e != nil {
		t.Fatal(e)
	}
	check(3)
	select {
	case <-stopped:
		t.Fatal("released while old call active")
	default:
	}
	once.Do(func() { close(release) })
	select {
	case e := <-done:
		if e != nil {
			t.Fatal(e)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("old call did not return")
	}
	select {
	case <-stopped:
	case <-time.After(5 * time.Second):
		t.Fatal("old instance not stopped")
	}
}
func TestPublishedLookupAllocationDoesNotGrowWithUnrelatedBindings(t *testing.T) {
	allocated := func(extra int) int64 {
		f := fixture(t, extra, nil)
		b := testing.Benchmark(func(b *testing.B) {
			e := f.Invoke(context.Background(), func(ctx context.Context) error {
				return forest.Use[reader](ctx, f, "consumer", "read", func(r reader) error {
					b.ReportAllocs()
					b.ResetTimer()
					for i := 0; i < b.N; i++ {
						v, e := r.Read(ctx)
						if e != nil {
							return e
						}
						if v != 1 {
							return fmt.Errorf("actual result %d", v)
						}
					}
					b.StopTimer()
					return nil
				})
			})
			if e != nil {
				b.Fatal(e)
			}
		})
		return b.AllocedBytesPerOp()
	}
	small, large := allocated(0), allocated(1024)
	if large > small+1024 {
		t.Fatalf("lookup allocation scaled with unrelated edges: %d -> %d", small, large)
	}
	t.Logf("actual published lookup allocations: %d -> %d B/op", small, large)
}
