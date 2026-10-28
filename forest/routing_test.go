package forest

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"runtime"
	"sync"
	"testing"
	"time"
)

type routeNumber interface{ Number() int }
type routeValue int

func (v routeValue) Number() int { return int(v) }

type routingFixture struct {
	f                *Forest
	one, many, other Ref[routeNumber]
	consumer         Definition
	providers        []Definition
}

func newRoutingFixture(t testing.TB, extra int) *routingFixture {
	t.Helper()
	x := &routingFixture{f: New(Options{})}
	for i := 1; i <= 3; i++ {
		i := i
		d := Definition{TypeID: "route.value", Version: fmt.Sprint(i), HotSwappable: true, Capabilities: []CapabilityDescriptor{Capability[routeNumber]("value", Public|Descendants|Ancestors)}, Build: func(_ context.Context, e *BuildEnv) (Lifecycle, error) {
			return Hooks{}, Provide(e, "value", routeValue(i))
		}}
		if e := x.f.Register(d); e != nil {
			t.Fatal(e)
		}
		x.providers = append(x.providers, d)
	}
	x.consumer = Definition{TypeID: "route.consumer", Version: "1", HotSwappable: true, Ports: []PortDescriptor{Port[routeNumber]("one", Required()), Port[routeNumber]("many", Many()), Port[routeNumber]("other")}, Build: func(_ context.Context, e *BuildEnv) (Lifecycle, error) {
		var err error
		x.one, err = Reference[routeNumber](e, "one")
		if err != nil {
			return nil, err
		}
		x.many, err = Reference[routeNumber](e, "many")
		if err != nil {
			return nil, err
		}
		x.other, err = Reference[routeNumber](e, "other")
		return Hooks{}, err
	}}
	unrelated := Definition{TypeID: "route.unrelated", Version: "1", Ports: []PortDescriptor{Port[routeNumber]("many", Many())}, Build: func(context.Context, *BuildEnv) (Lifecycle, error) { return Hooks{}, nil }}
	for _, d := range []Definition{x.consumer, unrelated} {
		if e := x.f.Register(d); e != nil {
			t.Fatal(e)
		}
	}
	ops := []Operation{AddRoot("p", x.providers[0].Ref()), AddRoot("q", x.providers[1].Ref()), AddRoot("consumer", x.consumer.Ref()), AddRoot("unrelated", unrelated.Ref()), Connect(Link{ID: "one", Consumer: "consumer", Port: "one", Provider: "p", Capability: "value"}), Connect(Link{ID: "many-a", Consumer: "consumer", Port: "many", Provider: "q", Capability: "value"}), Connect(Link{ID: "many-b", Consumer: "consumer", Port: "many", Provider: "p", Capability: "value"})}
	for i := 0; i < extra; i++ {
		ops = append(ops, Connect(Link{ID: fmt.Sprintf("unrelated-%d", i), Consumer: "unrelated", Port: "many", Provider: "p", Capability: "value"}))
	}
	if _, e := x.f.Apply(context.Background(), ChangePlan{Operations: ops}); e != nil {
		t.Fatal(e)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if e := x.f.Close(ctx); e != nil {
			t.Error(e)
		}
	})
	return x
}
func routeRead(ctx context.Context, r Ref[routeNumber]) (int, error) {
	var n int
	e := r.Use(ctx, func(v routeNumber) error { n = v.Number(); return nil })
	return n, e
}
func routeReadMany(ctx context.Context, r Ref[routeNumber]) ([]int, error) {
	out := []int{}
	e := r.UseAll(ctx, func(values []routeNumber) error {
		for _, v := range values {
			out = append(out, v.Number())
		}
		if len(values) > 1 {
			values[0] = values[1]
		}
		return nil
	})
	return out, e
}

func TestRoutesKeepOldBindingsAndManyOrderAcrossRebindAndReplace(t *testing.T) {
	x := newRoutingFixture(t, 64)
	one, many := x.one, x.many
	entered, release := make(chan struct{}), make(chan struct{})
	done := make(chan error, 1)
	var releaseOnce sync.Once
	defer releaseOnce.Do(func() { close(release) })
	go func() {
		done <- x.f.Invoke(context.Background(), func(ctx context.Context) error {
			for i := 0; i < 2; i++ {
				n, e := routeRead(ctx, one)
				if e != nil || n != 1 {
					return fmt.Errorf("old single %d: %w", n, e)
				}
				values, e := routeReadMany(ctx, many)
				if e != nil || !reflect.DeepEqual(values, []int{2, 1}) {
					return fmt.Errorf("old many %v: %w", values, e)
				}
				if i == 0 {
					close(entered)
					<-release
				}
			}
			return nil
		})
	}()
	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("old call did not enter")
	}
	_, e := x.f.Apply(context.Background(), ChangePlan{BaseGeneration: 1, Operations: []Operation{Rebind(Link{ID: "one", Consumer: "consumer", Port: "one", Provider: "q", Capability: "value"}), Unlink("many-a"), Connect(Link{ID: "many-a", Consumer: "consumer", Port: "many", Provider: "q", Capability: "value"}), Replace("q", x.providers[2].Ref(), false)}})
	if e != nil {
		t.Fatal(e)
	}
	e = x.f.Invoke(context.Background(), func(ctx context.Context) error {
		n, e := routeRead(ctx, one)
		if e != nil || n != 3 {
			return fmt.Errorf("new single %d: %w", n, e)
		}
		for i := 0; i < 2; i++ {
			values, e := routeReadMany(ctx, many)
			if e != nil || !reflect.DeepEqual(values, []int{1, 3}) {
				return fmt.Errorf("new many %v: %w", values, e)
			}
		}
		return nil
	})
	if e != nil {
		t.Fatal(e)
	}
	// A newer consumer may not use a Ref owned by the original physical consumer.
	if _, e = x.f.Apply(context.Background(), ChangePlan{BaseGeneration: 2, Operations: []Operation{Replace("consumer", x.consumer.Ref(), false)}}); e != nil {
		t.Fatal(e)
	}
	e = x.f.Invoke(context.Background(), func(ctx context.Context) error { _, e := routeRead(ctx, one); return e })
	if !errors.Is(e, ErrBindingUnavailable) {
		t.Fatal("stale Ref accepted", e)
	}
	// Allow the old call to finish and join without keeping its context in the test.
	releaseOnce.Do(func() { close(release) })
	select {
	case e = <-done:
		if e != nil {
			t.Fatal(e)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("old call did not join")
	}
}

func TestCandidateRoutesServeConstructionStartupAndRejectRuntimeEarly(t *testing.T) {
	f := New(Options{})
	defer f.Close(context.Background())
	provider := Definition{TypeID: "route.owner", Version: "1", ExportChildren: true, Capabilities: []CapabilityDescriptor{Capability[routeNumber]("value", Public|Descendants)}, Build: func(_ context.Context, e *BuildEnv) (Lifecycle, error) {
		return Hooks{}, Provide(e, "value", routeValue(7))
	}}
	if e := f.Register(provider); e != nil {
		t.Fatal(e)
	}
	for _, phase := range []Phase{Construction, Startup, Runtime} {
		phase := phase
		d := Definition{TypeID: fmt.Sprint("route.phase-", phase), Version: "1", ExportChildren: true, Ports: []PortDescriptor{Port[routeNumber]("input", Required(), During(phase))}, Build: func(ctx context.Context, e *BuildEnv) (Lifecycle, error) {
			ref, err := Reference[routeNumber](e, "input")
			if err != nil {
				return nil, err
			}
			check := func(ctx context.Context) error {
				n, err := routeRead(ctx, ref)
				if err != nil || n != 7 {
					return fmt.Errorf("candidate phase %d = %d: %w", phase, n, err)
				}
				return nil
			}
			if phase == Construction {
				if err = check(ctx); err != nil {
					return nil, err
				}
			}
			if phase == Runtime {
				if _, err = routeRead(ctx, ref); !errors.Is(err, ErrBindingUnavailable) {
					return nil, errors.New("Runtime available during construction")
				}
			}
			return Hooks{OnStart: func(ctx context.Context) error {
				if phase == Startup {
					return check(ctx)
				}
				return nil
			}}, nil
		}}
		if e := f.Register(d); e != nil {
			t.Fatal(e)
		}
	}
	ops := []Operation{AddRoot("parent", provider.Ref()), Nest("parent", "child", DefinitionRef{TypeID: "route.phase-1", Version: "1"}), Nest("child", "grand", DefinitionRef{TypeID: "route.phase-2", Version: "1"}), AddRoot("runtime", DefinitionRef{TypeID: "route.phase-0", Version: "1"})}
	for _, id := range []TreeID{"child", "grand"} {
		ops = append(ops, BindNest(Link{ID: string(id), Consumer: id, Port: "input", Provider: "parent", Capability: "value"}))
	}
	ops = append(ops, Connect(Link{ID: "runtime", Consumer: "runtime", Port: "input", Provider: "parent", Capability: "value"}))
	if _, e := f.Apply(context.Background(), ChangePlan{Operations: ops}); e != nil {
		t.Fatal(e)
	}
}

func TestSinglePortAllocationDoesNotScaleWithUnrelatedEdges(t *testing.T) {
	bytes := func(extra int) int64 {
		x := newRoutingFixture(t, extra)
		r := x.one
		result := testing.Benchmark(func(b *testing.B) {
			e := x.f.Invoke(context.Background(), func(ctx context.Context) error {
				b.ReportAllocs()
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					n, e := routeRead(ctx, r)
					if e != nil || n != 1 {
						return fmt.Errorf("actual read %d: %w", n, e)
					}
				}
				b.StopTimer()
				return nil
			})
			if e != nil {
				b.Fatal(e)
			}
		})
		if result.N == 0 {
			t.Fatal("benchmark did not run")
		}
		return result.AllocedBytesPerOp()
	}
	small, large := bytes(0), bytes(1024)
	if large > small+1024 {
		t.Fatalf("single-port allocation scales with unrelated bindings: %d -> %d B/op", small, large)
	}
}
func TestRoutesOwnTheirDescriptorsAndRetiredDiagnosticsDropImplementations(t *testing.T) {
	x := newRoutingFixture(t, 32)
	old := x.f.current.Load()
	before := old.nodes["p"].instance
	key := routeKey{consumer: "consumer", port: "one"}
	if _, e := x.f.Apply(context.Background(), ChangePlan{BaseGeneration: 1, Operations: []Operation{Replace("p", x.providers[2].Ref(), false)}}); e != nil {
		t.Fatal(e)
	}
	next := x.f.current.Load()
	if &old.routes[key][0] == &next.routes[key][0] {
		t.Fatal("graphs share writable route storage")
	}
	if old.refs != 0 {
		t.Fatal("unoccupied old graph still pinned", old.refs)
	}
	// Diagnostic records remain, but successful Stop must eventually release
	// implementation references. Wait/Stopped can precede the final gate section.
	deadline := time.Now().Add(5 * time.Second)
	for {
		x.f.gate.RLock()
		cleared := before.object == nil && before.capabilities == nil && before.dependencies == nil
		active := len(x.f.scopes)
		x.f.gate.RUnlock()
		if cleared {
			if before.info().State != Stopped || active != 0 {
				t.Fatal("incorrect retirement evidence")
			}
			x.f.observe.Lock()
			retained := x.f.instances[before.id] == before
			x.f.observe.Unlock()
			if !retained {
				t.Fatal("retired diagnostic was removed")
			}
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("stopped diagnostic retained implementation")
		}
		runtime.Gosched()
	}
	e := x.f.Invoke(context.Background(), func(ctx context.Context) error {
		if _, e := routeRead(ctx, x.other); !errors.Is(e, ErrBindingUnavailable) {
			return fmt.Errorf("optional result: %v", e)
		}
		if e := x.one.UseAll(ctx, func([]routeNumber) error { return nil }); !errors.Is(e, ErrContractMismatch) {
			return fmt.Errorf("wrong cardinality: %v", e)
		}
		return nil
	})
	if e != nil {
		t.Fatal(e)
	}
}

// BenchmarkRouteIndexBuild times only index construction using descriptors from
// a real validated Forest: two small fixed ports and one unrelated Many port.
// Clone/validation are excluded from this construction-only measurement.
func BenchmarkRouteIndexBuild(b *testing.B) {
	for _, edges := range []int{16, 256, 2048, 10001} {
		b.Run(fmt.Sprint(edges), func(b *testing.B) {
			x := newRoutingFixture(b, edges-3)
			bindings := x.f.current.Load().bindings
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				candidate := &graph{bindings: bindings}
				candidate.indexRoutes()
				if len(candidate.routes) != 3 {
					b.Fatal("incorrect route groups")
				}
			}
		})
	}
}
