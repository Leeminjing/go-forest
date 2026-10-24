package tests

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"go-forest/forest"
)

func TestRetainedLifecycleDependencyRejectsReusedBindingID(t *testing.T) {
	for _, phase := range []struct {
		name  string
		value forest.Phase
	}{{"construction", forest.Construction}, {"startup", forest.Startup}} {
		for _, scenario := range []string{"different-consumer", "different-port", "nest-binding"} {
			t.Run(phase.name+"/"+scenario, func(t *testing.T) {
				f := newForest(t)
				provider := register(t, f, clockDef("1", "available", forest.Public|forest.Descendants))
				consumer := printerDef(false)
				consumer.Ports[0].Phase = phase.value
				if scenario == "different-port" {
					consumer.Ports = append(consumer.Ports, forest.Port[clock]("runtime"))
				}
				definition := register(t, f, consumer)
				oldLink := binding("link-1", "a", "clock", "provider", "now")
				newLink := binding("link-1", "b", "clock", "provider", "now")
				initial := []forest.Operation{forest.AddRoot("provider", provider)}
				var operations []forest.Operation
				switch scenario {
				case "different-consumer":
					initial = append(initial, forest.AddRoot("a", definition), forest.AddRoot("b", definition), forest.Connect(oldLink))
					operations = []forest.Operation{forest.Remove("a"), forest.Connect(newLink)}
				case "different-port":
					oldLink.Consumer, oldLink.Port = "b", "runtime"
					initial = append(initial, forest.AddRoot("b", definition), forest.Connect(oldLink))
					operations = []forest.Operation{forest.Unlink("link-1"), forest.Connect(newLink)}
				case "nest-binding":
					initial = append(initial, forest.Nest("provider", "a", definition), forest.Nest("provider", "b", definition), forest.BindNest(oldLink))
					operations = []forest.Operation{forest.Remove("a"), forest.BindNest(newLink)}
				}
				apply(t, f, initial...)
				before := f.Snapshot()
				generation, err := f.Apply(context.Background(), forest.ChangePlan{BaseGeneration: before.Generation, Operations: operations})
				if !errors.Is(err, forest.ErrDependencyInUse) {
					t.Fatalf("retained consumer accepted a new lifecycle binding with a reused ID: generation=%d err=%v", generation, err)
				}
				var detail *forest.Error
				if !errors.As(err, &detail) || detail.Tree != "b" || detail.Port != "clock" || detail.Stage != "retained-resource" {
					t.Fatalf("missing consumer/port error context: %v", err)
				}
				after := f.Snapshot()
				if generation != before.Generation || after.Generation != before.Generation || len(after.Trees) != len(before.Trees) || after.Trees["b"].InstanceID != before.Trees["b"].InstanceID {
					t.Fatal("rejected plan changed the generation, topology, or consumer instance")
				}
				links := after.Links
				if scenario == "nest-binding" {
					links = after.NestBindings
				}
				if links["link-1"].Link != oldLink {
					t.Fatal("rejected plan changed the original binding")
				}
				// The same plan is legal when the lifecycle consumer is rebuilt.
				operations = append(operations, forest.Replace("b", definition, false))
				apply(t, f, operations...)
				after = f.Snapshot()
				if after.Generation != before.Generation+1 || after.Trees["b"].InstanceID == before.Trees["b"].InstanceID {
					t.Fatal("consumer was not rebuilt atomically")
				}
				links = after.Links
				if scenario == "nest-binding" {
					links = after.NestBindings
				}
				if links["link-1"].Link != newLink {
					t.Fatal("replacement did not receive the new binding")
				}
			})
		}
	}
}

func TestRetainedLifecycleDependencyPreservesProviderOrder(t *testing.T) {
	for _, phase := range []struct {
		name  string
		value forest.Phase
	}{{"construction", forest.Construction}, {"startup", forest.Startup}} {
		t.Run(phase.name, func(t *testing.T) {
			f := newForest(t)
			first := register(t, f, clockDef("1", "first", forest.Public))
			second := register(t, f, clockDef("2", "second", forest.Public))
			consumer := printerDef(false)
			consumer.Ports[0] = forest.Port[clock]("clock", forest.Many(), forest.During(phase.value))
			consumer.Build = func(ctx context.Context, env *forest.BuildEnv) (forest.Lifecycle, error) {
				ref, err := forest.Reference[clock](env, "clock")
				if err != nil {
					return nil, err
				}
				return forest.Hooks{}, forest.Provide(env, "print", multiPrinter{ref})
			}
			definition := register(t, f, consumer)
			link := binding("first", "printer", "clock", "first", "now")
			apply(t, f, forest.AddRoot("first", first), forest.AddRoot("second", second), forest.AddRoot("printer", definition), forest.Connect(link), forest.Connect(binding("second", "printer", "clock", "second", "now")))
			if got := readPrint(t, f); got != "first,second" {
				t.Fatal(got)
			}
			operations := []forest.Operation{forest.Unlink(link.ID), forest.Connect(link)}
			generation, err := f.Apply(context.Background(), forest.ChangePlan{BaseGeneration: 1, Operations: operations})
			if !errors.Is(err, forest.ErrDependencyInUse) || generation != 1 || f.Snapshot().Generation != 1 {
				t.Fatalf("changed lifecycle dependency order: generation=%d err=%v", generation, err)
			}
			if got := readPrint(t, f); got != "first,second" {
				t.Fatalf("rejected plan changed provider order: %s", got)
			}
			apply(t, f, append(operations, forest.Replace("printer", definition, false))...)
			if got := readPrint(t, f); got != "second,first" {
				t.Fatalf("replacement did not receive the new provider order: %s", got)
			}
		})
	}
}

func TestLifecycleConsumerRetainsChildAndOwnerUntilStop(t *testing.T) {
	f := newForest(t)
	var parentStopped, childStopped atomic.Bool
	parent := emptyDef()
	parent.TypeID = "owner"
	parent.ExportChildren = true
	parent.Build = func(context.Context, *forest.BuildEnv) (forest.Lifecycle, error) {
		return forest.Hooks{OnStop: func(context.Context) error { parentStopped.Store(true); return nil }}, nil
	}
	child := clockDef("1", "child", forest.Public)
	childBuild := child.Build
	child.Build = func(ctx context.Context, env *forest.BuildEnv) (forest.Lifecycle, error) {
		_, err := childBuild(ctx, env)
		return forest.Hooks{OnStop: func(context.Context) error { childStopped.Store(true); return nil }}, err
	}
	consumer := printerDef(true)
	consumer.Ports[0].Phase = forest.Construction
	entered, release := make(chan struct{}), make(chan struct{})
	consumerBuild := consumer.Build
	consumer.Build = func(ctx context.Context, env *forest.BuildEnv) (forest.Lifecycle, error) {
		_, err := consumerBuild(ctx, env)
		return forest.Hooks{OnStop: func(context.Context) error { close(entered); <-release; return nil }}, err
	}
	p := register(t, f, parent)
	c := register(t, f, child)
	r := register(t, f, consumer)
	apply(t, f, forest.AddRoot("owner", p), forest.Nest("owner", "child", c), forest.AddRoot("printer", r), forest.Connect(binding("l", "printer", "clock", "child", "now")))
	apply(t, f, forest.Remove("printer"), forest.RemoveSubtree("owner"))
	receive(t, entered)
	if parentStopped.Load() || childStopped.Load() {
		close(release)
		t.Fatal("resources released while lifecycle consumer still uses child")
	}
	close(release)
	wait(t, f)
	if !childStopped.Load() || !parentStopped.Load() {
		t.Fatal("resources were not eventually released")
	}
}

func TestReversedOwnershipAcrossPinnedVersionsDoesNotLeak(t *testing.T) {
	f := newForest(t)
	var aStopped, bStopped atomic.Bool
	a := emptyDef()
	a.TypeID = "a"
	a.Build = func(context.Context, *forest.BuildEnv) (forest.Lifecycle, error) {
		return forest.Hooks{OnStop: func(context.Context) error { aStopped.Store(true); return nil }}, nil
	}
	b := emptyDef()
	b.TypeID = "b"
	b.Build = func(context.Context, *forest.BuildEnv) (forest.Lifecycle, error) {
		return forest.Hooks{OnStop: func(context.Context) error {
			if !aStopped.Load() {
				return errors.New("last child has not stopped")
			}
			bStopped.Store(true)
			return nil
		}}, nil
	}
	da, db := register(t, f, a), register(t, f, b)
	apply(t, f, forest.AddRoot("a", da), forest.Nest("a", "b", db))
	pin := func() (chan struct{}, chan error) {
		entered, release := make(chan struct{}), make(chan struct{})
		result := make(chan error, 1)
		go func() {
			result <- f.Invoke(context.Background(), func(context.Context) error { close(entered); <-release; return nil })
		}()
		receive(t, entered)
		return release, result
	}
	release1, result1 := pin()
	apply(t, f, forest.Unnest("b"), forest.Move("a", "b"))
	release2, result2 := pin()
	apply(t, f, forest.RemoveSubtree("b"))
	close(release1)
	if err := receive(t, result1); err != nil {
		t.Fatal(err)
	}
	close(release2)
	if err := receive(t, result2); err != nil {
		t.Fatal(err)
	}
	wait(t, f)
	if !aStopped.Load() || !bStopped.Load() {
		t.Fatal("retired ownership cycle leaked instances")
	}
}

func TestPreparationJoinsEnteredCallbacksBeforeRollback(t *testing.T) {
	f := newForest(t)
	providerStopped := make(chan struct{})
	provider := clockDef("1", "value", forest.Public)
	build := provider.Build
	provider.Build = func(ctx context.Context, env *forest.BuildEnv) (forest.Lifecycle, error) {
		_, err := build(ctx, env)
		return forest.Hooks{OnStop: func(context.Context) error { close(providerStopped); return nil }}, err
	}
	entered, release, returned := make(chan struct{}), make(chan struct{}), make(chan struct{})
	consumer := emptyDef()
	consumer.TypeID = "parallel-build"
	consumer.Ports = []forest.PortDescriptor{forest.Port[clock]("clock", forest.Required(), forest.During(forest.Construction))}
	callbackResult := make(chan error, 1)
	consumer.Build = func(ctx context.Context, env *forest.BuildEnv) (forest.Lifecycle, error) {
		r, err := forest.Reference[clock](env, "clock")
		if err != nil {
			return nil, err
		}
		go func() {
			callbackResult <- r.Use(ctx, func(c clock) error { close(entered); <-release; _ = c.Now(ctx); return nil })
		}()
		<-entered
		close(returned)
		return forest.Hooks{OnStart: func(context.Context) error { return errors.New("intentional failure") }}, nil
	}
	p := register(t, f, provider)
	c := register(t, f, consumer)
	finished := make(chan error, 1)
	go func() {
		_, err := f.Apply(context.Background(), forest.ChangePlan{Operations: []forest.Operation{forest.AddRoot("provider", p), forest.AddRoot("consumer", c), forest.Connect(binding("l", "consumer", "clock", "provider", "now"))}})
		finished <- err
	}()
	receive(t, returned)
	select {
	case <-providerStopped:
		close(release)
		t.Fatal("provider stopped during an entered preparation callback")
	case <-time.After(20 * time.Millisecond):
	}
	close(release)
	if err := receive(t, callbackResult); err != nil {
		t.Fatal(err)
	}
	if err := receive(t, finished); !errors.Is(err, forest.ErrPrepareFailed) {
		t.Fatal(err)
	}
	wait(t, f)
	receive(t, providerStopped)
}

func TestStopFailureRetainsStructuredContext(t *testing.T) {
	f := forest.New(forest.Options{})
	cause := errors.New("resource could not close")
	d := emptyDef()
	d.Build = func(context.Context, *forest.BuildEnv) (forest.Lifecycle, error) {
		return forest.Hooks{OnStop: func(context.Context) error { return cause }}, nil
	}
	r := register(t, f, d)
	apply(t, f, forest.AddRoot("broken", r))
	apply(t, f, forest.Remove("broken"))
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	err := f.Wait(ctx)
	var detail *forest.Error
	if !errors.Is(err, cause) || !errors.As(err, &detail) || detail.Tree != "broken" || detail.Stage != "stop" {
		t.Fatalf("stop context: %v", err)
	}
	// Stop has not demonstrated release, so Close correctly reports the failure.
	if err := f.Close(ctx); !errors.Is(err, cause) {
		t.Fatal(err)
	}
}

func TestCooperativeStopDeadlinePreservesTimeoutCause(t *testing.T) {
	f := forest.New(forest.Options{DrainTimeout: 10 * time.Millisecond})
	d := emptyDef()
	d.Build = func(context.Context, *forest.BuildEnv) (forest.Lifecycle, error) {
		return forest.Hooks{OnStop: func(ctx context.Context) error { <-ctx.Done(); return ctx.Err() }}, nil
	}
	r := register(t, f, d)
	apply(t, f, forest.AddRoot("timeout", r))
	apply(t, f, forest.Remove("timeout"))
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	err := f.Wait(ctx)
	if !errors.Is(err, forest.ErrDrainTimeout) || !errors.Is(err, context.DeadlineExceeded) {
		t.Fatal(err)
	}
	if err := f.Close(ctx); !errors.Is(err, forest.ErrDrainTimeout) {
		t.Fatal(err)
	}
}

func TestConstructionCanCallThroughTransitiveDeclaredDependencies(t *testing.T) {
	f := newForest(t)
	c := register(t, f, clockDef("1", "nested", forest.Public))
	pd := printerDef(true)
	pd.Ports[0].Phase = forest.Construction
	p := register(t, f, pd)
	reader := emptyDef()
	reader.TypeID = "reader"
	reader.Ports = []forest.PortDescriptor{forest.Port[printer]("printer", forest.Required(), forest.During(forest.Construction))}
	var observed string
	reader.Build = func(ctx context.Context, env *forest.BuildEnv) (forest.Lifecycle, error) {
		r, err := forest.Reference[printer](env, "printer")
		if err != nil {
			return nil, err
		}
		err = r.Use(ctx, func(p printer) error { var err error; observed, err = p.Print(ctx); return err })
		return forest.Hooks{}, err
	}
	r := register(t, f, reader)
	apply(t, f, forest.AddRoot("clock", c), forest.AddRoot("printer", p), forest.AddRoot("reader", r), forest.Connect(binding("clock", "printer", "clock", "clock", "now")), forest.Connect(binding("printer", "reader", "printer", "printer", "print")))
	if observed != "printed: nested" {
		t.Fatal(observed)
	}
}
