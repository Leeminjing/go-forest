package tests

import (
	"context"
	"errors"
	"strings"
	"testing"

	"go-forest/forest"
)

// The consumer owns these interfaces. Providers never import a consumer package.
type clock interface{ Now(context.Context) string }
type printer interface {
	Print(context.Context) (string, error)
}
type clockService struct{ label string }

func (c *clockService) Now(context.Context) string { return c.label }

type printerService struct{ clock forest.Ref[clock] }

func (p *printerService) Print(ctx context.Context) (string, error) {
	var result string
	err := p.clock.Use(ctx, func(c clock) error { result = "printed: " + c.Now(ctx); return nil })
	return result, err
}

func clockDef(version, label string, visibility forest.Visibility) forest.Definition {
	return forest.Definition{TypeID: "clock", Version: version, HotSwappable: true, Reparentable: true, PreserveChildren: true,
		Capabilities: []forest.CapabilityDescriptor{forest.Capability[*clockService]("now", visibility)},
		Build: func(ctx context.Context, env *forest.BuildEnv) (forest.Lifecycle, error) {
			if err := forest.Provide(env, "now", &clockService{label: label}); err != nil {
				return nil, err
			}
			return forest.Hooks{}, nil
		}}
}
func printerDef(required bool) forest.Definition {
	port := forest.Port[clock]("clock")
	if required {
		port.Required = true
	}
	return forest.Definition{TypeID: "printer", Version: "1", HotSwappable: true,
		Ports: []forest.PortDescriptor{port}, Capabilities: []forest.CapabilityDescriptor{forest.Capability[printer]("print", forest.Public)},
		Build: func(ctx context.Context, env *forest.BuildEnv) (forest.Lifecycle, error) {
			ref, err := forest.Reference[clock](env, "clock")
			if err != nil {
				return nil, err
			}
			if err := forest.Provide(env, "print", &printerService{clock: ref}); err != nil {
				return nil, err
			}
			return forest.Hooks{}, nil
		}}
}
func readPrint(t *testing.T, f *forest.Forest) string {
	t.Helper()
	var result string
	err := f.Invoke(context.Background(), func(ctx context.Context) error {
		return forest.Use[printer](ctx, f, "printer", "print", func(p printer) error { var err error; result, err = p.Print(ctx); return err })
	})
	if err != nil {
		t.Fatal(err)
	}
	return result
}
func binding(id string, consumer forest.TreeID, port forest.PortID, provider forest.TreeID, cap forest.CapabilityID) forest.Link {
	return forest.Link{ID: id, Consumer: consumer, Port: port, Provider: provider, Capability: cap}
}

func TestTypedLinkRebindAndRequiredDependency(t *testing.T) {
	f := newForest(t)
	c1 := register(t, f, clockDef("1", "v1", forest.Public))
	c2 := register(t, f, clockDef("2", "v2", forest.Public))
	p := register(t, f, printerDef(true))
	link := binding("clock-link", "printer", "clock", "clock1", "now")
	apply(t, f, forest.AddRoot("clock1", c1), forest.AddRoot("clock2", c2), forest.AddRoot("printer", p), forest.Connect(link))
	if got := readPrint(t, f); got != "printed: v1" {
		t.Fatal(got)
	}
	_, err := f.Apply(context.Background(), forest.ChangePlan{BaseGeneration: 1, Operations: []forest.Operation{forest.Unlink("clock-link")}})
	if !errors.Is(err, forest.ErrDependencyInUse) {
		t.Fatal(err)
	}
	link.Provider = "clock2"
	apply(t, f, forest.Rebind(link), forest.Remove("clock1"))
	if got := readPrint(t, f); got != "printed: v2" {
		t.Fatal(got)
	}
	if len(f.Snapshot().Trees) != 2 || f.Snapshot().Generation != 2 {
		t.Fatal("partial transaction")
	}
}

func TestNestAssemblyAndAccessBoundaries(t *testing.T) {
	f := newForest(t)
	c := register(t, f, clockDef("1", "parent", forest.Descendants))
	p := register(t, f, printerDef(true))
	l := binding("local", "printer", "clock", "clock", "now")
	apply(t, f, forest.AddRoot("clock", c), forest.Nest("clock", "printer", p), forest.BindNest(l))
	if len(f.Snapshot().Links) != 0 || len(f.Snapshot().NestBindings) != 1 {
		t.Fatal("Nest is not Link")
	}
	// Entry into a nested tree requires an explicit export from its owner.
	err := f.Invoke(context.Background(), func(ctx context.Context) error {
		return forest.Use[printer](ctx, f, "printer", "print", func(printer) error { return nil })
	})
	if !errors.Is(err, forest.ErrMissingCapability) {
		t.Fatal(err)
	}
	_, err = f.Apply(context.Background(), forest.ChangePlan{BaseGeneration: 1, Operations: []forest.Operation{forest.Unlink("local"), forest.Connect(l)}})
	if !errors.Is(err, forest.ErrInvalidLink) {
		t.Fatal(err)
	}
	_, err = f.Apply(context.Background(), forest.ChangePlan{BaseGeneration: 1, Operations: []forest.Operation{forest.Unnest("printer")}})
	if !errors.Is(err, forest.ErrInvalidLink) {
		t.Fatal(err)
	}
}

func TestOptionalUnavailableAndManyOrder(t *testing.T) {
	f := newForest(t)
	pd := printerDef(false)
	pd.Ports[0].Many = true
	pd.Build = func(ctx context.Context, env *forest.BuildEnv) (forest.Lifecycle, error) {
		r, err := forest.Reference[clock](env, "clock")
		if err != nil {
			return nil, err
		}
		return forest.Hooks{}, forest.Provide(env, "print", multiPrinter{r})
	}
	p := register(t, f, pd)
	c := register(t, f, clockDef("1", "a", forest.Public))
	d := register(t, f, clockDef("2", "b", forest.Public))
	apply(t, f, forest.AddRoot("printer", p), forest.AddRoot("a", c), forest.AddRoot("b", d))
	err := f.Invoke(context.Background(), func(ctx context.Context) error {
		return forest.Use[printer](ctx, f, "printer", "print", func(p printer) error { _, err := p.Print(ctx); return err })
	})
	if !errors.Is(err, forest.ErrBindingUnavailable) {
		t.Fatal(err)
	}
	apply(t, f, forest.Connect(binding("z", "printer", "clock", "b", "now")), forest.Connect(binding("a", "printer", "clock", "a", "now")))
	if got := readPrint(t, f); got != "b,a" {
		t.Fatal(got)
	}
	apply(t, f, forest.Unlink("z"))
	if got := readPrint(t, f); got != "a" {
		t.Fatal(got)
	}
}

type multiPrinter struct{ clocks forest.Ref[clock] }

func (p multiPrinter) Print(ctx context.Context) (string, error) {
	var results []string
	err := p.clocks.UseAll(ctx, func(cs []clock) error {
		for _, c := range cs {
			results = append(results, c.Now(ctx))
		}
		return nil
	})
	return strings.Join(results, ","), err
}

func TestInvalidContractsRollbackAndSingleCardinality(t *testing.T) {
	f := newForest(t)
	c := register(t, f, clockDef("1", "ok", forest.Public))
	p := register(t, f, printerDef(true))
	bad := emptyDef()
	bad.TypeID = "bad"
	bad.Capabilities = []forest.CapabilityDescriptor{forest.Capability[int]("now", forest.Public)}
	bad.Build = func(ctx context.Context, env *forest.BuildEnv) (forest.Lifecycle, error) {
		return forest.Hooks{}, forest.Provide(env, "now", 42)
	}
	b := register(t, f, bad)
	_, err := f.Apply(context.Background(), forest.ChangePlan{Operations: []forest.Operation{forest.AddRoot("bad", b), forest.AddRoot("printer", p), forest.Connect(binding("l", "printer", "clock", "bad", "now"))}})
	if !errors.Is(err, forest.ErrContractMismatch) || f.Snapshot().Generation != 0 {
		t.Fatal(err)
	}
	apply(t, f, forest.AddRoot("clock", c), forest.AddRoot("printer", p), forest.Connect(binding("l", "printer", "clock", "clock", "now")))
	_, err = f.Apply(context.Background(), forest.ChangePlan{BaseGeneration: 1, Operations: []forest.Operation{forest.Connect(binding("second", "printer", "clock", "clock", "now"))}})
	if !errors.Is(err, forest.ErrPortAlreadyBound) {
		t.Fatal(err)
	}
}
