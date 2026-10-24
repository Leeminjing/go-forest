package tests

import (
	"context"
	"testing"
	"time"

	exampleClock "go-forest/examples/trees/clock"
	"go-forest/examples/trees/formatter"
	"go-forest/examples/trees/printer"
	"go-forest/forest"
)

func TestThreeIndependentTreesUseConsumerOwnedContracts(t *testing.T) {
	f := newForest(t)
	c := register(t, f, exampleClock.Definition("1", func() time.Time { return time.Date(2026, 10, 24, 8, 0, 0, 0, time.UTC) }))
	format := register(t, f, formatter.Definition())
	print := register(t, f, printer.Definition())
	apply(t, f, forest.AddRoot("clock", c), forest.AddRoot("formatter", format), forest.AddRoot("printer", print),
		forest.Connect(binding("time", "formatter", "clock", "clock", "time")),
		forest.Connect(binding("format", "printer", "formatter", "formatter", "format")))
	err := f.Invoke(context.Background(), func(ctx context.Context) error {
		return forest.Use[printer.Output](ctx, f, "printer", "print", func(p printer.Output) error {
			got, err := p.Print(ctx)
			if err != nil {
				return err
			}
			if got != "forest: 2026-10-24T08:00:00Z" {
				t.Fatal(got)
			}
			return nil
		})
	})
	if err != nil {
		t.Fatal(err)
	}
}
