package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"go-forest/examples/trees/clock"
	"go-forest/examples/trees/formatter"
	"go-forest/examples/trees/printer"
	"go-forest/forest"
)

func main() {
	if err := run(); err != nil {
		log.Fatal(err)
	}
}
func run() error {
	f := forest.New(forest.Options{})
	defer f.Close(context.Background())
	c := clock.Definition("1", func() time.Time { return time.Date(2026, 10, 24, 8, 0, 0, 0, time.UTC) })
	format, print := formatter.Definition(), printer.Definition()
	for _, d := range []forest.Definition{c, format, print} {
		if err := f.Register(d); err != nil {
			return err
		}
	}
	_, err := f.Apply(context.Background(), forest.ChangePlan{Operations: []forest.Operation{
		forest.AddRoot("clock", c.Ref()), forest.AddRoot("formatter", format.Ref()), forest.AddRoot("printer", print.Ref()),
		forest.Connect(forest.Link{ID: "time", Consumer: "formatter", Port: "clock", Provider: "clock", Capability: "time"}),
		forest.Connect(forest.Link{ID: "format", Consumer: "printer", Port: "formatter", Provider: "formatter", Capability: "format"}),
	}})
	if err != nil {
		return err
	}
	return f.Invoke(context.Background(), func(ctx context.Context) error {
		return forest.Use[printer.Output](ctx, f, "printer", "print", func(p printer.Output) error {
			line, err := p.Print(ctx)
			if err != nil {
				return err
			}
			fmt.Println(line)
			fmt.Printf("generation=%d trees=%d links=%d\n", f.Snapshot().Generation, len(f.Snapshot().Trees), len(f.Snapshot().Links))
			return nil
		})
	})
}
