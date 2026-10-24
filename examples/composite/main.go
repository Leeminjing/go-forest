package main

import (
	"context"
	"fmt"
	"log"

	"github.com/Leeminjing/go-forest/examples/trees/clockwork"
	"github.com/Leeminjing/go-forest/examples/trees/printer"
	"github.com/Leeminjing/go-forest/forest"
)

func main() {
	if err := run(); err != nil {
		log.Fatal(err)
	}
}
func run() error {
	f := forest.New(forest.Options{})
	defer f.Close(context.Background())
	if err := clockwork.Register(f); err != nil {
		return err
	}
	p := printer.Definition()
	if err := f.Register(p); err != nil {
		return err
	}
	b, err := clockwork.Blueprint()
	if err != nil {
		return err
	}
	left, err := b.Instantiate("left", "")
	if err != nil {
		return err
	}
	right, err := b.Instantiate("right", "")
	if err != nil {
		return err
	}
	operations := append(left.Plan(0).Operations, right.Plan(0).Operations...)
	operations = append(operations, forest.AddRoot("printer", p.Ref()), forest.Connect(forest.Link{ID: "printer", Consumer: "printer", Port: "formatter", Provider: left.Root(), Capability: "format"}))
	if _, err := f.Apply(context.Background(), forest.ChangePlan{Operations: operations}); err != nil {
		return err
	}
	return f.Invoke(context.Background(), func(ctx context.Context) error {
		return forest.Use[printer.Output](ctx, f, "printer", "print", func(p printer.Output) error {
			line, err := p.Print(ctx)
			if err != nil {
				return err
			}
			fmt.Println(line)
			fmt.Printf("generation=%d trees=%d independent composites=%s,%s\n", f.Snapshot().Generation, len(f.Snapshot().Trees), left.Root(), right.Root())
			return nil
		})
	})
}
