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
	c, format, print := clock.Definition("1", time.Now), formatter.Definition(), printer.Definition()
	for _, d := range []forest.Definition{c, format, print} {
		if err := f.Register(d); err != nil {
			return err
		}
	}
	_, err := f.Apply(context.Background(), forest.ChangePlan{Operations: []forest.Operation{
		forest.AddRoot("clock", c.Ref()), forest.Nest("clock", "formatter", format.Ref()), forest.Nest("formatter", "printer", print.Ref()),
		forest.BindNest(forest.Link{ID: "time", Consumer: "formatter", Port: "clock", Provider: "clock", Capability: "time"}),
		forest.BindNest(forest.Link{ID: "format", Consumer: "printer", Port: "formatter", Provider: "formatter", Capability: "format"}),
	}})
	if err != nil {
		return err
	}
	for _, id := range []forest.TreeID{"clock", "formatter", "printer"} {
		info, _ := f.InspectTree(id)
		fmt.Printf("%s parent=%q\n", id, info.ParentID)
	}
	return f.Invoke(context.Background(), func(ctx context.Context) error {
		return forest.Use[printer.Output](ctx, f, "printer", "print", func(p printer.Output) error {
			line, err := p.Print(ctx)
			if err == nil {
				fmt.Println(line)
			}
			return err
		})
	})
}
