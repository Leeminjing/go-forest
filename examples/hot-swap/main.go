package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/Leeminjing/go-forest/examples/trees/clock"
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
	v1 := clock.Definition("1", func() time.Time { return time.Unix(1, 0) })
	v2 := clock.Definition("2", func() time.Time { return time.Unix(2, 0) })
	for _, d := range []forest.Definition{v1, v2} {
		if err := f.Register(d); err != nil {
			return err
		}
	}
	if _, err := f.Apply(context.Background(), forest.ChangePlan{Operations: []forest.Operation{forest.AddRoot("clock", v1.Ref())}}); err != nil {
		return err
	}
	entered, release := make(chan struct{}), make(chan struct{})
	oldResult := make(chan error, 1)
	go func() {
		oldResult <- f.Invoke(context.Background(), func(ctx context.Context) error {
			return forest.Use[clock.Source](ctx, f, "clock", "time", func(c clock.Source) error {
				close(entered)
				<-release
				fmt.Printf("old invocation: %d\n", c.Now(ctx).Unix())
				return nil
			})
		})
	}()
	<-entered
	_, err := f.Apply(context.Background(), forest.ChangePlan{BaseGeneration: 1, Operations: []forest.Operation{forest.Replace("clock", v2.Ref(), false)}})
	if err != nil {
		close(release)
		<-oldResult
		return err
	}
	err = f.Invoke(context.Background(), func(ctx context.Context) error {
		return forest.Use[clock.Source](ctx, f, "clock", "time", func(c clock.Source) error { fmt.Printf("new invocation: %d\n", c.Now(ctx).Unix()); return nil })
	})
	close(release)
	if oldErr := <-oldResult; oldErr != nil {
		return oldErr
	}
	return err
}
