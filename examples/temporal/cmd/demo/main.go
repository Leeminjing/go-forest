package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"time"

	demo "github.com/Leeminjing/go-forest/examples/temporal"
	"github.com/Leeminjing/go-forest/examples/temporal/flow"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
func run() (err error) {
	address := flag.String("address", "127.0.0.1:7233", "Temporal endpoint")
	queue := flag.String("queue", "forest-demo", "isolated task queue")
	directory := flag.String("directory", ".scratch/temporal-demo", "owned Agent checkpoints")
	id := flag.String("id", "", "stable business operation and Workflow ID (required)")
	text := flag.String("text", "oak", "deterministic planning input")
	setup := flag.Bool("setup", false, "explicitly provision the example business table")
	flag.Parse()
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	o := demo.Options{Address: *address, DSN: os.Getenv("FOREST_POSTGRES_DSN"), Directory: *directory, TaskQueue: *queue}
	if *id == "" {
		return fmt.Errorf("-id is required; retain it to reconcile retry outcomes")
	}
	if *setup {
		if err := demo.Setup(ctx, o.DSN); err != nil {
			return err
		}
	}
	r, err := demo.New(ctx, o)
	if err != nil {
		return err
	}
	defer func() {
		closeCtx, end := context.WithTimeout(context.Background(), 10*time.Second)
		defer end()
		if closeErr := r.Close(closeCtx); closeErr != nil {
			if err == nil {
				err = closeErr
			} else {
				fmt.Fprintln(os.Stderr, closeErr)
			}
		}
	}()
	ref, err := r.StartRun(ctx, flow.Input{ID: *id, Text: *text})
	if err != nil {
		return err
	}
	out, err := r.Result(ctx, ref)
	if err != nil {
		return err
	}
	if err = json.NewEncoder(os.Stdout).Encode(struct {
		Run    demo.Ref
		Result flow.Result
	}{ref, out}); err != nil {
		return err
	}
	return nil
}
