package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"time"

	demo "github.com/Leeminjing/go-forest/examples/eino"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
func run() error {
	directory := flag.String("checkpoints", ".scratch/eino-demo", "separate native checkpoint directories")
	flag.Parse()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	r, err := demo.New(ctx, demo.Options{Directory: *directory})
	if err != nil {
		return err
	}
	defer func() { c, x := context.WithTimeout(context.Background(), 5*time.Second); defer x(); r.Close(c) }()
	first, err := r.Ask(ctx, "oak")
	if err != nil {
		return err
	}
	if _, err = r.Rebind(ctx, "forest"); err != nil {
		return err
	}
	second, err := r.Ask(ctx, "oak")
	if err != nil {
		return err
	}
	return json.NewEncoder(os.Stdout).Encode(map[string]any{"eino": first, "forest_agent": second, "generation": r.Forest.Snapshot().Generation, "trees": len(r.Forest.Snapshot().Trees)})
}
