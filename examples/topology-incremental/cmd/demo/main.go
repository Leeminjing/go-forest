package main

import (
	"context"
	"encoding/json"
	"fmt"
	demo "github.com/Leeminjing/go-forest/examples/topology-incremental"
	"os"
	"time"
)

func main() {
	dsn := os.Getenv("FOREST_POSTGRES_DSN")
	if dsn == "" {
		fmt.Fprintln(os.Stderr, "FOREST_POSTGRES_DSN is required (real PostgreSQL)")
		os.Exit(1)
	}
	dir, err := os.MkdirTemp("", "forest-incremental-")
	if err != nil {
		fmt.Fprintln(os.Stderr, "create local directory failed")
		os.Exit(1)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 40*time.Second)
	defer cancel()
	evidence, err := demo.Run(ctx, dsn, dir)
	if err != nil {
		fmt.Fprintln(os.Stderr, "incremental demo failed:", err)
		os.Exit(1)
	}
	// Keep the small journal as explicit audit evidence; do not print credentials.
	if err = json.NewEncoder(os.Stdout).Encode(evidence); err != nil {
		os.Exit(1)
	}
	fmt.Fprintln(os.Stderr, "topology journal:", dir)
}
