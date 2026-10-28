package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"time"

	performance "github.com/Leeminjing/go-forest/tests/performance"
)

func main() {
	mode := flag.String("mode", "profile", "profile/load/longevity")
	n := flag.Int("n", 32, "actual nodes")
	e := flag.Int("e", 16, "actual bindings")
	d := flag.Int("d", 1, "target degree")
	ports := flag.Int("ports", 1, "consumer port declarations")
	many := flag.Bool("many", false, "Many target")
	kind := flag.String("kind", "cpu", "cpu/heap/allocs/block/mutex")
	out := flag.String("out", "cpu.pprof", "profile destination")
	duration := flag.Duration("duration", 2*time.Second, "work interval")
	workers := flag.Int("workers", 4, "workers")
	enter := flag.Bool("enter", false, "profile includes a fresh invocation per call")
	flag.Parse()
	cfg := performance.Config{N: *n, E: *e, D: *d, Ports: *ports, Many: *many}
	if *mode != "profile" {
		fmt.Fprintln(os.Stderr, "unsupported mode")
		os.Exit(1)
	}
	count, err := performance.Profile(cfg, *kind, *out, *duration, *workers, *enter)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	_ = json.NewEncoder(os.Stdout).Encode(struct {
		Mode, Kind string
		Config     performance.Config
		Calls      uint64
	}{*mode, *kind, cfg, count})
}
