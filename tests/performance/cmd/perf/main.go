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
	rounds := flag.Int("rounds", 1000, "longevity rounds")
	holds := flag.Int("holds", 1, "old scopes")
	rate := flag.Int("rate", 1000, "planned arrivals/second")
	queue := flag.Int("queue", 1024, "bounded load queue")
	requests := flag.Int("requests", 100000, "maximum request/sample bound")
	closed := flag.Bool("closed", false, "closed-loop workers")
	updates := flag.Duration("updates", 0, "mutation period (zero disables)")
	flag.Parse()
	cfg := performance.Config{N: *n, E: *e, D: *d, Ports: *ports, Many: *many}
	switch *mode {
	case "load":
		result, e := performance.Load(performance.LoadConfig{Graph: cfg, Workers: *workers, Rate: *rate, Queue: *queue, MaxRequests: *requests, Holds: *holds, Duration: *duration, UpdateEvery: *updates, Closed: *closed})
		_ = json.NewEncoder(os.Stdout).Encode(result)
		if e != nil {
			fmt.Fprintln(os.Stderr, e)
			os.Exit(1)
		}
	case "longevity":
		dir, e := os.MkdirTemp("", "forest-longevity-")
		if e != nil {
			fmt.Fprintln(os.Stderr, e)
			os.Exit(1)
		}
		result, e := performance.Longevity(*rounds, *holds, *kind, dir, *out)
		_ = json.NewEncoder(os.Stdout).Encode(result)
		if e != nil {
			fmt.Fprintln(os.Stderr, e)
			os.Exit(1)
		}
		if e = os.Remove(dir); e != nil {
			fmt.Fprintln(os.Stderr, e)
			os.Exit(1)
		}
	case "profile":
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
	default:
		fmt.Fprintln(os.Stderr, "unsupported mode")
		os.Exit(1)
	}
}
