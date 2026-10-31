//go:build planningprobe

// This command only builds against an explicitly instrumented temporary archive.
package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"os"

	"github.com/Leeminjing/go-forest/extensions/topology"
	perf "github.com/Leeminjing/go-forest/tests/performance"
)

func main() {
	kind := flag.String("operation", "Preview", "public operation")
	flag.Parse()
	op, close, e := perf.PlanningOperation(*kind)
	if e != nil {
		panic(e)
	}
	defer func() {
		if e := close(); e != nil {
			panic(e)
		}
	}()
	topology.PlanningReset()
	if e = op(); e != nil {
		fmt.Fprintln(os.Stderr, e)
		os.Exit(1)
	}
	if e = json.NewEncoder(os.Stdout).Encode(topology.PlanningReport()); e != nil {
		panic(e)
	}
}
