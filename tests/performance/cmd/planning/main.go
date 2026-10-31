package main

import (
	"flag"
	"fmt"
	"os"

	perf "github.com/Leeminjing/go-forest/tests/performance"
)

func main() {
	mode := flag.String("mode", "input", "input/profile")
	input := flag.String("out", "", "output JSON or profile")
	journal := flag.String("journal", "", "journal output")
	kind := flag.String("operation", "Preview", "public operation")
	profile := flag.String("profile", "cpu", "cpu/allocs")
	flag.Parse()
	var e error
	if *mode == "input" {
		e = perf.CreatePlanningInput(*input, *journal)
	} else if *mode == "profile" {
		e = perf.PlanningProfile(*kind, *input, *profile)
	} else {
		e = fmt.Errorf("unknown mode")
	}
	if e != nil {
		fmt.Fprintln(os.Stderr, e)
		os.Exit(1)
	}
}
