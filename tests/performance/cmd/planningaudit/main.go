package main

import (
	"encoding/json"
	"fmt"
	"os"

	perf "github.com/Leeminjing/go-forest/tests/performance"
)

func main() {
	semantic, raw, err := perf.PlanningAudit()
	if err == nil {
		err = json.NewEncoder(os.Stdout).Encode(map[string]any{"Semantic": semantic, "Raw": raw})
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
