package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"os"

	perf "github.com/Leeminjing/go-forest/tests/performance"
)

func main() {
	mode := flag.String("mode", "fixture", "fixture/info/memory/profile")
	n := flag.Int("n", 32, "real tree descriptors")
	h := flag.Int("history", 16, "additional real revisions")
	input := flag.String("input", "", "frozen journal input")
	out := flag.String("out", "", "output file")
	operation := flag.String("operation", "open", "open/append")
	kind := flag.String("kind", "cpu", "cpu/allocs")
	flag.Parse()
	var e error
	switch *mode {
	case "fixture":
		e = perf.CreateJournal(*out, *n, *h)
	case "info":
		var b []byte
		b, e = perf.JournalInputInfo(*input)
		if e == nil {
			_, e = os.Stdout.Write(b)
		}
	case "memory":
		var r perf.JournalMemoryResult
		r, e = perf.JournalMemory(*input, *operation, *out)
		_ = json.NewEncoder(os.Stdout).Encode(r)
	case "profile":
		e = perf.JournalProfile(*input, *operation, *kind, *out)
	default:
		e = fmt.Errorf("invalid mode")
	}
	if e != nil {
		fmt.Fprintln(os.Stderr, e)
		os.Exit(1)
	}
}
