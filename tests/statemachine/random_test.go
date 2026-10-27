package statemachine

import (
	"os"
	"strconv"
	"testing"

	m "github.com/Leeminjing/go-forest/tests/internal/topologymodel"
)

func TestModelRandom(t *testing.T) {
	count, seeds, limit := 128, 16, 16
	if os.Getenv("FOREST_MODEL_PROFILE") == "extended" {
		count, seeds, limit = 256, 96, 32
	}
	base := int64(81276)
	if raw := os.Getenv("FOREST_MODEL_SEED"); raw != "" {
		var err error
		base, err = strconv.ParseInt(raw, 10, 64)
		if err != nil {
			t.Fatal(err)
		}
		seeds = 1
	}
	var batch []m.Trace
	accepted, rejected := 0, 0
	coverage := map[string]int{}
	flush := func() {
		for _, r := range requireNative(t, batch...) {
			accepted += r.Accepted
			rejected += r.Rejected
			for k, n := range r.Coverage {
				coverage[k] += n
			}
		}
		batch = nil
	}
	for i := 0; i < seeds; i++ {
		batch = append(batch, m.Generate(Catalog(), base+int64(i)*7919, count, limit))
		if len(batch) == 4 {
			flush()
		}
	}
	if len(batch) > 0 {
		flush()
	}
	if accepted == 0 || rejected == 0 {
		t.Fatal("missing actual accepted/rejected plans", accepted, rejected)
	}
	t.Logf("seeds=%d plans=%d accepted=%d rejected=%d coverage=%v", seeds, seeds*count, accepted, rejected, coverage)
}

func TestModelSupervisedKnownRules(t *testing.T) {
	var traces []m.Trace
	for _, c := range ruleCases() {
		traces = append(traces, knownTrace(c))
	}
	for len(traces) > 0 {
		n := 4
		if len(traces) < n {
			n = len(traces)
		}
		requireNative(t, traces[:n]...)
		traces = traces[n:]
	}
}
