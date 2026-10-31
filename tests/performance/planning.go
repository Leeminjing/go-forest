package performance

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"runtime/pprof"
	"time"

	"github.com/Leeminjing/go-forest/extensions/topology"
)

type PlanningInput struct {
	Before, Target topology.Manifest
	Generation     uint64
}

func NewPlanning(directory string) (*Fixture, error) {
	x, e := New(config(), directory)
	if e != nil {
		return nil, e
	}
	for i := 0; i < dimension("HISTORY", 0); i++ {
		target, e := x.Target()
		if e != nil {
			_ = x.Close(context.Background())
			return nil, e
		}
		p, e := x.C.Preview(target)
		if e == nil {
			_, e = x.C.Reconfigure(context.Background(), topology.Request{ID: fmt.Sprintf("history-%d", i), Expected: p.Expected, Approval: "real managed prefix"}, p)
		}
		if e != nil {
			_ = x.Close(context.Background())
			return nil, e
		}
	}
	return x, nil
}

func PlanningData(x *Fixture) (PlanningInput, error) {
	m, e := topology.Capture(x.F.Snapshot(), x.Catalog)
	if e != nil {
		return PlanningInput{}, e
	}
	t, e := planningTarget(x)
	if e != nil {
		return PlanningInput{}, e
	}
	return PlanningInput{m, t, x.F.Snapshot().Generation}, nil
}

func planningTarget(x *Fixture) (topology.Manifest, error) {
	mode := os.Getenv("FOREST_PLANNING_CHANGE")
	if mode == "" || mode == "replace" {
		return x.Target()
	}
	m, e := topology.Capture(x.F.Snapshot(), x.Catalog)
	if e != nil {
		return m, e
	}
	switch mode {
	case "noop":
	case "rebind":
		for i := range m.Bindings {
			if m.Bindings[i].ID == "target-0" {
				m.Bindings[i].Provider = Provider(1)
			}
		}
	case "reorder":
		if !x.Config.Many || x.Config.D < 2 {
			return m, fmt.Errorf("reorder needs many fixture")
		}
		for i, j := 0, x.Config.D-1; i < j; i, j = i+1, j-1 {
			m.Bindings[i], m.Bindings[j] = m.Bindings[j], m.Bindings[i]
		}
		for i := range m.Bindings {
			m.Bindings[i].Ordinal = uint64(i + 1)
		}
	default:
		return m, fmt.Errorf("unknown change")
	}
	return m, nil
}

func CreatePlanningInput(path, journal string) (err error) {
	dir, e := os.MkdirTemp("", "forest-planning-input-")
	if e != nil {
		return e
	}
	defer os.RemoveAll(dir)
	x, e := NewPlanning(dir)
	if e != nil {
		return e
	}
	defer func() { err = errors.Join(err, x.Close(context.Background())) }()
	data, e := PlanningData(x)
	if e != nil {
		return e
	}
	raw, e := json.Marshal(data)
	if e != nil {
		return e
	}
	if e = os.WriteFile(path, raw, 0600); e != nil {
		return e
	}
	if e = x.Close(context.Background()); e != nil {
		return e
	}
	raw, e = os.ReadFile(filepath.Join(dir, "journal"))
	if e != nil {
		return e
	}
	return os.WriteFile(journal, raw, 0600)
}

// PlanningOperation prepares a real operation but does not execute it. Its
// closure is also used by the separate instrumented attribution program.
func PlanningOperation(kind string) (func() error, func() error, error) {
	dir, e := os.MkdirTemp("", "forest-planning-operation-")
	if e != nil {
		return nil, nil, e
	}
	x, e := NewPlanning(dir)
	if e != nil {
		os.RemoveAll(dir)
		return nil, nil, e
	}
	var committed bool
	var target topology.Manifest
	var expectedGeneration uint64
	cleanup := func() error {
		var check error
		if committed {
			actual, e := topology.Capture(x.F.Snapshot(), x.Catalog)
			if e != nil || !equalPlanningManifest(actual, target) || x.F.Snapshot().Generation != expectedGeneration {
				check = fmt.Errorf("actual commit differs from target/generation: %v", e)
			}
		}
		e := x.Close(context.Background())
		return errors.Join(check, e, os.RemoveAll(dir))
	}
	data, e := PlanningData(x)
	if e != nil {
		_ = cleanup()
		return nil, nil, e
	}
	if path := os.Getenv("FOREST_PLANNING_INPUT"); path != "" {
		raw, e := os.ReadFile(path)
		if e != nil {
			_ = cleanup()
			return nil, nil, e
		}
		var expected PlanningInput
		if e = json.Unmarshal(raw, &expected); e != nil {
			_ = cleanup()
			return nil, nil, e
		}
		a, _ := json.Marshal(data)
		b, _ := json.Marshal(expected)
		if string(a) != string(b) {
			_ = cleanup()
			return nil, nil, fmt.Errorf("frozen planning input differs")
		}
	}
	snapshot := x.F.Snapshot()
	target = data.Target
	expectedGeneration = data.Generation
	if !equalPlanningManifest(data.Before, data.Target) {
		expectedGeneration++
	}
	var p topology.Proposal
	if kind == "Commit" {
		p, e = x.C.Preview(data.Target)
		if e != nil {
			_ = cleanup()
			return nil, nil, e
		}
	}
	var journal []byte
	if kind == "Open" {
		if e = x.Close(context.Background()); e != nil {
			_ = cleanup()
			return nil, nil, e
		}
		path := os.Getenv("FOREST_PLANNING_JOURNAL")
		if path == "" {
			path = filepath.Join(dir, "journal")
		}
		journal, e = os.ReadFile(path)
		if e != nil {
			_ = cleanup()
			return nil, nil, e
		}
		openDir, e := os.MkdirTemp("", "forest-planning-open-")
		if e != nil {
			_ = cleanup()
			return nil, nil, e
		}
		if e = os.WriteFile(filepath.Join(openDir, "journal"), journal, 0600); e != nil {
			os.RemoveAll(openDir)
			_ = cleanup()
			return nil, nil, e
		}
		var store *topology.Store
		return func() error { var err error; store, err = topology.Open(openDir, topology.Options{}); return err }, func() error {
			var err error
			if store != nil {
				err = store.Close()
			}
			return errors.Join(err, os.RemoveAll(openDir), cleanup())
		}, nil
	}
	operation := func() error {
		var e error
		switch kind {
		case "Capture":
			_, e = topology.Capture(snapshot, x.Catalog)
		case "Normalize":
			_, e = topology.Normalize(data.Target)
		case "Encode":
			_, e = topology.Encode(data.Target)
		case "Compare":
			_, e = topology.Compare(data.Before, data.Target)
		case "Digest":
			_, e = topology.Digest(data.Target)
		case "Preview":
			_, e = x.C.Preview(data.Target)
		case "Commit":
			_, e = x.C.Reconfigure(context.Background(), topology.Request{ID: "measured", Expected: p.Expected, Approval: "frozen complete validation"}, p)
			committed = e == nil
		case "PreviewCommit":
			var proposal topology.Proposal
			proposal, e = x.C.Preview(data.Target)
			if e == nil {
				_, e = x.C.Reconfigure(context.Background(), topology.Request{ID: "measured", Expected: proposal.Expected, Approval: "frozen complete validation"}, proposal)
			}
			committed = e == nil
		default:
			e = fmt.Errorf("unknown planning operation %q", kind)
		}
		return e
	}
	return operation, cleanup, nil
}

func PlanningProfile(kind, output, profile string) (err error) {
	operation, close, e := PlanningOperation(kind)
	if e != nil {
		return e
	}
	defer func() { err = errors.Join(err, close()) }()
	f, e := os.Create(output)
	if e != nil {
		return e
	}
	defer func() { err = errors.Join(err, f.Close()) }()
	if profile == "cpu" {
		if e = pprof.StartCPUProfile(f); e != nil {
			return e
		}
	}
	n := 10
	if kind == "Commit" || kind == "PreviewCommit" || kind == "Open" {
		n = 1
	}
	for i := 0; i < n; i++ {
		if e = operation(); e != nil {
			break
		}
	}
	if profile == "cpu" {
		pprof.StopCPUProfile()
	} else if profile == "allocs" {
		// Flush the delayed memory profile outside the operation. Ordinary timing
		// remains on its frozen harness; this is a separate diagnostic run.
		runtime.GC()
		runtime.GC()
		e = errors.Join(e, pprof.Lookup("allocs").WriteTo(f, 0))
	} else {
		return fmt.Errorf("unknown profile")
	}
	return e
}

func equalPlanningManifest(a, b topology.Manifest) bool {
	x, _ := json.Marshal(a)
	y, _ := json.Marshal(b)
	return string(x) == string(y)
}

type PlanningMemoryResult struct {
	Operation               string
	ElapsedNS               int64
	Allocated, Allocations  uint64
	Before, After, Released JournalMemorySample
}

func PlanningMemory(kind, output string) (out PlanningMemoryResult, err error) {
	op, close, e := PlanningOperation(kind)
	if e != nil {
		return out, e
	}
	runtime.GC()
	out.Operation = kind
	out.Before = journalMemorySample()
	var before, after runtime.MemStats
	runtime.ReadMemStats(&before)
	start := time.Now()
	e = op()
	out.ElapsedNS = time.Since(start).Nanoseconds()
	runtime.ReadMemStats(&after)
	out.Allocated = after.TotalAlloc - before.TotalAlloc
	out.Allocations = after.Mallocs - before.Mallocs
	if e != nil {
		_ = close()
		return out, e
	}
	runtime.GC()
	out.After = journalMemorySample()
	if output != "" {
		f, e := os.Create(output)
		if e != nil {
			_ = close()
			return out, e
		}
		e = pprof.WriteHeapProfile(f)
		if e = errors.Join(e, f.Close()); e != nil {
			_ = close()
			return out, e
		}
	}
	if e = close(); e != nil {
		return out, e
	}
	op = nil
	close = nil
	runtime.GC()
	out.Released = journalMemorySample()
	return out, nil
}
