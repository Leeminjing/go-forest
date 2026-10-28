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
	"strconv"
	"strings"
	"time"

	"github.com/Leeminjing/go-forest/extensions/topology"
	"github.com/Leeminjing/go-forest/forest"
)

// CreateJournal produces a real input through public APIs. H counts additional
// successful historical selections, not physical records.
func CreateJournal(path string, n, h int) (err error) {
	if n < 0 || n > 10000 || h < 0 || h > 2000 {
		return fmt.Errorf("invalid journal dimensions")
	}
	m := topology.Empty()
	for i := 0; i < n; i++ {
		m.Trees = append(m.Trees, topology.Tree{ID: forest.TreeID(fmt.Sprintf("node-%05d", i)), Definition: forest.DefinitionRef{TypeID: "journal.value", Version: "1"}, ContractKey: "value", ImplementationDigest: strings.Repeat("a", 64)})
	}
	dir, e := os.MkdirTemp("", "forest-journal-input-")
	if e != nil {
		return e
	}
	defer os.RemoveAll(dir)
	s, e := topology.Open(dir, topology.Options{})
	if e != nil {
		return e
	}
	defer func() { err = errors.Join(err, s.Close()) }()
	r, e := s.Seed(context.Background(), topology.Request{ID: "seed", Expected: s.Inspect().Position, Approval: "real benchmark seed"}, m)
	if e != nil {
		return e
	}
	for i := 0; i < h; i++ {
		_, e = s.SelectHistorical(context.Background(), topology.Request{ID: fmt.Sprintf("prefix-%05d", i), Expected: s.Inspect().Position, Approval: "real historical selection"}, r.Revision)
		if e != nil {
			return e
		}
	}
	if e = s.Close(); e != nil {
		return e
	}
	data, e := os.ReadFile(filepath.Join(dir, "journal"))
	if e != nil {
		return e
	}
	return os.WriteFile(path, data, 0600)
}
func copyJournal(input []byte, dir string) error {
	return os.WriteFile(filepath.Join(dir, "journal"), input, 0600)
}
func appendSelection(s *topology.Store) error {
	_, e := s.SelectHistorical(context.Background(), topology.Request{ID: "measured-selection", Expected: s.Inspect().Position, Approval: "measured durable append"}, 2)
	return e
}

type JournalMemorySample struct {
	HeapAlloc, HeapInuse, TotalAlloc, RSS, PeakRSS uint64
	RSSAvailable                                   bool
}
type JournalMemoryResult struct {
	Mode                    string
	InputBytes              int
	ElapsedNS               int64
	Records                 uint64
	Revisions               int
	Before, After, Released JournalMemorySample
}

func journalMemorySample() JournalMemorySample {
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	s := JournalMemorySample{HeapAlloc: m.HeapAlloc, HeapInuse: m.HeapInuse, TotalAlloc: m.TotalAlloc}
	if raw, e := os.ReadFile("/proc/self/status"); e == nil {
		for _, line := range strings.Split(string(raw), "\n") {
			f := strings.Fields(line)
			if len(f) < 2 {
				continue
			}
			v, _ := strconv.ParseUint(f[1], 10, 64)
			switch f[0] {
			case "VmRSS:":
				s.RSS = v * 1024
				s.RSSAvailable = true
			case "VmHWM:":
				s.PeakRSS = v * 1024
			}
		}
	}
	return s
}
func JournalMemory(path, mode, profile string) (out JournalMemoryResult, err error) {
	input, e := os.ReadFile(path)
	if e != nil {
		return out, e
	}
	dir, e := os.MkdirTemp("", "forest-journal-memory-")
	if e != nil {
		return out, e
	}
	defer os.RemoveAll(dir)
	if e = copyJournal(input, dir); e != nil {
		return out, e
	}
	var s *topology.Store
	defer func() {
		if s != nil {
			err = errors.Join(err, s.Close())
		}
	}()
	if mode == "append" {
		s, e = topology.Open(dir, topology.Options{})
		if e != nil {
			return out, e
		}
	}
	runtime.GC()
	out = JournalMemoryResult{Mode: mode, InputBytes: len(input), Before: journalMemorySample()}
	start := time.Now()
	if mode == "open" {
		s, e = topology.Open(dir, topology.Options{})
	} else if mode == "append" {
		e = appendSelection(s)
	} else {
		return out, fmt.Errorf("unknown memory mode")
	}
	out.ElapsedNS = time.Since(start).Nanoseconds()
	if e != nil {
		return out, e
	}
	allocated := journalMemorySample().TotalAlloc
	runtime.GC()
	out.After = journalMemorySample()
	out.After.TotalAlloc = allocated
	if profile != "" {
		f, e := os.Create(profile)
		if e != nil {
			return out, e
		}
		e = pprof.WriteHeapProfile(f)
		if e = errors.Join(e, f.Close()); e != nil {
			return out, e
		}
	}
	out.Records = s.Inspect().Position.Sequence
	out.Revisions = len(s.History())
	if e = s.Close(); e != nil {
		return out, e
	}
	s = nil
	runtime.GC()
	out.Released = journalMemorySample()
	runtime.KeepAlive(input)
	return out, nil
}
func JournalProfile(inputPath, mode, kind, path string) (err error) {
	input, e := os.ReadFile(inputPath)
	if e != nil {
		return e
	}
	dir, e := os.MkdirTemp("", "forest-journal-profile-")
	if e != nil {
		return e
	}
	defer os.RemoveAll(dir)
	output, e := os.Create(path)
	if e != nil {
		return e
	}
	defer func() { err = errors.Join(err, output.Close()) }()
	if kind == "cpu" {
		if e = pprof.StartCPUProfile(output); e != nil {
			return e
		}
		defer pprof.StopCPUProfile()
	} else if kind != "allocs" {
		return fmt.Errorf("invalid profile kind")
	}
	// Includes copy/reopen setup; distinct appendLocked/scan stacks separate its
	// attribution. Instrumented batch time is not an append-only benchmark.
	for i := 0; i < 12; i++ {
		if e = copyJournal(input, dir); e != nil {
			return e
		}
		s, e := topology.Open(dir, topology.Options{})
		if e != nil {
			return e
		}
		if mode == "append" {
			e = appendSelection(s)
		} else if mode != "open" {
			e = fmt.Errorf("invalid profile mode")
		}
		if e = errors.Join(e, s.Close()); e != nil {
			return e
		}
	}
	if kind == "allocs" {
		runtime.GC()
		return pprof.Lookup("allocs").WriteTo(output, 0)
	}
	return nil
}
func JournalInputInfo(path string) (out []byte, err error) {
	data, e := os.ReadFile(path)
	if e != nil {
		return nil, e
	}
	dir, e := os.MkdirTemp("", "forest-journal-info-")
	if e != nil {
		return nil, e
	}
	defer os.RemoveAll(dir)
	if e = copyJournal(data, dir); e != nil {
		return nil, e
	}
	s, e := topology.Open(dir, topology.Options{})
	if e != nil {
		return nil, e
	}
	defer func() { err = errors.Join(err, s.Close()) }()
	return json.Marshal(struct {
		Bytes, Revisions, Entries int
		Position                  topology.Position
	}{len(data), len(s.History()), len(s.Journal()), s.Inspect().Position})
}
