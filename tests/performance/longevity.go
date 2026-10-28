package performance

import (
	"context"
	"errors"
	"fmt"
	"os"
	"runtime"
	"runtime/pprof"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Leeminjing/go-forest/forest"
)

type MemorySample struct {
	Stage                                                                           string
	Generation                                                                      uint64
	Active, Stopped, Draining, Diagnostics, OpenResources, HeldVersions, Goroutines int
	HeapAlloc, HeapInuse, TotalAlloc                                                uint64
	SnapshotNS, WaitNS                                                              int64
}
type LongevityResult struct {
	Kind          string
	Rounds, Holds int
	Samples       []MemorySample
}
type resourceTracker struct {
	mu     sync.Mutex
	files  map[uint64]*os.File
	serial atomic.Uint64
}
type physical struct {
	id      uint64
	file    *os.File
	payload []byte
}

func (p *physical) Value(ctx context.Context) (uint64, error) {
	if e := ctx.Err(); e != nil {
		return 0, e
	}
	var b [32]byte
	n, e := p.file.ReadAt(b[:], 0)
	if n == 0 {
		return 0, e
	}
	if string(b[:n]) != strconv.FormatUint(p.id, 10) || p.payload[len(p.payload)-1] != 17 {
		return 0, errors.New("physical resource corrupted")
	}
	return p.id, nil
}
func (t *resourceTracker) definition(directory, version string) forest.Definition {
	return forest.Definition{TypeID: "longevity.resource", Version: version, HotSwappable: true, Capabilities: []forest.CapabilityDescriptor{forest.Capability[Value]("value", forest.Public)}, Build: func(_ context.Context, e *forest.BuildEnv) (forest.Lifecycle, error) {
		file, err := os.CreateTemp(directory, "physical-")
		if err != nil {
			return nil, err
		}
		id := t.serial.Add(1)
		if _, err = file.WriteString(strconv.FormatUint(id, 10)); err != nil {
			file.Close()
			os.Remove(file.Name())
			return nil, err
		}
		object := &physical{id: id, file: file, payload: make([]byte, 128<<10)}
		for i := 0; i < len(object.payload); i += 4096 {
			object.payload[i] = 17
		}
		object.payload[len(object.payload)-1] = 17
		t.mu.Lock()
		t.files[id] = file
		t.mu.Unlock()
		return forest.Hooks{OnStop: func(context.Context) error {
			t.mu.Lock()
			defer t.mu.Unlock()
			if err := file.Close(); err != nil {
				return err
			}
			var byteProbe [1]byte
			if _, err := file.ReadAt(byteProbe[:], 0); !errors.Is(err, os.ErrClosed) {
				return errors.New("resource did not actually close")
			}
			if err := os.Remove(file.Name()); err != nil {
				return err
			}
			delete(t.files, id)
			return nil
		}}, forest.Provide(e, "value", object)
	}}
}

func Longevity(rounds, holds int, kind, directory, profilePath string) (out LongevityResult, err error) {
	if rounds < 1 || holds < 0 || holds > rounds || (kind != "replace" && kind != "rebind" && kind != "mixed") {
		return out, errors.New("invalid longevity workload")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	f := forest.New(forest.Options{EventCapacity: 128})
	defer func() {
		if f != nil {
			err = errors.Join(err, f.Close(ctx))
		}
	}()
	tracker := &resourceTracker{files: map[uint64]*os.File{}}
	one, two := tracker.definition(directory, "1"), tracker.definition(directory, "2")
	consumer := forest.Definition{TypeID: "longevity.consumer", Version: "1", Ports: []forest.PortDescriptor{forest.Port[Value]("input", forest.Required())}, Capabilities: []forest.CapabilityDescriptor{forest.Capability[Runner]("read", forest.Public)}, Build: func(_ context.Context, e *forest.BuildEnv) (forest.Lifecycle, error) {
		ref, err := forest.Reference[Value](e, "input")
		if err != nil {
			return nil, err
		}
		return forest.Hooks{}, forest.Provide(e, "read", &reader{ref: ref})
	}}
	for _, d := range []forest.Definition{one, two, consumer} {
		if err = f.Register(d); err != nil {
			return out, err
		}
	}
	generation, err := f.Apply(ctx, forest.ChangePlan{Operations: []forest.Operation{forest.AddRoot("resource", one.Ref()), forest.AddRoot("alternative", two.Ref()), forest.AddRoot("consumer", consumer.Ref()), forest.Connect(forest.Link{ID: "input", Consumer: "consumer", Port: "input", Provider: "resource", Capability: "value"})}})
	if err != nil {
		return out, err
	}
	out = LongevityResult{Kind: kind, Rounds: rounds, Holds: holds, Samples: []MemorySample{}}
	heldCalls := []*held{}
	defer func() {
		if len(heldCalls) > 0 {
			err = errors.Join(err, releaseHeld(heldCalls))
		}
	}()
	sample := func(stage string, wait bool) error {
		var waitNS int64
		if wait {
			start := time.Now()
			if err := f.Wait(ctx); err != nil {
				return err
			}
			waitNS = time.Since(start).Nanoseconds()
		}
		start := time.Now()
		s := f.Snapshot()
		snapshotNS := time.Since(start).Nanoseconds()
		row := MemorySample{Stage: stage, Generation: s.Generation, Diagnostics: len(s.Instances), SnapshotNS: snapshotNS, WaitNS: waitNS, Goroutines: runtime.NumGoroutine()}
		for _, i := range s.Instances {
			switch i.State {
			case forest.Active:
				row.Active++
			case forest.Stopped:
				row.Stopped++
			default:
				row.Draining++
			}
		}
		versions := map[uint64]bool{}
		for _, h := range heldCalls {
			versions[h.generation] = true
		}
		row.HeldVersions = len(versions)
		tracker.mu.Lock()
		row.OpenResources = len(tracker.files)
		for _, file := range tracker.files {
			if _, e := file.Stat(); e != nil {
				tracker.mu.Unlock()
				return e
			}
		}
		tracker.mu.Unlock()
		s = forest.Snapshot{}
		runtime.GC()
		var m runtime.MemStats
		runtime.ReadMemStats(&m)
		row.HeapAlloc, row.HeapInuse, row.TotalAlloc = m.HeapAlloc, m.HeapInuse, m.TotalAlloc
		out.Samples = append(out.Samples, row)
		return nil
	}
	if err = sample("initial", true); err != nil {
		return out, err
	}
	version, rebound := "1", false
	for i := 0; i < rounds; i++ {
		if len(heldCalls) < holds && i%(max(1, rounds/max(1, holds))) == 0 {
			h, e := hold(ctx, f)
			if e != nil {
				return out, e
			}
			heldCalls = append(heldCalls, h)
		}
		var op forest.Operation
		if kind == "replace" || (kind == "mixed" && i%2 == 0) {
			if version == "1" {
				version = "2"
			} else {
				version = "1"
			}
			ref := one.Ref()
			if version == "2" {
				ref = two.Ref()
			}
			op = forest.Replace("resource", ref, false)
		} else {
			rebound = !rebound
			provider := forest.TreeID("resource")
			if rebound {
				provider = "alternative"
			}
			op = forest.Rebind(forest.Link{ID: "input", Consumer: "consumer", Port: "input", Provider: provider, Capability: "value"})
		}
		generation, err = f.Apply(ctx, forest.ChangePlan{BaseGeneration: generation, Operations: []forest.Operation{op}})
		if err != nil {
			return out, err
		}
		if (i+1)%100 == 0 || i+1 == rounds {
			if err = sample(fmt.Sprintf("round-%d", i+1), len(heldCalls) == 0); err != nil {
				return out, err
			}
		}
	}
	if err = sample("held", len(heldCalls) == 0); err != nil {
		return out, err
	}
	if err = releaseHeld(heldCalls); err != nil {
		return out, err
	}
	heldCalls = nil
	if err = sample("released", true); err != nil {
		return out, err
	}
	if out.Samples[len(out.Samples)-1].OpenResources != 2 {
		return out, errors.New("retired native resources remained open")
	}
	if profilePath != "" {
		file, e := os.Create(profilePath)
		if e != nil {
			return out, e
		}
		runtime.GC()
		e = pprof.WriteHeapProfile(file)
		err = errors.Join(e, file.Close())
		if err != nil {
			return out, err
		}
	}
	if err = f.Close(ctx); err != nil {
		return out, err
	}
	if err = sample("closed", true); err != nil {
		return out, err
	}
	if out.Samples[len(out.Samples)-1].OpenResources != 0 {
		return out, errors.New("closed forest still owns open resources")
	}
	f = nil
	return out, nil
}
