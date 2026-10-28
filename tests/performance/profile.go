package performance

import (
	"context"
	"fmt"
	"os"
	"runtime"
	"runtime/pprof"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Leeminjing/go-forest/forest"
)

func emptyPlan(g uint64) forest.ChangePlan { return forest.ChangePlan{BaseGeneration: g} }

// Profile excludes fixture construction and captures only real pinned reads.
// Each kind runs separately; these instrumented runs are not benchmark results.
func Profile(cfg Config, kind, path string, duration time.Duration, workers int, enter bool) (uint64, error) {
	if duration <= 0 || workers < 1 {
		return 0, fmt.Errorf("invalid profiling duration/workers")
	}
	x, err := New(cfg, "")
	if err != nil {
		return 0, err
	}
	defer x.Close(context.Background())
	output, err := os.Create(path)
	if err != nil {
		return 0, err
	}
	defer output.Close()
	start := make(chan struct{})
	var ready, joined sync.WaitGroup
	ready.Add(workers)
	joined.Add(workers)
	var count atomic.Uint64
	failure := make(chan error, workers)
	deadline := time.Time{}
	for i := 0; i < workers; i++ {
		go func() {
			defer joined.Done()
			invoke := func(fn func(context.Context, Runner) error) error { return x.Pinned(context.Background(), fn) }
			if enter {
				invoke = func(fn func(context.Context, Runner) error) error { return fn(context.Background(), nil) }
			}
			signaled := false
			defer func() {
				if !signaled {
					ready.Done()
				}
			}()
			err := invoke(func(ctx context.Context, r Runner) error {
				ready.Done()
				signaled = true
				<-start
				var local uint64
				for time.Now().Before(deadline) {
					for batch := 0; batch < 256; batch++ {
						var got uint64
						var e error
						if enter {
							got, e = x.Call(ctx)
						} else {
							got, e = r.Read(ctx)
						}
						if !checkedResult(x, got, e) {
							return fmt.Errorf("profile read: %d %w", got, e)
						}
						local++
					}
				}
				count.Add(local)
				return nil
			})
			if err != nil {
				failure <- err
			}
		}()
	}
	ready.Wait()
	switch kind {
	case "cpu":
		err = pprof.StartCPUProfile(output)
	case "block":
		runtime.SetBlockProfileRate(1)
	case "mutex":
		runtime.SetMutexProfileFraction(1)
	case "heap", "allocs":
	default:
		err = fmt.Errorf("unknown profile %q", kind)
	}
	deadline = time.Now().Add(duration)
	if err != nil {
		deadline = time.Now()
	}
	close(start)
	joined.Wait()
	switch kind {
	case "cpu":
		if err == nil {
			pprof.StopCPUProfile()
		}
	case "block":
		runtime.SetBlockProfileRate(0)
	case "mutex":
		runtime.SetMutexProfileFraction(0)
	}
	if err != nil {
		return count.Load(), err
	}
	select {
	case e := <-failure:
		return count.Load(), e
	default:
	}
	if kind != "cpu" {
		if kind == "heap" || kind == "allocs" {
			runtime.GC()
		}
		if err = pprof.Lookup(kind).WriteTo(output, 0); err != nil {
			return count.Load(), err
		}
	}
	return count.Load(), nil
}
func checkedResult(x *Fixture, n uint64, e error) bool { return e == nil && n == x.Expected }
