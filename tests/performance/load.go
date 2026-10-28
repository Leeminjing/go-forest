package performance

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"sync"
	"sync/atomic"
	"time"
)

type LoadConfig struct {
	Graph                                    Config
	Workers, Rate, Queue, MaxRequests, Holds int
	Duration, UpdateEvery                    time.Duration
	Closed                                   bool
}
type Latency struct {
	Count                      int
	P50NS, P95NS, P99NS, MaxNS int64
}
type LoadResult struct {
	Config                                                                   LoadConfig
	Planned, Completed, Failed, Dropped, Unfinished, Updates, InvalidResults int64
	StopReason                                                               string
	ElapsedNS                                                                int64
	Success, Failure, Execution                                              Latency
	HeldGenerations                                                          []uint64
	ReleaseAtNS                                                              int64
}
type observation struct {
	total, execution int64
	failed, invalid  bool
}

func quantiles(values []int64) Latency {
	if len(values) == 0 {
		return Latency{}
	}
	sort.Slice(values, func(i, j int) bool { return values[i] < values[j] })
	q := func(p int) int64 { return values[(len(values)*p+99)/100-1] }
	return Latency{len(values), q(50), q(95), q(99), values[len(values)-1]}
}

// Load measures synthetic Go API requests with two reads in one Scope. Open
// mode includes queue delay from planned arrival, closed mode starts at execution.
func Load(c LoadConfig) (out LoadResult, err error) {
	if c.Workers < 1 || c.Workers > 128 || c.Queue < 0 || c.MaxRequests < 1 || c.MaxRequests > 1_000_000 || c.Duration <= 0 || c.Duration > time.Minute || c.Rate < 1 || c.Holds < 0 || c.Holds > 8 || c.Graph.D < 1 {
		return out, errors.New("invalid bounded load configuration")
	}
	planned := c.MaxRequests
	if !c.Closed {
		count := int(c.Duration.Seconds() * float64(c.Rate))
		if count > c.MaxRequests || count < 1 {
			return out, errors.New("open arrival plan exceeds sample bound")
		}
		planned = count
	}
	x, e := New(c.Graph, "")
	if e != nil {
		return out, e
	}
	ctx, cancel := context.WithTimeout(context.Background(), c.Duration+20*time.Second)
	defer cancel()
	defer func() { err = errors.Join(err, x.Close(ctx)) }()
	out.Config = c
	out.HeldGenerations = []uint64{}
	holds := []*held{}
	for i := 0; i < c.Holds; i++ {
		h, e := hold(ctx, x.F)
		if e != nil {
			return out, e
		}
		holds = append(holds, h)
		out.HeldGenerations = append(out.HeldGenerations, h.generation)
	}
	defer func() {
		if len(holds) > 0 {
			err = errors.Join(err, releaseHeld(holds))
		}
	}()
	// Capacity is fixed before measured work. Each worker owns its sample slice.
	if int64(planned)*int64(c.Workers) > 2_000_000 {
		return out, errors.New("worker sample capacity exceeds bound")
	}
	samples := make([][]observation, c.Workers)
	for i := range samples {
		samples[i] = make([]observation, 0, planned)
	}
	jobs := make(chan time.Time, c.Queue)
	start := make(chan struct{})
	var ready, workers, updater sync.WaitGroup
	ready.Add(c.Workers)
	workers.Add(c.Workers)
	var began time.Time
	var attempts, updates, failures atomic.Int64
	invoke := func() error {
		return x.Pinned(ctx, func(call context.Context, r Runner) error {
			a, e := r.Read(call)
			if e != nil {
				return e
			}
			g, e := x.F.Generation(call)
			if e != nil {
				return e
			}
			b, e := r.Read(call)
			if e != nil {
				return e
			}
			after, e := x.F.Generation(call)
			if e != nil {
				return e
			}
			if a != b || g != after || (a != x.Expected && a != x.Expected+1) {
				return fmt.Errorf("mixed load result %d/%d/%d/%d", a, b, g, after)
			}
			return nil
		})
	}
	for id := 0; id < c.Workers; id++ {
		id := id
		go func() {
			defer workers.Done()
			ready.Done()
			<-start
			perform := func(due time.Time) {
				execution := time.Now()
				e := invoke()
				finish := time.Now()
				samples[id] = append(samples[id], observation{finish.Sub(due).Nanoseconds(), finish.Sub(execution).Nanoseconds(), e != nil, e != nil && !errors.Is(e, context.Canceled) && !errors.Is(e, context.DeadlineExceeded)})
			}
			if c.Closed {
				limit := planned / c.Workers
				if id < planned%c.Workers {
					limit++
				}
				for j := 0; j < limit && time.Since(began) < c.Duration; j++ {
					attempts.Add(1)
					perform(time.Now())
				}
			} else {
				for due := range jobs {
					perform(due)
				}
			}
		}()
	}
	stopUpdates := make(chan struct{})
	if c.UpdateEvery > 0 {
		updater.Add(1)
		go func() {
			defer updater.Done()
			<-start
			ticker := time.NewTicker(c.UpdateEvery)
			defer ticker.Stop()
			replace := true
			for {
				select {
				case <-stopUpdates:
					return
				case <-ctx.Done():
					return
				case <-ticker.C:
					if e := x.Change(ctx, replace); e != nil {
						failures.Add(1)
					} else {
						updates.Add(1)
					}
					replace = !replace
				}
			}
		}()
	}
	ready.Wait()
	began = time.Now()
	close(start)
	type releaseResult struct {
		err    error
		offset int64
	}
	released := make(chan releaseResult, 1)
	stopped := make(chan struct{})
	if len(holds) > 0 {
		go func() {
			timer := time.NewTimer(c.Duration / 2)
			defer timer.Stop()
			select {
			case <-timer.C:
			case <-ctx.Done():
			case <-stopped:
			}
			e := releaseHeld(holds)
			released <- releaseResult{e, time.Since(began).Nanoseconds()}
		}()
	} else {
		released <- releaseResult{}
	}
	var dropped int64
	if !c.Closed {
		for i := 0; i < planned; i++ {
			due := began.Add(time.Duration(float64(time.Second) * float64(i) / float64(c.Rate)))
			if wait := time.Until(due); wait > 0 {
				timer := time.NewTimer(wait)
				select {
				case <-timer.C:
				case <-ctx.Done():
				}
				timer.Stop()
			}
			attempts.Add(1)
			select {
			case jobs <- due:
			default:
				dropped++
			}
		}
		close(jobs)
	}
	workers.Wait()
	close(stopped)
	close(stopUpdates)
	updater.Wait()
	out.ElapsedNS = time.Since(began).Nanoseconds()
	releasedResult := <-released
	if releasedResult.err != nil {
		return out, releasedResult.err
	}
	out.ReleaseAtNS = releasedResult.offset
	holds = nil
	out.StopReason = "arrival-plan-completed"
	if c.Closed {
		out.StopReason = "duration"
		if out.ElapsedNS < c.Duration.Nanoseconds() {
			out.StopReason = "request-cap"
		}
	}
	successes, failed, execution := []int64{}, []int64{}, []int64{}
	for _, worker := range samples {
		for _, v := range worker {
			if v.invalid {
				out.InvalidResults++
			}
			execution = append(execution, v.execution)
			if v.failed {
				failed = append(failed, v.total)
			} else {
				successes = append(successes, v.total)
			}
		}
	}
	out.Planned = attempts.Load()
	out.Completed = int64(len(execution))
	out.Failed = int64(len(failed))
	out.Dropped = dropped
	out.Unfinished = out.Planned - out.Completed - dropped
	out.Updates = updates.Load()
	out.Success, out.Failure, out.Execution = quantiles(successes), quantiles(failed), quantiles(execution)
	if failures.Load() > 0 || out.Unfinished != 0 || out.InvalidResults != 0 {
		return out, errors.New("load updates failed or requests were unaccounted")
	}
	return out, nil
}
