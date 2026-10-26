package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"time"

	demo "github.com/Leeminjing/go-forest/examples/temporal-eino"
	"github.com/Leeminjing/go-forest/examples/temporal-eino/protocol"
)

func run() (err error) {
	address := flag.String("address", os.Getenv("FOREST_TEMPORAL_ADDRESS"), "Temporal address")
	queue := flag.String("queue", "", "isolated task queue")
	id := flag.String("id", "", "stable execution/operation ID")
	dir := flag.String("directory", "", "owned state directory")
	version := flag.String("version", "v1", "initial business version")
	worker := flag.Bool("worker", false, "supervised worker mode")
	ready := flag.String("ready", "", "ready report")
	stop := flag.String("stop", "", "stop marker")
	barrier := flag.String("barrier", "", "acceptance fault report")
	mode := flag.String("barrier-mode", "", "acceptance-only paused/completed/unknown barrier")
	handover := flag.String("handover-after-exit", "", "trusted supervisor: old token after actual OS Wait")
	audit := flag.Bool("audit", false, "read immutable evidence without mounting Eino state")
	flag.Parse()
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt)
	defer cancel()
	if *handover != "" {
		return demo.HandoverAfterExit(*dir, *handover)
	}
	in := protocol.Input{ExecutionID: *id, OperationID: *id, Text: "oak"}
	if *audit {
		out, e := demo.Audit(*dir, in)
		if e != nil {
			return e
		}
		return json.NewEncoder(os.Stdout).Encode(out)
	}
	o := demo.Options{Address: *address, Queue: *queue, Directory: *dir, Version: *version}
	if *worker {
		return demo.RunWorker(ctx, o, demo.WorkerFiles{Ready: *ready, Stop: *stop, Barrier: *barrier, Mode: *mode})
	}
	ctx, end := context.WithTimeout(ctx, 60*time.Second)
	defer end()
	r, err := demo.New(ctx, o)
	if r != nil {
		defer func() {
			c, done := context.WithTimeout(context.Background(), 20*time.Second)
			defer done()
			err = errors.Join(err, r.Close(c))
		}()
	}
	if err != nil {
		return err
	}
	if err = r.Enable(ctx); err != nil {
		return err
	}
	ref, err := r.Start(ctx, *id, in)
	if err != nil {
		return err
	}
	var state protocol.WorkflowState
	for {
		state, err = r.Query(ctx, ref)
		if err == nil && state.Stage == "paused" {
			break
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(10 * time.Millisecond):
		}
	}
	if len(state.Pause.Targets) != 1 {
		return protocol.ErrUnknown
	}
	if err = r.Switch(ctx, "replace"); err != nil {
		return err
	}
	if err = r.Decide(ctx, ref, protocol.Decision{ID: "continue", ExecutionID: *id, Token: state.Pause.Token, Target: state.Pause.Targets[0], Value: "continue"}); err != nil {
		return err
	}
	out, err := r.Result(ctx, ref)
	if err != nil {
		return err
	}
	ev, err := r.Inspect(ctx, in)
	if err != nil {
		return err
	}
	counts := r.Counts()
	if err = r.Replay(ctx, ref); err != nil {
		return err
	}
	if counts != r.Counts() {
		return errors.New("replay invoked native execution")
	}
	if err = r.Close(ctx); err != nil {
		return err
	}
	resources := r.Resources()
	return json.NewEncoder(os.Stdout).Encode(struct {
		State, Text, EffectVersion string
		ResumeGeneration           uint64
		Artifacts                  int
		Query, Resume, Apply       int64
		Released                   bool
	}{out.State, out.Text, out.Receipt.Version, out.Generation, ev.Artifacts, counts.Query, counts.Resume, counts.Apply, !resources.Claim && !resources.NativeOpen && !resources.BusinessOpen})
}
func main() {
	if e := run(); e != nil {
		fmt.Fprintf(os.Stderr, "temporal-eino command failed (%T)\n", e)
		os.Exit(1)
	}
}
