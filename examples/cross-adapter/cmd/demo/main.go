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

	demo "github.com/Leeminjing/go-forest/examples/cross-adapter"
	"github.com/Leeminjing/go-forest/examples/cross-adapter/protocol"
)

func run() (err error) {
	address := flag.String("address", os.Getenv("FOREST_TEMPORAL_ADDRESS"), "Temporal address")
	queue := flag.String("queue", "", "isolated task queue")
	id := flag.String("id", "", "stable operation/workflow ID")
	directory := flag.String("directory", "", "owned native resource directory")
	setup := flag.Bool("setup", false, "explicitly create the example tables")
	worker := flag.Bool("worker", false, "run a supervised worker process")
	version := flag.String("version", "v1", "initial business implementation")
	ready := flag.String("ready", "", "worker readiness report path")
	stop := flag.String("stop", "", "worker graceful-stop marker path")
	commit := flag.String("commit-marker", "", "acceptance-only post-commit barrier report")
	flag.Parse()
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt)
	defer cancel()
	o := demo.Options{Address: *address, DSN: os.Getenv("FOREST_POSTGRES_DSN"), TaskQueue: *queue, Directory: *directory, Version: *version}
	if *setup {
		c, end := context.WithTimeout(ctx, 15*time.Second)
		err = demo.Setup(c, o.DSN)
		end()
		if err != nil {
			return err
		}
	}
	if *worker {
		return demo.RunWorker(ctx, o, demo.WorkerFiles{Ready: *ready, Stop: *stop, Commit: *commit})
	}
	if *id == "" {
		return protocol.ErrConflict
	}
	ctx, end := context.WithTimeout(ctx, 45*time.Second)
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
	in := protocol.Request{OperationID: *id, Target: *id, Payload: "oak"}
	firstID, err := r.Start(ctx, *id, in, false)
	if err != nil {
		return err
	}
	first, err := r.Result(ctx, firstID)
	if err != nil {
		return err
	}
	if err = r.Switch(ctx, "replace"); err != nil {
		return err
	}
	laterID, err := r.Start(ctx, *id+"-reconcile", in, false)
	if err != nil {
		return err
	}
	later, err := r.Result(ctx, laterID)
	if err != nil {
		return err
	}
	observation, err := r.Inspect(ctx, in)
	if err != nil {
		return err
	}
	if first.Receipt != later.Receipt || !later.Reused || observation.EffectCount != 1 {
		return errors.New("business receipt evidence inconsistent")
	}
	for _, pair := range []struct {
		result     protocol.Result
		generation uint64
	}{{first, 1}, {later, 2}} {
		for _, p := range pair.result.Trace {
			if p.Generation != pair.generation {
				return errors.New("cross-adapter generation mismatch")
			}
		}
	}
	before := r.BusinessCalls()
	if err = r.Replay(ctx, laterID); err != nil {
		return err
	}
	if before != r.BusinessCalls() {
		return errors.New("replay executed business")
	}
	if err = r.Close(ctx); err != nil {
		return err
	}
	return json.NewEncoder(os.Stdout).Encode(struct {
		FirstVersion, LaterVersion, ReceiptVersion string
		Reused                                     bool
		EffectCount                                int64
		Generation                                 uint64
	}{first.VersionAfter, later.VersionAfter, later.Receipt.EffectVersion, later.Reused, observation.EffectCount, 2})
}
func main() {
	if err := run(); err != nil {
		fmt.Fprintf(os.Stderr, "cross-adapter command failed (%T)\n", err)
		os.Exit(1)
	}
}
