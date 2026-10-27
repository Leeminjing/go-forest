package testkit_test

import (
	"context"
	"errors"
	tk "github.com/Leeminjing/go-forest/testkit"
	"os"
	"testing"
)

func TestDrainRequiresActualExitAndPreservesBorrowedResources(t *testing.T) {
	file, err := os.CreateTemp(t.TempDir(), "owned")
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	gate := tk.NewBarrier()
	defer gate.Release()
	done := make(chan error, 1)
	go func() { <-ctx.Done(); done <- gate.Hold(context.WithoutCancel(ctx)) }()
	cancel()
	if err = tk.Await(context.Background(), gate.Arrived()); err != nil {
		t.Fatal(err)
	}
	if _, err = file.Stat(); err != nil {
		t.Fatal("real file prematurely closed", err)
	}
	live := []tk.Resource{{ID: "owned", Ownership: tk.Owned, Usable: true}, {ID: "borrowed", Ownership: tk.Borrowed, Usable: true, CloseCount: 2}}
	gate.Release()
	if err = <-done; err != nil {
		t.Fatal(err)
	}
	if err = file.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err = file.Read(make([]byte, 1)); !errors.Is(err, os.ErrClosed) {
		t.Fatal(err)
	}
	closed := append([]tk.Resource(nil), live...)
	closed[0].Usable = false
	closed[0].CloseCount = 1
	events := []tk.Event{{Stage: "entered", Resources: live}, {Stage: "notified"}, {Stage: "held", Resources: live}, {Stage: "released"}, {Stage: "exited"}, {Stage: "joined", Resources: closed}}
	good := tk.DrainEvidence{Label: "real owned file", Trace: trace(t, events...)}
	if err = tk.CheckDrain(good); err != nil {
		t.Fatal(err)
	}
	for _, kind := range []string{"early-close", "double-close", "borrowed-close", "no-exit", "probe-error", "counter-rollover"} {
		t.Run(kind, func(t *testing.T) {
			bad := good
			points := good.Trace.Entries()
			updates := []tk.Event{}
			for _, p := range points {
				if kind == "counter-rollover" {
					if p.Stage == "entered" || p.Stage == "held" {
						p.Resources[0].CloseCount = ^uint64(0)
					}
					if p.Stage == "joined" {
						p.Resources[0].CloseCount = 0
					}
				}
				if kind == "no-exit" && p.Stage == "exited" {
					continue
				}
				if kind == "early-close" && p.Stage == "held" {
					p.Resources[0].Usable = false
					p.Resources[0].CloseCount = 1
				}
				if kind == "double-close" && p.Stage == "joined" {
					p.Resources[0].CloseCount = 2
				}
				if kind == "borrowed-close" && p.Stage == "joined" {
					p.Resources[1].Usable = false
					p.Resources[1].CloseCount = 3
				}
				updates = append(updates, p.Event)
			}
			bad.Trace = trace(t, updates...)
			if kind == "probe-error" {
				bad.ProbeError = os.ErrPermission
			}
			err := tk.CheckDrain(bad)
			if kind == "probe-error" {
				if !errors.Is(err, os.ErrPermission) {
					t.Fatal(err)
				}
			} else if !errors.Is(err, tk.ErrViolation) {
				t.Fatal(kind, err)
			}
		})
	}
}
