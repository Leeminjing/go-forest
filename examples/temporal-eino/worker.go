package einodemo

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"time"

	"github.com/Leeminjing/go-forest/examples/temporal-eino/internal/state"
	"github.com/Leeminjing/go-forest/examples/temporal-eino/protocol"
)

// WorkerFiles is local process supervision; barriers are acceptance faults.
type WorkerFiles struct{ Ready, Stop, Barrier, Mode string }
type WorkerReport struct {
	Process, Worker, Agent string
	Resources              protocol.ResourceReport
	Counts                 protocol.Counts
	Evidence               protocol.Evidence
}

func report(path string, v any) error {
	data, e := json.Marshal(v)
	if e != nil {
		return e
	}
	f, e := os.CreateTemp(filepath.Dir(path), ".report-")
	if e != nil {
		return e
	}
	defer os.Remove(f.Name())
	if e = f.Chmod(0600); e == nil {
		_, e = f.Write(data)
	}
	if e == nil {
		e = f.Sync()
	}
	ce := f.Close()
	if e == nil {
		e = ce
	}
	if e != nil {
		return e
	}
	return os.Rename(f.Name(), path)
}

// HandoverAfterExit is a trusted supervisor operation. The caller must already
// have positively joined the old OS process, not inferred death from a timeout.
func HandoverAfterExit(directory, oldOwner string) error {
	return state.HandoverAfterExit(directory, oldOwner)
}

// Audit only reads immutable application evidence; it never mounts FileStore.
func Audit(directory string, in protocol.Input) (protocol.Evidence, error) {
	return state.Audit(directory, in)
}
func RunWorker(ctx context.Context, o Options, files WorkerFiles) (err error) {
	if files.Ready == "" || files.Stop == "" || ((files.Mode != "") && (files.Barrier == "" || (files.Mode != "paused" && files.Mode != "completed" && files.Mode != "unknown"))) {
		return protocol.ErrConflict
	}
	var r *Runtime
	original := o.Hook
	o.Hook = func(c context.Context, p protocol.Point) error {
		if original != nil {
			if e := original(c, p); e != nil {
				return e
			}
		}
		matches := files.Mode == "unknown" && p.Stage == "effect.published"
		matches = matches || (files.Mode != "" && p.Stage == "step.sealed" && p.Result != nil && p.Result.State == files.Mode)
		if !matches {
			return nil
		}
		ev, e := r.Inspect(c, p.Input)
		if e != nil {
			return e
		}
		frame := WorkerReport{Process: r.Process, Worker: r.bridge.WorkerIdentity, Agent: r.Process + "/" + r.Forest.Snapshot().Trees["app/adk"].InstanceID, Resources: r.Resources(), Counts: r.Counts(), Evidence: ev}
		if e = report(files.Barrier, frame); e != nil {
			return e
		}
		<-c.Done()
		_ = report(files.Barrier+".returned", struct{ Returned bool }{true})
		return c.Err()
	}
	r, err = New(ctx, o)
	if r != nil {
		defer func() {
			c, cancel := context.WithTimeout(context.Background(), 20*time.Second)
			defer cancel()
			err = errors.Join(err, r.Close(c))
		}()
	}
	if err != nil {
		return err
	}
	if err = r.Enable(ctx); err != nil {
		return err
	}
	frame := WorkerReport{Process: r.Process, Worker: r.bridge.WorkerIdentity, Agent: r.Process + "/" + r.Forest.Snapshot().Trees["app/adk"].InstanceID, Resources: r.Resources(), Counts: r.Counts()}
	if err = report(files.Ready, frame); err != nil {
		return err
	}
	ticker := time.NewTicker(20 * time.Millisecond)
	defer ticker.Stop()
	for {
		_, e := os.Stat(files.Stop)
		if e == nil || ctx.Err() != nil {
			break
		}
		if !errors.Is(e, os.ErrNotExist) {
			return e
		}
		select {
		case <-ctx.Done():
		case <-ticker.C:
		}
	}
	c, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	if err = r.Close(c); err != nil {
		return err
	}
	frame.Resources = r.Resources()
	frame.Counts = r.Counts()
	return report(files.Ready+".stopped", frame)
}
