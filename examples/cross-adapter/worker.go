package crossdemo

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"time"

	"github.com/Leeminjing/go-forest/examples/cross-adapter/protocol"
	"github.com/Leeminjing/go-forest/forest"
)

// WorkerFiles is explicit process supervision for the example, not workflow
// persistence. The commit barrier is an opt-in acceptance fault injection.
type WorkerFiles struct{ Ready, Stop, Commit string }
type WorkerReport struct {
	Process, WorkerIdentity, FxIdentity string
	Generation                          uint64
	Resources                           ResourceReport
	Stopped                             bool
}

func writeJSON(path string, value any) error {
	data, err := json.Marshal(value)
	if err != nil {
		return err
	}
	file, err := os.CreateTemp(filepath.Dir(path), ".cross-report-")
	if err != nil {
		return err
	}
	name := file.Name()
	defer os.Remove(name)
	if err = file.Chmod(0600); err == nil {
		_, err = file.Write(data)
	}
	if err == nil {
		err = file.Sync()
	}
	closed := file.Close()
	if err == nil {
		err = closed
	}
	if err != nil {
		return err
	}
	return os.Rename(name, path)
}
func RunWorker(ctx context.Context, o Options, files WorkerFiles) (err error) {
	if files.Ready == "" || files.Stop == "" {
		return errors.New("ready and stop paths required")
	}
	if files.Commit != "" {
		o.AfterCommit = func(c context.Context, receipt protocol.Receipt) error {
			if err := writeJSON(files.Commit, receipt); err != nil {
				return err
			}
			<-c.Done()
			_ = writeJSON(files.Commit+".returned", struct{ Returned bool }{true})
			return c.Err()
		}
	}
	r, err := New(ctx, o)
	if err != nil {
		return err
	}
	defer func() {
		c, cancel := context.WithTimeout(context.Background(), 20*time.Second)
		defer cancel()
		err = errors.Join(err, r.Close(c))
	}()
	if err = r.Enable(ctx); err != nil {
		return err
	}
	report := WorkerReport{Process: r.Process, WorkerIdentity: r.bridge.WorkerIdentity, FxIdentity: r.bridge.FxIdentity, Generation: r.Forest.Snapshot().Generation, Resources: r.Resources(ctx)}
	if err = writeJSON(files.Ready, report); err != nil {
		return err
	}
	ticker := time.NewTicker(20 * time.Millisecond)
	defer ticker.Stop()
	for {
		_, probe := os.Stat(files.Stop)
		if probe == nil || ctx.Err() != nil {
			break
		}
		if !errors.Is(probe, os.ErrNotExist) {
			return probe
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
	report.Resources = r.Resources(c)
	report.Stopped = true
	for _, i := range r.Forest.Snapshot().Instances {
		if i.State != forest.Stopped {
			report.Stopped = false
		}
	}
	return writeJSON(files.Ready+".stopped", report)
}
