package einodemo_test

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	ef "github.com/Leeminjing/go-forest/adapters/eino"
	demo "github.com/Leeminjing/go-forest/examples/temporal-eino"
	"github.com/Leeminjing/go-forest/examples/temporal-eino/protocol"
	"go.temporal.io/sdk/temporal"
)

func TestCheckpointFaultsNeverRestartTheNativeAgent(t *testing.T) {
	for _, mode := range []string{"missing", "corrupt", "changed", "profile"} {
		t.Run(mode, func(t *testing.T) {
			ctx, o := config(t)
			a, e := demo.New(ctx, o)
			if e != nil {
				t.Fatal(e)
			}
			defer a.Close(ctx)
			if e = a.Enable(ctx); e != nil {
				t.Fatal(e)
			}
			id, e := a.Start(ctx, o.Queue, input(o))
			if e != nil {
				t.Fatal(e)
			}
			pause := paused(t, ctx, a, id).Pause
			if e = a.Close(ctx); e != nil {
				t.Fatal(e)
			}
			if mode == "changed" {
				store, e := ef.NewFileStore(filepath.Join(o.Directory, "native"), ef.Profile{Engine: "eino/0.9.22", Namespace: "temporal-eino", Schema: protocol.Profile})
				if e != nil {
					t.Fatal(e)
				}
				if e = store.Set(ctx, pause.Checkpoint, []byte{0xff, 0xfe, 0, 1}); e != nil {
					t.Fatal(e)
				}
			} else if mode == "profile" {
				o.Profile = "incompatible-execution-schema"
			} else {
				var files []string
				e = filepath.WalkDir(filepath.Join(o.Directory, "native"), func(path string, d os.DirEntry, e error) error {
					if e != nil {
						return e
					}
					if !d.IsDir() && filepath.Ext(path) == ".json" {
						files = append(files, path)
					}
					return nil
				})
				if e != nil || len(files) != 1 {
					t.Fatal(files, e)
				}
				if mode == "missing" {
					e = os.Remove(files[0])
				} else {
					e = os.WriteFile(files[0], []byte("corrupt"), 0600)
				}
				if e != nil {
					t.Fatal(e)
				}
			}
			b, e := demo.New(ctx, o)
			if e != nil {
				t.Fatal(e)
			}
			defer b.Close(ctx)
			if e = b.Enable(ctx); e != nil {
				t.Fatal(e)
			}
			if e = b.Decide(ctx, id, protocol.Decision{ID: "continue", ExecutionID: o.Queue, Token: pause.Token, Target: pause.Targets[0], Value: "continue"}); e != nil {
				t.Fatal(e)
			}
			_, e = b.Result(ctx, id)
			var app *temporal.ApplicationError
			want := "CheckpointInvalid"
			if mode == "changed" {
				want = "OutcomeUnknown"
			}
			if !errors.As(e, &app) || app.Type() != want {
				t.Fatal("checkpoint error lost", e)
			}
			if counts := b.Counts(); counts.Query != 0 || counts.Resume != 0 || counts.Apply != 0 {
				t.Fatal("native fallback executed", counts)
			}
		})
	}
}
func TestOrphanNativeCheckpointCannotAuthorizeFreshRun(t *testing.T) {
	ctx, o := config(t)
	s, e := ef.NewFileStore(filepath.Join(o.Directory, "native"), ef.Profile{Engine: "eino/0.9.22", Namespace: "temporal-eino", Schema: protocol.Profile})
	if e != nil {
		t.Fatal(e)
	}
	if e = s.Set(ctx, protocol.Checkpoint(input(o)), []byte("native state without a step receipt")); e != nil {
		t.Fatal(e)
	}
	r, e := demo.New(ctx, o)
	if e != nil {
		t.Fatal(e)
	}
	defer r.Close(ctx)
	if e = r.Enable(ctx); e != nil {
		t.Fatal(e)
	}
	id, e := r.Start(ctx, o.Queue, input(o))
	if e != nil {
		t.Fatal(e)
	}
	bounded, end := context.WithTimeout(ctx, 5*time.Second)
	defer end()
	_, e = r.Result(bounded, id)
	var app *temporal.ApplicationError
	if !errors.As(e, &app) || app.Type() != "OutcomeUnknown" {
		t.Fatal("orphan checkpoint restarted instead of reconciliation", e)
	}
	if r.Counts().Query != 0 || r.Counts().Apply != 0 {
		t.Fatal(r.Counts())
	}
}
