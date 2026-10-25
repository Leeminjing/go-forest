package einoforest_test

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"

	ef "github.com/Leeminjing/go-forest/adapters/eino"
)

func TestCheckpointProfileCorruptionAndFailedSave(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	p := ef.Profile{Engine: "eino/0.9.22", Namespace: "native", Schema: "one"}
	s, err := ef.NewFileStore(dir, p)
	if err != nil {
		t.Fatal(err)
	}
	if err = s.Set(ctx, "key", []byte("first")); err != nil {
		t.Fatal(err)
	}
	if err = s.Set(ctx, "key", []byte("second")); err != nil {
		t.Fatal(err)
	}
	reopened, _ := ef.NewFileStore(dir, p)
	v, ok, err := reopened.Get(ctx, "key")
	if err != nil || !ok || string(v) != "second" {
		t.Fatal(ok, err)
	}
	foreign := p
	foreign.Engine = "other-engine"
	wrong, _ := ef.NewFileStore(dir, foreign)
	if _, _, err = wrong.Get(ctx, "key"); !errors.Is(err, ef.ErrProfile) {
		t.Fatal(err)
	}
	if err = wrong.Set(ctx, "key", []byte("foreign")); !errors.Is(err, ef.ErrProfile) {
		t.Fatal(err)
	}
	canceled, cancel := context.WithCancel(ctx)
	cancel()
	if err = s.Set(canceled, "key", []byte("canceled")); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	files, err := filepath.Glob(filepath.Join(dir, "*", "*.json"))
	if err != nil || len(files) != 2 {
		t.Fatal(files, err)
	}
	if err = os.WriteFile(files[1], []byte("broken"), 0600); err != nil {
		t.Fatal(err)
	}
	if _, _, err = s.Get(ctx, "key"); !errors.Is(err, ef.ErrCorrupt) {
		t.Fatalf("corrupt newest revision silently fell back: %v", err)
	}
	if err = s.Set(ctx, "key", []byte("overwrite")); !errors.Is(err, ef.ErrCorrupt) {
		t.Fatal(err)
	}
}
