package einodemo_test

import (
	"context"
	"errors"
	"io"
	"strings"
	"sync"
	"testing"
	"time"

	ef "github.com/Leeminjing/go-forest/adapters/eino"
	demo "github.com/Leeminjing/go-forest/examples/eino"
	"github.com/Leeminjing/go-forest/forest"
	"github.com/cloudwego/eino/schema"
)

func newRuntime(t *testing.T, o demo.Options) *demo.Runtime {
	t.Helper()
	o.Directory = t.TempDir()
	r, err := demo.New(context.Background(), o)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		if err := r.Close(ctx); err != nil {
			t.Error(err)
		}
	})
	return r
}
func TestActualEnginesCoexistAndNativeGraphBranches(t *testing.T) {
	r := newRuntime(t, demo.Options{})
	ctx := context.Background()
	var wg sync.WaitGroup
	results := make(chan string, 16)
	fail := make(chan error, 16)
	for i := 0; i < 8; i++ {
		for _, engine := range []string{"eino", "forest"} {
			engine := engine
			wg.Add(1)
			go func() {
				defer wg.Done()
				answer, err := r.AskEngine(ctx, engine, "oak")
				if err != nil {
					fail <- err
				}
				results <- answer
			}()
		}
	}
	wg.Wait()
	close(results)
	close(fail)
	for err := range fail {
		t.Error(err)
	}
	for s := range results {
		if s != "eino:v1:oak/v1:oak" && s != "forest:v1:oak/v1:oak" {
			t.Fatal(s)
		}
	}
	err := r.Forest.Invoke(ctx, func(c context.Context) error {
		return forest.Use[ef.Graph[string, string]](c, r.Forest, "catalog/graph", "graph", func(g ef.Graph[string, string]) error {
			out, err := g.Invoke(c, "oak", ef.GraphCall{})
			if err != nil || out != "format:v1:oak" {
				t.Fatalf("graph=%s err=%v", out, err)
			}
			return g.Stream(c, "oak", ef.GraphCall{}, func(reader *schema.StreamReader[string]) error {
				var out strings.Builder
				for {
					v, err := reader.Recv()
					if err == io.EOF {
						break
					}
					if err != nil {
						return err
					}
					out.WriteString(v)
				}
				if out.String() != "format:v1:oak" {
					t.Fatal(out.String())
				}
				return nil
			})
		})
	})
	if err != nil {
		t.Fatal(err)
	}
	snap := r.Forest.Snapshot()
	if len(snap.Trees) != 12 || len(snap.NestBindings) != 2 {
		t.Fatalf("topology=%+v", snap)
	}
}
func TestEngineRebindAndActiveNativeReplacement(t *testing.T) {
	entered := make(chan struct{}, 1)
	release := make(chan struct{})
	var once sync.Once
	r := newRuntime(t, demo.Options{BeforeLookup: func(ctx context.Context, key string) error {
		if key == "hold" {
			once.Do(func() { entered <- struct{}{} })
			select {
			case <-release:
			case <-ctx.Done():
				return ctx.Err()
			}
		}
		return nil
	}})
	defer func() {
		select {
		case <-release:
		default:
			close(release)
		}
	}()
	ctx := context.Background()
	done := make(chan string, 1)
	errs := make(chan error, 1)
	go func() { out, err := r.Ask(ctx, "hold"); done <- out; errs <- err }()
	select {
	case <-entered:
	case <-time.After(3 * time.Second):
		t.Fatal("native tool not entered")
	}
	if _, err := r.Rebind(ctx, "forest"); err != nil {
		t.Fatal(err)
	}
	if _, err := r.ReplaceCatalogAndEino(ctx); err != nil {
		t.Fatal(err)
	}
	out, err := r.Ask(ctx, "oak")
	if err != nil || out != "forest:v2:oak/v2:oak" {
		t.Fatalf("new engine=%s err=%v", out, err)
	}
	out, err = r.AskEngine(ctx, "eino", "oak")
	if err != nil || out != "eino:v2:oak/v2:oak" {
		t.Fatalf("new native=%s err=%v", out, err)
	}
	old := 0
	for _, i := range r.Forest.Snapshot().Instances {
		if (i.TreeID == "catalog/eino" || i.TreeID == "catalog") && i.State == forest.Draining {
			old++
		}
	}
	if old != 2 {
		t.Fatal("old native/provider released during active task")
	}
	close(release)
	select {
	case out = <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("old call stuck")
	}
	if err = <-errs; err != nil || out != "eino:v1:hold/v1:hold" {
		t.Fatalf("mixed old call=%s err=%v", out, err)
	}
	if err = r.Forest.Wait(ctx); err != nil {
		t.Fatal(err)
	}
}
func TestNarrowContractDoesNotPretendNativeApprovalIsText(t *testing.T) {
	r := newRuntime(t, demo.Options{RequireForestApproval: true})
	_, err := r.AskEngine(context.Background(), "forest", "oak")
	var native *demo.ContinuationRequired
	if !errors.As(err, &native) || native.Engine != "forest-agent" || native.ID == "" {
		t.Fatalf("native state hidden: %v", err)
	}
}
