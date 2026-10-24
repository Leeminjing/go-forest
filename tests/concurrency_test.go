package tests

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"

	"github.com/Leeminjing/go-forest/forest"
)

func TestConcurrentChangesConflictWithoutLostUpdates(t *testing.T) {
	f := newForest(t)
	d := register(t, f, emptyDef())
	start := make(chan struct{})
	results := make(chan error, 2)
	for _, id := range []forest.TreeID{"a", "b"} {
		go func(id forest.TreeID) {
			<-start
			_, err := f.Apply(context.Background(), forest.ChangePlan{Operations: []forest.Operation{forest.AddRoot(id, d)}})
			results <- err
		}(id)
	}
	close(start)
	success, conflict := 0, 0
	for range 2 {
		err := receive(t, results)
		if err == nil {
			success++
		} else if errors.Is(err, forest.ErrGenerationConflict) {
			conflict++
		} else {
			t.Fatal(err)
		}
	}
	if success != 1 || conflict != 1 || f.Snapshot().Generation != 1 || len(f.Snapshot().Trees) != 1 {
		t.Fatal("lost update")
	}
}

func TestOneInvocationUsesOneGenerationAcrossMultipleLinks(t *testing.T) {
	f := newForest(t)
	c1 := register(t, f, clockDef("1", "old", forest.Public))
	c2 := register(t, f, clockDef("2", "new", forest.Public))
	p := register(t, f, printerDef(true))
	l := binding("clock", "printer", "clock", "a", "now")
	apply(t, f, forest.AddRoot("a", c1), forest.AddRoot("printer", p), forest.Connect(l))
	first, continueCall := make(chan struct{}), make(chan struct{})
	result := make(chan error, 1)
	go func() {
		result <- f.Invoke(context.Background(), func(ctx context.Context) error {
			generation, _ := f.Generation(ctx)
			for i := 0; i < 2; i++ {
				err := forest.Use[printer](ctx, f, "printer", "print", func(p printer) error {
					got, err := p.Print(ctx)
					if err != nil {
						return err
					}
					if got != "printed: old" {
						return fmt.Errorf("mixed snapshot: %s", got)
					}
					return nil
				})
				if err != nil {
					return err
				}
				if i == 0 {
					close(first)
					<-continueCall
				}
			}
			if got, _ := f.Generation(ctx); got != generation {
				return errors.New("generation changed within scope")
			}
			return nil
		})
	}()
	receive(t, first)
	apply(t, f, forest.Replace("a", c2, false))
	if got := readPrint(t, f); got != "printed: new" {
		t.Fatal(got)
	}
	close(continueCall)
	if err := receive(t, result); err != nil {
		t.Fatal(err)
	}
}

func TestConcurrentInvocationsAndRepeatedHotSwap(t *testing.T) {
	f := newForest(t)
	c1 := register(t, f, clockDef("1", "one", forest.Public))
	c2 := register(t, f, clockDef("2", "two", forest.Public))
	p := register(t, f, printerDef(true))
	apply(t, f, forest.AddRoot("clock", c1), forest.AddRoot("printer", p), forest.Connect(binding("l", "printer", "clock", "clock", "now")))
	var wg sync.WaitGroup
	errs := make(chan error, 8)
	for range 8 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for range 100 {
				err := f.Invoke(context.Background(), func(ctx context.Context) error {
					var first string
					for i := 0; i < 2; i++ {
						if err := forest.Use[printer](ctx, f, "printer", "print", func(p printer) error {
							got, err := p.Print(ctx)
							if err != nil {
								return err
							}
							if i == 0 {
								first = got
							} else if first != got {
								return errors.New("mixed providers")
							}
							return nil
						}); err != nil {
							return err
						}
					}
					return nil
				})
				if err != nil {
					errs <- err
					return
				}
			}
		}()
	}
	for i := 0; i < 30; i++ {
		d := c1
		if i%2 == 0 {
			d = c2
		}
		apply(t, f, forest.Replace("clock", d, false))
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Error(err)
	}
	wait(t, f)
}

func TestScopeClosedAndManagedCancellation(t *testing.T) {
	f := newForest(t)
	var escaped context.Context
	if err := f.Invoke(context.Background(), func(ctx context.Context) error { escaped = ctx; return nil }); err != nil {
		t.Fatal(err)
	}
	if err := f.Invoke(escaped, func(context.Context) error { return nil }); !errors.Is(err, forest.ErrScopeClosed) {
		t.Fatal(err)
	}
	entered := make(chan struct{})
	result := make(chan error, 1)
	go func() {
		result <- f.Invoke(context.Background(), func(ctx context.Context) error { close(entered); <-ctx.Done(); return ctx.Err() })
	}()
	receive(t, entered)
	if got := f.CancelGeneration(0); got != 1 {
		t.Fatal(got)
	}
	if err := receive(t, result); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
}
