package fxforest_test

import (
	"context"
	"errors"
	"reflect"
	"sync"
	"testing"

	ff "github.com/Leeminjing/go-forest/adapters/fx"
	"github.com/Leeminjing/go-forest/forest"
	"go.uber.org/fx"
)

func TestFailedInitializationAndStartCleanAllOwnedResourcesOnce(t *testing.T) {
	boom := errors.New("module failure")
	for _, stage := range []string{"factory", "invoke", "start", "panic"} {
		t.Run(stage, func(t *testing.T) {
			ctx := testContext(t)
			f := forest.New(forest.Options{})
			var mu sync.Mutex
			var closed []string
			d, err := ff.Definition[service](f, ff.Options{TypeID: "failing", Version: "1", TreeID: "fx", BuildModule: func(_ *forest.BuildEnv, b *ff.Bridge) (fx.Option, error) {
				var tickets []*ff.Ticket
				for _, name := range []string{"A", "B", "C"} {
					name := name
					ticket, e := b.Defer(func(context.Context) error { mu.Lock(); defer mu.Unlock(); closed = append(closed, name); return nil })
					if e != nil {
						return nil, e
					}
					tickets = append(tickets, ticket)
				}
				if stage == "factory" {
					return nil, boom
				}
				return fx.Options(fx.Provide(func() service { return serviceFunc(func(context.Context) (string, error) { return "ok", nil }) }), fx.Invoke(func(l fx.Lifecycle) error {
					if stage == "invoke" {
						return boom
					}
					l.Append(fx.Hook{OnStart: func(context.Context) error { return nil }, OnStop: tickets[0].Close})
					l.Append(fx.Hook{OnStart: func(context.Context) error {
						if stage == "panic" {
							panic("private payload")
						}
						return boom
					}, OnStop: tickets[1].Close})
					l.Append(fx.Hook{OnStop: tickets[2].Close})
					return nil
				})), nil
			}})
			if err != nil {
				t.Fatal(err)
			}
			register(t, f, d)
			g, err := f.Apply(ctx, forest.ChangePlan{Operations: []forest.Operation{forest.AddRoot("fx", d.Ref())}})
			if !errors.Is(err, forest.ErrPrepareFailed) || g != 0 {
				t.Fatalf("generation=%d error=%v", g, err)
			}
			if stage == "panic" {
				if !errors.Is(err, ff.ErrPanic) {
					t.Fatal(err)
				}
			} else if !errors.Is(err, boom) {
				t.Fatal(err)
			}
			var failure *ff.Failure
			if !errors.As(err, &failure) || failure.Tree != "fx" {
				t.Fatal("missing error context", err)
			}
			if err = f.Wait(ctx); err != nil {
				t.Fatal(err)
			}
			mu.Lock()
			got := append([]string(nil), closed...)
			mu.Unlock()
			want := []string{"C", "B", "A"}
			if stage == "start" || stage == "panic" {
				want = []string{"A", "C", "B"}
			}
			if !reflect.DeepEqual(got, want) {
				t.Fatalf("cleanup=%v want=%v", got, want)
			}
			if err = f.Close(ctx); err != nil {
				t.Fatal(err)
			}
		})
	}
}
