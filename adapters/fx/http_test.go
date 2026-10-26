package fxforest_test

import (
	"context"
	"errors"
	"io"
	"net"
	"net/http"
	"sync/atomic"
	"testing"
	"time"

	ff "github.com/Leeminjing/go-forest/adapters/fx"
	"github.com/Leeminjing/go-forest/forest"
	"go.uber.org/fx"
)

func httpStatus(t *testing.T, url string) int {
	t.Helper()
	client := &http.Client{Timeout: time.Second}
	r, err := client.Get(url)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Body.Close()
	io.Copy(io.Discard, r.Body)
	return r.StatusCode
}

func TestCandidateHTTPAndLoopNeverExecuteAndFailedPlanReleasesListener(t *testing.T) {
	ctx := testContext(t)
	f := forest.New(forest.Options{})
	defer f.Close(ctx)
	var address string
	var calls atomic.Int32
	ready, fail := make(chan struct{}), make(chan struct{})
	boom := errors.New("later readiness failed")
	stable := forest.Definition{TypeID: "stable", Version: "1", Capabilities: []forest.CapabilityDescriptor{forest.Capability[service]("value", forest.Public)}, Build: func(_ context.Context, e *forest.BuildEnv) (forest.Lifecycle, error) {
		return forest.Hooks{}, forest.Provide(e, "value", serviceFunc(func(context.Context) (string, error) { return "old forest alive", nil }))
	}}
	register(t, f, stable)
	apply(t, ctx, f, 0, forest.AddRoot("stable", stable.Ref()))
	d, err := ff.Definition[service](f, ff.Options{TypeID: "http", Version: "1", TreeID: "fx", BuildModule: func(_ *forest.BuildEnv, b *ff.Bridge) (fx.Option, error) {
		return fx.Options(fx.Provide(func() service { return serviceFunc(func(context.Context) (string, error) { return "ok", nil }) }), fx.Invoke(func(l fx.Lifecycle) {
			l.Append(fx.Hook{OnStart: func(context.Context) error {
				ln, e := net.Listen("tcp", "127.0.0.1:0")
				if e != nil {
					return e
				}
				address = ln.Addr().String()
				server := &http.Server{Handler: b.Handler(http.HandlerFunc(func(http.ResponseWriter, *http.Request) { calls.Add(1) }))}
				done := make(chan struct{})
				if e = b.OwnIngress(server.Close, done); e != nil {
					ln.Close()
					return e
				}
				go func() { defer close(done); server.Serve(ln) }()
				return b.Loop(func(c context.Context) error {
					return b.Run(c, func(context.Context) error { calls.Add(1); return nil })
				})
			}})
		})), nil
	}})
	if err != nil {
		t.Fatal(err)
	}
	bad := forest.Definition{TypeID: "bad", Version: "1", Ports: []forest.PortDescriptor{forest.Port[ff.Control]("prepared", forest.Required(), forest.During(forest.Startup))}, Build: func(context.Context, *forest.BuildEnv) (forest.Lifecycle, error) {
		return forest.Hooks{OnReady: func(context.Context) error { close(ready); <-fail; return boom }}, nil
	}}
	register(t, f, d, bad)
	result := make(chan error, 1)
	go func() {
		_, e := f.Apply(ctx, forest.ChangePlan{BaseGeneration: 1, Operations: []forest.Operation{forest.AddRoot("fx", d.Ref()), forest.AddRoot("bad", bad.Ref()), forest.Connect(forest.Link{ID: "startup", Consumer: "bad", Port: "prepared", Provider: "fx", Capability: "control"})}})
		result <- e
	}()
	wait(t, ctx, ready)
	if s := httpStatus(t, "http://"+address); s != 503 {
		t.Fatalf("precommit HTTP=%d", s)
	}
	if calls.Load() != 0 {
		t.Fatal("candidate executed business")
	}
	if err = f.Invoke(ctx, func(c context.Context) error {
		return forest.Use[service](c, f, "stable", "value", func(s service) error {
			v, e := s.Value(c)
			if v != "old forest alive" {
				return errors.New("old service changed")
			}
			return e
		})
	}); err != nil {
		t.Fatal("old calls blocked during preparation", err)
	}
	close(fail)
	select {
	case err = <-result:
		if !errors.Is(err, boom) {
			t.Fatal(err)
		}
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	drained(t, ctx, f)
	ln, e := net.Listen("tcp", address)
	if e != nil {
		t.Fatal("failed candidate retained socket", e)
	}
	ln.Close()
	if f.Snapshot().Generation != 1 || calls.Load() != 0 {
		t.Fatal("failed candidate changed forest")
	}
}

func TestRealHTTPAndLoopQuiesceBeforeNativeResourceStop(t *testing.T) {
	ctx := testContext(t)
	f := forest.New(forest.Options{})
	defer f.Close(ctx)
	var address string
	var stopped atomic.Int32
	entered, release, worked := make(chan struct{}), make(chan struct{}), make(chan struct{})
	d, err := ff.Definition[service](f, ff.Options{TypeID: "real-http", Version: "1", TreeID: "fx", BuildModule: func(_ *forest.BuildEnv, b *ff.Bridge) (fx.Option, error) {
		return fx.Options(fx.Provide(func() service { return serviceFunc(func(context.Context) (string, error) { return "ok", nil }) }), fx.Invoke(func(l fx.Lifecycle) {
			l.Append(fx.Hook{OnStart: func(context.Context) error {
				ln, e := net.Listen("tcp", "127.0.0.1:0")
				if e != nil {
					return e
				}
				address = ln.Addr().String()
				server := &http.Server{Handler: b.Handler(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					if r.URL.Path == "/hold" {
						close(entered)
						<-release
					}
					w.WriteHeader(200)
				}))}
				done := make(chan struct{})
				if e = b.OwnIngress(server.Close, done); e != nil {
					ln.Close()
					return e
				}
				go func() { defer close(done); server.Serve(ln) }()
				return b.Loop(func(c context.Context) error {
					if e := b.Run(c, func(context.Context) error { close(worked); return nil }); e != nil {
						return e
					}
					<-c.Done()
					return nil
				})
			}, OnStop: func(context.Context) error { stopped.Add(1); return nil }})
		})), nil
	}})
	if err != nil {
		t.Fatal(err)
	}
	register(t, f, d)
	apply(t, ctx, f, 0, forest.AddRoot("fx", d.Ref()))
	if s := httpStatus(t, "http://"+address); s != 503 {
		t.Fatal(s)
	}
	if err = ff.Enable(ctx, f, "fx"); err != nil {
		t.Fatal(err)
	}
	wait(t, ctx, worked)
	if s := httpStatus(t, "http://"+address); s != 200 {
		t.Fatal(s)
	}
	requestDone := make(chan struct{})
	go func() {
		defer close(requestDone)
		client := &http.Client{Timeout: 3 * time.Second}
		r, e := client.Get("http://" + address + "/hold")
		if e == nil {
			r.Body.Close()
		}
	}()
	wait(t, ctx, entered)
	short, cancel := context.WithTimeout(ctx, 20*time.Millisecond)
	defer cancel()
	if err = ff.Quiesce(short, f, "fx"); !errors.Is(err, ff.ErrDrainTimeout) {
		t.Fatalf("quiesce=%v", err)
	}
	s, err := ff.Inspect(ctx, f, "fx")
	if err != nil || s.Inflight != 1 || s.Accepting {
		t.Fatalf("status=%+v %v", s, err)
	}
	if stopped.Load() != 0 {
		t.Fatal("native OnStop ran before actual handler exit")
	}
	if _, e := value(ctx, f, "fx"); !errors.Is(e, ff.ErrUnavailable) {
		t.Fatal("sealed app accepted call", e)
	}
	close(release)
	if err = ff.Quiesce(ctx, f, "fx"); err != nil {
		t.Fatal(err)
	}
	if err = ff.Quiesce(ctx, f, "fx"); err != nil {
		t.Fatal(err)
	}
	apply(t, ctx, f, 1, forest.Remove("fx"))
	drained(t, ctx, f)
	wait(t, ctx, requestDone)
	if stopped.Load() != 1 {
		t.Fatal("native resources not stopped once")
	}
}
