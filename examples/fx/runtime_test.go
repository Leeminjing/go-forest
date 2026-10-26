package fxdemo_test

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"sync"
	"testing"
	"time"

	ff "github.com/Leeminjing/go-forest/adapters/fx"
	demo "github.com/Leeminjing/go-forest/examples/fx"
)

func TestRealModuleHTTPBackgroundAndConsumerShareDynamicBusiness(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	entered, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	r, err := demo.New(ctx, demo.Options{BetweenReads: func(_ context.Context, text string) {
		if text == "hold" {
			once.Do(func() { close(entered) })
			<-release
		}
	}})
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close(ctx)
	type result struct {
		out string
		err error
	}
	old := make(chan result, 1)
	go func() { v, e := r.Render(ctx, "hold"); old <- result{v, e} }()
	select {
	case <-entered:
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	if err = r.ReplaceBusiness(ctx); err != nil {
		t.Fatal(err)
	}
	if v, e := r.Render(ctx, "oak"); e != nil || v != "v2:oak/v2:oak" {
		t.Fatal(v, e)
	}
	if v, e := r.Background(ctx, "elm"); e != nil || v != "v2:elm/v2:elm" {
		t.Fatal(v, e)
	}
	client := &http.Client{Timeout: time.Second}
	resp, e := client.Get(r.Address() + "?text=ash")
	if e != nil {
		t.Fatal(e)
	}
	var wire demo.Response
	e = json.NewDecoder(resp.Body).Decode(&wire)
	resp.Body.Close()
	if e != nil || resp.StatusCode != 200 || wire.Text != "v2:ash/v2:ash" || wire.Generation != 2 {
		t.Fatalf("wire=%+v status=%d %v", wire, resp.StatusCode, e)
	}
	close(release)
	select {
	case v := <-old:
		if v.err != nil || v.out != "v1:hold/v1:hold" {
			t.Fatal(v)
		}
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	if err = r.Close(ctx); err != nil {
		t.Fatal(err)
	}
}

func TestClosingRuntimeWakesQueuedAndFutureBackgroundCalls(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	entered, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	defer once.Do(func() { close(release) })
	r, err := demo.New(ctx, demo.Options{BetweenReads: func(_ context.Context, text string) {
		if text == "hold" {
			close(entered)
			<-release
		}
	}})
	if err != nil {
		t.Fatal(err)
	}
	active := make(chan error, 1)
	go func() { _, e := r.Background(ctx, "hold"); active <- e }()
	select {
	case <-entered:
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	queued := make(chan error, 1)
	go func() { _, e := r.Background(ctx, "queued"); queued <- e }()
	closeResult := make(chan error, 1)
	go func() { closeResult <- r.Close(ctx) }()
	select {
	case e := <-queued:
		if !errors.Is(e, ff.ErrUnavailable) {
			t.Fatal(e)
		}
	case <-ctx.Done():
		t.Fatal("queued call stuck after shutdown", ctx.Err())
	}
	once.Do(func() { close(release) })
	select {
	case e := <-closeResult:
		if e != nil {
			t.Fatal(e)
		}
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	select {
	case <-active:
	case <-ctx.Done():
		t.Fatal("active caller stuck")
	}
	after := make(chan error, 1)
	go func() { _, e := r.Background(ctx, "after"); after <- e }()
	select {
	case e := <-after:
		if !errors.Is(e, ff.ErrUnavailable) {
			t.Fatal(e)
		}
	case <-ctx.Done():
		t.Fatal("post-close call stuck", ctx.Err())
	}
}
