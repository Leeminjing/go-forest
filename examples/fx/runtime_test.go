package fxdemo_test

import (
	"context"
	"encoding/json"
	"net/http"
	"sync"
	"testing"
	"time"

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
