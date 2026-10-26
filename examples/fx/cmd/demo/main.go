package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"time"

	demo "github.com/Leeminjing/go-forest/examples/fx"
)

func run() error {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	r, err := demo.New(ctx, demo.Options{})
	if err != nil {
		return err
	}
	defer r.Close(ctx)
	before, err := r.Render(ctx, "oak")
	if err != nil {
		return err
	}
	if err = r.ReplaceBusiness(ctx); err != nil {
		return err
	}
	after, err := r.Background(ctx, "elm")
	if err != nil {
		return err
	}
	client := &http.Client{Timeout: time.Second}
	resp, err := client.Get(r.Address() + "?text=ash")
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	var wire demo.Response
	if err = json.NewDecoder(resp.Body).Decode(&wire); err != nil {
		return err
	}
	if err = r.Close(ctx); err != nil {
		return err
	}
	return json.NewEncoder(os.Stdout).Encode(struct {
		Before, Background string
		HTTP               demo.Response
	}{before, after, wire})
}
func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
