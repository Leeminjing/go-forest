package main

import (
	"context"
	"fmt"
	"io"
	"log"
	"net/http"
	"time"

	"go-forest/examples/trees/agent"
	"go-forest/examples/trees/model"
	"go-forest/examples/trees/web"
	"go-forest/forest"
)

func main() {
	if err := run(); err != nil {
		log.Fatal(err)
	}
}
func run() (result error) {
	f := forest.New(forest.Options{})
	defer func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := f.Close(ctx); result == nil {
			result = err
		}
	}()
	md, ad, wd := model.Definition(), agent.Definition(), web.Definition(f)
	for _, d := range []forest.Definition{md, ad, wd} {
		if err := f.Register(d); err != nil {
			return err
		}
	}
	_, err := f.Apply(context.Background(), forest.ChangePlan{Operations: []forest.Operation{
		forest.AddRoot("model", md.Ref()), forest.AddRoot("agent", ad.Ref()), forest.AddRoot("web", wd.Ref()),
		forest.Connect(forest.Link{ID: "model", Consumer: "agent", Port: "model", Provider: "model", Capability: "generate"}),
		forest.Connect(forest.Link{ID: "agent", Consumer: "web", Port: "agent", Provider: "agent", Capability: "reply"}),
	}})
	if err != nil {
		return err
	}
	var url string
	err = f.Invoke(context.Background(), func(ctx context.Context) error {
		return forest.Use[web.Endpoint](ctx, f, "web", "http", func(e web.Endpoint) error { url = e.URL(); return nil })
	})
	if err != nil {
		return err
	}
	client := &http.Client{Timeout: 5 * time.Second}
	response, err := client.Get(url + "?q=hello")
	if err != nil {
		return err
	}
	defer response.Body.Close()
	body, err := io.ReadAll(response.Body)
	if err != nil {
		return err
	}
	if response.StatusCode != http.StatusOK {
		return fmt.Errorf("HTTP %d: %s", response.StatusCode, body)
	}
	fmt.Println(string(body))
	return nil
}
