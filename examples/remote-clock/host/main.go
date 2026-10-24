package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"time"

	bridge "github.com/Leeminjing/go-forest/bridge/httpjson"
	"github.com/Leeminjing/go-forest/examples/trees/localtime"
	"github.com/Leeminjing/go-forest/examples/trees/remoteclock"
	"github.com/Leeminjing/go-forest/forest"
)

func main() {
	address := flag.String("address", "127.0.0.1:8088", "listen address")
	flag.Parse()
	if err := run(*address); err != nil {
		log.Fatal(err)
	}
}
func run(address string) (result error) {
	token := os.Getenv("FOREST_BRIDGE_TOKEN")
	if token == "" {
		return errors.New("set FOREST_BRIDGE_TOKEN in both processes")
	}
	f := forest.New(forest.Options{})
	defer func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := f.Close(ctx); result == nil {
			result = err
		}
	}()
	provider := localtime.Definition()
	host := remoteclock.HostDefinition(f, bridge.HostOptions{Address: address, Grants: []bridge.Grant{{Token: token, Routes: []string{remoteclock.RouteName}}}})
	for _, d := range []forest.Definition{provider, host} {
		if err := f.Register(d); err != nil {
			return err
		}
	}
	if _, err := f.Apply(context.Background(), forest.ChangePlan{Operations: []forest.Operation{forest.AddRoot("clock", provider.Ref()), forest.AddRoot("host", host.Ref()), forest.Connect(forest.Link{ID: "clock", Consumer: "host", Port: "clock", Provider: "clock", Capability: "clock"})}}); err != nil {
		return err
	}
	if err := f.Invoke(context.Background(), func(ctx context.Context) error {
		return forest.Use[bridge.Endpoint](ctx, f, "host", "endpoint", func(e bridge.Endpoint) error { fmt.Println("listening:", e.URL()); return nil })
	}); err != nil {
		return err
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()
	<-ctx.Done()
	f.CancelGeneration(f.Snapshot().Generation)
	return nil
}
