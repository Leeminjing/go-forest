package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log"
	"os"
	"time"

	bridge "github.com/Leeminjing/go-forest/bridge/httpjson"
	"github.com/Leeminjing/go-forest/examples/trees/remoteclock"
	"github.com/Leeminjing/go-forest/examples/trees/remoteformatter"
	"github.com/Leeminjing/go-forest/forest"
)

func main() {
	url := flag.String("url", "http://127.0.0.1:8088", "remote endpoint")
	flag.Parse()
	if err := run(*url); err != nil {
		log.Fatal(err)
	}
}
func run(url string) (result error) {
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
	proxy := remoteclock.ProxyDefinition(bridge.ClientOptions{URL: url, Token: token})
	formatter := remoteformatter.Definition()
	for _, d := range []forest.Definition{proxy, formatter} {
		if err := f.Register(d); err != nil {
			return err
		}
	}
	if _, err := f.Apply(context.Background(), forest.ChangePlan{Operations: []forest.Operation{forest.AddRoot("proxy", proxy.Ref()), forest.AddRoot("formatter", formatter.Ref()), forest.Connect(forest.Link{ID: "clock", Consumer: "formatter", Port: "clock", Provider: "proxy", Capability: "clock"})}}); err != nil {
		return err
	}
	return f.Invoke(context.Background(), func(ctx context.Context) error {
		return forest.Use[remoteformatter.Output](ctx, f, "formatter", "format", func(output remoteformatter.Output) error {
			value, err := output.Format(ctx)
			if err == nil {
				fmt.Println(value)
			}
			return err
		})
	})
}
