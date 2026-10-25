package main

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/Leeminjing/go-forest/examples/frameworks"
	"github.com/gin-gonic/gin"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
func run() (err error) {
	http := flag.String("http-address", "127.0.0.1:8080", "HTTP address")
	rpc := flag.String("grpc-address", "127.0.0.1:8090", "gRPC address")
	observe := flag.String("address", "127.0.0.1:8089", "Observatory address")
	flag.Parse()
	gin.SetMode(gin.ReleaseMode)
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()
	r, err := frameworks.New(ctx, frameworks.Options{Token: os.Getenv("FOREST_OBSERVATORY_TOKEN"), HTTPAddress: *http, GRPCAddress: *rpc, ObservatoryAddress: *observe})
	if err != nil {
		return err
	}
	defer func() {
		closeCtx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
		defer cancel()
		err = errors.Join(err, r.Close(closeCtx))
	}()
	write := func(value any) { _ = json.NewEncoder(os.Stdout).Encode(value) }
	write(map[string]any{"event": "ready", "http_url": r.HTTPURL, "grpc_target": r.GRPCTarget, "observatory_url": r.ObservatoryURL, "commands": []string{"wait", "replace", "release", "quit"}})
	commands := make(chan string)
	go func() {
		defer close(commands)
		s := bufio.NewScanner(os.Stdin)
		for s.Scan() {
			select {
			case commands <- strings.TrimSpace(s.Text()):
			case <-ctx.Done():
				return
			}
		}
	}()
	for {
		select {
		case <-ctx.Done():
			return nil
		case command, ok := <-commands:
			if !ok {
				return nil
			}
			switch command {
			case "wait":
				select {
				case <-r.Entered:
					write(map[string]string{"event": "blocked"})
				case <-ctx.Done():
					return nil
				}
			case "replace":
				g, err := r.Replace(ctx)
				if err != nil {
					write(map[string]string{"event": "replace_failed"})
				} else {
					write(map[string]any{"event": "replaced", "generation": g})
				}
			case "release":
				r.Release()
				write(map[string]string{"event": "released"})
			case "quit":
				return nil
			default:
				write(map[string]string{"event": "unknown_command"})
			}
		}
	}
}
