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

	"github.com/Leeminjing/go-forest/extensions/observatory/internal/demo"
	"github.com/gin-gonic/gin"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
func run() (err error) {
	app := flag.String("app-address", "127.0.0.1:8080", "application address")
	dashboard := flag.String("address", "127.0.0.1:8089", "Observatory loopback address")
	flag.Parse()
	gin.SetMode(gin.ReleaseMode)
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()
	r, err := demo.New(ctx, demo.Options{Token: os.Getenv("FOREST_OBSERVATORY_TOKEN"), AppAddress: *app, ObservatoryAddress: *dashboard})
	if err != nil {
		return fmt.Errorf("start demo: %w", err)
	}
	defer func() {
		closeCtx, stop := context.WithTimeout(context.Background(), 8*time.Second)
		defer stop()
		err = errors.Join(err, r.Close(closeCtx))
	}()
	write := func(value any) { _ = json.NewEncoder(os.Stdout).Encode(value) }
	write(map[string]any{"event": "ready", "app_url": r.AppURL, "observatory_url": r.ObservatoryURL, "commands": []string{"wait", "replace", "release", "quit"}})
	commands := make(chan string)
	go func() {
		defer close(commands)
		scanner := bufio.NewScanner(os.Stdin)
		for scanner.Scan() {
			select {
			case commands <- strings.TrimSpace(scanner.Text()):
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
				generation, err := r.Replace(ctx)
				if err != nil {
					write(map[string]string{"event": "replace_failed"})
				} else {
					write(map[string]any{"event": "replaced", "generation": generation})
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
