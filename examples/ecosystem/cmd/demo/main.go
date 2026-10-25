package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/Leeminjing/go-forest/examples/ecosystem"
	"github.com/gin-gonic/gin"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "ecosystem operation failed; inspect service diagnostics and configuration")
		os.Exit(1)
	}
}
func run() error {
	command := "serve"
	if len(os.Args) > 1 {
		command = os.Args[1]
	}
	flags := flag.NewFlagSet(command, flag.ContinueOnError)
	ns := flags.String("namespace", "forest_demo", "JetStream stream / application namespace")
	cp := flags.String("checkpoints", ".scratch/checkpoints", "local checkpoint directory")
	http := flags.String("http", "127.0.0.1:0", "loopback HTTP address")
	rpc := flags.String("grpc", "127.0.0.1:0", "loopback RPC address")
	observe := flags.String("observe", "127.0.0.1:0", "loopback Observatory address")
	if len(os.Args) > 2 {
		if err := flags.Parse(os.Args[2:]); err != nil {
			return err
		}
	}
	o := ecosystem.Options{DSN: os.Getenv("FOREST_POSTGRES_DSN"), NATSURL: os.Getenv("FOREST_NATS_URL"), Token: os.Getenv("FOREST_TOKEN"), Namespace: *ns, CheckpointDir: *cp, HTTPAddress: *http, GRPCAddress: *rpc, ObservatoryAddress: *observe}
	gin.SetMode(gin.ReleaseMode)
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()
	if command == "init" {
		c, x := context.WithTimeout(ctx, 15*time.Second)
		defer x()
		return ecosystem.Initialize(c, o)
	}
	if command != "serve" {
		return fmt.Errorf("unknown command")
	}
	r, err := ecosystem.New(ctx, o)
	if err != nil {
		return err
	}
	if err = r.Start(ctx); err != nil {
		return err
	}
	if err = json.NewEncoder(os.Stdout).Encode(map[string]string{"http": r.HTTPURL, "grpc": r.GRPCTarget, "observatory": r.ObservatoryURL, "namespace": o.Namespace}); err != nil {
		return err
	}
	<-ctx.Done()
	closeCtx, x := context.WithTimeout(context.Background(), 10*time.Second)
	defer x()
	return r.Close(closeCtx)
}
