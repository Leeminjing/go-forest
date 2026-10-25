package grpcforest_test

import (
	"context"
	"errors"
	"net"
	"sync"
	"testing"
	"time"

	grpcforest "github.com/Leeminjing/go-forest/adapters/grpc"
	"github.com/Leeminjing/go-forest/forest"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	pb "google.golang.org/grpc/interop/grpc_testing"
)

func TestRegistrationFailureAndPanicStopAllocatedServer(t *testing.T) {
	for _, kind := range []string{"error", "panic", "duplicate", "mismatch", "unary-slot", "stream-slot"} {
		t.Run(kind, func(t *testing.T) {
			f := forest.New(forest.Options{})
			defer f.Close(context.Background())
			opts := grpcforest.Options{TypeID: "broken", Version: "1", Register: func(ctx context.Context, env *forest.BuildEnv, r grpc.ServiceRegistrar) error {
				if kind == "panic" {
					panic("registration failed")
				}
				if kind == "duplicate" {
					pb.RegisterTestServiceServer(r, &probe{})
					pb.RegisterTestServiceServer(r, &probe{})
					return nil
				}
				if kind == "mismatch" {
					r.RegisterService(&pb.TestService_ServiceDesc, struct{}{})
					return nil
				}
				return errors.New("registration failed")
			}}
			if kind == "unary-slot" {
				opts.ServerOptions = []grpc.ServerOption{grpc.UnaryInterceptor(func(ctx context.Context, r any, i *grpc.UnaryServerInfo, h grpc.UnaryHandler) (any, error) {
					return h(ctx, r)
				})}
			}
			if kind == "stream-slot" {
				opts.ServerOptions = []grpc.ServerOption{grpc.StreamInterceptor(func(s any, stream grpc.ServerStream, i *grpc.StreamServerInfo, h grpc.StreamHandler) error {
					return h(s, stream)
				})}
			}
			d, err := grpcforest.Definition(f, opts)
			if err != nil {
				t.Fatal(err)
			}
			register(t, f, d)
			_, err = f.Apply(context.Background(), forest.ChangePlan{Operations: []forest.Operation{forest.AddRoot("broken", d.Ref())}})
			if !errors.Is(err, forest.ErrPrepareFailed) || f.Snapshot().Generation != 0 {
				t.Fatal(err)
			}
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()
			if err = f.Wait(ctx); err != nil {
				t.Fatal(err)
			}
			for _, i := range f.Snapshot().Instances {
				if i.State != forest.Stopped {
					t.Fatal(i)
				}
			}
		})
	}
}

func TestBindFailureRollsBackAlreadyStartedServer(t *testing.T) {
	f := forest.New(forest.Options{})
	defer f.Close(context.Background())
	busy, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer busy.Close()
	address := freeAddress(t)
	makeServer := func(id, address string) forest.Definition {
		d, err := grpcforest.Definition(f, grpcforest.Options{TypeID: id, Version: "1", Address: address, Register: func(context.Context, *forest.BuildEnv, grpc.ServiceRegistrar) error { return nil }})
		if err != nil {
			t.Fatal(err)
		}
		return d
	}
	a, b := makeServer("first", address), makeServer("busy", busy.Addr().String())
	register(t, f, a, b)
	_, err = f.Apply(context.Background(), forest.ChangePlan{Operations: []forest.Operation{forest.AddRoot("a", a.Ref()), forest.AddRoot("b", b.Ref())}})
	if err == nil || f.Snapshot().Generation != 0 {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err = f.Wait(ctx); err != nil {
		t.Fatal(err)
	}
	l, err := net.Listen("tcp", address)
	if err != nil {
		t.Fatal("rolled-back server kept its port", err)
	}
	l.Close()
}

func TestRollbackStopDeadlineDoesNotReleaseRunningHandler(t *testing.T) {
	f := forest.New(forest.Options{DrainTimeout: 30 * time.Millisecond})
	entered, canceled, release, prepared, fail := make(chan struct{}), make(chan struct{}), make(chan struct{}), make(chan struct{}), make(chan struct{})
	var releaseOnce sync.Once
	unblock := func() { releaseOnce.Do(func() { close(release) }) }
	defer unblock()
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		if err := f.Close(ctx); err != nil {
			t.Error(err)
		}
	})
	address := freeAddress(t)
	p := &probe{empty: func(ctx context.Context) (*pb.Empty, error) {
		close(entered)
		<-ctx.Done()
		close(canceled)
		<-release
		return &pb.Empty{}, nil
	}}
	d, err := grpcforest.Definition(f, grpcforest.Options{TypeID: "candidate", Version: "1", Address: address, Register: func(ctx context.Context, env *forest.BuildEnv, r grpc.ServiceRegistrar) error {
		pb.RegisterTestServiceServer(r, p)
		return nil
	}})
	if err != nil {
		t.Fatal(err)
	}
	gate := forest.Definition{TypeID: "gate", Version: "1", Build: func(context.Context, *forest.BuildEnv) (forest.Lifecycle, error) {
		return forest.Hooks{OnReady: func(context.Context) error { close(prepared); <-fail; return errors.New("candidate rejected") }}, nil
	}}
	register(t, f, d, gate)
	applied := make(chan error, 1)
	go func() {
		_, err := f.Apply(context.Background(), forest.ChangePlan{Operations: []forest.Operation{forest.AddRoot("a-server", d.Ref()), forest.AddRoot("z-gate", gate.Ref())}})
		applied <- err
	}()
	wait(t, prepared)
	connection, err := grpc.NewClient(address, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	defer connection.Close()
	client := pb.NewTestServiceClient(connection)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	rpcDone := make(chan error, 1)
	go func() { _, err := client.EmptyCall(ctx, &pb.Empty{}); rpcDone <- err }()
	wait(t, entered)
	close(fail)
	if err := wait(t, applied); !errors.Is(err, forest.ErrPrepareFailed) {
		t.Fatal(err)
	}
	wait(t, canceled)
	if err := wait(t, rpcDone); err == nil {
		t.Fatal("transport was not canceled at Stop deadline")
	}
	deadline := time.NewTimer(3 * time.Second)
	defer deadline.Stop()
	tick := time.NewTicker(time.Millisecond)
	defer tick.Stop()
	for {
		found := false
		for _, i := range f.Snapshot().Instances {
			if i.TreeID == "a-server" && i.State == forest.Draining && errors.Is(i.Error, forest.ErrDrainTimeout) {
				found = true
			}
		}
		if found {
			break
		}
		select {
		case <-tick.C:
		case <-deadline.C:
			t.Fatal("missing observable drain timeout")
		}
	}
	// The handler is still alive. Core must retain the candidate until it exits.
	if f.Snapshot().Generation != 0 || len(f.Snapshot().Trees) != 0 {
		t.Fatal("failed graph was published")
	}
	unblock()
	for {
		stopped := false
		for _, i := range f.Snapshot().Instances {
			if i.TreeID == "a-server" && i.State == forest.Stopped {
				stopped = true
			}
		}
		if stopped {
			break
		}
		select {
		case <-tick.C:
		case <-deadline.C:
			t.Fatal("actual handler completion did not release candidate")
		}
	}
}
