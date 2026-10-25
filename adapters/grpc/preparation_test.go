package grpcforest

import (
	"context"
	"errors"
	"net"
	"testing"
	"time"

	"github.com/Leeminjing/go-forest/forest"
	"google.golang.org/grpc"
)

// The private preparation seam lets this test prove that an allocated native
// server was actually stopped, rather than relying solely on Core metadata.
func TestNativeServerIsStoppedAfterRegistrationPanic(t *testing.T) {
	f := forest.New(forest.Options{})
	defer f.Close(context.Background())
	var native *grpc.Server
	d, err := Definition(f, Options{TypeID: "panic", Version: "1", Register: func(ctx context.Context, env *forest.BuildEnv, r grpc.ServiceRegistrar) error {
		native = r.(*registration).native
		panic("configuration panic")
	}})
	if err != nil {
		t.Fatal(err)
	}
	if err = f.Register(d); err != nil {
		t.Fatal(err)
	}
	_, err = f.Apply(context.Background(), forest.ChangePlan{Operations: []forest.Operation{forest.AddRoot("panic", d.Ref())}})
	if !errors.Is(err, forest.ErrPrepareFailed) {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err = f.Wait(ctx); err != nil {
		t.Fatal(err)
	}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	if err = native.Serve(listener); !errors.Is(err, grpc.ErrServerStopped) {
		t.Fatal("native resource survived rollback", err)
	}
}
