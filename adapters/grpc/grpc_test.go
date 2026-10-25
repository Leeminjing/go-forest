package grpcforest_test

import (
	"context"
	"errors"
	"io"
	"net"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	grpcforest "github.com/Leeminjing/go-forest/adapters/grpc"
	"github.com/Leeminjing/go-forest/forest"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	pb "google.golang.org/grpc/interop/grpc_testing"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

type worker interface {
	Read(context.Context, bool) (string, error)
}
type work struct {
	label                 string
	entered, canceled     chan struct{}
	release               <-chan struct{}
	ignore                bool
	enterOnce, cancelOnce sync.Once
}

func (w *work) Read(ctx context.Context, hold bool) (string, error) {
	if hold && w.release != nil {
		w.enterOnce.Do(func() { close(w.entered) })
		if w.ignore {
			<-w.release
		} else {
			select {
			case <-w.release:
			case <-ctx.Done():
				w.cancelOnce.Do(func() { close(w.canceled) })
				return "", ctx.Err()
			}
		}
	}
	return w.label, nil
}
func workDefinition(version string, w *work, stopped chan struct{}) forest.Definition {
	return forest.Definition{TypeID: "test.worker", Version: version, HotSwappable: true, Capabilities: []forest.CapabilityDescriptor{forest.Capability[worker]("work", forest.Public, forest.Contract{ID: "test.work", Version: 1})}, Build: func(ctx context.Context, env *forest.BuildEnv) (forest.Lifecycle, error) {
		return forest.Hooks{OnStop: func(context.Context) error { close(stopped); return nil }}, forest.Provide(env, "work", w)
	}}
}

type probe struct {
	pb.UnimplementedTestServiceServer
	forest *forest.Forest
	ref    forest.Ref[worker]
	seen   chan string
	empty  func(context.Context) (*pb.Empty, error)
}

func (p *probe) value(ctx context.Context, hold bool) (string, error) {
	var a, b string
	err := p.ref.Use(ctx, func(w worker) error { var err error; a, err = w.Read(ctx, hold); return err })
	if err == nil {
		err = p.ref.Use(ctx, func(w worker) error { var err error; b, err = w.Read(ctx, false); return err })
	}
	if err != nil {
		return "", err
	}
	g, err := p.forest.Generation(ctx)
	return a + "/" + b + "@" + strconv.FormatUint(g, 10), err
}
func (p *probe) EmptyCall(ctx context.Context, _ *pb.Empty) (*pb.Empty, error) {
	if p.empty != nil {
		return p.empty(ctx)
	}
	return &pb.Empty{}, nil
}
func (p *probe) UnaryCall(ctx context.Context, r *pb.SimpleRequest) (*pb.SimpleResponse, error) {
	if string(r.GetPayload().GetBody()) == "reject" {
		return nil, status.Error(codes.PermissionDenied, "denied by application")
	}
	value, err := p.value(ctx, string(r.GetPayload().GetBody()) == "hold")
	if err != nil {
		return nil, err
	}
	return &pb.SimpleResponse{Payload: &pb.Payload{Body: []byte(value)}}, nil
}
func (p *probe) StreamingOutputCall(r *pb.StreamingOutputCallRequest, s grpc.ServerStreamingServer[pb.StreamingOutputCallResponse]) error {
	for n := 0; n < 2; n++ {
		value, err := p.value(s.Context(), n == 1 && string(r.GetPayload().GetBody()) == "hold")
		if err != nil {
			return err
		}
		if err = s.Send(&pb.StreamingOutputCallResponse{Payload: &pb.Payload{Body: []byte(value)}}); err != nil {
			return err
		}
	}
	return nil
}
func (p *probe) FullDuplexCall(s grpc.BidiStreamingServer[pb.StreamingOutputCallRequest, pb.StreamingOutputCallResponse]) error {
	for {
		request, err := s.Recv()
		if errors.Is(err, io.EOF) {
			return nil
		}
		if err != nil {
			return err
		}
		value, err := p.value(s.Context(), string(request.GetPayload().GetBody()) == "hold")
		if err != nil {
			return err
		}
		if err = s.Send(&pb.StreamingOutputCallResponse{Payload: &pb.Payload{Body: []byte(value)}}); err != nil {
			return err
		}
	}
}
func (p *probe) StreamingInputCall(s grpc.ClientStreamingServer[pb.StreamingInputCallRequest, pb.StreamingInputCallResponse]) error {
	var count int32
	for {
		_, err := s.Recv()
		if errors.Is(err, io.EOF) {
			return s.SendAndClose(&pb.StreamingInputCallResponse{AggregatedPayloadSize: count})
		}
		if err != nil {
			return err
		}
		value, err := p.value(s.Context(), false)
		if err != nil {
			return err
		}
		if strings.HasPrefix(value, "v1/") {
			count++
		} else {
			count += 2
		}
		p.seen <- value
	}
}

type fixture struct {
	f       *forest.Forest
	client  pb.TestServiceClient
	old     *work
	stopped chan struct{}
	probe   *probe
	next    forest.DefinitionRef
	unblock func()
}

func wait[T any](t *testing.T, ch <-chan T) T {
	t.Helper()
	select {
	case v := <-ch:
		return v
	case <-time.After(3 * time.Second):
		t.Fatal("synchronization timeout")
		var zero T
		return zero
	}
}
func apply(t *testing.T, f *forest.Forest, ops ...forest.Operation) {
	t.Helper()
	if _, err := f.Apply(context.Background(), forest.ChangePlan{BaseGeneration: f.Snapshot().Generation, Operations: ops}); err != nil {
		t.Fatal(err)
	}
}
func register(t *testing.T, f *forest.Forest, defs ...forest.Definition) {
	t.Helper()
	for _, d := range defs {
		if err := f.Register(d); err != nil {
			t.Fatal(err)
		}
	}
}
func newFixture(t *testing.T, ignore bool, options ...grpc.ServerOption) *fixture {
	t.Helper()
	f := forest.New(forest.Options{})
	release := make(chan struct{})
	var once sync.Once
	x := &fixture{f: f, stopped: make(chan struct{}), unblock: func() { once.Do(func() { close(release) }) }}
	x.old = &work{label: "v1", entered: make(chan struct{}), canceled: make(chan struct{}), release: release, ignore: ignore}
	p := &probe{forest: f, seen: make(chan string, 32)}
	x.probe = p
	v1 := workDefinition("1", x.old, x.stopped)
	v2 := workDefinition("2", &work{label: "v2"}, make(chan struct{}))
	x.next = v2.Ref()
	d, err := grpcforest.Definition(f, grpcforest.Options{TypeID: "test.grpc", Version: "1", Ports: []forest.PortDescriptor{forest.Port[worker]("worker", forest.Required(), forest.RequiresContract("test.work", 1, 1))}, ServerOptions: options, Register: func(ctx context.Context, env *forest.BuildEnv, r grpc.ServiceRegistrar) error {
		var err error
		p.ref, err = forest.Reference[worker](env, "worker")
		if err != nil {
			return err
		}
		pb.RegisterTestServiceServer(r, p)
		return nil
	}})
	if err != nil {
		t.Fatal(err)
	}
	if d.HotSwappable {
		t.Fatal("listener declared hot-swappable")
	}
	register(t, f, d, v1, v2)
	t.Cleanup(func() {
		x.unblock()
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		if err := f.Close(ctx); err != nil {
			t.Error(err)
		}
	})
	apply(t, f, forest.AddRoot("worker", v1.Ref()), forest.AddRoot("grpc", d.Ref()), forest.Connect(forest.Link{ID: "worker", Consumer: "grpc", Port: "worker", Provider: "worker", Capability: "work"}))
	var target string
	err = f.Invoke(context.Background(), func(ctx context.Context) error {
		return forest.Use[grpcforest.Endpoint](ctx, f, "grpc", "rpc", func(e grpcforest.Endpoint) error { target = e.Target(); return e.ServeError() })
	})
	if err != nil {
		t.Fatal(err)
	}
	connection, err := grpc.NewClient(target, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { connection.Close() })
	x.client = pb.NewTestServiceClient(connection)
	return x
}
func unary(t *testing.T, x *fixture, want string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	r, err := x.client.UnaryCall(ctx, &pb.SimpleRequest{})
	if err != nil || string(r.GetPayload().GetBody()) != want {
		t.Fatal(r, err)
	}
}

func TestUnaryHotReplacementPinsWholeCall(t *testing.T) {
	x := newFixture(t, false)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	type result struct {
		value string
		err   error
	}
	old := make(chan result, 1)
	go func() {
		r, err := x.client.UnaryCall(ctx, &pb.SimpleRequest{Payload: &pb.Payload{Body: []byte("hold")}})
		old <- result{string(r.GetPayload().GetBody()), err}
	}()
	wait(t, x.old.entered)
	apply(t, x.f, forest.Replace("worker", x.next, false))
	unary(t, x, "v2/v2@2")
	select {
	case <-x.stopped:
		t.Fatal("old provider stopped during RPC")
	default:
	}
	x.unblock()
	got := wait(t, old)
	if got.err != nil || got.value != "v1/v1@1" {
		t.Fatal(got)
	}
	wait(t, x.stopped)
}

func TestBidirectionalStreamPinsAcrossMessages(t *testing.T) {
	x := newFixture(t, false)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	s, err := x.client.FullDuplexCall(ctx)
	if err != nil {
		t.Fatal(err)
	}
	message := func(want string) {
		t.Helper()
		if err := s.Send(&pb.StreamingOutputCallRequest{}); err != nil {
			t.Fatal(err)
		}
		r, err := s.Recv()
		if err != nil || string(r.GetPayload().GetBody()) != want {
			t.Fatal(r, err)
		}
	}
	message("v1/v1@1")
	apply(t, x.f, forest.Replace("worker", x.next, false))
	message("v1/v1@1")
	unary(t, x, "v2/v2@2")
	select {
	case <-x.stopped:
		t.Fatal("old provider stopped during stream")
	default:
	}
	if err = s.CloseSend(); err != nil {
		t.Fatal(err)
	}
	if _, err = s.Recv(); !errors.Is(err, io.EOF) {
		t.Fatal(err)
	}
	wait(t, x.stopped)
	newStream, err := x.client.FullDuplexCall(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err = newStream.Send(&pb.StreamingOutputCallRequest{}); err != nil {
		t.Fatal(err)
	}
	r, err := newStream.Recv()
	if err != nil || string(r.GetPayload().GetBody()) != "v2/v2@2" {
		t.Fatal(r, err)
	}
	newStream.CloseSend()
	_, _ = newStream.Recv()
}

func TestServerAndClientStreamingRemainNative(t *testing.T) {
	t.Run("server-stream", func(t *testing.T) {
		x := newFixture(t, false)
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		s, err := x.client.StreamingOutputCall(ctx, &pb.StreamingOutputCallRequest{Payload: &pb.Payload{Body: []byte("hold")}})
		if err != nil {
			t.Fatal(err)
		}
		r, err := s.Recv()
		if err != nil || string(r.GetPayload().GetBody()) != "v1/v1@1" {
			t.Fatal(r, err)
		}
		wait(t, x.old.entered)
		apply(t, x.f, forest.Replace("worker", x.next, false))
		unary(t, x, "v2/v2@2")
		x.unblock()
		r, err = s.Recv()
		if err != nil || string(r.GetPayload().GetBody()) != "v1/v1@1" {
			t.Fatal(r, err)
		}
		if _, err = s.Recv(); !errors.Is(err, io.EOF) {
			t.Fatal(err)
		}
		wait(t, x.stopped)
	})
	t.Run("client-stream", func(t *testing.T) {
		x := newFixture(t, false)
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		s, err := x.client.StreamingInputCall(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if err = s.Send(&pb.StreamingInputCallRequest{}); err != nil {
			t.Fatal(err)
		}
		if value := wait(t, x.probe.seen); value != "v1/v1@1" {
			t.Fatal(value)
		}
		apply(t, x.f, forest.Replace("worker", x.next, false))
		if err = s.Send(&pb.StreamingInputCallRequest{}); err != nil {
			t.Fatal(err)
		}
		r, err := s.CloseAndRecv()
		if err != nil || r.GetAggregatedPayloadSize() != 2 {
			t.Fatal(r, err)
		}
		wait(t, x.stopped)
	})
}

func TestNativeInterceptorsMetadataStatusAndLimits(t *testing.T) {
	var f *forest.Forest
	var mu sync.Mutex
	var observed []uint64
	check := func(ctx context.Context) error {
		g, err := f.Generation(ctx)
		if err == nil {
			mu.Lock()
			observed = append(observed, g)
			mu.Unlock()
		}
		return err
	}
	x := newFixture(t, false, grpc.MaxRecvMsgSize(512), grpc.ChainUnaryInterceptor(func(ctx context.Context, r any, info *grpc.UnaryServerInfo, h grpc.UnaryHandler) (any, error) {
		if err := check(ctx); err != nil {
			return nil, err
		}
		md, _ := metadata.FromIncomingContext(ctx)
		if md.Get("trace")[0] != "consumer" {
			return nil, status.Error(codes.InvalidArgument, "missing metadata")
		}
		_ = grpc.SetHeader(ctx, metadata.Pairs("scoped", "yes"))
		out, err := h(ctx, r)
		if e := check(ctx); e != nil {
			return nil, e
		}
		return out, err
	}), grpc.ChainStreamInterceptor(func(srv any, s grpc.ServerStream, info *grpc.StreamServerInfo, h grpc.StreamHandler) error {
		if err := check(s.Context()); err != nil {
			return err
		}
		err := h(srv, s)
		if e := check(s.Context()); e != nil {
			return e
		}
		return err
	}))
	f = x.f
	ctx, cancel := context.WithTimeout(metadata.NewOutgoingContext(context.Background(), metadata.Pairs("trace", "consumer")), 3*time.Second)
	defer cancel()
	var header metadata.MD
	if _, err := x.client.UnaryCall(ctx, &pb.SimpleRequest{}, grpc.Header(&header)); err != nil {
		t.Fatal(err)
	}
	if values := header.Get("scoped"); len(values) != 1 || values[0] != "yes" {
		t.Fatal(header)
	}
	if _, err := x.client.UnaryCall(ctx, &pb.SimpleRequest{Payload: &pb.Payload{Body: []byte("reject")}}); status.Code(err) != codes.PermissionDenied {
		t.Fatal(err)
	}
	if _, err := x.client.UnaryCall(ctx, &pb.SimpleRequest{Payload: &pb.Payload{Body: make([]byte, 1024)}}); status.Code(err) != codes.ResourceExhausted {
		t.Fatal(err)
	}
	s, err := x.client.FullDuplexCall(ctx)
	if err != nil {
		t.Fatal(err)
	}
	s.CloseSend()
	_, _ = s.Recv()
	mu.Lock()
	defer mu.Unlock()
	if len(observed) != 6 {
		t.Fatal(observed)
	}
	for _, g := range observed {
		if g != 1 {
			t.Fatal(observed)
		}
	}
}

func TestCancellationDeadlineAndIgnoredCancellation(t *testing.T) {
	for _, kind := range []string{"cancel", "deadline", "generation", "ignore"} {
		t.Run(kind, func(t *testing.T) {
			x := newFixture(t, kind == "ignore")
			unary(t, x, "v1/v1@1")
			var ctx context.Context
			var cancel context.CancelFunc
			if kind == "deadline" {
				ctx, cancel = context.WithTimeout(context.Background(), 250*time.Millisecond)
			} else {
				ctx, cancel = context.WithCancel(context.Background())
			}
			defer cancel()
			done := make(chan error, 1)
			go func() {
				_, err := x.client.UnaryCall(ctx, &pb.SimpleRequest{Payload: &pb.Payload{Body: []byte("hold")}})
				done <- err
			}()
			wait(t, x.old.entered)
			if kind == "generation" {
				if n := x.f.CancelGeneration(1); n != 1 {
					t.Fatal(n)
				}
			} else if kind != "deadline" {
				cancel()
			}
			err := wait(t, done)
			want := codes.Canceled
			if kind == "deadline" {
				want = codes.DeadlineExceeded
			}
			if status.Code(err) != want {
				t.Fatal(err)
			}
			if kind != "ignore" {
				wait(t, x.old.canceled)
				return
			}
			apply(t, x.f, forest.Replace("worker", x.next, false))
			select {
			case <-x.stopped:
				t.Fatal("canceled client released running user code")
			default:
			}
			closeCtx, stop := context.WithTimeout(context.Background(), 20*time.Millisecond)
			defer stop()
			if err = x.f.Close(closeCtx); !errors.Is(err, forest.ErrDrainTimeout) {
				t.Fatal(err)
			}
			requestCtx, requestCancel := context.WithTimeout(context.Background(), time.Second)
			defer requestCancel()
			if _, err = x.client.UnaryCall(requestCtx, &pb.SimpleRequest{}); status.Code(err) != codes.Unavailable || status.Convert(err).Message() != "forest unavailable" {
				t.Fatal(err)
			}
			x.unblock()
			wait(t, x.stopped)
		})
	}
}

func TestInvalidOptions(t *testing.T) {
	f := forest.New(forest.Options{})
	defer f.Close(context.Background())
	for _, o := range []grpcforest.Options{{}, {TypeID: "x", Version: "1", Address: "invalid", Register: func(context.Context, *forest.BuildEnv, grpc.ServiceRegistrar) error { return nil }}} {
		if _, err := grpcforest.Definition(f, o); !errors.Is(err, grpcforest.ErrInvalidOptions) {
			t.Fatal(o, err)
		}
	}
}

func TestStreamCancellationReleasesScopedProvider(t *testing.T) {
	for _, kind := range []string{"client", "deadline", "generation"} {
		t.Run(kind, func(t *testing.T) {
			x := newFixture(t, false)
			unary(t, x, "v1/v1@1")
			var ctx context.Context
			var cancel context.CancelFunc
			if kind == "deadline" {
				ctx, cancel = context.WithTimeout(context.Background(), 250*time.Millisecond)
			} else {
				ctx, cancel = context.WithCancel(context.Background())
			}
			defer cancel()
			stream, err := x.client.FullDuplexCall(ctx)
			if err != nil {
				t.Fatal(err)
			}
			if err = stream.Send(&pb.StreamingOutputCallRequest{Payload: &pb.Payload{Body: []byte("hold")}}); err != nil {
				t.Fatal(err)
			}
			wait(t, x.old.entered)
			apply(t, x.f, forest.Replace("worker", x.next, false))
			if kind == "generation" {
				if n := x.f.CancelGeneration(1); n != 1 {
					t.Fatal(n)
				}
			} else if kind == "client" {
				cancel()
			}
			_, err = stream.Recv()
			want := codes.Canceled
			if kind == "deadline" {
				want = codes.DeadlineExceeded
			}
			if status.Code(err) != want {
				t.Fatal(err)
			}
			wait(t, x.old.canceled)
			wait(t, x.stopped)
			unary(t, x, "v2/v2@2")
		})
	}
}

// Used by rollback tests below, kept as a real TCP address rather than a fake listener.
func freeAddress(t *testing.T) string {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	address := l.Addr().String()
	l.Close()
	return address
}
