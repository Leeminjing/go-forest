// Package grpcforest hosts native gRPC services as ordinary Forest trees.
package grpcforest

import (
	"context"
	"errors"
	"fmt"
	"net"
	"reflect"
	"sync"

	"github.com/Leeminjing/go-forest/forest"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

var ErrInvalidOptions = errors.New("invalid gRPC tree options")

type Endpoint interface {
	Target() string
	ServeError() error
}

type Options struct {
	TypeID  string
	Version string
	Address string
	Ports   []forest.PortDescriptor
	// The adapter owns UnaryInterceptor and StreamInterceptor. Use native
	// ChainUnaryInterceptor/ChainStreamInterceptor for application middleware.
	ServerOptions []grpc.ServerOption
	// Register runs only during preparation. Register generated services and
	// create typed Refs here. Do not retain env or the preparation context.
	Register func(context.Context, *forest.BuildEnv, grpc.ServiceRegistrar) error
}

func Definition(f *forest.Forest, options Options) (forest.Definition, error) {
	if f == nil || options.TypeID == "" || options.Version == "" || options.Register == nil {
		return forest.Definition{}, ErrInvalidOptions
	}
	if options.Address == "" {
		options.Address = "127.0.0.1:0"
	}
	if _, _, err := net.SplitHostPort(options.Address); err != nil {
		return forest.Definition{}, errors.Join(ErrInvalidOptions, err)
	}
	options.Ports = append([]forest.PortDescriptor(nil), options.Ports...)
	options.ServerOptions = append([]grpc.ServerOption(nil), options.ServerOptions...)
	return forest.Definition{TypeID: options.TypeID, Version: options.Version, Ports: options.Ports,
		Capabilities: []forest.CapabilityDescriptor{forest.Capability[Endpoint]("rpc", forest.Public)},
		Build: func(ctx context.Context, env *forest.BuildEnv) (lifecycle forest.Lifecycle, err error) {
			// A registration panic must return the already-created lifecycle so
			// Core can roll back native server resources, not merely its listener.
			defer func() {
				if p := recover(); p != nil {
					err = fmt.Errorf("%w: server preparation panicked: %v", ErrInvalidOptions, p)
				}
			}()
			serverOptions := []grpc.ServerOption{grpc.UnaryInterceptor(unaryScope(f)), grpc.StreamInterceptor(streamScope(f))}
			serverOptions = append(serverOptions, options.ServerOptions...)
			s := &server{address: options.Address, native: grpc.NewServer(serverOptions...), stopped: make(chan struct{})}
			lifecycle = s
			registrar := &registration{native: s.native, names: map[string]bool{}}
			defer registrar.seal()
			if err = options.Register(ctx, env, registrar); err != nil {
				return s, err
			}
			if err = registrar.seal(); err != nil {
				return s, err
			}
			return s, forest.Provide(env, "rpc", s)
		},
	}, nil
}

// Native grpc.RegisterService exits the process for duplicate services and
// incompatible implementations. Validate those preparation errors before
// delegating so a rejected candidate can roll back without killing the Forest.
type registration struct {
	native *grpc.Server
	mu     sync.Mutex
	names  map[string]bool
	closed bool
	err    error
}

func (r *registration) RegisterService(desc *grpc.ServiceDesc, implementation any) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.err != nil {
		return
	}
	if r.closed {
		r.err = fmt.Errorf("%w: registration already finished", ErrInvalidOptions)
		return
	}
	if desc == nil {
		r.err = fmt.Errorf("%w: nil service descriptor", ErrInvalidOptions)
		return
	}
	if r.names[desc.ServiceName] {
		r.err = fmt.Errorf("%w: duplicate service %q", ErrInvalidOptions, desc.ServiceName)
		return
	}
	if implementation != nil {
		t := reflect.TypeOf(desc.HandlerType)
		if t == nil || t.Kind() != reflect.Pointer || t.Elem().Kind() != reflect.Interface || !reflect.TypeOf(implementation).Implements(t.Elem()) {
			r.err = fmt.Errorf("%w: service %q implementation mismatch", ErrInvalidOptions, desc.ServiceName)
			return
		}
	}
	r.names[desc.ServiceName] = true
	r.native.RegisterService(desc, implementation)
}
func (r *registration) GetServiceInfo() map[string]grpc.ServiceInfo { return r.native.GetServiceInfo() }
func (r *registration) seal() error                                 { r.mu.Lock(); defer r.mu.Unlock(); r.closed = true; return r.err }

func admissionError(err error) error {
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return status.FromContextError(err).Err()
	}
	return status.Error(codes.Unavailable, "forest unavailable")
}

func unaryScope(f *forest.Forest) grpc.UnaryServerInterceptor {
	return func(ctx context.Context, request any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (response any, err error) {
		entered := false
		err = f.Invoke(ctx, func(callCtx context.Context) error {
			entered = true
			var err error
			response, err = handler(callCtx, request)
			return err
		})
		if !entered {
			return nil, admissionError(err)
		}
		return response, err
	}
}

type scopedStream struct {
	grpc.ServerStream
	ctx context.Context
}

func (s scopedStream) Context() context.Context { return s.ctx }
func streamScope(f *forest.Forest) grpc.StreamServerInterceptor {
	return func(service any, stream grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		entered := false
		err := f.Invoke(stream.Context(), func(callCtx context.Context) error {
			entered = true
			return handler(service, scopedStream{stream, callCtx})
		})
		if !entered {
			return admissionError(err)
		}
		return err
	}
}

type server struct {
	address             string
	native              *grpc.Server
	mu                  sync.Mutex
	target              string
	done                chan struct{}
	err                 error
	closed              bool
	stopOnce, forceOnce sync.Once
	stopped             chan struct{}
}

func (s *server) Target() string    { s.mu.Lock(); defer s.mu.Unlock(); return s.target }
func (s *server) ServeError() error { s.mu.Lock(); defer s.mu.Unlock(); return s.err }
func (s *server) Start(ctx context.Context) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed || s.done != nil {
		return ErrInvalidOptions
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	listener, err := net.Listen("tcp", s.address)
	if err != nil {
		return err
	}
	s.target = listener.Addr().String()
	s.done = make(chan struct{})
	go func() {
		err := s.native.Serve(listener)
		if errors.Is(err, grpc.ErrServerStopped) {
			err = nil
		}
		s.mu.Lock()
		s.err = err
		s.mu.Unlock()
		close(s.done)
	}()
	return nil
}
func (s *server) Ready(context.Context) error { return s.ServeError() }
func (s *server) Stop(ctx context.Context) error {
	s.mu.Lock()
	s.closed = true
	done := s.done
	s.mu.Unlock()
	s.stopOnce.Do(func() { go func() { s.native.GracefulStop(); close(s.stopped) }() })
	select {
	case <-s.stopped:
	case <-ctx.Done():
		s.forceOnce.Do(func() { s.native.Stop() })
		// A disconnected transport does not prove that user code finished.
		// Core observes its own drain deadline while this callback still waits.
		<-s.stopped
	}
	if done != nil {
		<-done
	}
	return s.ServeError()
}
