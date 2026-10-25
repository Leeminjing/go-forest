package ecosystem

import (
	"context"
	"crypto/subtle"
	"encoding/json"
	"errors"
	"io"
	"net/http"

	ginforest "github.com/Leeminjing/go-forest/adapters/gin"
	grpcforest "github.com/Leeminjing/go-forest/adapters/grpc"
	"github.com/Leeminjing/go-forest/forest"
	"github.com/gin-gonic/gin"
	"go.opentelemetry.io/otel/propagation"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	pb "google.golang.org/grpc/interop/grpc_testing"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

type measure interface {
	Measure(context.Context, string, func(context.Context) error) error
	ReadRuntime(context.Context) (json.RawMessage, error)
}
type input struct {
	ID    string `json:"id"`
	Input string `json:"input"`
}

func entryPorts() []forest.PortDescriptor {
	return []forest.PortDescriptor{forest.Port[Service]("tasks", forest.Required()), forest.Port[measure]("telemetry", forest.Required())}
}
func authorized(actual, expected string) bool {
	return subtle.ConstantTimeCompare([]byte(actual), []byte("Bearer "+expected)) == 1
}
func httpDefinition(f *forest.Forest, o Options) (forest.Definition, error) {
	return ginforest.Definition(f, ginforest.Options{TypeID: "ecosystem.http", Version: "1", Address: o.HTTPAddress, Ports: entryPorts(), BuildRouter: func(_ context.Context, e *forest.BuildEnv) (*gin.Engine, error) {
		tasks, err := forest.Reference[Service](e, "tasks")
		if err != nil {
			return nil, err
		}
		metrics, err := forest.Reference[measure](e, "telemetry")
		if err != nil {
			return nil, err
		}
		r := gin.New()
		r.Use(func(c *gin.Context) {
			c.Header("Cache-Control", "no-store")
			if !authorized(c.GetHeader("Authorization"), o.Token) {
				c.AbortWithStatus(http.StatusUnauthorized)
				return
			}
			c.Next()
		}, ginforest.Scope(f))
		run := func(c *gin.Context, op string, fn func(context.Context, Service) (Task, error)) {
			ctx := tracePropagation.Extract(c.Request.Context(), propagation.HeaderCarrier(c.Request.Header))
			var task Task
			err := metrics.Use(ctx, func(m measure) error {
				return m.Measure(ctx, op, func(ctx context.Context) error {
					return tasks.Use(ctx, func(s Service) error { var err error; task, err = fn(ctx, s); return err })
				})
			})
			if err != nil {
				code := http.StatusServiceUnavailable
				if errors.Is(err, ErrInvalid) {
					code = 400
				}
				if errors.Is(err, ErrConflict) {
					code = 409
				}
				c.JSON(code, gin.H{"error": "task_unavailable"})
				return
			}
			c.JSON(http.StatusOK, task)
		}
		r.POST("/tasks", func(c *gin.Context) {
			var in input
			decoder := json.NewDecoder(http.MaxBytesReader(c.Writer, c.Request.Body, 8192))
			decoder.DisallowUnknownFields()
			if decoder.Decode(&in) != nil || decoder.Decode(new(any)) != io.EOF {
				c.AbortWithStatus(400)
				return
			}
			run(c, "http.submit", func(ctx context.Context, s Service) (Task, error) { return s.Submit(ctx, in.ID, in.Input) })
		})
		r.GET("/tasks/:id", func(c *gin.Context) {
			run(c, "http.get", func(ctx context.Context, s Service) (Task, error) { return s.Get(ctx, c.Param("id")) })
		})
		r.GET("/runtime", func(c *gin.Context) {
			ctx := c.Request.Context()
			var data json.RawMessage
			err := metrics.Use(ctx, func(m measure) error { var err error; data, err = m.ReadRuntime(ctx); return err })
			if err != nil {
				c.AbortWithStatus(503)
				return
			}
			c.Data(200, "application/json", data)
		})
		return r, nil
	}})
}

type rpcService struct {
	pb.UnimplementedTestServiceServer
	tasks   forest.Ref[Service]
	metrics forest.Ref[measure]
	token   string
}

func (s *rpcService) UnaryCall(ctx context.Context, req *pb.SimpleRequest) (*pb.SimpleResponse, error) {
	md, _ := metadata.FromIncomingContext(ctx)
	carrier := propagation.MapCarrier{}
	for _, key := range []string{"traceparent", "tracestate"} {
		if values := md.Get(key); len(values) == 1 {
			carrier[key] = values[0]
		}
	}
	ctx = tracePropagation.Extract(ctx, carrier)
	values := md.Get("authorization")
	if len(values) != 1 || !authorized(values[0], s.token) {
		return nil, status.Error(codes.Unauthenticated, "unauthorized")
	}
	var in input
	data := req.GetPayload().GetBody()
	if len(data) > 8192 || json.Unmarshal(data, &in) != nil {
		return nil, status.Error(codes.InvalidArgument, "invalid task")
	}
	var task Task
	err := s.metrics.Use(ctx, func(m measure) error {
		return m.Measure(ctx, "rpc.submit", func(ctx context.Context) error {
			return s.tasks.Use(ctx, func(api Service) error { var err error; task, err = api.Submit(ctx, in.ID, in.Input); return err })
		})
	})
	if err != nil {
		return nil, status.Error(codes.Unavailable, "task unavailable")
	}
	body, err := json.Marshal(task)
	return &pb.SimpleResponse{Payload: &pb.Payload{Body: body}}, err
}
func rpcDefinition(f *forest.Forest, o Options) (forest.Definition, error) {
	return grpcforest.Definition(f, grpcforest.Options{TypeID: "ecosystem.grpc", Version: "1", Address: o.GRPCAddress, Ports: entryPorts(), Register: func(_ context.Context, e *forest.BuildEnv, r grpc.ServiceRegistrar) error {
		tasks, err := forest.Reference[Service](e, "tasks")
		if err != nil {
			return err
		}
		metrics, err := forest.Reference[measure](e, "telemetry")
		if err != nil {
			return err
		}
		pb.RegisterTestServiceServer(r, &rpcService{tasks: tasks, metrics: metrics, token: o.Token})
		return nil
	}})
}
