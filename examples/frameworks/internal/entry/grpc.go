package entry

import (
	"context"
	"encoding/json"
	"errors"
	"io"

	grpcforest "github.com/Leeminjing/go-forest/adapters/grpc"
	"github.com/Leeminjing/go-forest/forest"
	"google.golang.org/grpc"
	pb "google.golang.org/grpc/interop/grpc_testing"
)

// The example reuses grpc-go's generated interoperability protocol so users do
// not need protoc just to run it. Applications register their own generated API.
type service struct {
	pb.UnimplementedTestServiceServer
	f   *forest.Forest
	ref forest.Ref[worker]
}

func (s *service) UnaryCall(ctx context.Context, r *pb.SimpleRequest) (*pb.SimpleResponse, error) {
	value, err := read(ctx, s.f, s.ref, string(r.GetPayload().GetBody()) == "hold")
	if err != nil {
		return nil, err
	}
	data, err := json.Marshal(value)
	return &pb.SimpleResponse{Payload: &pb.Payload{Body: data}}, err
}
func (s *service) FullDuplexCall(stream grpc.BidiStreamingServer[pb.StreamingOutputCallRequest, pb.StreamingOutputCallResponse]) error {
	for {
		r, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			return nil
		}
		if err != nil {
			return err
		}
		value, err := read(stream.Context(), s.f, s.ref, string(r.GetPayload().GetBody()) == "hold")
		if err != nil {
			return err
		}
		data, err := json.Marshal(value)
		if err != nil {
			return err
		}
		if err = stream.Send(&pb.StreamingOutputCallResponse{Payload: &pb.Payload{Body: data}}); err != nil {
			return err
		}
	}
}
func GRPC(f *forest.Forest, address string) (forest.Definition, error) {
	return grpcforest.Definition(f, grpcforest.Options{TypeID: "frameworks.grpc", Version: "1", Address: address, Ports: ports(), Register: func(ctx context.Context, env *forest.BuildEnv, r grpc.ServiceRegistrar) error {
		ref, err := forest.Reference[worker](env, "business")
		if err != nil {
			return err
		}
		pb.RegisterTestServiceServer(r, &service{f: f, ref: ref})
		return nil
	}})
}
