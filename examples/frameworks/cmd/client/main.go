package main

import (
	"bufio"
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	pb "google.golang.org/grpc/interop/grpc_testing"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
func run() error {
	target := flag.String("target", "127.0.0.1:8090", "gRPC target")
	stream := flag.Bool("stream", false, "send two messages in one RPC; press Enter between them")
	hold := flag.Bool("hold", false, "hold the unary business call until the server release command")
	flag.Parse()
	connection, err := grpc.NewClient(*target, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return err
	}
	defer connection.Close()
	client := pb.NewTestServiceClient(connection)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	if !*stream {
		request := &pb.SimpleRequest{}
		if *hold {
			request.Payload = &pb.Payload{Body: []byte("hold")}
		}
		r, err := client.UnaryCall(ctx, request)
		if err != nil {
			return err
		}
		fmt.Println(string(r.GetPayload().GetBody()))
		return nil
	}
	s, err := client.FullDuplexCall(ctx)
	if err != nil {
		return err
	}
	for n := 0; n < 2; n++ {
		if err = s.Send(&pb.StreamingOutputCallRequest{}); err != nil {
			return err
		}
		r, err := s.Recv()
		if err != nil {
			return err
		}
		fmt.Println(string(r.GetPayload().GetBody()))
		if n == 0 {
			fmt.Fprintln(os.Stderr, "Replace the business tree, then press Enter to send on the same stream.")
			if !bufio.NewScanner(os.Stdin).Scan() {
				return nil
			}
		}
	}
	if err = s.CloseSend(); err != nil {
		return err
	}
	_, err = s.Recv()
	if errors.Is(err, io.EOF) {
		return nil
	}
	return err
}
