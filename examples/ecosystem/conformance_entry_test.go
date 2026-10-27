package ecosystem

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"testing"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	pb "google.golang.org/grpc/interop/grpc_testing"
	"google.golang.org/grpc/metadata"
)

func (f *confFixture) submit(t *testing.T, transport, id string) Task {
	t.Helper()
	data, err := json.Marshal(input{ID: id, Input: "compute 2 + 3"})
	if err != nil {
		t.Fatal(err)
	}
	var out Task
	if transport == "rpc" {
		conn, err := grpc.NewClient(f.r.GRPCTarget, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			t.Fatal(err)
		}
		defer conn.Close()
		ctx := metadata.AppendToOutgoingContext(f.ctx, "authorization", "Bearer "+f.o.Token, "traceparent", f.parentTrace)
		response, err := pb.NewTestServiceClient(conn).UnaryCall(ctx, &pb.SimpleRequest{Payload: &pb.Payload{Body: data}})
		if err != nil {
			t.Fatal(err)
		}
		if err = json.Unmarshal(response.GetPayload().GetBody(), &out); err != nil {
			t.Fatal(err)
		}
	} else {
		req, err := http.NewRequestWithContext(f.ctx, http.MethodPost, f.r.HTTPURL+"/tasks", bytes.NewReader(data))
		if err != nil {
			t.Fatal(err)
		}
		if f.parentTrace != "" {
			req.Header.Set("traceparent", f.parentTrace)
		}
		req.Header.Set("Authorization", "Bearer "+f.o.Token)
		req.Header.Set("Content-Type", "application/json")
		response, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		defer response.Body.Close()
		if response.StatusCode != 200 {
			t.Fatal(fmt.Errorf("HTTP %d", response.StatusCode))
		}
		if err = json.NewDecoder(response.Body).Decode(&out); err != nil {
			t.Fatal(err)
		}
	}
	if out.ID != id {
		t.Fatal(out)
	}
	return out
}

func (f *confFixture) readHTTP(t *testing.T, id string) Task {
	t.Helper()
	req, err := http.NewRequestWithContext(f.ctx, http.MethodGet, f.r.HTTPURL+"/tasks/"+id, nil)
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Authorization", "Bearer "+f.o.Token)
	response, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	if response.StatusCode != 200 {
		t.Fatal(response.StatusCode)
	}
	var task Task
	if err = json.NewDecoder(response.Body).Decode(&task); err != nil {
		t.Fatal(err)
	}
	return task
}
