package frameworks_test

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/Leeminjing/go-forest/examples/frameworks"
	"github.com/Leeminjing/go-forest/extensions/observe"
	"github.com/gin-gonic/gin"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	pb "google.golang.org/grpc/interop/grpc_testing"
)

type reply struct{ Value, Again, Generation string }

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
func TestGinGRPCAndObservatoryShareOneBusinessTree(t *testing.T) {
	gin.SetMode(gin.ReleaseMode)
	const token = "frameworks-integration-credential"
	r, err := frameworks.New(context.Background(), frameworks.Options{Token: token, SampleInterval: 10 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		if err := r.Close(ctx); err != nil {
			t.Error(err)
		}
	})
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	connection, err := grpc.NewClient(r.GRPCTarget, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	defer connection.Close()
	client := pb.NewTestServiceClient(connection)
	httpClient := &http.Client{Timeout: 5 * time.Second}
	defer httpClient.CloseIdleConnections()
	getState := func() observe.Frame {
		req, _ := http.NewRequest("GET", r.ObservatoryURL+"/api/v1/state", nil)
		req.Header.Set("Authorization", "Bearer "+token)
		resp, err := httpClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		defer resp.Body.Close()
		var f observe.Frame
		if err = json.NewDecoder(resp.Body).Decode(&f); err != nil {
			t.Fatal(err)
		}
		return f
	}
	request, _ := http.NewRequestWithContext(ctx, "GET", r.ObservatoryURL+"/api/v1/events", nil)
	request.Header.Set("Authorization", "Bearer "+token)
	events, err := http.DefaultClient.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	defer events.Body.Close()
	scanner := bufio.NewScanner(events.Body)
	for scanner.Scan() {
		if strings.HasPrefix(scanner.Text(), "data: ") {
			break
		}
	}
	stream, err := client.FullDuplexCall(ctx)
	if err != nil {
		t.Fatal(err)
	}
	streamMessage := func() reply {
		if err := stream.Send(&pb.StreamingOutputCallRequest{}); err != nil {
			t.Fatal(err)
		}
		response, err := stream.Recv()
		if err != nil {
			t.Fatal(err)
		}
		var got reply
		if err = json.Unmarshal(response.GetPayload().GetBody(), &got); err != nil {
			t.Fatal(err)
		}
		return got
	}
	if got := streamMessage(); got.Value != "v1" || got.Again != "v1" || got.Generation != "1" {
		t.Fatal(got)
	}
	type result struct {
		reply
		err error
	}
	old := make(chan result, 1)
	go func() {
		response, err := httpClient.Get(r.HTTPURL + "/work?hold=1")
		if err != nil {
			old <- result{err: err}
			return
		}
		defer response.Body.Close()
		var got reply
		err = json.NewDecoder(response.Body).Decode(&got)
		old <- result{got, err}
	}()
	wait(t, r.Entered)
	if _, err = r.Replace(ctx); err != nil {
		t.Fatal(err)
	}
	state := getState()
	if state.Snapshot.Counts.Trees != 5 || state.Snapshot.Counts.Links != 2 || state.Snapshot.Counts.NestBindings != 1 || state.Snapshot.Counts.Draining != 1 {
		t.Fatal(state)
	}
	response, err := httpClient.Get(r.HTTPURL + "/work")
	if err != nil {
		t.Fatal(err)
	}
	var got reply
	err = json.NewDecoder(response.Body).Decode(&got)
	response.Body.Close()
	if err != nil || got.Value != "v2" || got.Again != "v2" || got.Generation != "2" {
		t.Fatal(got, err)
	}
	rpcReply, err := client.UnaryCall(ctx, &pb.SimpleRequest{})
	if err != nil {
		t.Fatal(err)
	}
	if err = json.Unmarshal(rpcReply.GetPayload().GetBody(), &got); err != nil || got.Value != "v2" || got.Again != "v2" || got.Generation != "2" {
		t.Fatal(got, err)
	}
	r.Release()
	prior := wait(t, old)
	if prior.err != nil || prior.Value != "v1" || prior.Again != "v1" || prior.Generation != "1" {
		t.Fatal(prior)
	}
	if getState().Snapshot.Counts.Draining != 1 {
		t.Fatal("long gRPC stream should still hold the old instance")
	}
	if got := streamMessage(); got.Value != "v1" || got.Again != "v1" || got.Generation != "1" {
		t.Fatal(got)
	}
	if err = stream.CloseSend(); err != nil {
		t.Fatal(err)
	}
	if _, err = stream.Recv(); !errors.Is(err, io.EOF) {
		t.Fatal(err)
	}
	if err = r.Forest.Wait(ctx); err != nil {
		t.Fatal(err)
	}
	stopped := false
	for _, i := range getState().Snapshot.Instances {
		if i.TreeID == "business" && i.Version == "1" && i.State == "stopped" {
			stopped = true
		}
	}
	if !stopped {
		t.Fatal("old provider did not stop after both protocols released")
	}
	if err = r.Close(ctx); err != nil {
		t.Fatal(err)
	}
}
