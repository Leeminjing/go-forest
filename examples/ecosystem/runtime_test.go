package ecosystem_test

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/Leeminjing/go-forest/examples/ecosystem"
	"github.com/Leeminjing/go-forest/forest"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	pb "google.golang.org/grpc/interop/grpc_testing"
	"google.golang.org/grpc/metadata"
)

const token = "ecosystem-integration-token"

func options(t *testing.T) ecosystem.Options {
	t.Helper()
	if os.Getenv("FOREST_INTEGRATION") != "1" {
		t.Skip("requires real PostgreSQL and JetStream: FOREST_INTEGRATION=1")
	}
	dsn, url := os.Getenv("FOREST_POSTGRES_DSN"), os.Getenv("FOREST_NATS_URL")
	if dsn == "" || url == "" {
		t.Fatal("FOREST_POSTGRES_DSN and FOREST_NATS_URL required")
	}
	o := ecosystem.Options{DSN: dsn, NATSURL: url, Namespace: fmt.Sprintf("F_%d", time.Now().UnixNano()), Token: token, CheckpointDir: t.TempDir()}
	if err := ecosystem.Initialize(context.Background(), o); err != nil {
		t.Fatal(err)
	}
	return o
}
func runtime(t *testing.T, o ecosystem.Options) *ecosystem.Runtime {
	t.Helper()
	r, err := ecosystem.New(context.Background(), o)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		c, x := context.WithTimeout(context.Background(), 5*time.Second)
		defer x()
		if err := r.Close(c); err != nil {
			t.Error(err)
		}
	})
	return r
}
func request(url, method, body string) (ecosystem.Task, error) {
	req, err := http.NewRequest(method, url, strings.NewReader(body))
	if err != nil {
		return ecosystem.Task{}, err
	}
	req.Header.Set("Authorization", "Bearer "+token)
	req.Header.Set("Content-Type", "application/json")
	client := &http.Client{Timeout: 5 * time.Second}
	res, err := client.Do(req)
	if err != nil {
		return ecosystem.Task{}, err
	}
	defer res.Body.Close()
	if res.StatusCode != 200 {
		return ecosystem.Task{}, fmt.Errorf("HTTP %d", res.StatusCode)
	}
	var task ecosystem.Task
	err = json.NewDecoder(res.Body).Decode(&task)
	return task, err
}
func TestSixFrameworksCompleteAndIdempotent(t *testing.T) {
	o := options(t)
	spans := tracetest.NewSpanRecorder()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(spans), sdktrace.WithSampler(sdktrace.AlwaysSample()))
	defer tp.Shutdown(context.Background())
	o.Telemetry.TracerProvider = tp
	r := runtime(t, o)
	ctx := context.Background()
	if err := r.Start(ctx); err != nil {
		t.Fatal(err)
	}
	task, err := request(r.HTTPURL+"/tasks", "POST", `{"id":"one","input":"compute 2 + 3"}`)
	if err != nil {
		t.Fatal(err)
	}
	if task.State != "pending" || task.Version != "v1" {
		t.Fatal(task)
	}
	conn, err := grpc.NewClient(r.GRPCTarget, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	rpcCtx := metadata.AppendToOutgoingContext(ctx, "authorization", "Bearer "+token)
	response, err := pb.NewTestServiceClient(conn).UnaryCall(rpcCtx, &pb.SimpleRequest{Payload: &pb.Payload{Body: []byte(`{"id":"one","input":"compute 2 + 3"}`)}})
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Contains(response.Payload.Body, []byte(`"id":"one"`)) {
		t.Fatal(string(response.Payload.Body))
	}
	deadline := time.Now().Add(8 * time.Second)
	for {
		task, err = request(r.HTTPURL+"/tasks/one", "GET", "")
		if err != nil {
			t.Fatal(err)
		}
		if task.State == "completed" {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("unfinished: %+v errors=%d", task, r.DriverErrors())
		}
		time.Sleep(20 * time.Millisecond)
	}
	if task.Result != "model v1: 5" {
		t.Fatal(task)
	}
	var submitted, received string
	for _, span := range spans.Ended() {
		if span.Name() == "http.submit" && submitted == "" {
			submitted = span.SpanContext().TraceID().String()
		}
		if span.Name() == "receive" {
			received = span.SpanContext().TraceID().String()
		}
	}
	if submitted == "" || received != submitted {
		t.Fatalf("trace context not propagated through persisted outbox and NATS: submit=%s receive=%s", submitted, received)
	}
	same, err := request(r.HTTPURL+"/tasks", "POST", `{"id":"one","input":"compute 2 + 3"}`)
	if err != nil || same.State != "completed" || same.Result != task.Result {
		t.Fatalf("non-idempotent: %+v %v", same, err)
	}
	req, _ := http.NewRequest("GET", r.ObservatoryURL+"/api/v1/runtime", nil)
	req.Header.Set("Authorization", "Bearer "+token)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	data, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if resp.StatusCode != 200 || !bytes.Contains(data, []byte("explicit boundaries only")) {
		t.Fatalf("runtime observation: %d %s", resp.StatusCode, data)
	}
	snap := r.Forest.Snapshot()
	if len(snap.Trees) < 18 || len(snap.Links) < 9 || len(snap.NestBindings) < 1 {
		t.Fatalf("not a composed forest: %+v", snap)
	}
}
func TestOldHTTPAndRPCPinBusinessVersion(t *testing.T) {
	o := options(t)
	entered := make(chan string, 2)
	release := make(chan struct{})
	o.BeforeSubmit = func(ctx context.Context, id string) error {
		entered <- id
		select {
		case <-release:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	r := runtime(t, o)
	defer close(release)
	type result struct {
		task ecosystem.Task
		err  error
	}
	httpDone := make(chan result, 1)
	go func() {
		task, err := request(r.HTTPURL+"/tasks", "POST", `{"id":"http-old","input":"task"}`)
		httpDone <- result{task, err}
	}()
	conn, err := grpc.NewClient(r.GRPCTarget, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	rpcDone := make(chan error, 1)
	go func() {
		c, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		c = metadata.AppendToOutgoingContext(c, "authorization", "Bearer "+token)
		resp, err := pb.NewTestServiceClient(conn).UnaryCall(c, &pb.SimpleRequest{Payload: &pb.Payload{Body: []byte(`{"id":"rpc-old","input":"task"}`)}})
		if err == nil && !bytes.Contains(resp.Payload.Body, []byte(`"version":"v1"`)) {
			err = fmt.Errorf("old RPC: %s", resp.Payload.Body)
		}
		rpcDone <- err
	}()
	for i := 0; i < 2; i++ {
		select {
		case <-entered:
		case <-time.After(3 * time.Second):
			t.Fatal("old requests not admitted")
		}
	}
	if _, err = r.Replace(context.Background()); err != nil {
		t.Fatal(err)
	}
	fresh, err := request(r.HTTPURL+"/tasks", "POST", `{"id":"new","input":"task"}`)
	if err != nil || fresh.Version != "v2" {
		t.Fatalf("new HTTP: %+v %v", fresh, err)
	}
	draining := false
	for _, instance := range r.Forest.Snapshot().Instances {
		if instance.TreeID == "app/service" && instance.State == forest.Draining {
			draining = true
		}
	}
	if !draining {
		t.Fatal("old instance released while used")
	}
	release <- struct{}{}
	release <- struct{}{}
	old := <-httpDone
	if old.err != nil || old.task.Version != "v1" {
		t.Fatalf("old HTTP: %+v", old)
	}
	if err = <-rpcDone; err != nil {
		t.Fatal(err)
	}
	frame := r.Telemetry.Snapshot()
	generations := map[uint64]bool{}
	for _, d := range frame.Details {
		generations[d.Generation] = true
	}
	if !generations[1] || !generations[2] {
		t.Fatal("telemetry mislabeled pinned generations")
	}
}

func TestTaskIdentityIsUnchangedAcrossURLAndMessageHeaders(t *testing.T) {
	o := options(t)
	r := runtime(t, o)
	for _, id := range []string{"one\r\nX-Header: other", "one two", "one/two", " leading", "trailing ", ""} {
		body, _ := json.Marshal(map[string]string{"id": id, "input": "task"})
		if _, err := request(r.HTTPURL+"/tasks", "POST", string(body)); err == nil || err.Error() != "HTTP 400" {
			t.Fatalf("unsafe task identity accepted: id=%q err=%v", id, err)
		}
	}
	first, err := request(r.HTTPURL+"/tasks", "POST", `{"id":"safe-1_2.3","input":"task"}`)
	if err != nil {
		t.Fatal(err)
	}
	again, err := request(r.HTTPURL+"/tasks", "POST", `{"id":"safe-1_2.3","input":"task"}`)
	if err != nil || again.ID != first.ID || again.Input != first.Input {
		t.Fatalf("identity changed: %+v %+v %v", first, again, err)
	}
}
