package consumer_test

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	ginforest "github.com/Leeminjing/go-forest/adapters/gin"
	grpcforest "github.com/Leeminjing/go-forest/adapters/grpc"
	"github.com/Leeminjing/go-forest/extensions/observatory"
	"github.com/Leeminjing/go-forest/extensions/observe"
	"github.com/Leeminjing/go-forest/forest"
	"github.com/gin-gonic/gin"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	pb "google.golang.org/grpc/interop/grpc_testing"
)

type service interface {
	Value(context.Context, bool) (string, error)
}
type implementation struct {
	label   string
	entered chan struct{}
	release <-chan struct{}
	once    sync.Once
}

func (s *implementation) Value(ctx context.Context, hold bool) (string, error) {
	if hold && s.release != nil {
		s.once.Do(func() { close(s.entered) })
		select {
		case <-s.release:
		case <-ctx.Done():
			return "", ctx.Err()
		}
	}
	return s.label, nil
}
func definition(version string, s *implementation) forest.Definition {
	return forest.Definition{TypeID: "consumer.service", Version: version, HotSwappable: true, Capabilities: []forest.CapabilityDescriptor{forest.Capability[service]("value", forest.Public)}, Build: func(ctx context.Context, env *forest.BuildEnv) (forest.Lifecycle, error) {
		return forest.Hooks{}, forest.Provide(env, "value", s)
	}}
}

// An external application owns its protocol adapter and consumes its own
// interface. Only the native registration and lifecycle come from grpcforest.
type rpcAdapter struct {
	pb.UnimplementedTestServiceServer
	f   *forest.Forest
	ref forest.Ref[service]
}

func (a *rpcAdapter) read(ctx context.Context) (string, error) {
	var first, second string
	err := a.ref.Use(ctx, func(s service) error { var err error; first, err = s.Value(ctx, false); return err })
	if err == nil {
		err = a.ref.Use(ctx, func(s service) error { var err error; second, err = s.Value(ctx, false); return err })
	}
	if err != nil {
		return "", err
	}
	generation, err := a.f.Generation(ctx)
	return first + "/" + second + "@" + strconv.FormatUint(generation, 10), err
}
func (a *rpcAdapter) UnaryCall(ctx context.Context, _ *pb.SimpleRequest) (*pb.SimpleResponse, error) {
	value, err := a.read(ctx)
	return &pb.SimpleResponse{Payload: &pb.Payload{Body: []byte(value)}}, err
}
func (a *rpcAdapter) FullDuplexCall(stream grpc.BidiStreamingServer[pb.StreamingOutputCallRequest, pb.StreamingOutputCallResponse]) error {
	for {
		_, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			return nil
		}
		if err != nil {
			return err
		}
		value, err := a.read(stream.Context())
		if err != nil {
			return err
		}
		if err = stream.Send(&pb.StreamingOutputCallResponse{Payload: &pb.Payload{Body: []byte(value)}}); err != nil {
			return err
		}
	}
}

func TestPublishedModulesComposeAndHotReplace(t *testing.T) {
	gin.SetMode(gin.ReleaseMode)
	f := forest.New(forest.Options{})
	release := make(chan struct{})
	var once sync.Once
	unblock := func() { once.Do(func() { close(release) }) }
	t.Cleanup(func() {
		unblock()
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		if err := f.Close(ctx); err != nil {
			t.Error(err)
		}
	})
	old := &implementation{label: "v1", entered: make(chan struct{}), release: release}
	v1, v2 := definition("1", old), definition("2", &implementation{label: "v2"})
	app, err := ginforest.Definition(f, ginforest.Options{TypeID: "consumer.http", Version: "1", Ports: []forest.PortDescriptor{forest.Port[service]("service", forest.Required())}, BuildRouter: func(ctx context.Context, env *forest.BuildEnv) (*gin.Engine, error) {
		ref, err := forest.Reference[service](env, "service")
		if err != nil {
			return nil, err
		}
		r := gin.New()
		r.Use(ginforest.Scope(f))
		r.GET("/value", func(c *gin.Context) {
			var first, second string
			err := ref.Use(c.Request.Context(), func(s service) error {
				var err error
				first, err = s.Value(c.Request.Context(), c.Query("hold") == "1")
				return err
			})
			if err == nil {
				err = ref.Use(c.Request.Context(), func(s service) error { var err error; second, err = s.Value(c.Request.Context(), false); return err })
			}
			if err != nil {
				c.Status(503)
				return
			}
			c.JSON(200, gin.H{"first": first, "second": second})
		})
		return r, nil
	}})
	if err != nil {
		t.Fatal(err)
	}
	rpcDefinition, err := grpcforest.Definition(f, grpcforest.Options{TypeID: "consumer.grpc", Version: "1", Ports: []forest.PortDescriptor{forest.Port[service]("service", forest.Required())}, Register: func(ctx context.Context, env *forest.BuildEnv, registrar grpc.ServiceRegistrar) error {
		ref, err := forest.Reference[service](env, "service")
		if err != nil {
			return err
		}
		pb.RegisterTestServiceServer(registrar, &rpcAdapter{f: f, ref: ref})
		return nil
	}})
	if err != nil {
		t.Fatal(err)
	}
	const token = "external-consumer-test-credential"
	bundle, err := observatory.New(f, observatory.Options{Token: token, SampleInterval: 10 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	for _, d := range append([]forest.Definition{v1, v2, app, rpcDefinition}, bundle.Definitions...) {
		if err = f.Register(d); err != nil {
			t.Fatal(err)
		}
	}
	placement, err := bundle.Blueprint.Instantiate("observatory", "")
	if err != nil {
		t.Fatal(err)
	}
	plan := placement.Plan(0)
	plan.Operations = append(plan.Operations, forest.AddRoot("service", v1.Ref()), forest.AddRoot("http", app.Ref()), forest.AddRoot("grpc", rpcDefinition.Ref()), forest.Connect(forest.Link{ID: "service", Consumer: "http", Port: "service", Provider: "service", Capability: "value"}), forest.Connect(forest.Link{ID: "rpc.service", Consumer: "grpc", Port: "service", Provider: "service", Capability: "value"}))
	if _, err = f.Apply(context.Background(), plan); err != nil {
		t.Fatal(err)
	}
	endpoint := func(id forest.TreeID) string {
		var value string
		err := f.Invoke(context.Background(), func(ctx context.Context) error {
			return forest.Use[ginforest.Endpoint](ctx, f, id, "http", func(e ginforest.Endpoint) error { value = e.URL(); return e.ServeError() })
		})
		if err != nil {
			t.Fatal(err)
		}
		return value
	}
	appURL, observeURL := endpoint("http"), endpoint("observatory")
	var rpcTarget string
	err = f.Invoke(context.Background(), func(ctx context.Context) error {
		return forest.Use[grpcforest.Endpoint](ctx, f, "grpc", "rpc", func(endpoint grpcforest.Endpoint) error { rpcTarget = endpoint.Target(); return endpoint.ServeError() })
	})
	if err != nil {
		t.Fatal(err)
	}
	connection, err := grpc.NewClient(rpcTarget, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	defer connection.Close()
	rpcClient := pb.NewTestServiceClient(connection)
	rpcCtx, rpcCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer rpcCancel()
	rpcStream, err := rpcClient.FullDuplexCall(rpcCtx)
	if err != nil {
		t.Fatal(err)
	}
	streamMessage := func(want string) {
		t.Helper()
		if err := rpcStream.Send(&pb.StreamingOutputCallRequest{}); err != nil {
			t.Fatal(err)
		}
		response, err := rpcStream.Recv()
		if err != nil || string(response.GetPayload().GetBody()) != want {
			t.Fatal(response, err)
		}
	}
	streamMessage("v1/v1@1")
	client := &http.Client{Timeout: 5 * time.Second}
	defer client.CloseIdleConnections()
	request := func(path string) *http.Response {
		req, _ := http.NewRequest("GET", observeURL+path, nil)
		req.Header.Set("Authorization", "Bearer "+token)
		r, err := client.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		if r.StatusCode != 200 {
			r.Body.Close()
			t.Fatal(r.Status)
		}
		return r
	}
	frame := func() observe.Frame {
		r := request("/api/v1/state")
		defer r.Body.Close()
		var value observe.Frame
		if err := json.NewDecoder(r.Body).Decode(&value); err != nil {
			t.Fatal(err)
		}
		return value
	}
	initial := frame()
	if initial.Snapshot.Counts.Trees != 5 || initial.Snapshot.Counts.Links != 2 || initial.Snapshot.Counts.NestBindings != 1 {
		t.Fatal(initial)
	}
	if _, err := observe.ParseCursor(initial.Cursor); err != nil {
		t.Fatal(err)
	}
	stream := request("/api/v1/events")
	defer stream.Body.Close()
	scanner := bufio.NewScanner(stream.Body)
	received := false
	for scanner.Scan() {
		if strings.HasPrefix(scanner.Text(), "data: ") {
			received = true
			break
		}
	}
	if !received {
		t.Fatal("missing initial SSE", scanner.Err())
	}
	type result struct {
		first, second string
		err           error
	}
	readValue := func(hold bool) result {
		path := "/value"
		if hold {
			path += "?hold=1"
		}
		r, err := client.Get(appURL + path)
		if err != nil {
			return result{err: err}
		}
		defer r.Body.Close()
		var data map[string]string
		err = json.NewDecoder(r.Body).Decode(&data)
		return result{data["first"], data["second"], err}
	}
	oldResult := make(chan result, 1)
	go func() { oldResult <- readValue(true) }()
	select {
	case <-old.entered:
	case <-time.After(3 * time.Second):
		t.Fatal("old request did not enter")
	}
	_, err = f.Apply(context.Background(), forest.ChangePlan{BaseGeneration: 1, Operations: []forest.Operation{forest.Replace("service", v2.Ref(), false)}})
	if err != nil {
		t.Fatal(err)
	}
	if got := readValue(false); got.err != nil || got.first != "v2" || got.second != "v2" {
		t.Fatal(got)
	}
	response, err := rpcClient.UnaryCall(rpcCtx, &pb.SimpleRequest{})
	if err != nil || string(response.GetPayload().GetBody()) != "v2/v2@2" {
		t.Fatal(response, err)
	}
	streamMessage("v1/v1@1")
	if next := frame(); next.Snapshot.Generation != 2 || next.Snapshot.Counts.Draining != 1 {
		t.Fatal(next)
	}
	unblock()
	select {
	case got := <-oldResult:
		if got.err != nil || got.first != "v1" || got.second != "v1" {
			t.Fatal(got)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("old request did not finish")
	}
	if frame().Snapshot.Counts.Draining != 1 {
		t.Fatal("old stream should retain the old provider after HTTP finishes")
	}
	if err = rpcStream.CloseSend(); err != nil {
		t.Fatal(err)
	}
	if _, err = rpcStream.Recv(); !errors.Is(err, io.EOF) {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err = f.Wait(ctx); err != nil {
		t.Fatal(err)
	}
	stopped := false
	for _, i := range frame().Snapshot.Instances {
		if i.TreeID == "service" && i.Version == "1" && i.State == "stopped" {
			stopped = true
		}
	}
	if !stopped {
		t.Fatal("old instance did not stop")
	}
	// The published gateway must let Forest.Close finish with SSE still open.
	if err = f.Close(ctx); err != nil {
		t.Fatal(err)
	}
}
