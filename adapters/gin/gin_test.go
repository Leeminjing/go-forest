package ginforest_test

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"strings"
	"testing"
	"time"

	ginforest "github.com/Leeminjing/go-forest/adapters/gin"
	"github.com/Leeminjing/go-forest/forest"
	"github.com/gin-gonic/gin"
)

type worker interface {
	Work(context.Context, bool) (string, error)
}
type work struct {
	label                      string
	entered, release, canceled chan struct{}
}

func (w *work) Work(ctx context.Context, hold bool) (string, error) {
	if hold {
		w.entered <- struct{}{}
		select {
		case <-w.release:
		case <-ctx.Done():
			w.canceled <- struct{}{}
			return "", ctx.Err()
		}
	}
	return w.label, nil
}
func business(version string, w *work, stopped chan struct{}) forest.Definition {
	return forest.Definition{TypeID: "test.business", Version: version, HotSwappable: true, Capabilities: []forest.CapabilityDescriptor{forest.Capability[worker]("work", forest.Public)}, Build: func(ctx context.Context, env *forest.BuildEnv) (forest.Lifecycle, error) {
		return forest.Hooks{OnStop: func(context.Context) error { close(stopped); return nil }}, forest.Provide(env, "work", w)
	}}
}
func wait[T any](t *testing.T, ch <-chan T) T {
	t.Helper()
	select {
	case value := <-ch:
		return value
	case <-time.After(3 * time.Second):
		t.Fatal("timed out")
		var zero T
		return zero
	}
}
func add(t *testing.T, f *forest.Forest, defs ...forest.Definition) {
	t.Helper()
	for _, d := range defs {
		if err := f.Register(d); err != nil {
			t.Fatal(err)
		}
	}
}
func apply(t *testing.T, f *forest.Forest, ops ...forest.Operation) {
	t.Helper()
	if _, err := f.Apply(context.Background(), forest.ChangePlan{BaseGeneration: f.Snapshot().Generation, Operations: ops}); err != nil {
		t.Fatal(err)
	}
}
func endpoint(t *testing.T, f *forest.Forest, id forest.TreeID) string {
	t.Helper()
	var url string
	if err := f.Invoke(context.Background(), func(ctx context.Context) error {
		return forest.Use[ginforest.Endpoint](ctx, f, id, "http", func(e ginforest.Endpoint) error { url = e.URL(); return e.ServeError() })
	}); err != nil {
		t.Fatal(err)
	}
	return url
}
func get(url string) (string, error) {
	c := http.Client{Timeout: 4 * time.Second}
	r, e := c.Get(url)
	if e != nil {
		return "", e
	}
	defer r.Body.Close()
	b, e := io.ReadAll(r.Body)
	return string(b), e
}

func TestNativeGinAndPinnedHotReplacement(t *testing.T) {
	gin.SetMode(gin.ReleaseMode)
	f := forest.New(forest.Options{})
	t.Cleanup(func() { _ = f.Close(context.Background()) })
	w := &work{label: "v1", entered: make(chan struct{}, 2), release: make(chan struct{}), canceled: make(chan struct{}, 2)}
	stopped := make(chan struct{})
	v1 := business("1", w, stopped)
	w2 := &work{label: "v2", entered: make(chan struct{}, 2), release: make(chan struct{}), canceled: make(chan struct{}, 2)}
	v2 := business("2", w2, make(chan struct{}))
	d, err := ginforest.Definition(f, ginforest.Options{TypeID: "test.gin", Version: "1", Ports: []forest.PortDescriptor{forest.Port[worker]("worker", forest.Required())}, BuildRouter: func(ctx context.Context, env *forest.BuildEnv) (*gin.Engine, error) {
		ref, err := forest.Reference[worker](env, "worker")
		if err != nil {
			return nil, err
		}
		r := gin.New()
		r.Use(ginforest.Scope(f))
		r.Use(func(c *gin.Context) { c.Header("X-Middleware", "native"); c.Next() })
		r.GET("/work/:name", func(c *gin.Context) {
			var a, b string
			err := ref.Use(c.Request.Context(), func(w worker) error {
				var e error
				a, e = w.Work(c.Request.Context(), c.Query("hold") == "1")
				return e
			})
			if err == nil {
				err = ref.Use(c.Request.Context(), func(w worker) error { var e error; b, e = w.Work(c.Request.Context(), false); return e })
			}
			if err != nil {
				c.Status(503)
				return
			}
			g, _ := f.Generation(c.Request.Context())
			c.String(200, "%s/%s@%d:%s", a, b, g, c.Param("name"))
		})
		r.POST("/json", func(c *gin.Context) {
			var payload struct {
				Name string `json:"name"`
			}
			if c.ShouldBindJSON(&payload) != nil {
				c.Status(400)
				return
			}
			c.JSON(201, payload)
		})
		return r, nil
	}})
	if err != nil {
		t.Fatal(err)
	}
	add(t, f, v1, v2, d)
	apply(t, f, forest.AddRoot("business", v1.Ref()), forest.AddRoot("app", d.Ref()), forest.Connect(forest.Link{ID: "worker", Consumer: "app", Port: "worker", Provider: "business", Capability: "work"}))
	url := endpoint(t, f, "app")
	old := make(chan string, 1)
	failure := make(chan error, 1)
	go func() { v, e := get(url + "/work/name?hold=1"); old <- v; failure <- e }()
	wait(t, w.entered)
	apply(t, f, forest.Replace("business", v2.Ref(), false))
	if value, err := get(url + "/work/new"); err != nil || value != "v2/v2@2:new" {
		t.Fatal(value, err)
	}
	select {
	case <-stopped:
		t.Fatal("stopped an active old request")
	default:
	}
	close(w.release)
	if value := wait(t, old); value != "v1/v1@1:name" {
		t.Fatal(value)
	}
	if err := wait(t, failure); err != nil {
		t.Fatal(err)
	}
	wait(t, stopped)
	r, err := http.Post(url+"/json", "application/json", strings.NewReader(`{"name":"Gin"}`))
	if err != nil {
		t.Fatal(err)
	}
	defer r.Body.Close()
	var payload map[string]string
	_ = json.NewDecoder(r.Body).Decode(&payload)
	if r.StatusCode != 201 || payload["name"] != "Gin" || r.Header.Get("X-Middleware") != "native" {
		t.Fatal(r.Status, payload)
	}
	ctx, cancel := context.WithCancel(context.Background())
	request, _ := http.NewRequestWithContext(ctx, "GET", url+"/work/cancel?hold=1", nil)
	done := make(chan error, 1)
	go func() {
		r, e := http.DefaultClient.Do(request)
		if r != nil {
			r.Body.Close()
		}
		done <- e
	}()
	wait(t, w2.entered)
	cancel()
	wait(t, w2.canceled)
	if wait(t, done) == nil {
		t.Fatal("cancellation lost")
	}
}

func TestPreparationRollbackReleasesListeningServer(t *testing.T) {
	busy, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer busy.Close()
	free, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	address := free.Addr().String()
	free.Close()
	f := forest.New(forest.Options{})
	t.Cleanup(func() { _ = f.Close(context.Background()) })
	var defs []forest.Definition
	for n, addr := range []string{address, busy.Addr().String()} {
		d, err := ginforest.Definition(f, ginforest.Options{TypeID: fmt.Sprintf("server.%d", n), Version: "1", Address: addr, BuildRouter: func(context.Context, *forest.BuildEnv) (*gin.Engine, error) { return gin.New(), nil }})
		if err != nil {
			t.Fatal(err)
		}
		defs = append(defs, d)
	}
	add(t, f, defs...)
	_, err = f.Apply(context.Background(), forest.ChangePlan{Operations: []forest.Operation{forest.AddRoot("a", defs[0].Ref()), forest.AddRoot("b", defs[1].Ref())}})
	if err == nil || f.Snapshot().Generation != 0 {
		t.Fatal("published failed preparation", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err = f.Wait(ctx); err != nil {
		t.Fatal(err)
	}
	listener, err := net.Listen("tcp", address)
	if err != nil {
		t.Fatal("listener leaked", err)
	}
	listener.Close()
}

func TestScopeRejectsClosedForestBeforeHandler(t *testing.T) {
	f := forest.New(forest.Options{})
	_ = f.Close(context.Background())
	r := gin.New()
	r.Use(ginforest.Scope(f))
	r.GET("/", func(c *gin.Context) { t.Error("entered after Close") })
	s := &http.Server{Handler: r}
	listener, _ := net.Listen("tcp", "127.0.0.1:0")
	go s.Serve(listener)
	defer s.Close()
	resp, err := http.Get("http://" + listener.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != 503 {
		t.Fatal(resp.Status)
	}
}
