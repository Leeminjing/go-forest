package observatory

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	ginforest "github.com/Leeminjing/go-forest/adapters/gin"
	"github.com/Leeminjing/go-forest/extensions/observe"
	"github.com/Leeminjing/go-forest/forest"
	"github.com/gin-gonic/gin"
)

type largeReader struct{ frame observe.Frame }

func (r largeReader) Read(context.Context, *observe.Cursor) (observe.Frame, error) {
	return r.frame, nil
}

type writeSignal struct {
	gin.ResponseWriter
	once    sync.Once
	started chan struct{}
}

func (w *writeSignal) Write(b []byte) (int, error) {
	w.once.Do(func() { close(w.started) })
	return w.ResponseWriter.Write(b)
}
func (w *writeSignal) Unwrap() http.ResponseWriter { return w.ResponseWriter }

func TestSlowSocketWriteTimeoutAndStopCancellation(t *testing.T) {
	for _, cancelStop := range []bool{false, true} {
		t.Run(fmt.Sprint(cancelStop), func(t *testing.T) {
			f := forest.New(forest.Options{})
			t.Cleanup(func() {
				ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
				defer cancel()
				_ = f.Close(ctx)
			})
			boot := strings.Repeat("a", 32)
			payload := largeReader{observe.Frame{SchemaVersion: 1, BootID: boot, Cursor: boot + ":0", Events: []observe.Change{{Trees: []string{strings.Repeat("x", 8<<20)}}}}}
			provider := forest.Definition{TypeID: "large.reader", Version: "1", Capabilities: []forest.CapabilityDescriptor{forest.Capability[reader]("read", forest.Public)}, Build: func(ctx context.Context, env *forest.BuildEnv) (forest.Lifecycle, error) {
				return forest.Hooks{}, forest.Provide(env, "read", payload)
			}}
			started, finished := make(chan struct{}), make(chan struct{})
			writeTimeout := 100 * time.Millisecond
			if cancelStop {
				writeTimeout = 30 * time.Second
			}
			root, err := ginforest.Definition(f, ginforest.Options{TypeID: "slow.dashboard", Version: "1", Ports: []forest.PortDescriptor{forest.Port[reader]("reader", forest.Required())}, BuildRouter: func(ctx context.Context, env *forest.BuildEnv) (*gin.Engine, error) {
				ref, err := forest.Reference[reader](env, "reader")
				if err != nil {
					return nil, err
				}
				h := gateway{forest: f, ref: ref, options: Options{SampleInterval: time.Second, WriteTimeout: writeTimeout}}
				r := gin.New()
				r.GET("/", func(c *gin.Context) {
					c.Writer = &writeSignal{ResponseWriter: c.Writer, started: started}
					defer close(finished)
					h.stream(c)
				})
				return r, nil
			}})
			if err != nil {
				t.Fatal(err)
			}
			for _, d := range []forest.Definition{root, provider} {
				if err = f.Register(d); err != nil {
					t.Fatal(err)
				}
			}
			_, err = f.Apply(context.Background(), forest.ChangePlan{Operations: []forest.Operation{forest.AddRoot("dashboard", root.Ref()), forest.AddRoot("provider", provider.Ref()), forest.Connect(forest.Link{ID: "reader", Consumer: "dashboard", Port: "reader", Provider: "provider", Capability: "read"})}})
			if err != nil {
				t.Fatal(err)
			}
			var address string
			err = f.Invoke(context.Background(), func(ctx context.Context) error {
				return forest.Use[ginforest.Endpoint](ctx, f, "dashboard", "http", func(e ginforest.Endpoint) error { address = strings.TrimPrefix(e.URL(), "http://"); return nil })
			})
			if err != nil {
				t.Fatal(err)
			}
			conn, err := net.Dial("tcp", address)
			if err != nil {
				t.Fatal(err)
			}
			defer conn.Close()
			if tcp, ok := conn.(*net.TCPConn); ok {
				_ = tcp.SetReadBuffer(512)
			}
			_, err = fmt.Fprintf(conn, "GET / HTTP/1.1\r\nHost: %s\r\n\r\n", address)
			if err != nil {
				t.Fatal(err)
			}
			select {
			case <-started:
			case <-time.After(3 * time.Second):
				t.Fatal("write did not start")
			}
			if cancelStop {
				ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
				defer cancel()
				if err = f.Close(ctx); err != nil {
					t.Fatal("Stop waited for blocked network write", err)
				}
			}
			select {
			case <-finished:
			case <-time.After(3 * time.Second):
				t.Fatal("slow client kept handler alive")
			}
		})
	}
}
