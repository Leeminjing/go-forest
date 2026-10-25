// Package observatory builds a read-only, self-observing Forest dashboard.
package observatory

import (
	"context"
	"crypto/subtle"
	"embed"
	"encoding/json"
	"errors"
	"net"
	"net/http"
	"net/url"
	"strings"
	"time"

	ginforest "github.com/Leeminjing/go-forest/adapters/gin"
	"github.com/Leeminjing/go-forest/extensions/observe"
	"github.com/Leeminjing/go-forest/forest"
	"github.com/Leeminjing/go-forest/forest/compose"
	"github.com/gin-gonic/gin"
)

//go:embed web/*
var assets embed.FS

var ErrInvalidOptions = errors.New("invalid Observatory options")

type Options struct {
	TypeID         string
	Address        string
	Token          string
	SampleInterval time.Duration
	WriteTimeout   time.Duration
}

// Bundle contains ordinary definitions and a pure composition template.
// Register Definitions once, then instantiate Blueprint at different roots.
// To use different configurations in one Forest, give each bundle a TypeID.
type Bundle struct {
	Definitions []forest.Definition
	Blueprint   *compose.Blueprint
}

// reader is owned by the consumer; Inspector satisfies it structurally.
type reader interface {
	Read(context.Context, *observe.Cursor) (observe.Frame, error)
}

func New(f *forest.Forest, options Options) (*Bundle, error) {
	if f == nil || len(options.Token) < 16 || len(options.Token) > 256 || strings.ContainsAny(options.Token, "\r\n\t ") {
		return nil, ErrInvalidOptions
	}
	if options.TypeID == "" {
		options.TypeID = "forest.observatory.dashboard"
	}
	if options.Address == "" {
		options.Address = "127.0.0.1:0"
	}
	host, _, err := net.SplitHostPort(options.Address)
	if err != nil {
		return nil, ErrInvalidOptions
	}
	ip := net.ParseIP(host)
	if ip == nil || !ip.IsLoopback() {
		return nil, ErrInvalidOptions
	}
	if options.SampleInterval == 0 {
		options.SampleInterval = time.Second
	}
	if options.WriteTimeout == 0 {
		options.WriteTimeout = 5 * time.Second
	}
	if options.SampleInterval < time.Millisecond || options.WriteTimeout < time.Millisecond {
		return nil, ErrInvalidOptions
	}
	inspector := observe.Definition(f)
	inspector.TypeID = options.TypeID + ".inspector"
	dashboard, err := ginforest.Definition(f, ginforest.Options{TypeID: options.TypeID, Version: "1", Address: options.Address,
		Ports: []forest.PortDescriptor{forest.Port[reader]("reader", forest.Required())},
		BuildRouter: func(ctx context.Context, env *forest.BuildEnv) (*gin.Engine, error) {
			ref, err := forest.Reference[reader](env, "reader")
			if err != nil {
				return nil, err
			}
			h := &gateway{forest: f, ref: ref, options: options}
			r := gin.New()
			r.Use(func(c *gin.Context) {
				c.Header("X-Content-Type-Options", "nosniff")
				c.Header("Referrer-Policy", "no-referrer")
				c.Header("Content-Security-Policy", "default-src 'self'; script-src 'self'; style-src 'self'; connect-src 'self'; object-src 'none'; base-uri 'none'; frame-ancestors 'none'")
				c.Next()
			})
			for path, file := range map[string]string{"/": "index.html", "/app.js": "app.js", "/style.css": "style.css"} {
				data, err := assets.ReadFile("web/" + file)
				if err != nil {
					return nil, err
				}
				contentType := "text/html; charset=utf-8"
				if strings.HasSuffix(file, ".js") {
					contentType = "text/javascript; charset=utf-8"
				}
				if strings.HasSuffix(file, ".css") {
					contentType = "text/css; charset=utf-8"
				}
				r.GET(path, func(c *gin.Context) { c.Data(http.StatusOK, contentType, data) })
			}
			api := r.Group("/api/v1", h.authorize)
			api.GET("/state", h.state)
			api.GET("/events", h.stream)
			return r, nil
		},
	})
	if err != nil {
		return nil, err
	}
	blueprint, err := compose.New(compose.Spec{Root: dashboard.Ref(), Trees: []compose.Tree{{ID: "inspector", Definition: inspector.Ref()}}, Bindings: []forest.Link{{ID: "reader", Port: "reader", Provider: "inspector", Capability: "read"}}})
	if err != nil {
		return nil, err
	}
	return &Bundle{Definitions: []forest.Definition{dashboard, inspector}, Blueprint: blueprint}, nil
}

type gateway struct {
	forest  *forest.Forest
	ref     forest.Ref[reader]
	options Options
}

func (h *gateway) authorize(c *gin.Context) {
	c.Header("Cache-Control", "no-store")
	token, ok := strings.CutPrefix(c.GetHeader("Authorization"), "Bearer ")
	if !ok || subtle.ConstantTimeCompare([]byte(token), []byte(h.options.Token)) != 1 {
		c.AbortWithStatusJSON(401, gin.H{"error": "unauthorized"})
		return
	}
	if origin := c.GetHeader("Origin"); origin != "" {
		u, err := url.Parse(origin)
		if err != nil || u.Scheme != "http" || u.Host != c.Request.Host || u.Path != "" || u.RawQuery != "" || u.Fragment != "" || u.User != nil {
			c.AbortWithStatusJSON(403, gin.H{"error": "origin_denied"})
			return
		}
	}
	c.Next()
}

func (h *gateway) read(ctx context.Context, after *observe.Cursor) (observe.Frame, error) {
	var frame observe.Frame
	err := h.forest.Invoke(ctx, func(callCtx context.Context) error {
		return h.ref.Use(callCtx, func(r reader) error { var err error; frame, err = r.Read(callCtx, after); return err })
	})
	return frame, err
}

func cursor(value string) (*observe.Cursor, error) {
	if value == "" {
		return nil, nil
	}
	return observe.ParseCursor(value)
}
func replyError(c *gin.Context, err error) {
	if errors.Is(err, observe.ErrInvalidCursor) {
		c.JSON(400, gin.H{"error": "invalid_cursor"})
	} else {
		c.JSON(503, gin.H{"error": "unavailable"})
	}
}
func (h *gateway) state(c *gin.Context) {
	after, err := cursor(c.Query("cursor"))
	if err != nil {
		replyError(c, err)
		return
	}
	frame, err := h.read(c.Request.Context(), after)
	if err != nil {
		replyError(c, err)
		return
	}
	c.JSON(200, frame)
}

// stream holds no Forest Scope while waiting or writing. Closing a connection
// must never keep the complete historical graph alive.
func (h *gateway) stream(c *gin.Context) {
	after, err := cursor(c.GetHeader("Last-Event-ID"))
	if err != nil {
		replyError(c, err)
		return
	}
	frame, err := h.read(c.Request.Context(), after)
	if err != nil {
		replyError(c, err)
		return
	}
	rc := http.NewResponseController(c.Writer)
	if err = rc.SetWriteDeadline(time.Now().Add(h.options.WriteTimeout)); err != nil {
		c.JSON(503, gin.H{"error": "stream_unsupported"})
		return
	}
	ctx := c.Request.Context()
	deadlineDone := make(chan struct{})
	stop := context.AfterFunc(ctx, func() { _ = rc.SetWriteDeadline(time.Now()); close(deadlineDone) })
	defer func() {
		if !stop() {
			<-deadlineDone
		}
	}()
	c.Header("Content-Type", "text/event-stream")
	c.Header("X-Accel-Buffering", "no")
	send := func(event, id string, value any) error {
		data, err := json.Marshal(value)
		if err != nil {
			return err
		}
		if err = rc.SetWriteDeadline(time.Now().Add(h.options.WriteTimeout)); err != nil {
			return err
		}
		if err = ctx.Err(); err != nil {
			return err
		}
		var body strings.Builder
		if id != "" {
			body.WriteString("id: " + id + "\n")
		}
		body.WriteString("event: " + event + "\ndata: ")
		body.Write(data)
		body.WriteString("\n\n")
		if _, err = c.Writer.Write([]byte(body.String())); err != nil {
			return err
		}
		return rc.Flush()
	}
	emit := func(initial bool) error {
		if frame.ResetReason != "" {
			return send("reset", frame.Cursor, frame)
		}
		if initial && after == nil {
			return send("sync", frame.Cursor, frame)
		}
		for _, change := range frame.Events {
			id := (observe.Cursor{BootID: frame.BootID, Sequence: change.Sequence}).String()
			if err := send("change", id, change); err != nil {
				return err
			}
		}
		frame.Events = []observe.Change{}
		return send("snapshot", "", frame)
	}
	if err = emit(true); err != nil {
		return
	}
	after, _ = observe.ParseCursor(frame.Cursor)
	tick := time.NewTicker(h.options.SampleInterval)
	defer tick.Stop()
	heartbeat := time.NewTicker(15 * time.Second)
	defer heartbeat.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-heartbeat.C:
			if err = rc.SetWriteDeadline(time.Now().Add(h.options.WriteTimeout)); err != nil {
				return
			}
			if ctx.Err() != nil {
				return
			}
			if _, err = c.Writer.Write([]byte(": heartbeat\n\n")); err != nil {
				return
			}
			if rc.Flush() != nil {
				return
			}
		case <-tick.C:
			frame, err = h.read(ctx, after)
			if err != nil {
				return
			}
			if err = emit(false); err != nil {
				return
			}
			after, _ = observe.ParseCursor(frame.Cursor)
		}
	}
}
