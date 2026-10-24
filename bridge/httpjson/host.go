package httpjson

import (
	"context"
	"crypto/subtle"
	"crypto/tls"
	"encoding/json"
	"errors"
	"mime"
	"net"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/Leeminjing/go-forest/forest"
)

type HostOptions struct {
	Address         string
	Routes          []Route
	Grants          []Grant
	MaxCallDuration time.Duration
	MaxBodyBytes    int64
	TLSConfig       *tls.Config
	AllowInsecure   bool
}
type permission struct {
	token  string
	routes map[string]bool
}
type Host struct {
	forest   *forest.Forest
	address  string
	routes   map[string]Route
	grants   []permission
	timeout  time.Duration
	limit    int64
	tls      *tls.Config
	mu       sync.Mutex
	url      string
	server   *http.Server
	done     chan struct{}
	serveErr error
	closed   bool
}

func NewHost(f *forest.Forest, options HostOptions) (*Host, error) {
	if f == nil || len(options.Routes) == 0 || len(options.Grants) == 0 {
		return nil, ErrInvalidConfig
	}
	if options.Address == "" {
		options.Address = "127.0.0.1:0"
	}
	host, _, err := net.SplitHostPort(options.Address)
	if err != nil {
		return nil, ErrInvalidConfig
	}
	if options.TLSConfig == nil && !loopback(host) && !options.AllowInsecure {
		return nil, ErrInvalidConfig
	}
	if options.MaxCallDuration == 0 {
		options.MaxCallDuration = 30 * time.Second
	}
	if options.MaxCallDuration < time.Millisecond || options.MaxCallDuration > time.Duration(1<<63-1)-5*time.Second {
		return nil, ErrInvalidConfig
	}
	limit, err := bodyLimit(options.MaxBodyBytes)
	if err != nil {
		return nil, err
	}
	h := &Host{forest: f, address: options.Address, routes: map[string]Route{}, timeout: options.MaxCallDuration, limit: limit}
	if options.TLSConfig != nil {
		h.tls = transportTLS(options.TLSConfig)
		if len(h.tls.Certificates) == 0 && h.tls.GetCertificate == nil && h.tls.GetConfigForClient == nil {
			return nil, ErrInvalidConfig
		}
	}
	for _, r := range options.Routes {
		if !validDescriptor(r.Descriptor) || r.Handle == nil {
			return nil, ErrInvalidConfig
		}
		if _, exists := h.routes[r.Name]; exists {
			return nil, ErrInvalidConfig
		}
		h.routes[r.Name] = r
	}
	seen := map[string]bool{}
	for _, grant := range options.Grants {
		if !validToken(grant.Token) || seen[grant.Token] || len(grant.Routes) == 0 {
			return nil, ErrInvalidConfig
		}
		seen[grant.Token] = true
		p := permission{token: grant.Token, routes: map[string]bool{}}
		for _, name := range grant.Routes {
			if _, exists := h.routes[name]; !exists {
				return nil, ErrInvalidConfig
			}
			p.routes[name] = true
		}
		h.grants = append(h.grants, p)
	}
	return h, nil
}

func (h *Host) URL() string { h.mu.Lock(); defer h.mu.Unlock(); return h.url }
func (h *Host) Start(ctx context.Context) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.closed {
		return ErrClosed
	}
	if h.server != nil {
		return ErrInvalidConfig
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	listener, err := net.Listen("tcp", h.address)
	if err != nil {
		return failure("listen", "", errors.Join(ErrUnavailable, err))
	}
	scheme := "http"
	if h.tls != nil {
		listener = tls.NewListener(listener, h.tls)
		scheme = "https"
	}
	h.url = scheme + "://" + listener.Addr().String()
	h.done = make(chan struct{})
	h.server = &http.Server{Handler: h, ReadHeaderTimeout: 5 * time.Second, ReadTimeout: 5 * time.Second, WriteTimeout: h.timeout + 5*time.Second, MaxHeaderBytes: 16 << 10}
	go func() { err := h.server.Serve(listener); h.mu.Lock(); h.serveErr = err; h.mu.Unlock(); close(h.done) }()
	return nil
}
func (h *Host) Stop(ctx context.Context) error {
	h.mu.Lock()
	h.closed = true
	server, done := h.server, h.done
	h.mu.Unlock()
	if server == nil {
		return nil
	}
	if err := server.Shutdown(ctx); err != nil {
		return failure("shutdown", "", err)
	}
	select {
	case <-done:
	case <-ctx.Done():
		return failure("shutdown", "", ctx.Err())
	}
	h.mu.Lock()
	err := h.serveErr
	h.mu.Unlock()
	if err != nil && !errors.Is(err, http.ErrServerClosed) {
		return failure("serve", "", err)
	}
	return nil
}
func (h *Host) authorized(token, route string) bool {
	for _, p := range h.grants {
		if subtle.ConstantTimeCompare([]byte(token), []byte(p.token)) == 1 && (route == "" || p.routes[route]) {
			return true
		}
	}
	return false
}

func (h *Host) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	result := response{Protocol: ProtocolVersion}
	token, hasToken := strings.CutPrefix(r.Header.Get("Authorization"), "Bearer ")
	if !hasToken || !h.authorized(token, "") {
		h.reply(w, result, ErrUnauthorized)
		return
	}
	if r.Method != http.MethodPost || (r.URL.Path != describePath && r.URL.Path != callPath) {
		h.reply(w, result, ErrInvalidRequest)
		return
	}
	media, _, err := mime.ParseMediaType(r.Header.Get("Content-Type"))
	if err != nil || media != "application/json" {
		h.reply(w, result, ErrInvalidRequest)
		return
	}
	body, err := readBody(r.Body, h.limit)
	if err != nil {
		if !errors.Is(err, ErrMessageTooLarge) {
			err = ErrInvalidRequest
		}
		h.reply(w, result, err)
		return
	}
	var input request
	if err := decode(body, &input); err != nil {
		h.reply(w, result, ErrInvalidRequest)
		return
	}
	if !h.authorized(token, input.Route) {
		h.reply(w, result, ErrUnauthorized)
		return
	}
	route, exists := h.routes[input.Route]
	if !exists {
		h.reply(w, result, ErrUnauthorized)
		return
	}
	result.Route, result.Contract = route.Name, route.Contract
	if input.Protocol != ProtocolVersion {
		h.reply(w, result, ErrProtocolMismatch)
		return
	}
	if input.Contract != route.Contract {
		h.reply(w, result, ErrContractMismatch)
		return
	}
	if input.TimeoutMillis <= 0 {
		h.reply(w, result, ErrInvalidRequest)
		return
	}
	// Compare integer budgets before conversion, avoiding duration overflow on
	// untrusted values. Network latency never requires synchronized clocks.
	budget := h.timeout
	if input.TimeoutMillis < milliseconds(h.timeout) {
		budget = time.Duration(input.TimeoutMillis) * time.Millisecond
	}
	ctx, cancel := context.WithTimeout(r.Context(), budget)
	defer cancel()
	err = h.forest.Invoke(ctx, func(callCtx context.Context) error {
		result.Generation, _ = h.forest.Generation(callCtx)
		if r.URL.Path == describePath {
			return nil
		}
		var err error
		result.Result, err = invoke(route.Handle, callCtx, input.Payload)
		// Detach result bytes and classify provider errors while the provider's
		// Scope is still pinned, before a replacement can release its resources.
		result.Result = append(json.RawMessage(nil), result.Result...)
		if err == nil && callCtx.Err() != nil {
			err = callCtx.Err()
		}
		if errors.Is(err, forest.ErrBindingUnavailable) || errors.Is(err, forest.ErrNotFound) || errors.Is(err, forest.ErrClosed) {
			return ErrUnavailable
		}
		if err != nil {
			return faultCause(faultFor(err))
		}
		return err
	})
	if errors.Is(err, forest.ErrClosed) {
		err = ErrUnavailable
	}
	h.reply(w, result, err)
}

func invoke(fn func(context.Context, json.RawMessage) (json.RawMessage, error), ctx context.Context, payload json.RawMessage) (result json.RawMessage, err error) {
	defer func() {
		if recover() != nil {
			result = nil
			err = ErrRemoteFailure
		}
	}()
	return fn(ctx, payload)
}
func (h *Host) reply(w http.ResponseWriter, result response, err error) {
	status := http.StatusOK
	if err != nil {
		f := faultFor(err)
		status = f.status
		result.Result = nil
		result.Route, result.Contract = "", Contract{}
		result.Error = &wireError{Code: f.code, Message: f.cause.Error()}
	}
	data, marshalErr := json.Marshal(result)
	if marshalErr != nil || int64(len(data)) > h.limit {
		f := faultFor(ErrMessageTooLarge)
		if marshalErr != nil {
			f = faultFor(ErrRemoteFailure)
		}
		status = f.status
		result.Result = nil
		result.Error = &wireError{Code: f.code, Message: f.cause.Error()}
		result.Route, result.Contract = "", Contract{}
		data, _ = json.Marshal(result)
	}
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "no-store")
	w.WriteHeader(status)
	_, _ = w.Write(data)
}
