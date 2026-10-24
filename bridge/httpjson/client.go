package httpjson

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/json"
	"errors"
	"mime"
	"net"
	"net/http"
	"net/url"
	"reflect"
	"strings"
	"sync"
	"time"
)

type ClientOptions struct {
	URL, Token    string
	Routes        []Descriptor
	Timeout       time.Duration
	MaxBodyBytes  int64
	TLSConfig     *tls.Config
	AllowInsecure bool
}
type Client struct {
	url, token      string
	routes          map[string]Descriptor
	timeout         time.Duration
	limit           int64
	http            *http.Client
	transport       *http.Transport
	ctx             context.Context
	cancel          context.CancelFunc
	mu              sync.Mutex
	started, closed bool
	active          sync.WaitGroup
}

func NewClient(options ClientOptions) (*Client, error) {
	u, err := url.Parse(options.URL)
	if err != nil || u.Host == "" || u.Hostname() == "" || (u.Scheme != "http" && u.Scheme != "https") || u.User != nil || u.RawQuery != "" || u.ForceQuery || u.Fragment != "" || (u.Path != "" && u.Path != "/") || !validToken(options.Token) || len(options.Routes) == 0 {
		return nil, ErrInvalidConfig
	}
	if u.Scheme == "http" && !loopback(u.Hostname()) && !options.AllowInsecure {
		return nil, ErrInvalidConfig
	}
	tlsConfig := &tls.Config{MinVersion: tls.VersionTLS12}
	if options.TLSConfig != nil {
		tlsConfig = options.TLSConfig.Clone()
		if tlsConfig.MinVersion < tls.VersionTLS12 {
			tlsConfig.MinVersion = tls.VersionTLS12
		}
	}
	if tlsConfig.InsecureSkipVerify && !options.AllowInsecure {
		return nil, ErrInvalidConfig
	}
	limit, err := bodyLimit(options.MaxBodyBytes)
	if err != nil {
		return nil, err
	}
	if options.Timeout == 0 {
		options.Timeout = 5 * time.Second
	}
	if options.Timeout < time.Millisecond {
		return nil, ErrInvalidConfig
	}
	routes := map[string]Descriptor{}
	for _, d := range options.Routes {
		if !validDescriptor(d) {
			return nil, ErrInvalidConfig
		}
		if _, ok := routes[d.Name]; ok {
			return nil, ErrInvalidConfig
		}
		routes[d.Name] = d
	}
	tlsConfig.NextProtos = []string{"http/1.1"}
	transport := &http.Transport{DialContext: (&net.Dialer{Timeout: 5 * time.Second, KeepAlive: 30 * time.Second}).DialContext, TLSClientConfig: tlsConfig, TLSNextProto: map[string]func(string, *tls.Conn) http.RoundTripper{}, MaxIdleConns: 100, IdleConnTimeout: 90 * time.Second, TLSHandshakeTimeout: 5 * time.Second}
	ctx, cancel := context.WithCancel(context.Background())
	return &Client{url: strings.TrimSuffix(options.URL, "/"), token: options.Token, routes: routes, timeout: options.Timeout, limit: limit, transport: transport, http: &http.Client{Transport: transport, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}, ctx: ctx, cancel: cancel}, nil
}

func (c *Client) Start(ctx context.Context) error {
	if err := c.enter(false); err != nil {
		return err
	}
	defer c.active.Done()
	// Configuration order is irrelevant to routing; each advertised contract is
	// checked before the proxy can be published by its owning Forest.
	for _, d := range c.routes {
		if _, err := c.perform(ctx, describePath, d, nil); err != nil {
			return err
		}
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return ErrClosed
	}
	c.started = true
	return nil
}
func (c *Client) Stop(ctx context.Context) error {
	c.mu.Lock()
	c.closed = true
	c.cancel()
	c.mu.Unlock()
	c.transport.CloseIdleConnections()
	done := make(chan struct{})
	go func() { c.active.Wait(); close(done) }()
	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
func (c *Client) enter(requireStart bool) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return ErrClosed
	}
	if requireStart && !c.started {
		return ErrUnavailable
	}
	c.active.Add(1)
	return nil
}
func (c *Client) Call(ctx context.Context, route string, input, output any) (CallInfo, error) {
	if err := c.enter(true); err != nil {
		return CallInfo{}, failure("call", route, err)
	}
	defer c.active.Done()
	d, ok := c.routes[route]
	if !ok {
		return CallInfo{}, failure("call", route, ErrInvalidRequest)
	}
	if output != nil {
		v := reflect.ValueOf(output)
		if v.Kind() != reflect.Pointer || v.IsNil() {
			return CallInfo{}, failure("call", route, ErrInvalidRequest)
		}
	}
	payload, err := json.Marshal(input)
	if err != nil {
		return CallInfo{}, failure("encode", route, ErrInvalidRequest)
	}
	result, err := c.perform(ctx, callPath, d, payload)
	if err != nil {
		return CallInfo{}, err
	}
	info := CallInfo{RemoteGeneration: result.Generation}
	if output != nil {
		if err := decode(result.Result, output); err != nil {
			return info, failure("decode", route, ErrProtocolMismatch)
		}
	}
	return info, nil
}

func (c *Client) perform(parent context.Context, path string, d Descriptor, payload json.RawMessage) (response, error) {
	ctx, cancel := context.WithTimeout(parent, c.timeout)
	defer cancel()
	stop := context.AfterFunc(c.ctx, cancel)
	defer stop()
	deadline, _ := ctx.Deadline()
	budget := time.Until(deadline)
	if ctx.Err() != nil {
		return response{}, failure("request", d.Name, requestError(ctx, ctx.Err()))
	}
	if budget <= 0 {
		return response{}, failure("request", d.Name, errors.Join(ErrTimeout, context.DeadlineExceeded))
	}
	data, err := json.Marshal(request{Protocol: ProtocolVersion, Route: d.Name, Contract: d.Contract, TimeoutMillis: milliseconds(budget), Payload: payload})
	if err != nil {
		return response{}, failure("encode", d.Name, ErrInvalidRequest)
	}
	if int64(len(data)) > c.limit {
		return response{}, failure("encode", d.Name, ErrMessageTooLarge)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.url+path, bytes.NewReader(data))
	if err != nil {
		return response{}, failure("request", d.Name, ErrInvalidConfig)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+c.token)
	// No retry loop, redirects, or idempotency headers. POST has a nonempty body.
	res, err := c.http.Do(req)
	if err != nil {
		return response{}, failure("transport", d.Name, requestError(ctx, err))
	}
	defer res.Body.Close()
	media, _, mediaErr := mime.ParseMediaType(res.Header.Get("Content-Type"))
	if mediaErr != nil || media != "application/json" {
		return response{}, failure("response", d.Name, ErrProtocolMismatch)
	}
	data, err = readBody(res.Body, c.limit)
	if err != nil {
		return response{}, failure("response", d.Name, requestError(ctx, err))
	}
	var result response
	if err := decode(data, &result); err != nil || result.Protocol != ProtocolVersion {
		return response{}, failure("response", d.Name, ErrProtocolMismatch)
	}
	if result.Error != nil {
		f, known := faultCode(result.Error.Code)
		if !known || res.StatusCode != f.status || len(result.Result) != 0 {
			return response{}, failure("response", d.Name, ErrProtocolMismatch)
		}
		return response{}, failure("remote", d.Name, faultCause(f))
	}
	if res.StatusCode != http.StatusOK || result.Route != d.Name || result.Contract != d.Contract {
		return response{}, failure("response", d.Name, ErrContractMismatch)
	}
	if path == callPath && len(result.Result) == 0 {
		return response{}, failure("response", d.Name, ErrProtocolMismatch)
	}
	return result, nil
}
func requestError(ctx context.Context, err error) error {
	if ctx.Err() != nil {
		err = ctx.Err()
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return errors.Join(ErrTimeout, context.DeadlineExceeded)
	}
	if errors.Is(err, context.Canceled) {
		return errors.Join(ErrCanceled, context.Canceled)
	}
	if errors.Is(err, ErrMessageTooLarge) {
		return err
	}
	return errors.Join(ErrUnavailable, err)
}
