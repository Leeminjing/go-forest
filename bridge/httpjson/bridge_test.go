package httpjson_test

import (
	"bytes"
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"errors"
	"io"
	"math/big"
	"net"
	"net/http"
	"net/url"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	bridge "github.com/Leeminjing/go-forest/bridge/httpjson"
	"github.com/Leeminjing/go-forest/forest"
)

var contract = bridge.Contract{ID: "test.read", Version: 1}

func descriptor() bridge.Descriptor { return bridge.Descriptor{Name: "read", Contract: contract} }
func receive[T any](t *testing.T, ch <-chan T) T {
	t.Helper()
	select {
	case v := <-ch:
		return v
	case <-time.After(3 * time.Second):
		t.Fatal("network test synchronization timed out")
		var zero T
		return zero
	}
}
func host(t *testing.T, options bridge.HostOptions) *bridge.Host {
	t.Helper()
	f := forest.New(forest.Options{})
	h, err := bridge.NewHost(f, options)
	if err != nil {
		t.Fatal(err)
	}
	if err := h.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		if err := h.Stop(ctx); err != nil {
			t.Error(err)
		}
		if err := f.Close(ctx); err != nil {
			t.Error(err)
		}
	})
	return h
}
func client(t *testing.T, h *bridge.Host, options bridge.ClientOptions) *bridge.Client {
	t.Helper()
	options.URL = h.URL()
	if options.Token == "" {
		options.Token = "reader"
	}
	if len(options.Routes) == 0 {
		options.Routes = []bridge.Descriptor{descriptor()}
	}
	c, err := bridge.NewClient(options)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		if err := c.Stop(ctx); err != nil {
			t.Error(err)
		}
	})
	return c
}
func options(route bridge.Route) bridge.HostOptions {
	return bridge.HostOptions{Routes: []bridge.Route{route}, Grants: []bridge.Grant{{Token: "reader", Routes: []string{"read"}}}}
}
func raw(t *testing.T, h *bridge.Host, token string, data []byte) (int, []byte) {
	t.Helper()
	req, err := http.NewRequest(http.MethodPost, h.URL()+"/bridge/v1/call", bytes.NewReader(data))
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Authorization", "Bearer "+token)
	req.Header.Set("Content-Type", "application/json")
	res, err := (&http.Client{Timeout: time.Second}).Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer res.Body.Close()
	body, err := io.ReadAll(res.Body)
	if err != nil {
		t.Fatal(err)
	}
	return res.StatusCode, body
}
func message(payload any) []byte {
	data, _ := json.Marshal(map[string]any{"protocol": 1, "route": "read", "contract": contract, "timeout_ms": 1000, "payload": payload})
	return data
}

func TestExplicitContractsAuthorizationAndConfigurationSnapshots(t *testing.T) {
	var calls atomic.Int32
	route := bridge.Handle("read", contract, func(context.Context, struct{}) (string, error) { calls.Add(1); return "ok", nil })
	config := options(route)
	config.Routes = append(config.Routes, bridge.Handle("admin", contract, func(context.Context, struct{}) (string, error) { calls.Add(1); return "admin", nil }))
	h := host(t, config)
	config.Grants[0].Routes[0] = "admin"
	config.Routes[0].Contract.Version = 99
	for _, tc := range []struct {
		name, token string
		d           bridge.Descriptor
		want        error
	}{
		{"bad-token", "wrong", descriptor(), bridge.ErrUnauthorized},
		{"wrong-contract", "reader", bridge.Descriptor{Name: "read", Contract: bridge.Contract{ID: "different", Version: 1}}, bridge.ErrContractMismatch},
		{"wrong-version", "reader", bridge.Descriptor{Name: "read", Contract: bridge.Contract{ID: contract.ID, Version: 2}}, bridge.ErrContractMismatch},
		{"denied-route", "reader", bridge.Descriptor{Name: "admin", Contract: contract}, bridge.ErrUnauthorized},
		{"unexposed-route", "reader", bridge.Descriptor{Name: "hidden", Contract: contract}, bridge.ErrUnauthorized},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c := client(t, h, bridge.ClientOptions{Token: tc.token, Routes: []bridge.Descriptor{tc.d}})
			if err := c.Start(context.Background()); !errors.Is(err, tc.want) {
				t.Fatal(err)
			}
		})
	}
	if calls.Load() != 0 {
		t.Fatal("startup/unauthorized request executed a capability")
	}
	ds := []bridge.Descriptor{descriptor()}
	c := client(t, h, bridge.ClientOptions{Routes: ds})
	ds[0].Contract.Version = 77
	if err := c.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	var result string
	if info, err := c.Call(context.Background(), "read", struct{}{}, &result); err != nil || result != "ok" || info.RemoteGeneration != 0 {
		t.Fatal(result, info, err)
	}
	if _, err := c.Call(context.Background(), "admin", struct{}{}, &result); !errors.Is(err, bridge.ErrInvalidRequest) {
		t.Fatal(err)
	}
	if _, err := c.Call(context.Background(), "read", struct{}{}, "not-a-pointer"); !errors.Is(err, bridge.ErrInvalidRequest) {
		t.Fatal(err)
	}
	if calls.Load() != 1 {
		t.Fatal("invalid local calls reached remote capability")
	}
	_, err := c.Call(context.Background(), "read", func() {}, &result)
	var encodeError *json.UnsupportedTypeError
	if !errors.Is(err, bridge.ErrInvalidRequest) || !errors.As(err, &encodeError) || calls.Load() != 1 {
		t.Fatal("local encoding cause lost", err, calls.Load())
	}
	_, err = c.Call(context.Background(), "read", struct{}{}, new(int))
	var decodeError *json.UnmarshalTypeError
	if !errors.Is(err, bridge.ErrProtocolMismatch) || !errors.As(err, &decodeError) {
		t.Fatal("local decoding cause lost", err)
	}
}

func TestMalformedAndOversizedMessagesDoNotInvokeProvider(t *testing.T) {
	var calls atomic.Int32
	h := host(t, bridge.HostOptions{MaxBodyBytes: 1024, Routes: []bridge.Route{bridge.Handle("read", contract, func(context.Context, struct{}) (string, error) { calls.Add(1); return "ok", nil })}, Grants: []bridge.Grant{{Token: "reader", Routes: []string{"read"}}}})
	valid := string(message(struct{}{}))
	for _, body := range []string{"{", valid + " {}", strings.Replace(valid, `"protocol":1`, `"protocol":2`, 1), strings.Replace(valid, `"timeout_ms":1000`, `"timeout_ms":0`, 1), strings.Replace(valid, `"payload":{}`, `"payload":{"tree":"secret"}`, 1), strings.Replace(valid, `"protocol":1`, `"extra":true,"protocol":1`, 1), strings.Repeat("x", 2048)} {
		status, _ := raw(t, h, "reader", []byte(body))
		if status == http.StatusOK {
			t.Fatalf("accepted malformed request %q", body)
		}
	}
	if calls.Load() != 0 {
		t.Fatal("malformed request reached provider")
	}
	status, body := raw(t, h, "wrong-token", []byte(valid))
	if status != http.StatusForbidden || strings.Contains(string(body), "wrong-token") {
		t.Fatal(status, string(body))
	}
	c := client(t, h, bridge.ClientOptions{MaxBodyBytes: 1024})
	if err := c.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	if _, err := c.Call(context.Background(), "read", strings.Repeat("x", 2048), new(string)); !errors.Is(err, bridge.ErrMessageTooLarge) {
		t.Fatal(err)
	}
	if calls.Load() != 0 {
		t.Fatal("oversized request was sent")
	}
}

func TestBoundedResponsesAndSanitizedBusinessFailures(t *testing.T) {
	for _, kind := range []string{"response-limit", "host-limit", "error", "panic"} {
		t.Run(kind, func(t *testing.T) {
			config := options(bridge.Handle("read", contract, func(context.Context, struct{}) (string, error) {
				if kind == "error" {
					return "", errors.New("INTERNAL-SECRET")
				}
				if kind == "panic" {
					panic("INTERNAL-SECRET")
				}
				return strings.Repeat("x", 2048), nil
			}))
			if kind == "host-limit" {
				config.MaxBodyBytes = 1024
			}
			h := host(t, config)
			c := client(t, h, bridge.ClientOptions{MaxBodyBytes: 1024})
			if err := c.Start(context.Background()); err != nil {
				t.Fatal(err)
			}
			_, err := c.Call(context.Background(), "read", struct{}{}, new(string))
			want := bridge.ErrMessageTooLarge
			if kind == "error" || kind == "panic" {
				want = bridge.ErrRemoteFailure
			}
			if !errors.Is(err, want) || strings.Contains(err.Error(), "INTERNAL-SECRET") {
				t.Fatal(err)
			}
			if kind == "error" || kind == "panic" {
				_, body := raw(t, h, "reader", message(struct{}{}))
				if strings.Contains(string(body), "INTERNAL-SECRET") {
					t.Fatal(string(body))
				}
			}
		})
	}
}

func TestContractIsRecheckedAfterRemoteRestart(t *testing.T) {
	h := host(t, options(bridge.Handle("read", contract, func(context.Context, struct{}) (string, error) { return "old", nil })))
	c := client(t, h, bridge.ClientOptions{})
	if err := c.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	address, err := url.Parse(h.URL())
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := h.Stop(ctx); err != nil {
		t.Fatal(err)
	}
	var calls atomic.Int32
	config := options(bridge.Handle("read", bridge.Contract{ID: contract.ID, Version: 2}, func(context.Context, struct{}) (string, error) { calls.Add(1); return "incompatible", nil }))
	config.Address = address.Host
	_ = host(t, config)
	if _, err := c.Call(context.Background(), "read", struct{}{}, new(string)); !errors.Is(err, bridge.ErrContractMismatch) {
		t.Fatal(err)
	}
	if calls.Load() != 0 {
		t.Fatal("incompatible replacement was invoked")
	}
}

func TestDeadlinesAndCancellationReachRemoteContext(t *testing.T) {
	for _, kind := range []string{"cancel", "client-timeout", "host-timeout", "stop"} {
		t.Run(kind, func(t *testing.T) {
			entered, canceled := make(chan struct{}), make(chan struct{})
			var calls atomic.Int32
			config := options(bridge.Handle("read", contract, func(ctx context.Context, _ struct{}) (string, error) {
				calls.Add(1)
				close(entered)
				<-ctx.Done()
				close(canceled)
				return "", ctx.Err()
			}))
			if kind == "host-timeout" {
				config.MaxCallDuration = 25 * time.Millisecond
			}
			h := host(t, config)
			co := bridge.ClientOptions{}
			if kind == "client-timeout" {
				co.Timeout = 25 * time.Millisecond
			}
			c := client(t, h, co)
			if err := c.Start(context.Background()); err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			result := make(chan error, 1)
			go func() { _, err := c.Call(ctx, "read", struct{}{}, new(string)); result <- err }()
			receive(t, entered)
			if kind == "cancel" {
				cancel()
			}
			if kind == "stop" {
				if err := c.Stop(context.Background()); err != nil {
					t.Fatal(err)
				}
				if _, err := c.Call(context.Background(), "read", struct{}{}, new(string)); !errors.Is(err, bridge.ErrClosed) {
					t.Fatal(err)
				}
			}
			err := receive(t, result)
			receive(t, canceled)
			want, cause := bridge.ErrTimeout, context.DeadlineExceeded
			if kind == "cancel" || kind == "stop" {
				want, cause = bridge.ErrCanceled, context.Canceled
			}
			if !errors.Is(err, want) || !errors.Is(err, cause) || calls.Load() != 1 {
				t.Fatal(err, calls.Load())
			}
		})
	}
}

func TestNoAutomaticRetryOrRedirect(t *testing.T) {
	for _, mode := range []string{"drop", "redirect", "wrong-protocol", "wrong-route", "wrong-status", "wrong-contract"} {
		t.Run(mode, func(t *testing.T) {
			var calls atomic.Int32
			listener, err := net.Listen("tcp", "127.0.0.1:0")
			if err != nil {
				t.Fatal(err)
			}
			server := &http.Server{ReadHeaderTimeout: time.Second, Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				if r.URL.Path == "/bridge/v1/describe" {
					protocol := 1
					if mode == "wrong-protocol" {
						protocol = 2
					}
					_ = json.NewEncoder(w).Encode(map[string]any{"protocol": protocol, "route": "read", "contract": contract, "generation": 0})
					return
				}
				calls.Add(1)
				_, _ = io.Copy(io.Discard, r.Body)
				if mode == "redirect" {
					w.Header().Set("Location", "/unexpected")
					w.WriteHeader(http.StatusTemporaryRedirect)
					return
				}
				if mode == "wrong-route" || mode == "wrong-status" || mode == "wrong-contract" {
					name, responseContract := "read", contract
					if mode == "wrong-route" {
						name = "different"
					}
					if mode == "wrong-contract" {
						responseContract.Version++
					}
					if mode == "wrong-status" {
						w.WriteHeader(http.StatusBadGateway)
					}
					_ = json.NewEncoder(w).Encode(map[string]any{"protocol": 1, "route": name, "contract": responseContract, "generation": 0, "result": "ok"})
					return
				}
				connection, _, err := w.(http.Hijacker).Hijack()
				if err == nil {
					_ = connection.Close()
				}
			})}
			go server.Serve(listener)
			defer server.Close()
			c, err := bridge.NewClient(bridge.ClientOptions{URL: "http://" + listener.Addr().String(), Token: "reader", Routes: []bridge.Descriptor{descriptor()}})
			if err != nil {
				t.Fatal(err)
			}
			defer c.Stop(context.Background())
			if err := c.Start(context.Background()); mode == "wrong-protocol" {
				if !errors.Is(err, bridge.ErrProtocolMismatch) {
					t.Fatal(err)
				}
				return
			} else if err != nil {
				t.Fatal(err)
			}
			_, err = c.Call(context.Background(), "read", struct{}{}, new(string))
			if err == nil || calls.Load() != 1 {
				t.Fatal("request retried or redirected", err, calls.Load())
			}
			if (mode == "wrong-route" || mode == "wrong-status") && !errors.Is(err, bridge.ErrProtocolMismatch) {
				t.Fatal("malformed response classified as contract mismatch", err)
			}
			if mode == "wrong-contract" && !errors.Is(err, bridge.ErrContractMismatch) {
				t.Fatal("contract mismatch not identified", err)
			}
		})
	}
}

func TestTLSVerificationAndInsecureConfiguration(t *testing.T) {
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	template := &x509.Certificate{SerialNumber: big.NewInt(1), NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(time.Hour), IPAddresses: []net.IP{net.ParseIP("127.0.0.1")}, ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth}, KeyUsage: x509.KeyUsageDigitalSignature}
	der, err := x509.CreateCertificate(rand.Reader, template, template, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	config := options(bridge.Handle("read", contract, func(context.Context, struct{}) (string, error) { return "tls", nil }))
	config.TLSConfig = &tls.Config{Certificates: []tls.Certificate{{Certificate: [][]byte{der}, PrivateKey: key}}}
	h := host(t, config)
	untrusted := client(t, h, bridge.ClientOptions{})
	if err := untrusted.Start(context.Background()); !errors.Is(err, bridge.ErrUnavailable) {
		t.Fatal(err)
	}
	certificate, err := x509.ParseCertificate(der)
	if err != nil {
		t.Fatal(err)
	}
	pool := x509.NewCertPool()
	pool.AddCert(certificate)
	trusted := client(t, h, bridge.ClientOptions{TLSConfig: &tls.Config{RootCAs: pool}})
	if err := trusted.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	var result string
	if _, err := trusted.Call(context.Background(), "read", struct{}{}, &result); err != nil || result != "tls" {
		t.Fatal(result, err)
	}
	// Selecting an alternate certificate/config must not weaken bridge policy.
	selected := &tls.Config{Certificates: config.TLSConfig.Certificates, MinVersion: tls.VersionTLS10, NextProtos: []string{"h2"}}
	config.TLSConfig = &tls.Config{GetConfigForClient: func(*tls.ClientHelloInfo) (*tls.Config, error) { return selected, nil }}
	callbackHost := host(t, config)
	address, err := url.Parse(callbackHost.URL())
	if err != nil {
		t.Fatal(err)
	}
	legacy, err := tls.DialWithDialer(&net.Dialer{Timeout: time.Second}, "tcp", address.Host, &tls.Config{RootCAs: pool, MinVersion: tls.VersionTLS10, MaxVersion: tls.VersionTLS11})
	if err == nil {
		legacy.Close()
		t.Fatal("TLS callback bypassed minimum version")
	}
	modern, err := tls.DialWithDialer(&net.Dialer{Timeout: time.Second}, "tcp", address.Host, &tls.Config{RootCAs: pool, MinVersion: tls.VersionTLS12, NextProtos: []string{"h2", "http/1.1"}})
	if err != nil {
		t.Fatal(err)
	}
	if state := modern.ConnectionState(); state.Version < tls.VersionTLS12 || state.NegotiatedProtocol != "http/1.1" {
		t.Fatal("TLS callback changed transport policy", state)
	}
	modern.Close()
	if selected.MinVersion != tls.VersionTLS10 || len(selected.NextProtos) != 1 || selected.NextProtos[0] != "h2" {
		t.Fatal("application TLS config mutated")
	}
	// Static trust pools and certificate data belong to the constructed bridge.
	emptyPool := x509.NewCertPool()
	isolated := client(t, h, bridge.ClientOptions{TLSConfig: &tls.Config{RootCAs: emptyPool}})
	emptyPool.AddCert(certificate)
	if err := isolated.Start(context.Background()); !errors.Is(err, bridge.ErrUnavailable) {
		t.Fatal("client trust changed through original pool", err)
	}
	ownedDER := append([]byte(nil), der...)
	staticConfig := options(bridge.Handle("read", contract, func(context.Context, struct{}) (string, error) { return "snapshot", nil }))
	staticConfig.TLSConfig = &tls.Config{Certificates: []tls.Certificate{{Certificate: [][]byte{ownedDER}, PrivateKey: key}}}
	staticHost := host(t, staticConfig)
	ownedDER[0] ^= 0xff
	staticConfig.TLSConfig.Certificates[0] = tls.Certificate{}
	staticClient := client(t, staticHost, bridge.ClientOptions{TLSConfig: &tls.Config{RootCAs: pool}})
	if err := staticClient.Start(context.Background()); err != nil {
		t.Fatal("host certificate changed through original configuration", err)
	}
	for _, co := range []bridge.ClientOptions{{URL: "http://192.0.2.1:80"}, {URL: "http://user:password@127.0.0.1"}, {URL: "http://127.0.0.1?token=secret"}, {URL: h.URL(), TLSConfig: &tls.Config{InsecureSkipVerify: true}}} {
		co.Token = "reader"
		co.Routes = []bridge.Descriptor{descriptor()}
		if c, err := bridge.NewClient(co); !errors.Is(err, bridge.ErrInvalidConfig) {
			if c != nil {
				c.Stop(context.Background())
			}
			t.Fatal("accepted unsafe configuration", err)
		}
	}
	f := forest.New(forest.Options{})
	defer f.Close(context.Background())
	config.TLSConfig = nil
	config.Address = "0.0.0.0:0"
	if _, err := bridge.NewHost(f, config); !errors.Is(err, bridge.ErrInvalidConfig) {
		t.Fatal(err)
	}
}
