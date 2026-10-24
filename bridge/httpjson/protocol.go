// Package httpjson bridges explicit capability adapters over HTTP/JSON.
// It adds no remote topology or distributed scope to Forest Core.
package httpjson

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"strings"
	"time"
	"unicode"
	"unicode/utf8"
)

const ProtocolVersion = 1
const describePath = "/bridge/v1/describe"
const callPath = "/bridge/v1/call"

var (
	ErrInvalidConfig    = errors.New("invalid bridge configuration")
	ErrInvalidRequest   = errors.New("invalid bridge request")
	ErrUnauthorized     = errors.New("remote capability unauthorized")
	ErrProtocolMismatch = errors.New("bridge protocol mismatch")
	ErrContractMismatch = errors.New("remote contract mismatch")
	ErrMessageTooLarge  = errors.New("bridge message too large")
	ErrUnavailable      = errors.New("remote capability unavailable")
	ErrTimeout          = errors.New("remote call timed out")
	ErrCanceled         = errors.New("remote call canceled")
	ErrRemoteFailure    = errors.New("remote operation failed")
	ErrClosed           = errors.New("bridge closed")
)

type Error struct {
	Stage, Route string
	Cause        error
}

func (e *Error) Error() string {
	return fmt.Sprintf("bridge %s route=%q: %v", e.Stage, e.Route, e.Cause)
}
func (e *Error) Unwrap() error                       { return e.Cause }
func failure(stage, route string, cause error) error { return &Error{stage, route, cause} }

type Contract struct {
	ID      string `json:"id"`
	Version uint64 `json:"version"`
}
type Descriptor struct {
	Name     string
	Contract Contract
}
type Route struct {
	Descriptor
	Handle func(context.Context, json.RawMessage) (json.RawMessage, error)
}
type Grant struct {
	Token  string
	Routes []string
}
type CallInfo struct{ RemoteGeneration uint64 }
type Endpoint interface{ URL() string }

func Handle[Input, Output any](name string, contract Contract, fn func(context.Context, Input) (Output, error)) Route {
	r := Route{Descriptor: Descriptor{Name: name, Contract: contract}}
	if fn == nil {
		return r
	}
	r.Handle = func(ctx context.Context, data json.RawMessage) (json.RawMessage, error) {
		var input Input
		if err := decode(data, &input); err != nil {
			return nil, ErrInvalidRequest
		}
		output, err := fn(ctx, input)
		if err != nil {
			return nil, err
		}
		data, err = json.Marshal(output)
		if err != nil {
			return nil, ErrRemoteFailure
		}
		return data, nil
	}
	return r
}

type request struct {
	Protocol      int             `json:"protocol"`
	Route         string          `json:"route"`
	Contract      Contract        `json:"contract"`
	TimeoutMillis int64           `json:"timeout_ms"`
	Payload       json.RawMessage `json:"payload,omitempty"`
}
type wireError struct {
	Code    string `json:"code"`
	Message string `json:"message"`
}
type response struct {
	Protocol   int             `json:"protocol"`
	Route      string          `json:"route"`
	Contract   Contract        `json:"contract"`
	Generation uint64          `json:"generation"`
	Result     json.RawMessage `json:"result,omitempty"`
	Error      *wireError      `json:"error,omitempty"`
}
type fault struct {
	code   string
	cause  error
	status int
}

var faults = []fault{
	{"invalid_request", ErrInvalidRequest, http.StatusBadRequest},
	{"unauthorized", ErrUnauthorized, http.StatusForbidden},
	{"protocol_mismatch", ErrProtocolMismatch, http.StatusBadRequest},
	{"contract_mismatch", ErrContractMismatch, http.StatusConflict},
	{"message_too_large", ErrMessageTooLarge, http.StatusRequestEntityTooLarge},
	{"unavailable", ErrUnavailable, http.StatusServiceUnavailable},
	{"deadline_exceeded", ErrTimeout, http.StatusGatewayTimeout},
	{"canceled", ErrCanceled, http.StatusRequestTimeout},
	{"remote_failure", ErrRemoteFailure, http.StatusInternalServerError},
}

func faultFor(err error) fault {
	if errors.Is(err, context.DeadlineExceeded) {
		err = ErrTimeout
	} else if errors.Is(err, context.Canceled) {
		err = ErrCanceled
	}
	for _, f := range faults {
		if errors.Is(err, f.cause) {
			return f
		}
	}
	return faults[len(faults)-1]
}
func faultCode(code string) (fault, bool) {
	for _, f := range faults {
		if f.code == code {
			return f, true
		}
	}
	return fault{}, false
}
func faultCause(f fault) error {
	if f.cause == ErrTimeout {
		return errors.Join(ErrTimeout, context.DeadlineExceeded)
	}
	if f.cause == ErrCanceled {
		return errors.Join(ErrCanceled, context.Canceled)
	}
	return f.cause
}
func validDescriptor(d Descriptor) bool {
	return d.Name != "" && d.Contract.ID != "" && d.Contract.Version > 0
}
func validToken(token string) bool {
	return len(token) > 0 && len(token) <= 4096 && strings.IndexFunc(token, func(r rune) bool { return unicode.IsSpace(r) || unicode.IsControl(r) }) < 0
}
func loopback(host string) bool {
	return strings.EqualFold(host, "localhost") || net.ParseIP(host).IsLoopback()
}
func bodyLimit(value int64) (int64, error) {
	if value == 0 {
		return 1 << 20, nil
	}
	if value < 1024 || value > 64<<20 {
		return 0, ErrInvalidConfig
	}
	return value, nil
}
func decode(data []byte, value any) error {
	if !utf8.Valid(data) {
		return ErrInvalidRequest
	}
	d := json.NewDecoder(bytes.NewReader(data))
	d.DisallowUnknownFields()
	if err := d.Decode(value); err != nil {
		return err
	}
	if err := d.Decode(new(any)); err != io.EOF {
		return ErrInvalidRequest
	}
	return nil
}
func readBody(reader io.Reader, limit int64) ([]byte, error) {
	data, err := io.ReadAll(io.LimitReader(reader, limit+1))
	if err != nil {
		return nil, err
	}
	if int64(len(data)) > limit {
		return nil, ErrMessageTooLarge
	}
	return data, nil
}
func milliseconds(d time.Duration) int64 {
	n := int64(d / time.Millisecond)
	if d%time.Millisecond != 0 {
		n++
	}
	return n
}
