package httpjson_test

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/url"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	bridge "github.com/Leeminjing/go-forest/bridge/httpjson"
	"github.com/Leeminjing/go-forest/examples/trees/remoteclock"
	"github.com/Leeminjing/go-forest/examples/trees/remoteformatter"
	"github.com/Leeminjing/go-forest/forest"
)

type processCommand struct {
	Op            string
	TimeoutMillis int
	Version       string
}
type processReply struct {
	Ready             bool
	PID               int
	URL, Code, Value  string
	Generation        uint64
	Trees             int
	Calls, Stops      int32
	Seconds           []int64
	RemoteGenerations []uint64
}

func code(err error) string {
	if err == nil {
		return ""
	}
	for _, entry := range []struct {
		err  error
		code string
	}{{bridge.ErrContractMismatch, "contract_mismatch"}, {bridge.ErrUnauthorized, "unauthorized"}, {bridge.ErrTimeout, "timeout"}, {bridge.ErrCanceled, "canceled"}, {bridge.ErrUnavailable, "unavailable"}, {bridge.ErrClosed, "closed"}} {
		if errors.Is(err, entry.err) {
			return entry.code
		}
	}
	return "failure"
}

type processGate struct {
	entered, canceled, release chan struct{}
	once                       sync.Once
}
type processClock struct {
	second       int64
	mu           sync.Mutex
	gate         *processGate
	closed       atomic.Bool
	calls, stops *atomic.Int32
}

func (c *processClock) Now(ctx context.Context) (time.Time, error) {
	c.calls.Add(1)
	c.mu.Lock()
	gate := c.gate
	c.mu.Unlock()
	if gate != nil {
		gate.once.Do(func() { close(gate.entered) })
		select {
		case <-gate.release:
		case <-ctx.Done():
			close(gate.canceled)
			return time.Time{}, ctx.Err()
		}
	}
	if c.closed.Load() {
		return time.Time{}, errors.New("provider released during call")
	}
	return time.Unix(c.second, 0), nil
}
func printReply(reply processReply) { _ = json.NewEncoder(os.Stdout).Encode(reply) }

// This test fixture is re-executed as real OS processes. Stdio only controls
// test synchronization; every Clock/Formatter business call travels over TCP.
func TestRemoteProcessHelper(t *testing.T) {
	mode := os.Getenv("FOREST_TEST_BRIDGE_MODE")
	if mode == "" {
		t.Skip("subprocess fixture")
	}
	if err := runProcess(mode); err != nil {
		_, _ = fmt.Fprintln(os.Stderr, "bridge helper failed:", code(err))
		os.Exit(2)
	}
	os.Exit(0)
}
func runProcess(mode string) (result error) {
	f := forest.New(forest.Options{})
	defer func() {
		f.CancelGeneration(f.Snapshot().Generation)
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		if err := f.Close(ctx); result == nil {
			result = err
		}
	}()
	token := os.Getenv("FOREST_TEST_BRIDGE_TOKEN")
	if mode == "host" {
		return hostProcess(f, token)
	}
	return clientProcess(f, token)
}
func hostProcess(f *forest.Forest, token string) error {
	var calls, stops atomic.Int32
	var current, held *processClock
	provider := func(version string, second int64) forest.Definition {
		return forest.Definition{TypeID: "process.clock", Version: version, HotSwappable: true,
			Capabilities: []forest.CapabilityDescriptor{forest.Capability[remoteclock.Source]("clock", forest.Public, remoteclock.CapabilityContract())},
			Build: func(ctx context.Context, env *forest.BuildEnv) (forest.Lifecycle, error) {
				clock := &processClock{second: second, calls: &calls, stops: &stops}
				current = clock
				return forest.Hooks{OnStop: func(context.Context) error { clock.closed.Store(true); stops.Add(1); return nil }}, forest.Provide(env, "clock", clock)
			}}
	}
	v1, v2 := provider("1", 1), provider("2", 2)
	host := remoteclock.HostDefinition(f, bridge.HostOptions{Address: os.Getenv("FOREST_TEST_BRIDGE_ADDRESS"), Grants: []bridge.Grant{{Token: token, Routes: []string{remoteclock.RouteName}}}})
	for _, d := range []forest.Definition{v1, v2, host} {
		if err := f.Register(d); err != nil {
			return err
		}
	}
	if _, err := f.Apply(context.Background(), forest.ChangePlan{Operations: []forest.Operation{forest.AddRoot("clock", v1.Ref()), forest.AddRoot("host", host.Ref()), forest.Connect(forest.Link{ID: "clock", Consumer: "host", Port: "clock", Provider: "clock", Capability: "clock"})}}); err != nil {
		return err
	}
	var address string
	if err := f.Invoke(context.Background(), func(ctx context.Context) error {
		return forest.Use[bridge.Endpoint](ctx, f, "host", "endpoint", func(e bridge.Endpoint) error { address = e.URL(); return nil })
	}); err != nil {
		return err
	}
	printReply(processReply{Ready: true, PID: os.Getpid(), URL: address, Generation: f.Snapshot().Generation})
	decoder := json.NewDecoder(os.Stdin)
	for {
		var command processCommand
		if err := decoder.Decode(&command); err != nil {
			if err == io.EOF {
				return nil
			}
			return err
		}
		reply := processReply{}
		switch command.Op {
		case "hold":
			held = current
			held.mu.Lock()
			held.gate = &processGate{entered: make(chan struct{}), canceled: make(chan struct{}), release: make(chan struct{})}
			held.mu.Unlock()
		case "entered", "canceled":
			if held == nil {
				return errors.New("no held provider")
			}
			held.mu.Lock()
			gate := held.gate
			held.mu.Unlock()
			ch := gate.entered
			if command.Op == "canceled" {
				ch = gate.canceled
			}
			select {
			case <-ch:
			case <-time.After(5 * time.Second):
				return errors.New("provider signal timeout")
			}
		case "release":
			if held != nil {
				held.mu.Lock()
				gate := held.gate
				held.mu.Unlock()
				select {
				case <-gate.release:
				default:
					close(gate.release)
				}
			}
		case "replace":
			definition := v2.Ref()
			if command.Version == "1" {
				definition = v1.Ref()
			}
			_, err := f.Apply(context.Background(), forest.ChangePlan{BaseGeneration: f.Snapshot().Generation, Operations: []forest.Operation{forest.Replace("clock", definition, false)}})
			reply.Code = code(err)
		case "drain":
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			reply.Code = code(f.Wait(ctx))
			cancel()
		case "stats":
		case "stop":
			return nil
		default:
			return errors.New("unknown host command")
		}
		reply.Generation = f.Snapshot().Generation
		reply.Calls = calls.Load()
		reply.Stops = stops.Load()
		printReply(reply)
	}
}

type localPing interface{ Ping() string }
type ping struct{}

func (ping) Ping() string { return "local-alive" }
func clientProcess(f *forest.Forest, token string) error {
	var transport *bridge.Client
	descriptor := remoteclock.Descriptor()
	if version := os.Getenv("FOREST_TEST_BRIDGE_CONTRACT"); version != "" {
		n, err := strconv.ParseUint(version, 10, 64)
		if err != nil {
			return err
		}
		descriptor.Contract.Version = n
	}
	proxy := forest.Definition{TypeID: "process.proxy", Version: "1", Capabilities: []forest.CapabilityDescriptor{forest.Capability[remoteclock.Source]("clock", forest.Public, remoteclock.CapabilityContract())},
		Build: func(ctx context.Context, env *forest.BuildEnv) (forest.Lifecycle, error) {
			var err error
			transport, err = bridge.NewClient(bridge.ClientOptions{URL: os.Getenv("FOREST_TEST_BRIDGE_ADDRESS"), Token: token, Routes: []bridge.Descriptor{descriptor}, Timeout: 2 * time.Second})
			if err != nil {
				return nil, err
			}
			return transport, forest.Provide(env, "clock", remoteclock.Proxy{Client: transport})
		}}
	formatter := remoteformatter.Definition()
	local := forest.Definition{TypeID: "process.ping", Version: "1", Capabilities: []forest.CapabilityDescriptor{forest.Capability[localPing]("ping", forest.Public)}, Build: func(ctx context.Context, env *forest.BuildEnv) (forest.Lifecycle, error) {
		return forest.Hooks{}, forest.Provide(env, "ping", ping{})
	}}
	for _, d := range []forest.Definition{proxy, formatter, local} {
		if err := f.Register(d); err != nil {
			return err
		}
	}
	_, err := f.Apply(context.Background(), forest.ChangePlan{Operations: []forest.Operation{forest.AddRoot("proxy", proxy.Ref()), forest.AddRoot("formatter", formatter.Ref()), forest.AddRoot("local", local.Ref()), forest.Connect(forest.Link{ID: "clock", Consumer: "formatter", Port: "clock", Provider: "proxy", Capability: "clock"})}})
	if err != nil {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		_ = f.Wait(ctx)
		printReply(processReply{PID: os.Getpid(), Code: code(err), Generation: f.Snapshot().Generation, Trees: len(f.Snapshot().Trees)})
		return nil
	}
	printReply(processReply{Ready: true, PID: os.Getpid(), Generation: f.Snapshot().Generation})
	call := func(ctx context.Context) processReply {
		r := processReply{}
		err := f.Invoke(ctx, func(callCtx context.Context) error {
			return forest.Use[remoteformatter.Output](callCtx, f, "formatter", "format", func(output remoteformatter.Output) error {
				var err error
				r.Value, err = output.Format(callCtx)
				return err
			})
		})
		r.Code = code(err)
		return r
	}
	var pending chan processReply
	var cancelPending context.CancelFunc
	var first chan processReply
	var resume chan struct{}
	decoder := json.NewDecoder(os.Stdin)
	for {
		var command processCommand
		if err := decoder.Decode(&command); err != nil {
			if err == io.EOF {
				return nil
			}
			return err
		}
		reply := processReply{}
		switch command.Op {
		case "call":
			timeout := 2 * time.Second
			if command.TimeoutMillis > 0 {
				timeout = time.Duration(command.TimeoutMillis) * time.Millisecond
			}
			ctx, cancel := context.WithTimeout(context.Background(), timeout)
			reply = call(ctx)
			cancel()
		case "begin":
			ctx, cancel := context.WithCancel(context.Background())
			cancelPending = cancel
			pending = make(chan processReply, 1)
			go func() { pending <- call(ctx) }()
		case "cancel":
			if cancelPending != nil {
				cancelPending()
			}
		case "await":
			if pending == nil {
				return errors.New("no pending call")
			}
			select {
			case reply = <-pending:
			case <-time.After(5 * time.Second):
				return errors.New("client signal timeout")
			}
		case "begin-scope":
			pending = make(chan processReply, 1)
			first = make(chan processReply, 1)
			resume = make(chan struct{})
			go func() {
				result := processReply{}
				err := f.Invoke(context.Background(), func(ctx context.Context) error {
					for n := 0; n < 2; n++ {
						var now remoteclock.Reply
						info, err := transport.Call(ctx, remoteclock.RouteName, struct{}{}, &now)
						if err != nil {
							return err
						}
						result.Seconds = append(result.Seconds, now.Time.Unix())
						result.RemoteGenerations = append(result.RemoteGenerations, info.RemoteGeneration)
						if n == 0 {
							first <- processReply{Seconds: append([]int64(nil), result.Seconds...), RemoteGenerations: append([]uint64(nil), result.RemoteGenerations...)}
							<-resume
						}
					}
					result.Generation, _ = f.Generation(ctx)
					return nil
				})
				result.Code = code(err)
				pending <- result
			}()
		case "first":
			reply = <-first
		case "continue":
			close(resume)
		case "local":
			err := f.Invoke(context.Background(), func(ctx context.Context) error {
				return forest.Use[localPing](ctx, f, "local", "ping", func(p localPing) error { reply.Value = p.Ping(); return nil })
			})
			reply.Code = code(err)
		case "stop":
			return nil
		default:
			return errors.New("unknown client command")
		}
		if reply.Generation == 0 {
			reply.Generation = f.Snapshot().Generation
		}
		printReply(reply)
	}
}

type lockedBuffer struct {
	mu   sync.Mutex
	data []byte
}

func (b *lockedBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.data = append(b.data, p...)
	return len(p), nil
}
func (b *lockedBuffer) String() string { b.mu.Lock(); defer b.mu.Unlock(); return string(b.data) }

type childProcess struct {
	command *exec.Cmd
	input   io.WriteCloser
	replies chan processReply
	done    chan struct{}
	err     error
	stderr  lockedBuffer
	ready   processReply
}

func startProcess(t *testing.T, mode, address, token, version string) *childProcess {
	t.Helper()
	executable, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	p := &childProcess{replies: make(chan processReply, 16), done: make(chan struct{})}
	p.command = exec.Command(executable, "-test.run=^TestRemoteProcessHelper$", "-test.timeout=40s")
	p.command.Env = append(os.Environ(), "FOREST_TEST_BRIDGE_MODE="+mode, "FOREST_TEST_BRIDGE_ADDRESS="+address, "FOREST_TEST_BRIDGE_TOKEN="+token, "FOREST_TEST_BRIDGE_CONTRACT="+version)
	p.command.Stderr = &p.stderr
	p.input, err = p.command.StdinPipe()
	if err != nil {
		t.Fatal(err)
	}
	output, err := p.command.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	if err := p.command.Start(); err != nil {
		t.Fatal(err)
	}
	outputDone := make(chan struct{})
	go func() {
		defer close(outputDone)
		scanner := bufio.NewScanner(output)
		for scanner.Scan() {
			var reply processReply
			if json.Unmarshal(scanner.Bytes(), &reply) == nil {
				p.replies <- reply
			}
		}
	}()
	go func() { <-outputDone; p.err = p.command.Wait(); close(p.done) }()
	t.Cleanup(func() {
		select {
		case <-p.done:
			return
		default:
		}
		_ = json.NewEncoder(p.input).Encode(processCommand{Op: "stop"})
		select {
		case <-p.done:
			if p.err != nil {
				t.Errorf("helper exit: %v %s", p.err, p.stderr.String())
			}
		case <-time.After(3 * time.Second):
			_ = p.command.Process.Kill()
			<-p.done
			t.Error("helper failed to stop gracefully")
		}
	})
	p.ready = p.next(t)
	return p
}
func (p *childProcess) next(t *testing.T) processReply {
	t.Helper()
	select {
	case reply := <-p.replies:
		return reply
	case <-p.done:
		select {
		case reply := <-p.replies:
			return reply
		default:
			t.Fatalf("helper exited: %v %s", p.err, p.stderr.String())
		}
	case <-time.After(7 * time.Second):
		t.Fatalf("helper response timeout: %s", p.stderr.String())
	}
	return processReply{}
}
func (p *childProcess) send(t *testing.T, command processCommand) processReply {
	t.Helper()
	if err := json.NewEncoder(p.input).Encode(command); err != nil {
		t.Fatal(err)
	}
	return p.next(t)
}
func (p *childProcess) op(t *testing.T, name string) processReply {
	t.Helper()
	r := p.send(t, processCommand{Op: name})
	if r.Code != "" {
		t.Fatalf("%s: %+v", name, r)
	}
	return r
}
func (p *childProcess) kill(t *testing.T) {
	t.Helper()
	if err := p.command.Process.Kill(); err != nil {
		t.Fatal(err)
	}
	select {
	case <-p.done:
	case <-time.After(5 * time.Second):
		t.Fatal("killed process did not exit")
	}
	if strings.Contains(p.stderr.String(), "WARNING: DATA RACE") {
		t.Fatalf("race report before intentional kill: %s", p.stderr.String())
	}
}

func TestTwoProcessesHotReplacementCancellationCrashAndRecovery(t *testing.T) {
	const token = "process-integration-token"
	b := startProcess(t, "host", "127.0.0.1:0", token, "")
	if !b.ready.Ready {
		t.Fatal(b.ready)
	}
	a := startProcess(t, "client", b.ready.URL, token, "")
	if !a.ready.Ready || a.ready.PID == b.ready.PID || a.ready.PID == os.Getpid() {
		t.Fatal("not independent processes", a.ready, b.ready)
	}
	if r := a.op(t, "call"); r.Value != "1970-01-01T00:00:01Z" {
		t.Fatal(r)
	}
	b.op(t, "hold")
	a.op(t, "begin")
	b.op(t, "entered")
	b.op(t, "replace")
	if r := b.op(t, "stats"); r.Stops != 0 {
		t.Fatal("old provider released prematurely", r)
	}
	if r := a.op(t, "call"); r.Value != "1970-01-01T00:00:02Z" {
		t.Fatal(r)
	}
	b.op(t, "release")
	if r := a.op(t, "await"); r.Value != "1970-01-01T00:00:01Z" {
		t.Fatal(r)
	}
	if r := b.op(t, "drain"); r.Stops != 1 {
		t.Fatal(r)
	}
	// One local A Scope legitimately observes distinct B generations.
	a.op(t, "begin-scope")
	first := a.op(t, "first")
	b.send(t, processCommand{Op: "replace", Version: "1"})
	a.op(t, "continue")
	scoped := a.op(t, "await")
	if len(scoped.RemoteGenerations) != 2 || scoped.RemoteGenerations[0] != first.RemoteGenerations[0] || scoped.RemoteGenerations[0] == scoped.RemoteGenerations[1] || scoped.Generation != a.ready.Generation || scoped.Seconds[0] != 2 || scoped.Seconds[1] != 1 {
		t.Fatal("incorrect cross-process generation semantics", scoped)
	}
	b.op(t, "hold")
	before := b.op(t, "stats").Calls
	if r := a.send(t, processCommand{Op: "call", TimeoutMillis: 250}); r.Code != "timeout" {
		t.Fatal(r)
	}
	b.op(t, "canceled")
	if after := b.op(t, "stats").Calls; after != before+1 {
		t.Fatal("timed-out request retried", before, after)
	}
	b.op(t, "hold")
	a.op(t, "begin")
	b.op(t, "entered")
	a.op(t, "cancel")
	b.op(t, "canceled")
	if r := a.send(t, processCommand{Op: "await"}); r.Code != "canceled" {
		t.Fatal(r)
	}
	address, err := url.Parse(b.ready.URL)
	if err != nil {
		t.Fatal(err)
	}
	b.op(t, "hold")
	a.op(t, "begin")
	b.op(t, "entered")
	b.kill(t)
	if r := a.send(t, processCommand{Op: "await"}); r.Code != "unavailable" {
		t.Fatal("in-flight crash not reported", r)
	}
	if r := a.send(t, processCommand{Op: "call", TimeoutMillis: 500}); r.Code != "unavailable" {
		t.Fatal("remote crash not reported", r)
	}
	if r := a.op(t, "local"); r.Value != "local-alive" || r.Generation != a.ready.Generation {
		t.Fatal("local forest damaged", r)
	}
	restarted := startProcess(t, "host", address.Host, token, "")
	if !restarted.ready.Ready || restarted.ready.PID == b.ready.PID {
		t.Fatal("remote process did not restart", restarted.ready)
	}
	if r := a.op(t, "call"); r.Value != "1970-01-01T00:00:01Z" || r.Generation != a.ready.Generation {
		t.Fatal("same local forest did not recover", r)
	}
}

func TestRemoteStartupRejectionDoesNotPublishProxyForest(t *testing.T) {
	b := startProcess(t, "host", "127.0.0.1:0", "allowed", "")
	if !b.ready.Ready {
		t.Fatal(b.ready)
	}
	for _, tc := range []struct{ token, version, want string }{{"wrong", "", "unauthorized"}, {"allowed", "2", "contract_mismatch"}} {
		a := startProcess(t, "client", b.ready.URL, tc.token, tc.version)
		if a.ready.Ready || a.ready.Code != tc.want || a.ready.Generation != 0 || a.ready.Trees != 0 {
			t.Fatal("failed proxy was published", a.ready)
		}
	}
}
