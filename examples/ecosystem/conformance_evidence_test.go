package ecosystem

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"strconv"
	"sync"

	nt "github.com/Leeminjing/go-forest/adapters/nats"
	"github.com/Leeminjing/go-forest/forest"
	tk "github.com/Leeminjing/go-forest/testkit"
	oteltrace "go.opentelemetry.io/otel/trace"
)

func confNonce() string {
	var v [12]byte
	if _, err := rand.Read(v[:]); err != nil {
		panic(err)
	}
	return hex.EncodeToString(v[:])
}

var confProcess = fmt.Sprintf("pid-%d/%s", os.Getpid(), confNonce())

func confDigest(value any) string {
	b, err := json.Marshal(value)
	if err != nil {
		panic(err)
	}
	h := sha256.Sum256(b)
	return hex.EncodeToString(h[:])
}

type confCall struct {
	Execution tk.Execution
	Message   nt.Message
	Version   string
}
type confCallKey struct{}
type confObservation struct {
	TraceID   string
	Execution tk.Execution
	Message   nt.Message
	Receipt   confReceipt
	Trace     tk.Trace
}
type confEvidence struct {
	mu       sync.Mutex
	calls    map[string]*tk.Journal
	finished map[uint64]confObservation
	rejected map[uint64]error
}

func (e *confEvidence) reject(seq uint64, err error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.rejected == nil {
		e.rejected = map[uint64]error{}
	}
	e.rejected[seq] = err
}
func (e *confEvidence) rejection(seq uint64) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.rejected[seq]
}

func (e *confEvidence) at(ctx context.Context, f *forest.Forest, stage string) error {
	call, ok := ctx.Value(confCallKey{}).(confCall)
	if !ok {
		return errorsNewEvidence()
	}
	g, err := f.Generation(ctx)
	if err != nil {
		return err
	}
	x := call.Execution
	x.Scope.Generation = g
	e.mu.Lock()
	if e.calls == nil {
		e.calls = map[string]*tk.Journal{}
	}
	j := e.calls[x.Scope.Call]
	if j == nil {
		j, err = tk.NewJournal(128)
		e.calls[x.Scope.Call] = j
	}
	e.mu.Unlock()
	if err != nil {
		return err
	}
	_, err = j.Record(tk.Event{Stage: stage, Execution: &x})
	return err
}
func errorsNewEvidence() error {
	return fmt.Errorf("missing actual delivery context: %w", tk.ErrInvalidEvidence)
}
func (e *confEvidence) finish(ctx context.Context, receipt confReceipt) error {
	call := ctx.Value(confCallKey{}).(confCall)
	e.mu.Lock()
	defer e.mu.Unlock()
	j := e.calls[call.Execution.Scope.Call]
	if j == nil {
		return errorsNewEvidence()
	}
	trace, err := j.Snapshot()
	if err != nil {
		return err
	}
	if e.finished == nil {
		e.finished = map[uint64]confObservation{}
	}
	e.finished[call.Message.StreamSequence] = confObservation{TraceID: oteltrace.SpanContextFromContext(ctx).TraceID().String(), Execution: call.Execution, Message: call.Message, Receipt: receipt, Trace: trace}
	return nil
}
func (e *confEvidence) result(seq uint64) (confObservation, bool) {
	e.mu.Lock()
	defer e.mu.Unlock()
	v, ok := e.finished[seq]
	return v, ok
}
func confMessageCall(forestID, stream, durable, instance, version string, m nt.Message, g uint64) confCall {
	task := stream + "/" + strconv.FormatUint(m.StreamSequence, 10)
	attempt := strconv.FormatUint(m.Delivery, 10)
	return confCall{Execution: tk.Execution{Scope: tk.Scope{Forest: forestID, Call: task + "/" + durable + "/" + attempt + "/" + confNonce(), Generation: g}, Task: task, Step: durable + "/handle", Attempt: attempt, Process: confProcess, Instance: instance}, Message: m, Version: version}
}
func (r confReceipt) effect() tk.Effect {
	return tk.Effect{Namespace: r.Namespace, Operation: r.ID, InputDigest: r.Digest, Receipt: confDigest(r), ContentDigest: confDigest(r.Content), OriginProcess: r.Process, OriginVersion: r.Version, OriginGeneration: r.Generation}
}

func (f *confFixture) topology() tk.Topology {
	snapshot := f.r.Forest.Snapshot()
	type node struct{ Parent, Instance, Type, Version string }
	projection := struct {
		Nodes       map[string]node
		Links, Nest map[string]forest.Binding
	}{Nodes: map[string]node{}, Links: map[string]forest.Binding{}, Nest: map[string]forest.Binding{}}
	for id, n := range snapshot.Trees {
		projection.Nodes[string(id)] = node{string(n.ParentID), n.InstanceID, n.TypeID, n.Version}
	}
	for id, b := range snapshot.Links {
		projection.Links[id] = b.Binding
	}
	for id, b := range snapshot.NestBindings {
		projection.Nest[id] = b.Binding
	}
	return tk.Topology{Forest: f.domain, Generation: snapshot.Generation, Fingerprint: confDigest(projection)}
}
