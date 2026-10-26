// Package protocol contains detached application values, never runtime handles.
package protocol

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"strings"
	"unicode/utf8"
)

const Profile = "eino-0.9.22/apply-await/v1/streaming"

var (
	ErrConflict    = errors.New("execution input conflict")
	ErrUnknown     = errors.New("execution outcome requires reconciliation")
	ErrBusy        = errors.New("execution is still active")
	ErrCorrupt     = errors.New("application state corrupt")
	ErrOwner       = errors.New("state directory already owned")
	ErrMissing     = errors.New("required checkpoint missing")
	ErrPartial     = errors.New("stream not completely consumed")
	ErrUnavailable = errors.New("application unavailable")
	ErrTransient   = errors.New("explicit pre-engine transient failure")
)

type Input struct {
	ExecutionID, OperationID, Text string
	AttemptMillis                  int
}

func Valid(in Input) bool {
	return Key(in.ExecutionID) && Key(in.OperationID) && len(in.Text) <= 512 && utf8.ValidString(in.Text) && !strings.ContainsRune(in.Text, 0) && (in.AttemptMillis == 0 || (in.AttemptMillis >= 250 && in.AttemptMillis <= 20000))
}
func Key(s string) bool {
	return s != "" && len(s) <= 128 && utf8.ValidString(s) && !strings.ContainsRune(s, 0)
}
func Hash(v any) string {
	b, _ := json.Marshal(v)
	return BytesHash(b)
}
func BytesHash(b []byte) string {
	h := sha256.Sum256(b)
	return hex.EncodeToString(h[:])
}
func Checkpoint(in Input) string {
	return Hash(struct{ Namespace, ID string }{"temporal-eino", in.ExecutionID})
}

type Receipt struct {
	OperationID, Digest, Content, Process, Version string
	Generation                                     uint64
}
type Pause struct {
	Checkpoint, Digest, Token string
	Targets                   []string
}
type Decision struct{ ID, ExecutionID, Token, Target, Value string }
type Request struct {
	Input             Input
	WorkflowID, RunID string
	Step              int
	Action            string
	Decision          Decision
}
type Delivery struct {
	Process, Worker string
	Generation      uint64
	Attempt         int32
}
type Point struct {
	Stage, Process, Instance, Version string
	Generation                        uint64
	Input                             Input
	Step                              int
	Result                            *Result
}
type Result struct {
	State, Text string
	Engine      string
	Receipt     Receipt
	Pause       Pause
	Process     string
	Generation  uint64
	Trace       []Point
	Reused      bool
	Delivery    Delivery
}

// CopyResult detaches observations from delivery and persistent values.
func CopyResult(v Result) Result {
	v.Pause.Targets = append([]string(nil), v.Pause.Targets...)
	v.Trace = append([]Point(nil), v.Trace...)
	for i := range v.Trace {
		if v.Trace[i].Result != nil {
			c := CopyResult(*v.Trace[i].Result)
			v.Trace[i].Result = &c
		}
	}
	return v
}

type Record struct {
	Request Request
	Owner   string
	State   string
	Result  Result
}
type Evidence struct {
	Artifacts int
	Receipt   Receipt
	Records   []Record
	Owner     string
}
type WorkflowState struct {
	Stage     string
	Pause     Pause
	Decisions map[string]string
	Last      Result
	Graph     GraphResult
}
type GraphResult struct {
	Text, Process, Instance string
	Generation              uint64
	Trace                   []Point
}
type Counts struct{ Model, Apply, Query, Resume, Graph int64 }
type ResourceReport struct {
	Claim          bool
	BusinessOpen   bool
	BusinessCloses int
	NativeOpen     bool
	NativeCloses   int
}
