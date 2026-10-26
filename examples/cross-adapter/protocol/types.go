// Package protocol contains only application data and domain errors. It does not
// depend on Forest, Fx, Temporal, GORM, or an execution engine.
package protocol

import "errors"

var (
	ErrConflict = errors.New("operation input conflict")
	ErrUnknown  = errors.New("operation outcome requires reconciliation")
)

type Request struct{ OperationID, Target, Payload string }
type Receipt struct {
	OperationID, Digest, Target, Output, EffectVersion, EffectProcess string
	EffectGeneration                                                  uint64
}
type Point struct {
	Stage, Process, Version string
	Generation              uint64
}
type Result struct {
	Receipt                     Receipt
	Reused                      bool
	VersionBefore, VersionAfter string
	Attempt                     int32
	WorkerIdentity, FxIdentity  string
	Trace                       []Point
}
type Observation struct {
	Found       bool
	Receipt     Receipt
	EffectCount int64
}
