package testkit

import (
	"errors"
	"fmt"
)

var (
	ErrBarrierUsed      = errors.New("barrier already entered")
	ErrBarrierReleased  = errors.New("barrier released before entry")
	ErrInvalidEvidence  = errors.New("invalid evidence")
	ErrEvidenceOverflow = errors.New("evidence capacity exceeded")
	ErrViolation        = errors.New("conformance violation")
)

// Failure preserves a machine-checkable cause and the field that disagreed.
type Failure struct {
	Rule, Label, Stage, Field string
	Expected, Actual          string
	Err                       error
}

func (f *Failure) Error() string {
	return fmt.Sprintf("%s %s stage=%s field=%s: expected %s, got %s: %v", f.Rule, f.Label, f.Stage, f.Field, f.Expected, f.Actual, f.Err)
}
func (f *Failure) Unwrap() error { return f.Err }
