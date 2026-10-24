// Package forest composes domain-independent trees through versioned changes.
package forest

import (
	"errors"
	"fmt"
)

var (
	ErrGenerationConflict    = errors.New("generation conflict")
	ErrNestCycle             = errors.New("nest cycle")
	ErrInvalidLink           = errors.New("invalid link")
	ErrPortAlreadyBound      = errors.New("port already bound")
	ErrMissingCapability     = errors.New("missing capability")
	ErrContractMismatch      = errors.New("contract mismatch")
	ErrDependencyInUse       = errors.New("dependency in use")
	ErrPrepareFailed         = errors.New("prepare failed")
	ErrBindingUnavailable    = errors.New("binding unavailable")
	ErrReparentUnsupported   = errors.New("reparent unsupported")
	ErrUnsupportedHotReplace = errors.New("unsupported hot replacement")
	ErrScopeClosed           = errors.New("scope closed")
	ErrDrainTimeout          = errors.New("drain timeout")
	ErrClosed                = errors.New("forest closed")
	ErrInvalidDefinition     = errors.New("invalid definition")
	ErrInvalidOperation      = errors.New("invalid operation")
	ErrNotFound              = errors.New("not found")
	ErrEventGap              = errors.New("event gap; read a fresh snapshot")
)

// Error preserves machine-readable causes and operation context.
type Error struct {
	Stage string
	Tree  TreeID
	Port  PortID
	Cause error
}

func (e *Error) Error() string {
	return fmt.Sprintf("forest %s tree=%q port=%q: %v", e.Stage, e.Tree, e.Port, e.Cause)
}
func (e *Error) Unwrap() error { return e.Cause }

// GenerationConflict identifies both versions without parsing an error string.
type GenerationConflict struct{ Expected, Actual uint64 }

func (e *GenerationConflict) Error() string {
	return fmt.Sprintf("%v: expected %d, actual %d", ErrGenerationConflict, e.Expected, e.Actual)
}
func (e *GenerationConflict) Unwrap() error { return ErrGenerationConflict }

func failure(stage string, tree TreeID, port PortID, cause error) error {
	return &Error{Stage: stage, Tree: tree, Port: port, Cause: cause}
}
