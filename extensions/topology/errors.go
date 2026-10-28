// Package topology persists desired Forest structures, not running objects or
// external business effects. See README.md for ownership and durability limits.
package topology

import (
	"context"
	"errors"
	"fmt"

	"github.com/Leeminjing/go-forest/forest"
)

var (
	ErrPlanBlocked            = errors.New("topology plan blocked")
	ErrUnsupportedPlanVersion = errors.New("unsupported topology plan version")
	ErrInvalidManifest        = errors.New("invalid topology manifest")
	ErrCorruptJournal         = errors.New("corrupt topology journal")
	ErrUnsupportedFormat      = errors.New("unsupported topology format")
	ErrUnsupportedPlatform    = errors.New("unsupported topology storage platform")
	ErrStoreBusy              = errors.New("topology store owned by another session")
	ErrHistoryConflict        = errors.New("topology history conflict")
	ErrRequestConflict        = errors.New("topology request identity conflict")
	ErrDefinitionUnavailable  = errors.New("topology definition unavailable")
	ErrDefinitionMismatch     = errors.New("topology definition identity mismatch")
	ErrReconciliationRequired = errors.New("topology intent requires host reconciliation")
	ErrPersistenceUnknown     = errors.New("topology persistence outcome unknown")
	ErrAdmissionClosed        = errors.New("topology managed admission closed")
	ErrManagedForestDiverged  = errors.New("managed Forest differs from observed history")
	ErrOperationFailed        = errors.New("recorded topology operation failed")
	ErrNestedScope            = errors.New("topology management requires a fresh top-level context")
)

// Error retains machine-checkable causes and the failed operation stage.
type Error struct {
	Stage       string
	Sequence    uint64
	Offset      int64
	OperationID string
	TreeID      string
	Cause       error
	// Prefix is an isolated, incomplete diagnostic only. It is not a valid head.
	Prefix *Inspection
}

func (e *Error) Error() string {
	return fmt.Sprintf("topology %s (record %d, offset %d, operation %q, tree %q): %v", e.Stage, e.Sequence, e.Offset, e.OperationID, e.TreeID, e.Cause)
}
func (e *Error) Unwrap() error { return e.Cause }

// Stable failure codes preserve standard native classifications across a
// process boundary without serializing arbitrary error values or messages.
var nativeCodes = []struct {
	code string
	err  error
}{
	{"prepare", forest.ErrPrepareFailed}, {"hot-replace", forest.ErrUnsupportedHotReplace}, {"contract", forest.ErrContractMismatch}, {"generation", forest.ErrGenerationConflict}, {"nest-cycle", forest.ErrNestCycle}, {"invalid-link", forest.ErrInvalidLink}, {"port-bound", forest.ErrPortAlreadyBound}, {"missing-capability", forest.ErrMissingCapability}, {"dependency-in-use", forest.ErrDependencyInUse}, {"binding-unavailable", forest.ErrBindingUnavailable}, {"reparent", forest.ErrReparentUnsupported}, {"scope-closed", forest.ErrScopeClosed}, {"drain", forest.ErrDrainTimeout}, {"closed", forest.ErrClosed}, {"definition", forest.ErrInvalidDefinition}, {"operation", forest.ErrInvalidOperation}, {"not-found", forest.ErrNotFound}, {"canceled", context.Canceled}, {"deadline", context.DeadlineExceeded},
}

func failureCodes(cause error) []string {
	codes := []string{}
	for _, item := range nativeCodes {
		if errors.Is(cause, item.err) {
			codes = append(codes, item.code)
		}
	}
	if len(codes) == 0 {
		codes = append(codes, "native")
	}
	return codes
}
func validFailureCodes(codes []string) bool {
	if len(codes) == 0 {
		return false
	}
	seen := map[string]bool{}
	for _, code := range codes {
		known := code == "native"
		for _, item := range nativeCodes {
			if item.code == code {
				known = true
			}
		}
		if !known || seen[code] {
			return false
		}
		seen[code] = true
	}
	return true
}
