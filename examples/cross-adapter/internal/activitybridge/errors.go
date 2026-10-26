package activitybridge

import (
	"context"
	"errors"

	ff "github.com/Leeminjing/go-forest/adapters/fx"
	gf "github.com/Leeminjing/go-forest/adapters/gorm"
	"github.com/Leeminjing/go-forest/examples/cross-adapter/protocol"
	"go.temporal.io/sdk/temporal"
)

// Translate is the last Activity boundary, after Fx has returned and joined.
// Temporal's failure converter needs the concrete native error, not a Join.
func Translate(err error, operation string) error {
	if err == nil {
		return nil
	}
	if errors.Is(err, protocol.ErrUnknown) || errors.Is(err, gf.ErrCommitUnknown) {
		return temporal.NewNonRetryableApplicationError("reconcile operation before retry", "OutcomeUnknown", nil, operation)
	}
	if errors.Is(err, protocol.ErrConflict) {
		return temporal.NewNonRetryableApplicationError("operation input conflict", "InputConflict", nil, operation)
	}
	if errors.Is(err, ff.ErrUnavailable) || errors.Is(err, gf.ErrUnavailable) {
		return temporal.NewApplicationError("dependency not admitted", "DependencyUnavailable", nil, operation)
	}
	var app *temporal.ApplicationError
	if errors.As(err, &app) {
		return app
	}
	var cancelled *temporal.CanceledError
	if errors.As(err, &cancelled) {
		return cancelled
	}
	var timeout *temporal.TimeoutError
	if errors.As(err, &timeout) {
		return timeout
	}
	if errors.Is(err, context.Canceled) {
		return temporal.NewCanceledError()
	}
	return temporal.NewApplicationError("operation did not complete", "OperationFailed", nil, operation)
}
