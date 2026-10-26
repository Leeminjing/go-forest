package activitybridge_test

import (
	"context"
	"errors"
	"testing"

	bridge "github.com/Leeminjing/go-forest/examples/cross-adapter/internal/activitybridge"
	"github.com/Leeminjing/go-forest/examples/cross-adapter/protocol"
	"go.temporal.io/sdk/temporal"
)

func TestFxJoinedDomainFailuresBecomeConcreteNativeFailures(t *testing.T) {
	for _, test := range []struct {
		cause error
		kind  string
	}{
		{protocol.ErrConflict, "InputConflict"},
		{protocol.ErrUnknown, "OutcomeUnknown"},
	} {
		err := bridge.Translate(errors.Join(test.cause, context.Canceled), "operation-1")
		app, ok := err.(*temporal.ApplicationError)
		if !ok || !app.NonRetryable() || app.Type() != test.kind {
			t.Fatalf("native failure lost: %T %v", err, err)
		}
	}
	if err := bridge.Translate(errors.Join(context.Canceled), "operation-1"); !temporal.IsCanceledError(err) {
		t.Fatal(err)
	}
	if bridge.Translate(nil, "operation-1") != nil {
		t.Fatal("successful Fx call classified as cancellation")
	}
}
