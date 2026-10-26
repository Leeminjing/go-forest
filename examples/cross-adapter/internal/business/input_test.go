package business_test

import (
	"testing"

	"github.com/Leeminjing/go-forest/examples/cross-adapter/internal/business"
	"github.com/Leeminjing/go-forest/examples/cross-adapter/protocol"
)

func TestInputRejectsValuesPostgresTextCannotStore(t *testing.T) {
	in := protocol.Request{OperationID: "op", Target: "tree", Payload: "oak\x00elm"}
	if business.Valid(in) {
		t.Fatal("NUL payload accepted")
	}
}
