package statemachine

import (
	m "github.com/Leeminjing/go-forest/tests/internal/topologymodel"
	"testing"
)

func TestModelAtomicPublicAPI(t *testing.T) {
	h := newHarness(t, Catalog(), []string{"p", "q", "c"})
	h.step(m.Plan{Ops: []m.Op{{Kind: "add-root", Tree: "p", Definition: "provider-v1"}, {Kind: "add-root", Tree: "q", Definition: "provider-v2"}, {Kind: "add-root", Tree: "c", Definition: "runtime-required"}, {Kind: "link", Binding: m.Binding{ID: "l", Consumer: "c", Port: "in", Provider: "p", Capability: "value"}}}})
	h.step(m.Plan{Base: 1, Ops: []m.Op{{Kind: "unlink", Binding: m.Binding{ID: "l"}}}})
	h.step(m.Plan{Base: 1, Ops: []m.Op{{Kind: "rebind", Binding: m.Binding{ID: "l", Consumer: "c", Port: "in", Provider: "q", Capability: "value"}}, {Kind: "remove", Tree: "p"}}})
}
