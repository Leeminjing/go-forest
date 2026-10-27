package topology

import (
	"context"
	"strings"
	"testing"

	"github.com/Leeminjing/go-forest/forest"
)

type number interface{ Number() int }
type value int

func (v value) Number() int { return int(v) }

func TestManifestPreservesBindingIdentityAndRelativeOrder(t *testing.T) {
	d := forest.Definition{TypeID: "number", Version: "1", Capabilities: []forest.CapabilityDescriptor{forest.Capability[number]("number", forest.Public)}, Build: func(_ context.Context, e *forest.BuildEnv) (forest.Lifecycle, error) {
		return forest.Hooks{}, forest.Provide(e, "number", value(7))
	}}
	c := forest.Definition{TypeID: "consumer", Version: "1", Ports: []forest.PortDescriptor{forest.Port[number]("numbers", forest.Many())}, Build: func(context.Context, *forest.BuildEnv) (forest.Lifecycle, error) { return forest.Hooks{}, nil }}
	catalog, err := NewCatalog([]Registration{{Definition: d, ContractKey: "number/1", ImplementationDigest: strings.Repeat("a", 64)}, {Definition: c, ContractKey: "consumer/1", ImplementationDigest: strings.Repeat("b", 64)}})
	if err != nil {
		t.Fatal(err)
	}
	f := forest.New(forest.Options{})
	defer f.Close(context.Background())
	for _, def := range []forest.Definition{d, c} {
		if err = f.Register(def); err != nil {
			t.Fatal(err)
		}
	}
	_, err = f.Apply(context.Background(), forest.ChangePlan{Operations: []forest.Operation{forest.AddRoot("p", d.Ref()), forest.AddRoot("q", d.Ref()), forest.AddRoot("c", c.Ref()), forest.Connect(forest.Link{ID: "discarded", Consumer: "c", Port: "numbers", Provider: "p", Capability: "number"}), forest.Unlink("discarded"), forest.Connect(forest.Link{ID: "second", Consumer: "c", Port: "numbers", Provider: "q", Capability: "number"}), forest.Connect(forest.Link{ID: "first", Consumer: "c", Port: "numbers", Provider: "p", Capability: "number"})}})
	if err != nil {
		t.Fatal(err)
	}
	m, err := Capture(f.Snapshot(), catalog)
	if err != nil {
		t.Fatal(err)
	}
	if len(m.Bindings) != 2 || m.Bindings[0].ID != "second" || m.Bindings[0].Ordinal != 1 || m.Bindings[1].ID != "first" || m.Bindings[1].Ordinal != 2 {
		t.Fatalf("binding order: %+v", m.Bindings)
	}
	before, _ := Digest(m)
	m.Trees[0].ID = "mutated"
	again, err := Capture(f.Snapshot(), catalog)
	if err != nil {
		t.Fatal(err)
	}
	after, _ := Digest(again)
	if before != after {
		t.Fatal("capture leaked mutable topology")
	}
}

func TestDecodeRejectsAmbiguousAndUnsupportedInput(t *testing.T) {
	for _, data := range []string{`{"version":1,"version":1,"trees":[],"bindings":[]}`, `{"version":2,"trees":[],"bindings":[]}`, `{"version":1,"trees":[],"bindings":[],"extra":true}`, `{"version":1,"trees":[],"bindings":[]} {}`} {
		if _, err := Decode([]byte(data)); err == nil {
			t.Fatalf("accepted %s", data)
		}
	}
	m, err := Decode([]byte(`{"version":1,"trees":[],"bindings":[]}`))
	if err != nil {
		t.Fatal(err)
	}
	data, err := Encode(m)
	if err != nil || strings.Contains(string(data), "null") {
		t.Fatalf("%s: %v", data, err)
	}
}
