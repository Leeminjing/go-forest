package clockwork

import (
	"time"

	"github.com/Leeminjing/go-forest/examples/trees/clock"
	"github.com/Leeminjing/go-forest/examples/trees/formatter"
	"github.com/Leeminjing/go-forest/forest"
	"github.com/Leeminjing/go-forest/forest/compose"
)

// Register installs the library's existing implementations once per Forest.
// Subsequent placements share these definitions, not their physical instances.
func Register(f *forest.Forest) error {
	c := clock.Definition("1", func() time.Time { return time.Date(2026, 10, 24, 8, 0, 0, 0, time.UTC) })
	format := formatter.Definition()
	format.TypeID = "example.clockwork.formatter"
	format.Capabilities[0].Visibility |= forest.Ancestors
	for _, d := range []forest.Definition{definition(), c, format} {
		if err := f.Register(d); err != nil {
			return err
		}
	}
	return nil
}

// Blueprint can itself be nested as a Composite in another library's template.
func Blueprint() (*compose.Blueprint, error) {
	return compose.New(compose.Spec{Root: forest.DefinitionRef{TypeID: "example.clockwork", Version: "1"},
		Trees:    []compose.Tree{{ID: "clock", Definition: forest.DefinitionRef{TypeID: "example.clock", Version: "1"}}, {ID: "formatter", Definition: forest.DefinitionRef{TypeID: "example.clockwork.formatter", Version: "1"}}},
		Bindings: []forest.Link{{ID: "formatter", Port: "formatter", Provider: "formatter", Capability: "format"}, {ID: "clock", Consumer: "formatter", Port: "clock", Provider: "clock", Capability: "time"}}})
}
