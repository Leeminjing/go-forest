// codec-golden records outputs from the actual fixed public release. It must
// not be used to refresh expectations against a candidate implementation.
package main

import (
	"encoding/json"
	"os"
	"runtime/debug"
	"strings"

	"github.com/Leeminjing/go-forest/extensions/topology"
	"github.com/Leeminjing/go-forest/forest"
)

func main() {
	info, ok := debug.ReadBuildInfo()
	if !ok {
		panic("missing build info")
	}
	found := false
	for _, d := range info.Deps {
		if d.Path == "github.com/Leeminjing/go-forest/extensions/topology" {
			if d.Version != "v0.2.1" || d.Replace != nil {
				panic("goldens require untouched published v0.2.1")
			}
			found = true
		}
	}
	if !found {
		panic("missing released dependency")
	}
	makeTree := func(id, parent forest.TreeID) topology.Tree {
		return topology.Tree{ID: id, ParentID: parent, Definition: forest.DefinitionRef{TypeID: "clock", Version: "1"}, ContractKey: "契约/<>&\"", ImplementationDigest: strings.Repeat("a", 64)}
	}
	m := topology.Empty()
	m.Trees = []topology.Tree{makeTree("z", ""), makeTree("b 子", "a<&\""), makeTree("a<&\"", "")}
	m.Bindings = []topology.Binding{{ID: "link", Consumer: "z", Port: "many", Provider: "a<&\"", Capability: "cap", Kind: "link", Ordinal: 2}, {ID: "nest", Consumer: "b 子", Port: "parent", Provider: "a<&\"", Capability: "cap", Kind: "nest", Ordinal: 1}}
	inputs := []topology.Manifest{topology.Empty(), {Version: 1}, m}
	type sample struct {
		Input, Normalized topology.Manifest
		Encoding, Digest  string
	}
	rows := []sample{}
	for _, m := range inputs {
		n, e := topology.Normalize(m)
		must(e)
		b, e := topology.Encode(m)
		must(e)
		d, e := topology.Digest(m)
		must(e)
		rows = append(rows, sample{m, n, string(b), d})
	}
	e := json.NewEncoder(os.Stdout)
	e.SetIndent("", "  ")
	must(e.Encode(struct {
		Release, Go string
		Samples     []sample
	}{"v0.2.1", info.GoVersion, rows}))
}
func must(e error) {
	if e != nil {
		panic(e)
	}
}
