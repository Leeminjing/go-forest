package topology

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Leeminjing/go-forest/forest"
)

type nativeProbe struct {
	file   *os.File
	marker string
	closed chan struct{}
}
type probes struct {
	mu  sync.Mutex
	all map[string]*nativeProbe
}

func (p *probes) add(f *os.File, marker string) *nativeProbe {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.all == nil {
		p.all = map[string]*nativeProbe{}
	}
	probe := &nativeProbe{f, marker, make(chan struct{})}
	p.all[marker] = probe
	return probe
}
func (p *probes) get(marker string) *nativeProbe {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.all[marker]
}
func (p *probes) definition(directory, version string) forest.Definition {
	return forest.Definition{TypeID: "resource", Version: version, HotSwappable: true, PreserveChildren: true, Reparentable: true, ExportChildren: true, Capabilities: []forest.CapabilityDescriptor{forest.Capability[fileCapability]("file", forest.Public|forest.Descendants|forest.Ancestors)}, Build: func(_ context.Context, env *forest.BuildEnv) (forest.Lifecycle, error) {
		f, e := os.CreateTemp(directory, "native-")
		if e != nil {
			return nil, e
		}
		id, e := randomID()
		marker := version + ":" + id
		if e == nil {
			_, e = f.WriteString(marker)
		}
		if e == nil {
			e = f.Sync()
		}
		if e != nil {
			f.Close()
			return nil, e
		}
		probe := p.add(f, marker)
		e = forest.Provide(env, "file", &fileValue{f})
		return forest.Hooks{OnStop: func(context.Context) error { e := f.Close(); close(probe.closed); return e }}, e
	}}
}

type readAll interface {
	ReadAll(context.Context) ([]string, error)
}
type readFunc func(context.Context) ([]string, error)

func (f readFunc) ReadAll(ctx context.Context) ([]string, error) { return f(ctx) }
func phaseReader(p *probes, phase forest.Phase, calls *atomic.Int32) forest.Definition {
	return forest.Definition{TypeID: fmt.Sprintf("reader-%d", phase), Version: "1", HotSwappable: true, PreserveChildren: true, ExportChildren: true, Ports: []forest.PortDescriptor{forest.Port[fileCapability]("files", forest.Required(), forest.Many(), forest.During(phase))}, Capabilities: []forest.CapabilityDescriptor{forest.Capability[readAll]("read", forest.Public)}, Build: func(ctx context.Context, env *forest.BuildEnv) (forest.Lifecycle, error) {
		ref, e := forest.Reference[fileCapability](env, "files")
		if e != nil {
			return nil, e
		}
		var acquired []string
		read := readFunc(func(call context.Context) ([]string, error) {
			var values []string
			e := ref.UseAll(call, func(files []fileCapability) error {
				for _, f := range files {
					v, e := f.Read(call)
					if e != nil {
						return e
					}
					values = append(values, v)
				}
				return nil
			})
			return values, e
		})
		check := func(call context.Context) error {
			values, e := read(call)
			if e == nil {
				acquired = values
				calls.Add(1)
			}
			return e
		}
		if phase == forest.Construction {
			if e = check(ctx); e != nil {
				return nil, e
			}
		}
		if e = forest.Provide(env, "read", read); e != nil {
			return nil, e
		}
		return forest.Hooks{OnStart: func(call context.Context) error {
			if phase == forest.Startup {
				return check(call)
			}
			return nil
		}, OnStop: func(context.Context) error {
			if phase != forest.Runtime {
				for _, marker := range acquired {
					probe := p.get(marker)
					if probe == nil {
						return errors.New("missing native dependency probe")
					}
					var one [1]byte
					if _, e := probe.file.ReadAt(one[:], 0); e != nil {
						return fmt.Errorf("dependency closed before consumer Stop: %w", e)
					}
				}
			}
			return nil
		}}, nil
	}}
}
func managed(t *testing.T, catalog *Catalog, m Manifest) (*Controller, *Store, *forest.Forest, Receipt) {
	t.Helper()
	ctx := context.Background()
	s, e := Open(t.TempDir(), Options{})
	if e != nil {
		t.Fatal(e)
	}
	seed, e := s.Seed(ctx, Request{ID: "seed", Expected: s.Inspect().Position, Approval: "initial"}, m)
	if e != nil {
		t.Fatal(e)
	}
	f := forest.New(forest.Options{})
	c, e := NewController(f, s, catalog)
	if e != nil {
		t.Fatal(e)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if e := c.Close(ctx); e != nil {
			t.Error(e)
		}
	})
	if _, e = c.Restore(ctx, Request{ID: "boot", Expected: s.Inspect().Position, Approval: "explicit resource recreation"}); e != nil {
		t.Fatal(e)
	}
	return c, s, f, seed
}

func TestRestoreAndRollbackPreserveNestOrderAndAllDependencyPhases(t *testing.T) {
	directory := t.TempDir()
	p := &probes{}
	var construction, startup, runtime atomic.Int32
	registrations := []Registration{}
	for _, d := range []forest.Definition{p.definition(directory, "1"), p.definition(directory, "2"), phaseReader(p, forest.Construction, &construction), phaseReader(p, forest.Startup, &startup), phaseReader(p, forest.Runtime, &runtime)} {
		registrations = append(registrations, Registration{Definition: d, ContractKey: d.TypeID + "/1", ImplementationDigest: strings.Repeat("a", 64)})
	}
	catalog, e := NewCatalog(registrations)
	if e != nil {
		t.Fatal(e)
	}
	m := Empty()
	add := func(id, parent string, ref forest.DefinitionRef) {
		r := catalog.entries[ref]
		m.Trees = append(m.Trees, Tree{ID: forest.TreeID(id), ParentID: forest.TreeID(parent), Definition: ref, ContractKey: r.ContractKey, ImplementationDigest: r.ImplementationDigest})
	}
	add("parent", "", forest.DefinitionRef{TypeID: "resource", Version: "1"})
	add("q", "", forest.DefinitionRef{TypeID: "resource", Version: "1"})
	add("child", "parent", forest.DefinitionRef{TypeID: "reader-1", Version: "1"})
	add("grand", "child", forest.DefinitionRef{TypeID: "reader-2", Version: "1"})
	add("reader", "", forest.DefinitionRef{TypeID: "reader-0", Version: "1"})
	for _, b := range []Binding{{ID: "q-first", Consumer: "reader", Port: "files", Provider: "q", Capability: "file", Kind: "link", Ordinal: 1}, {ID: "construction", Consumer: "child", Port: "files", Provider: "parent", Capability: "file", Kind: "nest", Ordinal: 2}, {ID: "startup", Consumer: "grand", Port: "files", Provider: "parent", Capability: "file", Kind: "nest", Ordinal: 3}, {ID: "parent-last", Consumer: "reader", Port: "files", Provider: "parent", Capability: "file", Kind: "link", Ordinal: 4}} {
		m.Bindings = append(m.Bindings, b)
	}
	m, e = Normalize(m)
	if e != nil {
		t.Fatal(e)
	}
	c, s, f, seed := managed(t, catalog, m)
	if construction.Load() != 1 || startup.Load() != 1 {
		t.Fatalf("phase calls: %d %d", construction.Load(), startup.Load())
	}
	captured, e := Capture(f.Snapshot(), catalog)
	if e != nil {
		t.Fatal(e)
	}
	want, _ := Digest(m)
	got, _ := Digest(captured)
	if got != want {
		t.Fatal("restored manifest differs")
	}
	if e = c.OpenAdmission(context.Background()); e != nil {
		t.Fatal(e)
	}
	read := func(ctx context.Context) ([]string, error) {
		var result []string
		e := forest.Use[readAll](ctx, f, "reader", "read", func(r readAll) error { var err error; result, err = r.ReadAll(ctx); return err })
		return result, e
	}
	var original []string
	if e = c.Invoke(context.Background(), func(ctx context.Context) error { var err error; original, err = read(ctx); return err }); e != nil {
		t.Fatal(e)
	}
	if len(original) != 2 || original[0] == original[1] {
		t.Fatalf("native provider order not distinct: %v", original)
	}
	var qMarker, parentMarker string
	if e = c.Invoke(context.Background(), func(ctx context.Context) error {
		for _, id := range []forest.TreeID{"q", "parent"} {
			if err := forest.Use[fileCapability](ctx, f, id, "file", func(v fileCapability) error {
				marker, err := v.Read(ctx)
				if id == "q" {
					qMarker = marker
				} else {
					parentMarker = marker
				}
				return err
			}); err != nil {
				return err
			}
		}
		return nil
	}); e != nil {
		t.Fatal(e)
	}
	if original[0] != qMarker || original[1] != parentMarker {
		t.Fatalf("actual Many callback order: got %v want [%s %s]", original, qMarker, parentMarker)
	}
	// Change every provider definition. Constructors and startup consumers are
	// rebuilt as well, rather than retained with stale raw resources.
	target := detached(m)
	for i := range target.Trees {
		if target.Trees[i].Definition.TypeID == "resource" {
			target.Trees[i].Definition.Version = "2"
		}
	}
	preview, e := c.Preview(target)
	if e != nil {
		t.Fatal(e)
	}
	_, e = c.Reconfigure(context.Background(), Request{ID: "new", Expected: preview.Expected, Approval: "full graph replacement"}, preview)
	if e != nil {
		t.Fatal(e)
	}
	if e = f.Wait(context.Background()); e != nil {
		t.Fatal(e)
	}
	for _, marker := range original {
		select {
		case <-p.get(marker).closed:
		default:
			t.Fatal("retired dependency still open")
		}
	}
	if construction.Load() != 2 || startup.Load() != 2 {
		t.Fatal("phase consumers were not rebuilt")
	}
	rollback, e := c.PreviewRollback(seed.Revision)
	if e != nil {
		t.Fatal(e)
	}
	_, e = c.Rollback(context.Background(), Request{ID: "back", Expected: rollback.Expected, Approval: "historical structure, fresh resources"}, rollback)
	if e != nil {
		t.Fatal(e)
	}
	if len(s.History()) != 3 || f.Snapshot().Generation != 3 {
		t.Fatal("history/generation rewound")
	}
	if e = c.OpenAdmission(context.Background()); e != nil {
		t.Fatal(e)
	}
	var restored []string
	e = c.Invoke(context.Background(), func(ctx context.Context) error { var err error; restored, err = read(ctx); return err })
	if e != nil {
		t.Fatal(e)
	}
	for i, marker := range restored {
		if !strings.HasPrefix(marker, "1:") || marker == original[i] {
			t.Fatalf("historical object reused: %v", restored)
		}
	}
}

func TestCloseRetainsLockAndRealResourceUntilEnteredCallJoins(t *testing.T) {
	directory := t.TempDir()
	p := &probes{}
	d := p.definition(directory, "1")
	r := Registration{Definition: d, ContractKey: "resource/1", ImplementationDigest: strings.Repeat("e", 64)}
	catalog, _ := NewCatalog([]Registration{r})
	m := Empty()
	m.Trees = append(m.Trees, Tree{ID: "resource", Definition: d.Ref(), ContractKey: r.ContractKey, ImplementationDigest: r.ImplementationDigest})
	c, s, f, _ := managed(t, catalog, m)
	if e := c.OpenAdmission(context.Background()); e != nil {
		t.Fatal(e)
	}
	entered := make(chan string, 1)
	release := make(chan struct{})
	var once sync.Once
	defer once.Do(func() { close(release) })
	done := make(chan error, 1)
	go func() {
		done <- c.Invoke(context.Background(), func(ctx context.Context) error {
			return forest.Use[fileCapability](ctx, f, "resource", "file", func(v fileCapability) error {
				marker, e := v.Read(ctx)
				if e != nil {
					return e
				}
				entered <- marker
				<-release
				again, e := v.Read(ctx)
				if e != nil || again != marker {
					return fmt.Errorf("resource ended before actual join: %v", e)
				}
				return nil
			})
		})
	}()
	var marker string
	select {
	case marker = <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("call did not enter")
	}
	deadline, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	if e := c.Close(deadline); !errors.Is(e, forest.ErrDrainTimeout) {
		t.Fatalf("close: %v", e)
	}
	if e := s.Close(); !errors.Is(e, ErrStoreBusy) {
		t.Fatalf("store ownership released: %v", e)
	}
	if other, e := Open(filepathDir(t, s), Options{}); !errors.Is(e, ErrStoreBusy) {
		if other != nil {
			other.Close()
		}
		t.Fatalf("second owner admitted: %v", e)
	}
	probe := p.get(marker)
	var one [1]byte
	if _, e := probe.file.ReadAt(one[:], 0); e != nil {
		t.Fatal("physical resource freed early")
	}
	if e := c.OpenAdmission(context.Background()); !errors.Is(e, ErrAdmissionClosed) {
		t.Fatalf("closing session reopened: %v", e)
	}
	once.Do(func() { close(release) })
	if e := <-done; e != nil {
		t.Fatal(e)
	}
	if e := c.Close(context.Background()); e != nil {
		t.Fatal(e)
	}
	select {
	case <-probe.closed:
	default:
		t.Fatal("Stop did not close native resource")
	}
	if _, e := probe.file.ReadAt(one[:], 0); !errors.Is(e, os.ErrClosed) {
		t.Fatalf("expected actual file closure: %v", e)
	}
}
func filepathDir(t *testing.T, s *Store) string {
	t.Helper()
	return strings.TrimSuffix(s.lock.Name(), string(os.PathSeparator)+"lock")
}
