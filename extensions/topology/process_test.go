package topology

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/Leeminjing/go-forest/forest"
)

type fileCapability interface {
	Read(context.Context) (string, error)
}
type fileValue struct{ file *os.File }

func (v *fileValue) Read(context.Context) (string, error) {
	var b [256]byte
	n, e := v.file.ReadAt(b[:], 0)
	if n > 0 {
		return string(b[:n]), nil
	}
	return "", e
}

func fileCatalog(directory string, stage func(string)) (*Catalog, Manifest, error) {
	d := forest.Definition{TypeID: "file", Version: "1", HotSwappable: true, Capabilities: []forest.CapabilityDescriptor{forest.Capability[fileCapability]("file", forest.Public)}, Build: func(_ context.Context, env *forest.BuildEnv) (forest.Lifecycle, error) {
		file, e := os.CreateTemp(directory, "physical-")
		if e != nil {
			return nil, e
		}
		id, e := randomID()
		if e == nil {
			_, e = file.WriteString(id)
		}
		if e == nil {
			e = file.Sync()
		}
		if e != nil {
			file.Close()
			return nil, e
		}
		if stage != nil {
			stage("build")
		}
		e = forest.Provide(env, "file", &fileValue{file})
		if e != nil {
			file.Close()
			return nil, e
		}
		return forest.Hooks{OnStart: func(context.Context) error {
			if stage != nil {
				stage("start")
			}
			return nil
		}, OnReady: func(context.Context) error {
			if stage != nil {
				stage("ready")
			}
			return nil
		}, OnStop: func(context.Context) error { return file.Close() }}, nil
	}}
	r := Registration{Definition: d, ContractKey: "file/1", ImplementationDigest: strings.Repeat("c", 64)}
	catalog, e := NewCatalog([]Registration{r})
	m := Empty()
	m.Trees = append(m.Trees, Tree{ID: "file", Definition: d.Ref(), ContractKey: r.ContractKey, ImplementationDigest: r.ImplementationDigest})
	return catalog, m, e
}

type processEvidence struct {
	Stage      string
	PID        int
	SessionID  string
	Generation uint64
	Marker     string
	Race       bool
	Pending    bool
	StoreID    string
}

type crashJournalFile struct {
	journalFile
	stage  string
	report func(string)
}

func (f *crashJournalFile) Write(data []byte) (int, error) {
	if len(data) >= 48 && string(data[:8]) == frameMagic {
		var r record
		_ = json.Unmarshal(data[16:len(data)-32], &r)
		if r.Kind == "intent" {
			if f.stage == "intent-partial" {
				n, e := f.journalFile.Write(data[:8])
				if e == nil {
					f.report("intent-partial")
				}
				return n, e
			}
			if f.stage == "intent-written" {
				n, e := f.journalFile.Write(data)
				if e == nil {
					f.report("intent-written")
				}
				return n, e
			}
		}
	}
	return f.journalFile.Write(data)
}

func TestTopologyWorker(t *testing.T) {
	directory := os.Getenv("FOREST_TOPOLOGY_WORKER")
	if directory == "" {
		t.Skip("supervised subprocess role")
	}
	stage := os.Getenv("FOREST_TOPOLOGY_STAGE")
	resources := filepath.Join(directory, "resources")
	s, e := Open(directory, Options{})
	if e != nil {
		t.Fatal(e)
	}
	f := forest.New(forest.Options{})
	var c *Controller
	report := func(name string) {
		if name != stage {
			return
		}
		session := ""
		if c != nil {
			session = c.session
		}
		data, _ := json.Marshal(processEvidence{Stage: name, PID: os.Getpid(), SessionID: session, Generation: f.Snapshot().Generation, Race: raceBuild})
		fmt.Println("TOPOLOGY_EVENT " + string(data))
		var one [1]byte
		_, _ = os.Stdin.Read(one[:])
		t.Fatal("supervisor released kill barrier")
	}
	catalog, _, e := fileCatalog(resources, report)
	if e != nil {
		t.Fatal(e)
	}
	c, e = NewController(f, s, catalog)
	if e != nil {
		t.Fatal(e)
	}
	c.barrier = report
	if stage == "intent-partial" || stage == "intent-written" {
		s.file = &crashJournalFile{journalFile: s.file, stage: stage, report: report}
	}
	id, _ := randomID()
	_, e = c.Restore(context.Background(), Request{ID: id, Expected: s.Inspect().Position, Approval: "supervisor waited for prior process; resources reviewed"})
	if e != nil {
		t.Fatal(e)
	}
	if e = c.OpenAdmission(context.Background()); e != nil {
		t.Fatal(e)
	}
	var marker string
	e = c.Invoke(context.Background(), func(ctx context.Context) error {
		return forest.Use[fileCapability](ctx, f, "file", "file", func(v fileCapability) error { var err error; marker, err = v.Read(ctx); return err })
	})
	if e != nil {
		t.Fatal(e)
	}
	evidence := processEvidence{Stage: "done", PID: os.Getpid(), SessionID: c.session, Generation: f.Snapshot().Generation, Marker: marker, Race: raceBuild}
	evidence.StoreID = s.Inspect().Position.StoreID
	if e = c.Close(context.Background()); e != nil {
		t.Fatal(e)
	}
	data, _ := json.Marshal(evidence)
	fmt.Println("TOPOLOGY_EVENT " + string(data))
}

func supervise(t *testing.T, directory, stage string, kill bool) processEvidence {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	executable, e := os.Executable()
	if e != nil {
		t.Fatal(e)
	}
	cmd := exec.CommandContext(ctx, executable, "-test.run=^TestTopologyWorker$", "-test.v")
	cmd.Env = append(os.Environ(), "FOREST_TOPOLOGY_WORKER="+directory, "FOREST_TOPOLOGY_STAGE="+stage, "GORACE=atexit_sleep_ms=0")
	hideWorker(cmd)
	stdout, e := cmd.StdoutPipe()
	if e != nil {
		t.Fatal(e)
	}
	stdin, e := cmd.StdinPipe()
	if e != nil {
		t.Fatal(e)
	}
	defer stdin.Close()
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	if e = cmd.Start(); e != nil {
		t.Fatal(e)
	}
	joined := false
	defer func() {
		if !joined {
			cmd.Process.Kill()
			cmd.Wait()
		}
	}()
	events := make(chan processEvidence, 1)
	scanDone := make(chan error, 1)
	go func() {
		scanner := bufio.NewScanner(stdout)
		for scanner.Scan() {
			line := scanner.Text()
			if strings.HasPrefix(line, "TOPOLOGY_EVENT ") {
				var evidence processEvidence
				if err := json.Unmarshal([]byte(strings.TrimPrefix(line, "TOPOLOGY_EVENT ")), &evidence); err != nil {
					scanDone <- err
					return
				}
				events <- evidence
			}
		}
		scanDone <- scanner.Err()
	}()
	var evidence processEvidence
	select {
	case evidence = <-events:
	case err := <-scanDone:
		select {
		case evidence = <-events:
			scanDone <- err
		default:
			cmd.Process.Kill()
			cmd.Wait()
			joined = true
			t.Fatalf("worker exited before barrier: %v %s", err, stderr.String())
		}
	case <-ctx.Done():
		cmd.Process.Kill()
		cmd.Wait()
		joined = true
		t.Fatal("worker barrier deadline")
	}
	if evidence.PID != cmd.Process.Pid || evidence.Race != raceBuild {
		t.Fatal("worker evidence identity/instrumentation mismatch")
	}
	if kill {
		if e = cmd.Process.Kill(); e != nil {
			t.Fatal(e)
		}
	}
	e = cmd.Wait()
	joined = true
	if !kill && e != nil {
		t.Fatalf("worker failed: %v %s", e, stderr.String())
	}
	if kill && e == nil {
		t.Fatal("worker was not killed")
	}
	if strings.Contains(stderr.String(), "DATA RACE") {
		t.Fatalf("child race: %s", stderr.String())
	}
	select {
	case err := <-scanDone:
		if err != nil {
			t.Fatal(err)
		}
	case <-ctx.Done():
		t.Fatal("worker output did not join")
	}
	return evidence
}

func TestKillWaitRecoveryRequiresExplicitIntentResolution(t *testing.T) {
	for _, stage := range []string{"before-intent", "intent-partial", "intent-written", "intent-synced", "build", "start", "ready", "apply-returned", "observation-synced"} {
		t.Run(stage, func(t *testing.T) {
			directory := t.TempDir()
			if e := os.Mkdir(filepath.Join(directory, "resources"), 0700); e != nil {
				t.Fatal(e)
			}
			_, m, e := fileCatalog(filepath.Join(directory, "resources"), nil)
			if e != nil {
				t.Fatal(e)
			}
			s, e := Open(directory, Options{})
			if e != nil {
				t.Fatal(e)
			}
			_, e = s.Seed(context.Background(), Request{ID: "seed", Expected: s.Inspect().Position, Approval: "initial"}, m)
			if e != nil {
				t.Fatal(e)
			}
			if e = s.Close(); e != nil {
				t.Fatal(e)
			}
			killed := supervise(t, directory, stage, true)
			oldMarkers := map[string]bool{}
			files, err := os.ReadDir(filepath.Join(directory, "resources"))
			if err != nil {
				t.Fatal(err)
			}
			for _, file := range files {
				b, e := os.ReadFile(filepath.Join(directory, "resources", file.Name()))
				if e != nil {
					t.Fatal(e)
				}
				oldMarkers[string(b)] = true
			}
			s, e = Open(directory, Options{})
			if stage == "intent-partial" {
				var detail *Error
				if !errors.Is(e, ErrCorruptJournal) || !errors.As(e, &detail) || detail.Prefix == nil || detail.Prefix.Desired == nil {
					t.Fatalf("partial process write: %v", e)
				}
				oldStore := detail.Prefix.Position.StoreID
				newDirectory := t.TempDir()
				if err = os.Mkdir(filepath.Join(newDirectory, "resources"), 0700); err != nil {
					t.Fatal(err)
				}
				s, err = Open(newDirectory, Options{})
				if err != nil {
					t.Fatal(err)
				}
				_, err = s.Seed(context.Background(), Request{ID: "host-import", Expected: s.Inspect().Position, Approval: "preserve damaged original; Kill+Wait and external resource review; explicitly choose verified previous topology"}, detail.Prefix.Desired.Manifest)
				if err != nil {
					t.Fatal(err)
				}
				if err = s.Close(); err != nil {
					t.Fatal(err)
				}
				restored := supervise(t, newDirectory, "", false)
				if restored.StoreID == oldStore || restored.SessionID == killed.SessionID {
					t.Fatal("damaged store was silently repaired or physical session reused")
				}
				t.Logf("partial-record Kill+Wait: preservedStore=%s newStore=%s oldPID=%d newPID=%d", oldStore, restored.StoreID, killed.PID, restored.PID)
				return
			}
			if e != nil {
				t.Fatal(e)
			}
			pending := s.Inspect().Pending
			if stage == "before-intent" {
				if pending != nil {
					t.Fatal("intent invented before write")
				}
			} else if stage == "observation-synced" {
				if pending != nil || s.Inspect().LastObserved == nil {
					t.Fatal("durable observation lost")
				}
			} else {
				if pending == nil {
					t.Fatal("missing pending intent")
				}
				catalog, _, _ := fileCatalog(filepath.Join(directory, "resources"), nil)
				if _, e = NewController(forest.New(forest.Options{}), s, catalog); e != ErrReconciliationRequired {
					t.Fatalf("unreviewed restore: %v", e)
				}
				_, e = s.Resolve(context.Background(), Request{ID: "resolve", Expected: s.Inspect().Position, Approval: "Kill+Wait completed; resources checked"}, pending.Request.ID, "target")
				if e != nil {
					t.Fatal(e)
				}
			}
			if e = s.Close(); e != nil {
				t.Fatal(e)
			}
			restored := supervise(t, directory, "", false)
			if restored.SessionID == killed.SessionID || restored.Generation != 1 || len(restored.Marker) != 32 || oldMarkers[restored.Marker] {
				t.Fatalf("not a fresh physical restore: %+v", restored)
			}
			t.Logf("Kill+Wait stage=%s store=%s oldPID=%d oldSession=%s newPID=%d newSession=%s generation=%d physicalMarker=%s race=%t", stage, restored.StoreID, killed.PID, killed.SessionID, restored.PID, restored.SessionID, restored.Generation, restored.Marker, restored.Race)
		})
	}
}
