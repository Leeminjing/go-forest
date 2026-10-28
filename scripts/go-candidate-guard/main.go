// go-candidate-guard is a test-host executable, never Forest runtime code. It
// validates the effective Core module before forwarding a build to the real Go.
package main

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"time"
)

type module struct {
	Path, Dir string
	Main      bool
	Replace   *module
}
type proof struct {
	Directory string
	Arguments []string
	Core      *module
	UsesCore  bool
	Error     string
	PID       int
}

func main() {
	real, root, logs := os.Getenv("FOREST_CANDIDATE_REAL_GO"), os.Getenv("FOREST_CANDIDATE_ROOT"), os.Getenv("FOREST_CANDIDATE_LOGS")
	if real == "" || root == "" || logs == "" {
		fmt.Fprintln(os.Stderr, "candidate Go guard requires explicit host configuration")
		os.Exit(2)
	}
	p := proof{Arguments: os.Args[1:], PID: os.Getpid()}
	p.Directory, _ = os.Getwd()
	verify := func() error {
		raw, e := os.ReadFile(os.Getenv("FOREST_CANDIDATE_MANIFEST"))
		if e != nil {
			return e
		}
		var hashes map[string]string
		if e = json.Unmarshal(raw, &hashes); e != nil {
			return e
		}
		for name, expected := range hashes {
			raw, e := os.ReadFile(filepath.Join(root, filepath.FromSlash(name)))
			if e != nil {
				return e
			}
			sum := sha256.Sum256(raw)
			if hex.EncodeToString(sum[:]) != expected {
				return fmt.Errorf("candidate source changed: %s", name)
			}
		}
		args := []string{"list", "-m", "-json"}
		for i, a := range os.Args[1:] {
			if len(a) > 9 && a[:9] == "-modfile=" {
				args = append(args, a)
			} else if a == "-modfile" && i+2 < len(os.Args) {
				args = append(args, a, os.Args[i+2])
			}
		}
		args = append(args, "github.com/Leeminjing/go-forest")
		query := exec.Command(real, args...)
		raw, e = query.Output()
		if e != nil {
			// A stdlib-only module, such as TestKit, has no Core to replace. Prove
			// absence using the complete module list rather than swallowing an error.
			all, e2 := exec.Command(real, "list", "-m", "-json", "all").Output()
			if e2 != nil {
				return fmt.Errorf("Core resolution failed: %w", e)
			}
			found, decodeErr := containsCore(all)
			if decodeErr != nil {
				return decodeErr
			}
			if found {
				return fmt.Errorf("Core resolution failed in a Core-using module: %w", e)
			}
			return nil
		}
		var selected module
		if e = json.Unmarshal(raw, &selected); e != nil {
			return e
		}
		p.Core = &selected
		p.UsesCore = true
		dir := selected.Dir
		if selected.Replace != nil {
			dir = selected.Replace.Dir
		}
		actual, e := os.Stat(dir)
		if e != nil {
			return e
		}
		wanted, e := os.Stat(root)
		if e != nil {
			return e
		}
		if !os.SameFile(actual, wanted) {
			return fmt.Errorf("build would use an old Core at %s", dir)
		}
		return nil
	}
	args := os.Args[1:]
	compiles := len(args) > 0 && (args[0] == "test" || args[0] == "build" || args[0] == "run" || args[0] == "vet" || args[0] == "list")
	var err error
	if compiles {
		err = verify()
		if err != nil {
			p.Error = err.Error()
		}
	}
	_ = os.MkdirAll(logs, 0700)
	file, e := os.CreateTemp(logs, fmt.Sprintf("go-%d-%d-*.json", os.Getpid(), time.Now().UnixNano()))
	if e != nil {
		fmt.Fprintln(os.Stderr, e)
		os.Exit(2)
	}
	e = json.NewEncoder(file).Encode(p)
	closeErr := file.Close()
	if e != nil || closeErr != nil {
		fmt.Fprintln(os.Stderr, "candidate proof could not be saved")
		os.Exit(2)
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(2)
	}
	command := exec.Command(real, args...)
	command.Stdin, command.Stdout, command.Stderr = os.Stdin, os.Stdout, os.Stderr
	if err = command.Run(); err != nil {
		if exit, ok := err.(*exec.ExitError); ok {
			os.Exit(exit.ExitCode())
		}
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
func containsCore(raw []byte) (bool, error) {
	decoder := json.NewDecoder(bytes.NewReader(raw))
	for {
		var m module
		err := decoder.Decode(&m)
		if err == io.EOF {
			return false, nil
		}
		if err != nil {
			return false, err
		}
		if m.Path == "github.com/Leeminjing/go-forest" {
			return true, nil
		}
	}
}
