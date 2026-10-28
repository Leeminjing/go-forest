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
	"strings"
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
	Checked   bool
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
	args := os.Args[1:]
	queryArgs := args
	var argumentErr error
	if len(queryArgs) > 0 && strings.HasPrefix(queryArgs[0], "-C") {
		var dir string
		if queryArgs[0] == "-C" && len(queryArgs) > 1 {
			dir, queryArgs = queryArgs[1], queryArgs[2:]
		} else if strings.HasPrefix(queryArgs[0], "-C=") {
			dir, queryArgs = strings.TrimPrefix(queryArgs[0], "-C="), queryArgs[1:]
		} else {
			argumentErr = fmt.Errorf("unsupported Go directory flag")
		}
		if !filepath.IsAbs(dir) {
			dir = filepath.Join(p.Directory, dir)
		}
		p.Directory = dir
	}
	if len(queryArgs) > 0 && strings.HasPrefix(queryArgs[0], "-") {
		argumentErr = fmt.Errorf("unrecognized global Go flag")
	}
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
		for i, a := range queryArgs {
			if len(a) > 9 && a[:9] == "-modfile=" {
				args = append(args, a)
			} else if a == "-modfile" && i+1 < len(queryArgs) {
				args = append(args, a, queryArgs[i+1])
			}
		}
		args = append(args, "github.com/Leeminjing/go-forest")
		query := exec.Command(real, args...)
		query.Dir = p.Directory
		raw, e = query.Output()
		if e != nil {
			// A stdlib-only module, such as TestKit, has no Core to replace. Prove
			// absence using the complete module list rather than swallowing an error.
			allArgs := append(append([]string{}, args[:len(args)-1]...), "all")
			allQuery := exec.Command(real, allArgs...)
			allQuery.Dir = p.Directory
			all, e2 := allQuery.Output()
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
	compiles := len(queryArgs) > 0 && (queryArgs[0] == "test" || queryArgs[0] == "build" || queryArgs[0] == "run" || queryArgs[0] == "vet" || queryArgs[0] == "list")
	err := argumentErr
	if compiles && err == nil {
		p.Checked = true
		err = verify()
	}
	if err != nil {
		p.Error = err.Error()
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
