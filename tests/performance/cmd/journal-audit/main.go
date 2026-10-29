// journal-audit compares full public projections and original offline receipts.
// It is supplementary correctness evidence, never part of timed benchmarks.
package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"

	"github.com/Leeminjing/go-forest/extensions/topology"
)

func digest(v any) string {
	b, err := json.Marshal(v)
	if err != nil {
		panic(err)
	}
	s := sha256.Sum256(b)
	return hex.EncodeToString(s[:])
}

func audit(path string) (map[string]any, error) {
	input, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	dir, err := os.MkdirTemp("", "forest-journal-audit-")
	if err != nil {
		return nil, err
	}
	defer os.RemoveAll(dir)
	if err = os.WriteFile(filepath.Join(dir, "journal"), input, 0600); err != nil {
		return nil, err
	}
	s, err := topology.Open(dir, topology.Options{})
	if err != nil {
		return nil, err
	}
	defer s.Close()
	inspection, history, journal := s.Inspect(), s.History(), s.Journal()
	receipts := []topology.Receipt{}
	// Open above validates the entire original stream, including hashes and
	// strict syntax. Decode only enough to resubmit original offline requests.
	for pos := 0; pos < len(input); {
		if len(input)-pos < 16 {
			return nil, fmt.Errorf("short frame")
		}
		n := int(binary.BigEndian.Uint64(input[pos+8 : pos+16]))
		end := pos + 16 + n
		if n < 0 || end < pos || end+32 > len(input) {
			return nil, fmt.Errorf("bad frame length")
		}
		var r struct {
			Kind         string             `json:"kind"`
			Request      *topology.Request  `json:"request"`
			Target       *topology.Manifest `json:"target"`
			FromRevision uint64             `json:"from_revision"`
		}
		if err = json.Unmarshal(input[pos+16:end], &r); err != nil {
			return nil, err
		}
		var receipt topology.Receipt
		switch r.Kind {
		case "seed":
			receipt, err = s.Seed(context.Background(), *r.Request, *r.Target)
		case "selected":
			receipt, err = s.SelectHistorical(context.Background(), *r.Request, r.FromRevision)
		default:
			pos = end + 32
			continue
		}
		if err != nil {
			return nil, err
		}
		receipts = append(receipts, receipt)
		pos = end + 32
	}
	actual, err := os.ReadFile(filepath.Join(dir, "journal"))
	if err != nil {
		return nil, err
	}
	if !bytes.Equal(input, actual) || digest(inspection) != digest(s.Inspect()) || digest(history) != digest(s.History()) || digest(journal) != digest(s.Journal()) {
		return nil, fmt.Errorf("idempotent requests changed evidence")
	}
	inputSum := sha256.Sum256(input)
	return map[string]any{"input_sha256": hex.EncodeToString(inputSum[:]), "inspection": digest(inspection), "history": digest(history), "journal": digest(journal), "original_receipts": digest(receipts), "requests": len(receipts), "records": len(journal)}, nil
}

func main() {
	results := map[string]any{}
	for _, path := range os.Args[1:] {
		value, err := audit(path)
		if err != nil {
			fmt.Fprintln(os.Stderr, path, err)
			os.Exit(1)
		}
		results[filepath.Base(path)] = value
	}
	if err := json.NewEncoder(os.Stdout).Encode(results); err != nil {
		panic(err)
	}
}
