package testkit_test

import (
	"errors"
	"fmt"
	tk "github.com/Leeminjing/go-forest/testkit"
	"sync"
	"testing"
)

func TestJournalKeepsConcurrentDetachedEvidenceAndFailsClosedOnLoss(t *testing.T) {
	j, err := tk.NewJournal(65)
	if err != nil {
		t.Fatal(err)
	}
	e := tk.Event{Stage: "enter", Execution: &tk.Execution{Task: "task"}, Effect: &tk.Effect{Operation: "effect"}, Resources: []tk.Resource{{ID: "file", Ownership: tk.Owned}}}
	if _, err = j.Record(e); err != nil {
		t.Fatal(err)
	}
	e.Execution.Task = "mutated"
	e.Effect.Operation = "mutated"
	e.Resources[0].ID = "mutated"
	var wg sync.WaitGroup
	for i := 0; i < 64; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			if _, err := j.Record(tk.Event{Stage: fmt.Sprint(i)}); err != nil {
				t.Error(err)
			}
		}(i)
	}
	wg.Wait()
	a, err := j.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	entries := a.Entries()
	if len(entries) != 65 || entries[0].Execution.Task != "task" || entries[0].Effect.Operation != "effect" || entries[0].Resources[0].ID != "file" {
		t.Fatal(entries)
	}
	for i, e := range entries {
		if e.Sequence != uint64(i+1) {
			t.Fatal("lost sequence", entries)
		}
	}
	entries[0].Execution.Task = "snapshot mutation"
	entries[0].Resources[0].ID = "snapshot mutation"
	if a.Entries()[0].Execution.Task != "task" || a.Entries()[0].Resources[0].ID != "file" {
		t.Fatal("trace aliases caller")
	}
	if _, err = j.Record(tk.Event{Stage: "overflow"}); !errors.Is(err, tk.ErrEvidenceOverflow) {
		t.Fatal(err)
	}
	if _, err = j.Snapshot(); !errors.Is(err, tk.ErrEvidenceOverflow) {
		t.Fatal("loss was forgotten", err)
	}
}
