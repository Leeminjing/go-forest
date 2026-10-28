package performance

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/Leeminjing/go-forest/extensions/topology"
)

func BenchmarkJournal(b *testing.B) {
	path := os.Getenv("FOREST_JOURNAL_INPUT")
	if path == "" {
		b.Skip("explicit frozen input required")
	}
	input, e := os.ReadFile(path)
	if e != nil {
		b.Fatal(e)
	}
	for _, kind := range []string{"Append", "Open", "Inspect", "History", "Journal"} {
		b.Run(kind, func(b *testing.B) {
			b.StopTimer()
			b.ReportAllocs()
			dir := b.TempDir()
			for i := 0; i < b.N; i++ {
				if e = copyJournal(input, dir); e != nil {
					b.Fatal(e)
				}
				var s *topology.Store
				if kind != "Open" {
					s, e = topology.Open(dir, topology.Options{})
					if e != nil {
						b.Fatal(e)
					}
				}
				b.StartTimer()
				switch kind {
				case "Open":
					s, e = topology.Open(dir, topology.Options{})
				case "Append":
					e = appendSelection(s)
				case "Inspect":
					_ = s.Inspect()
				case "History":
					_ = s.History()
				case "Journal":
					_ = s.Journal()
				}
				b.StopTimer()
				if e != nil {
					b.Fatal(e)
				}
				if s == nil {
					b.Fatal("missing actual store")
				}
				if e = s.Close(); e != nil {
					b.Fatal(e)
				}
				if e = os.Remove(filepath.Join(dir, "journal")); e != nil {
					b.Fatal(e)
				}
			}
		})
	}
}
func TestJournalFixtureCountsAndReopen(t *testing.T) {
	path := filepath.Join(t.TempDir(), "input")
	if e := CreateJournal(path, 8, 3); e != nil {
		t.Fatal(e)
	}
	for _, mode := range []string{"open", "append"} {
		r, e := JournalMemory(path, mode, "")
		if e != nil {
			t.Fatal(e)
		}
		want := uint64(5)
		if mode == "append" {
			want++
		}
		if r.Records != want || r.Revisions != int(want)-1 {
			t.Fatalf("actual history %+v", r)
		}
	}
}
