package performance

import "testing"

// Setup, frozen-input checks, cleanup and semantic result verification are
// outside each operation's timed interval. Every Commit uses a fresh prefix.
func BenchmarkPlanning(b *testing.B) {
	for _, kind := range []string{"Capture", "Normalize", "Encode", "Compare", "Digest", "Preview", "Commit", "PreviewCommit", "Open"} {
		b.Run(kind, func(b *testing.B) {
			b.StopTimer()
			op, close, e := PlanningOperation(kind)
			if e != nil {
				b.Fatal(e)
			}
			b.ReportAllocs()
			b.ResetTimer()
			b.StartTimer()
			for i := 0; i < b.N; i++ {
				err := op()
				b.StopTimer()
				cleanup := close()
				if err != nil || cleanup != nil {
					b.Fatal(err, cleanup)
				}
				if i+1 < b.N {
					op, close, e = PlanningOperation(kind)
					if e != nil {
						b.Fatal(e)
					}
				}
				b.StartTimer()
			}
			b.StopTimer()
		})
	}
}
