package performance

import (
	"context"
	"fmt"
	"os"
	"testing"

	"github.com/Leeminjing/go-forest/extensions/topology"
)

func BenchmarkManagement(b *testing.B) {
	cfg := config()
	ctx := context.Background()
	for _, kind := range []string{"Preview", "ApplyRebind", "ApplyReplaceFresh", "CommitFresh", "Noop", "Snapshot", "Wait"} {
		b.Run(kind, func(b *testing.B) {
			b.StopTimer()
			managed := kind == "Preview" || kind == "CommitFresh"
			var x *Fixture
			var err error
			setup := func() {
				directory := ""
				if managed {
					directory, err = os.MkdirTemp(b.TempDir(), "store-")
					if err != nil {
						b.Fatal(err)
					}
				}
				x, err = New(cfg, directory)
				if err != nil {
					b.Fatal(err)
				}
				for j := 0; j < dimension("HISTORY", 0); j++ {
					if managed {
						target, e := x.Target()
						if e != nil {
							b.Fatal(e)
						}
						if j%2 == 1 {
							for k := range target.Trees {
								if target.Trees[k].ID == Provider(0) {
									target.Trees[k].Definition.Version = "1"
								}
							}
						}
						p, e := x.C.Preview(target)
						if e != nil {
							b.Fatal(e)
						}
						_, e = x.C.Reconfigure(ctx, topology.Request{ID: fmt.Sprintf("prefix-%d", j), Expected: p.Expected, Approval: "controlled genuine journal prefix"}, p)
						if e != nil {
							b.Fatal(e)
						}
					} else {
						if e := x.Change(ctx, true); e != nil {
							b.Fatal(e)
						}
					}
					if e := x.F.Wait(ctx); e != nil {
						b.Fatal(e)
					}
				}
			}
			setup()
			defer func() {
				if x != nil {
					if e := x.Close(ctx); e != nil {
						b.Error(e)
					}
				}
			}()
			target, e := x.Target()
			if e != nil {
				b.Fatal(e)
			}
			b.ReportAllocs()
			b.ResetTimer()
			b.StartTimer()
			for i := 0; i < b.N; i++ {
				switch kind {
				case "Preview":
					_, err = x.C.Preview(target)
				case "ApplyRebind":
					err = x.Change(ctx, false)
				case "ApplyReplaceFresh":
					err = x.Change(ctx, true)
				case "CommitFresh":
					p, e := x.C.Preview(target)
					if e != nil {
						err = e
						break
					}
					_, err = x.C.Reconfigure(ctx, topology.Request{ID: "measured", Expected: p.Expected, Approval: "durable measured commit including validation"}, p)
				case "Noop":
					_, err = x.F.Apply(ctx, emptyPlan(x.generation))
				case "Snapshot":
					s := x.F.Snapshot()
					if len(s.Trees) != cfg.N {
						b.Fatal("snapshot dimensions")
					}
				case "Wait":
					err = x.F.Wait(ctx)
				}
				if err != nil {
					b.Fatal(err)
				}
				if kind == "ApplyReplaceFresh" || kind == "CommitFresh" {
					b.StopTimer()
					if e = x.Close(ctx); e != nil {
						b.Fatal(e)
					}
					x = nil
					if i+1 < b.N {
						setup()
						target, e = x.Target()
						if e != nil {
							b.Fatal(e)
						}
					}
					b.StartTimer()
				}
			}
			b.StopTimer()
		})
	}
}
