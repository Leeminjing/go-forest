package performance

import (
	"context"
	"errors"
	"os"
	"strconv"
	"sync/atomic"
	"testing"

	"github.com/Leeminjing/go-forest/forest"
)

func dimension(key string, fallback int) int {
	if v := os.Getenv("FOREST_PERF_" + key); v != "" {
		n, e := strconv.Atoi(v)
		if e != nil {
			panic(e)
		}
		return n
	}
	return fallback
}
func config() Config {
	return Config{dimension("N", 32), dimension("E", 16), dimension("D", 1), dimension("PORTS", 1), os.Getenv("FOREST_PERF_MANY") == "1"}
}
func checked(x *Fixture, v uint64, e error) bool {
	if x.Config.D == 0 {
		return errors.Is(e, forest.ErrBindingUnavailable)
	}
	return e == nil && v == x.Expected
}
func BenchmarkCalls(b *testing.B) {
	cfg := config()
	ctx := context.Background()
	x, e := New(cfg, "")
	if e != nil {
		b.Fatal(e)
	}
	b.Cleanup(func() {
		if e := x.Close(ctx); e != nil {
			b.Error(e)
		}
	})
	b.Run("DirectInterface", func(b *testing.B) {
		values := []Value{}
		for i := 0; i < cfg.D; i++ {
			tag := uint64(1)
			if i == 1 {
				tag = 2
			}
			values = append(values, &scalar{tag})
		}
		var sum uint64
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			var n uint64
			for _, v := range values {
				got, e := Dispatch(ctx, v)
				if e != nil {
					b.Fatal(e)
				}
				n += got
			}
			sum += n
		}
		b.StopTimer()
		if sum != uint64(b.N)*x.Expected {
			b.Fatal("direct result")
		}
	})
	b.Run("InvokeEnter", func(b *testing.B) {
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			if e := x.F.Invoke(ctx, func(context.Context) error { return nil }); e != nil {
				b.Fatal(e)
			}
		}
	})
	b.Run("InvokeNested", func(b *testing.B) {
		e := x.F.Invoke(ctx, func(call context.Context) error {
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if e := x.F.Invoke(call, func(context.Context) error { return nil }); e != nil {
					return e
				}
			}
			b.StopTimer()
			return nil
		})
		if e != nil {
			b.Fatal(e)
		}
	})
	b.Run("RefPinned", func(b *testing.B) {
		e := x.Pinned(ctx, func(call context.Context, r Runner) error {
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				got, e := r.Read(call)
				if !checked(x, got, e) {
					return errors.New("pinned result")
				}
			}
			b.StopTimer()
			return nil
		})
		if e != nil {
			b.Fatal(e)
		}
	})
	b.Run("RefEnter", func(b *testing.B) {
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			var got uint64
			e := x.Pinned(ctx, func(call context.Context, r Runner) error { var e error; got, e = r.Read(call); return e })
			if !checked(x, got, e) {
				b.Fatal(got, e)
			}
		}
	})
	b.Run("ControllerEnter", func(b *testing.B) {
		managed, err := New(cfg, b.TempDir())
		if err != nil {
			b.Fatal(err)
		}
		b.Cleanup(func() {
			if err := managed.Close(ctx); err != nil {
				b.Error(err)
			}
		})
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			if e := managed.C.Invoke(ctx, func(context.Context) error { return nil }); e != nil {
				b.Fatal(e)
			}
		}
	})
	b.Run("ControllerRef", func(b *testing.B) {
		managed, err := New(cfg, b.TempDir())
		if err != nil {
			b.Fatal(err)
		}
		b.Cleanup(func() {
			if err := managed.Close(ctx); err != nil {
				b.Error(err)
			}
		})
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			got, e := managed.Call(ctx)
			if !checked(x, got, e) {
				b.Fatal(got, e)
			}
		}
	})
	b.Run("RefParallel", func(b *testing.B) {
		var failed atomic.Bool
		b.SetParallelism(dimension("PARALLEL", 1))
		b.ReportAllocs()
		b.ResetTimer()
		b.RunParallel(func(pb *testing.PB) {
			e := x.Pinned(ctx, func(call context.Context, r Runner) error {
				for pb.Next() {
					got, e := r.Read(call)
					if !checked(x, got, e) {
						failed.Store(true)
					}
				}
				return nil
			})
			if e != nil {
				failed.Store(true)
			}
		})
		if failed.Load() {
			b.Fatal("parallel result")
		}
	})
}
