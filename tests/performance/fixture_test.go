package performance

import (
	"context"
	"errors"
	"github.com/Leeminjing/go-forest/forest"
	"testing"
)

func TestFixtureActualCallsAndDimensions(t *testing.T) {
	for _, cfg := range []Config{{32, 16, 1, 1, false}, {32, 16, 4, 1, true}, {32, 0, 0, 1, false}} {
		x, e := New(cfg, "")
		if e != nil {
			t.Fatal(e)
		}
		got, e := x.Call(context.Background())
		if cfg.D == 0 {
			if !errors.Is(e, forest.ErrBindingUnavailable) {
				t.Fatal(e)
			}
		} else if e != nil || got != x.Expected {
			t.Fatal(got, e)
		}
		if e = x.Close(context.Background()); e != nil {
			t.Fatal(e)
		}
	}
}
