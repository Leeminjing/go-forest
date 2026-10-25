package einoforest_test

import (
	"context"
	"errors"
	"testing"

	ef "github.com/Leeminjing/go-forest/adapters/eino"
	"github.com/Leeminjing/go-forest/forest"
	"github.com/cloudwego/eino/adk"
)

func TestIdentityAndProfileRejectLossyOrReservedKeys(t *testing.T) {
	f := forest.New(forest.Options{})
	defer f.Close(context.Background())
	owner, _ := ef.NewOwner(f, 8)
	for _, bad := range []string{"bad\x00key", string([]byte{0xff})} {
		for _, p := range []ef.Profile{{Engine: bad, Namespace: "n", Schema: "s"}, {Engine: "e", Namespace: bad, Schema: "s"}, {Engine: "e", Namespace: "n", Schema: bad}} {
			if _, err := ef.NewFileStore(t.TempDir(), p); !errors.Is(err, ef.ErrInvalid) {
				t.Fatalf("profile accepted %q: %v", bad, err)
			}
		}
		if _, err := nativeDefinition(ef.Config{TypeID: "bad", Version: "1", Namespace: bad, Owner: owner}, t.TempDir(), nativeTool{}); !errors.Is(err, ef.ErrInvalid) {
			t.Fatalf("namespace accepted %q: %v", bad, err)
		}
	}
	d, err := nativeDefinition(ef.Config{TypeID: "native", Version: "1", Namespace: "n", Owner: owner}, t.TempDir(), nativeTool{})
	if err != nil {
		t.Fatal(err)
	}
	mount(t, f, d)
	for _, bad := range []string{"bad\x00key", string([]byte{0xff})} {
		err := invokeADK(f, func(ctx context.Context, a ef.ADK) error {
			_, err := a.Query(ctx, "unused", ef.RunOptions{CheckpointID: bad}, func(*adk.AgentEvent) error { t.Error("invalid call executed"); return nil })
			return err
		})
		if !errors.Is(err, ef.ErrInvalid) {
			t.Fatal(err)
		}
	}
}
