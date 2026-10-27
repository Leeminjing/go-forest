package ecosystem

import (
	"testing"

	tk "github.com/Leeminjing/go-forest/testkit"
)

func TestConformanceNativeMessageAndAgentKeepDistinctEvidence(t *testing.T) {
	f := newConf(t)
	defer f.close(t)
	f.submit(t, "http", "one")
	seq := f.relay(t, "one")
	f.enable(t)
	observation := f.await(t, seq)
	if err := tk.CheckPinned(f.pinned(t, seq, observation, f.generation())); err != nil {
		t.Fatal(err)
	}
	if observation.Receipt.Version != "v1" || observation.Receipt.Count != 1 {
		t.Fatal(observation)
	}
	f.acked(t, seq)
	task := f.complete(t, "one")
	if task.Result != "model v1: 5" {
		t.Fatal(task)
	}
}
