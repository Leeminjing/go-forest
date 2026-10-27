package crossdemo_test

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	tk "github.com/Leeminjing/go-forest/testkit"
	"go.temporal.io/sdk/activity"
	"os"
	"strconv"
	"testing"
)

// Test-only native callback witness. It is not a production checkpoint or a
// guessed attempt reconstructed from Workflow history (which can omit retries).
type attemptWitness struct {
	Task, Activity, Call, Step, Attempt string
	Generation                          uint64
}

func saveAttempt(path string, ctx context.Context, step string, g uint64) error {
	info := activity.GetInfo(ctx)
	n := strconv.Itoa(int(info.Attempt))
	b, err := json.Marshal(attemptWitness{Task: info.WorkflowExecution.ID + "/" + info.WorkflowExecution.RunID, Activity: info.ActivityID, Call: info.ActivityID + "/" + n, Step: step, Attempt: n, Generation: g})
	if err != nil {
		return err
	}
	return os.WriteFile(path, b, 0600)
}
func readAttempt(t *testing.T, path string) attemptWitness {
	t.Helper()
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var a attemptWitness
	if err = json.Unmarshal(raw, &a); err != nil {
		t.Fatal(err)
	}
	return a
}
func digestEvidence(value any) string {
	b, err := json.Marshal(value)
	if err != nil {
		panic(err)
	}
	h := sha256.Sum256(b)
	return hex.EncodeToString(h[:])
}
func executionWitness(a attemptWitness, process, instance string, g uint64) tk.Execution {
	return tk.Execution{Scope: tk.Scope{Forest: process, Call: process + "/" + a.Call, Generation: g}, Task: a.Task, Step: a.Step, Attempt: a.Attempt, Process: process, Instance: instance}
}
