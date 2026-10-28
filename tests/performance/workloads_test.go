package performance

import (
	"testing"
	"time"
)

func TestBoundedLoadAccountsForEveryArrivalAndJoinsOldScope(t *testing.T) {
	result, e := Load(LoadConfig{Graph: Config{32, 16, 1, 1, false}, Workers: 2, Rate: 200, Queue: 16, MaxRequests: 100, Holds: 1, Duration: 100 * time.Millisecond, UpdateEvery: 10 * time.Millisecond})
	if e != nil || result.Planned != 20 || result.Completed+result.Dropped+result.Unfinished != 20 || result.InvalidResults != 0 || result.ReleaseAtNS <= 0 || result.Updates == 0 {
		t.Fatalf("invalid load evidence: %+v %v", result, e)
	}
}
func TestLongevityClosesResourcesButRetainsDiagnostics(t *testing.T) {
	result, e := Longevity(20, 2, "replace", t.TempDir(), "")
	if e != nil {
		t.Fatal(e)
	}
	initial := result.Samples[0]
	released := result.Samples[len(result.Samples)-2]
	closed := result.Samples[len(result.Samples)-1]
	if initial.Diagnostics != 3 || released.Diagnostics != 23 || released.Stopped != 20 || released.OpenResources != 2 || released.HeldVersions != 0 || closed.OpenResources != 0 || closed.Stopped != 23 {
		t.Fatalf("resource/metadata evidence %+v", result)
	}
}
