package topology

import (
	"errors"
	"reflect"
	"sync/atomic"
	"testing"

	"github.com/Leeminjing/go-forest/forest"
)

func TestCaptureKeepsItsPartialErrorResult(t *testing.T) {
	var builds atomic.Int32
	catalog, m := numberCatalog(t, &builds)
	_, _, f, _ := managed(t, catalog, m)
	snapshot := f.Snapshot()
	snapshot.Links["invalid"] = forest.LinkInfo{}
	got, e := Capture(snapshot, catalog)
	if !errors.Is(e, ErrInvalidManifest) || !reflect.DeepEqual(got, m) {
		t.Fatalf("capture discarded its verified tree prefix: %+v %v", got, e)
	}
	zero, e := Capture(snapshot, nil)
	if !errors.Is(e, ErrDefinitionUnavailable) || !reflect.DeepEqual(zero, Manifest{}) {
		t.Fatal("nil catalog error/result changed", e)
	}
}
