package incrementaldemo

import (
	"context"
	"github.com/Leeminjing/go-forest/forest"
	"os"
	"reflect"
	"testing"
	"time"
)

func TestRealGinAndPostgresKeepNativeInstancesAcrossChangeAndRollback(t *testing.T) {
	dsn := os.Getenv("FOREST_POSTGRES_DSN")
	if dsn == "" {
		t.Skip("real PostgreSQL: set FOREST_POSTGRES_DSN")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 40*time.Second)
	defer cancel()
	ev, e := Run(ctx, dsn, t.TempDir())
	if e != nil {
		t.Fatal(e)
	}
	for _, item := range []struct {
		result     Result
		version    string
		generation uint64
	}{{ev.Old, "1", 1}, {ev.New, "2", 2}, {ev.Rollback, "1", 3}} {
		if !reflect.DeepEqual(item.result.Versions, []string{item.version, item.version}) || !reflect.DeepEqual(item.result.Generations, []uint64{item.generation, item.generation}) || !reflect.DeepEqual(item.result.Values, []int{41, 41}) || !reflect.DeepEqual(item.result.Pools, []uintptr{ev.Pool, ev.Pool}) {
			t.Fatal("request mixed versions/resources", item)
		}
	}
	for _, id := range []string{"gin", "db"} {
		if ev.Before[forest.TreeID(id)] != ev.After[forest.TreeID(id)] || ev.Before[forest.TreeID(id)] != ev.Reverted[forest.TreeID(id)] || ev.Counts[id] != (Counts{1, 1, 1}) {
			t.Fatal("infrastructure reconstructed", id, ev)
		}
	}
	if ev.Before["business"] == ev.After["business"] || ev.Before["business"] == ev.Reverted["business"] || ev.After["business"] == ev.Reverted["business"] {
		t.Fatal("rollback resurrected physical instance")
	}
	if ev.EndpointBefore == 0 || ev.EndpointBefore != ev.EndpointAfter || ev.EndpointBefore != ev.EndpointRollback || ev.Engine == 0 || ev.Pool == 0 || !ev.PoolClosed {
		t.Fatal("native resource identity/lifecycle", ev)
	}
	if ev.ClosedBeforeOpen != 503 || ev.ClosedDuringPrepare != 503 || ev.BusinessCallsAfterBlocked != ev.BusinessCallsBeforeBlocked || ev.ReceiptVersion != "2" {
		t.Fatal("admission/effect evidence", ev)
	}
	t.Logf("actual Gin/PostgreSQL evidence: %+v", ev)
}
