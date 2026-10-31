package performance

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"

	"github.com/Leeminjing/go-forest/extensions/topology"
	"github.com/Leeminjing/go-forest/forest"
)

// PlanningAudit verifies results independently of the frozen timing program.
// Raw results are retained; only verified per-process identities are renamed
// in the semantic projection used to compare independently built binaries.
func PlanningAudit() (semantic, raw map[string]any, err error) {
	dir, err := os.MkdirTemp("", "forest-planning-audit-")
	if err != nil {
		return nil, nil, err
	}
	defer os.RemoveAll(dir)
	x, err := NewPlanning(dir)
	if err != nil {
		return nil, nil, err
	}
	defer func() { err = errors.Join(err, x.Close(context.Background())) }()
	input, err := PlanningData(x)
	if err != nil {
		return nil, nil, err
	}
	if path := os.Getenv("FOREST_PLANNING_INPUT"); path != "" {
		data, e := os.ReadFile(path)
		if e != nil {
			return nil, nil, e
		}
		var frozen PlanningInput
		if e = json.Unmarshal(data, &frozen); e != nil {
			return nil, nil, e
		}
		if !reflect.DeepEqual(input, frozen) {
			return nil, nil, fmt.Errorf("audit input differs")
		}
	}
	semantic, raw = map[string]any{}, map[string]any{}
	add := func(name string, value any, e error) error {
		if e == nil {
			semantic[name] = value
		}
		return e
	}
	beforeSnapshot := x.F.Snapshot()
	value, e := topology.Capture(beforeSnapshot, x.Catalog)
	if e = add("Capture", value, e); e != nil {
		return nil, nil, e
	}
	value, e = topology.Normalize(input.Target)
	if e = add("Normalize", value, e); e != nil {
		return nil, nil, e
	}
	encoded, e := topology.Encode(input.Target)
	if e = add("Encode", string(encoded), e); e != nil {
		return nil, nil, e
	}
	digest, e := topology.Digest(input.Target)
	if e = add("Digest", digest, e); e != nil {
		return nil, nil, e
	}
	diff, e := topology.Compare(input.Before, input.Target)
	if e = add("Compare", diff, e); e != nil {
		return nil, nil, e
	}
	proposal, e := x.C.Preview(input.Target)
	if e != nil {
		return nil, nil, e
	}
	if proposal.Expected != x.Store.Inspect().Position || proposal.Generation != x.F.Snapshot().Generation {
		return nil, nil, fmt.Errorf("proposal does not describe actual position/generation")
	}
	raw["Preview"] = proposal
	projected := proposal
	projected.Expected.StoreID = "verified-store"
	projected.Expected.Digest = "verified-position-digest"
	semantic["Preview"] = projected
	receipt, e := x.C.Reconfigure(context.Background(), topology.Request{ID: "audited", Expected: proposal.Expected, Approval: "real result audit"}, proposal)
	if e != nil {
		return nil, nil, e
	}
	status, snapshot := x.Store.Inspect(), x.F.Snapshot()
	if !reflect.DeepEqual(status.LastObserved, &receipt) || receipt.Generation != snapshot.Generation || receipt.SessionID != status.SessionID || len(receipt.Instances) != len(snapshot.Trees) {
		return nil, nil, fmt.Errorf("receipt differs from confirmed/runtime evidence")
	}
	raw["Commit"] = receipt
	raw["ActualPosition"] = status.Position
	stable := receipt
	stable.SessionID = "verified-session"
	stable.Instances = append([]topology.InstanceObservation{}, receipt.Instances...)
	for i, observation := range receipt.Instances {
		if observation.InstanceID == "" || snapshot.Trees[forest.TreeID(observation.TreeID)].InstanceID != observation.InstanceID {
			return nil, nil, fmt.Errorf("receipt instance differs from actual tree")
		}
		prefix := "new:"
		if beforeSnapshot.Trees[forest.TreeID(observation.TreeID)].InstanceID == observation.InstanceID {
			prefix = "retained:"
		}
		stable.Instances[i].InstanceID = prefix + observation.TreeID
	}
	semantic["Commit"] = stable
	actual, e := topology.Capture(snapshot, x.Catalog)
	if e != nil || !equalPlanningManifest(actual, input.Target) {
		return nil, nil, fmt.Errorf("audit commit target differs: %v", e)
	}
	semantic["After"] = actual
	if path := os.Getenv("FOREST_PLANNING_JOURNAL"); path != "" {
		journal, e := os.ReadFile(path)
		if e != nil {
			return nil, nil, e
		}
		openDir := filepath.Join(dir, "frozen-open")
		if e = os.Mkdir(openDir, 0700); e != nil {
			return nil, nil, e
		}
		if e = os.WriteFile(filepath.Join(openDir, "journal"), journal, 0600); e != nil {
			return nil, nil, e
		}
		s, e := topology.Open(openDir, topology.Options{})
		if e != nil {
			return nil, nil, e
		}
		semantic["Open"] = map[string]any{"Inspect": s.Inspect(), "History": s.History(), "Journal": s.Journal()}
		if e = s.Close(); e != nil {
			return nil, nil, e
		}
		after, e := os.ReadFile(filepath.Join(openDir, "journal"))
		if e != nil || string(after) != string(journal) {
			return nil, nil, fmt.Errorf("open changed frozen journal: %v", e)
		}
	}
	return semantic, raw, nil
}
