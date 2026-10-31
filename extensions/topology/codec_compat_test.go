package topology

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"reflect"
	"strings"
	"testing"

	"github.com/Leeminjing/go-forest/forest"
)

func TestPublishedCodecBytesAndOwnership(t *testing.T) {
	data, e := os.ReadFile("testdata/codec-v0.2.1.json")
	if e != nil {
		t.Fatal(e)
	}
	var fixture struct {
		Release, Go string
		Samples     []struct {
			Input, Normalized Manifest
			Encoding, Digest  string
		}
	}
	if e = json.Unmarshal(data, &fixture); e != nil {
		t.Fatal(e)
	}
	if fixture.Release != "v0.2.1" || len(fixture.Samples) != 3 {
		t.Fatal("wrong independent release fixture")
	}
	for i, row := range fixture.Samples {
		t.Run(fmt.Sprint(i), func(t *testing.T) {
			n, e := Normalize(row.Input)
			if e != nil || !reflect.DeepEqual(n, row.Normalized) {
				t.Fatal("canonical value changed", e)
			}
			b, e := Encode(row.Input)
			if e != nil || string(b) != row.Encoding {
				t.Fatal("released encoding changed", e)
			}
			d, e := Digest(row.Input)
			if e != nil || d != row.Digest {
				t.Fatal("released digest changed", e)
			}
			decoded, e := Decode([]byte(row.Encoding))
			if e != nil || !reflect.DeepEqual(decoded, row.Normalized) {
				t.Fatal("released decode changed", e)
			}
			if len(n.Trees) > 0 {
				row.Input.Trees[0].ID = "caller-input"
				if !reflect.DeepEqual(n, row.Normalized) {
					t.Fatal("normalized value borrows input")
				}
				n.Trees[0].ID = "caller-output"
				decoded.Trees[0].ID = "caller-decode"
			}
			if len(b) > 0 {
				b[0] = '!'
			}
			again, e := Encode(row.Normalized)
			if e != nil || string(again) != row.Encoding {
				t.Fatal("public mutation poisoned another call", e)
			}
		})
	}
}

func TestManifestExactEncodedSizeLimit(t *testing.T) {
	// Independent JSON size construction: all IDs and fields remain valid, so
	// the one-byte difference exercises the size boundary rather than a field cap.
	m := Empty()
	for i := 0; i < 10000; i++ {
		m.Trees = append(m.Trees, Tree{ID: forest.TreeID(fmt.Sprintf("n%05d", i)), Definition: forest.DefinitionRef{TypeID: "clock", Version: "1"}, ContractKey: "c", ImplementationDigest: strings.Repeat("a", 64)})
	}
	base, e := json.Marshal(m)
	if e != nil {
		t.Fatal(e)
	}
	left := MaxManifestBytes - len(base)
	if left <= 0 {
		t.Fatal("invalid boundary fixture")
	}
	for i := range m.Trees {
		add := left
		if add > 511 {
			add = 511
		}
		m.Trees[i].ContractKey += strings.Repeat("x", add)
		left -= add
		if left == 0 {
			break
		}
	}
	if left != 0 {
		t.Fatal("cannot construct valid exact boundary")
	}
	if b, _ := json.Marshal(m); len(b) != MaxManifestBytes {
		t.Fatal("independent size is not exact")
	}
	if b, e := Encode(m); e != nil || len(b) != MaxManifestBytes {
		t.Fatal("valid exact limit rejected", e)
	}
	for i := range m.Trees {
		if len(m.Trees[i].ContractKey) < 512 {
			m.Trees[i].ContractKey += "x"
			break
		}
	}
	if _, e := Normalize(m); !errors.Is(e, ErrInvalidManifest) {
		t.Fatal("size overflow accepted", e)
	}
	if _, e := Encode(m); !errors.Is(e, ErrInvalidManifest) {
		t.Fatal("encoding overflow accepted", e)
	}
	if _, e := Digest(m); !errors.Is(e, ErrInvalidManifest) {
		t.Fatal("digest overflow accepted", e)
	}
}
