package topology

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"sort"
	"strings"
	"unicode/utf8"

	"github.com/Leeminjing/go-forest/forest"
)

const FormatVersion = 1
const MaxManifestBytes = 4 << 20
const maxFrameBytes = 16 << 20

type Tree struct {
	ID                   forest.TreeID        `json:"id"`
	ParentID             forest.TreeID        `json:"parent_id"`
	Definition           forest.DefinitionRef `json:"definition"`
	ContractKey          string               `json:"contract_key"`
	ImplementationDigest string               `json:"implementation_digest"`
}
type Binding struct {
	ID         string              `json:"id"`
	Consumer   forest.TreeID       `json:"consumer"`
	Port       forest.PortID       `json:"port"`
	Provider   forest.TreeID       `json:"provider"`
	Capability forest.CapabilityID `json:"capability"`
	Kind       string              `json:"kind"`
	Ordinal    uint64              `json:"ordinal"`
}
type Manifest struct {
	Version  int       `json:"version"`
	Trees    []Tree    `json:"trees"`
	Bindings []Binding `json:"bindings"`
}

func Empty() Manifest {
	return Manifest{Version: FormatVersion, Trees: []Tree{}, Bindings: []Binding{}}
}
func validID(s string) bool {
	return s != "" && len(s) <= 512 && utf8.ValidString(s) && !strings.ContainsRune(s, 0)
}
func validDigest(s string) bool {
	b, e := hex.DecodeString(s)
	return e == nil && len(b) == 32 && s == strings.ToLower(s)
}
func hash(domain string, data []byte) string {
	s := sha256.Sum256(append([]byte(domain+"\x00"), data...))
	return hex.EncodeToString(s[:])
}

// Normalize returns an isolated, canonical value. It does not build any tree.
func Normalize(m Manifest) (Manifest, error) {
	if m.Version != FormatVersion {
		return Manifest{}, ErrUnsupportedFormat
	}
	if len(m.Trees) > 10000 || len(m.Bindings) > 50000 {
		return Manifest{}, ErrInvalidManifest
	}
	n := Empty()
	n.Trees = append(n.Trees, m.Trees...)
	n.Bindings = append(n.Bindings, m.Bindings...)
	sort.Slice(n.Trees, func(i, j int) bool { return n.Trees[i].ID < n.Trees[j].ID })
	sort.Slice(n.Bindings, func(i, j int) bool { return n.Bindings[i].Ordinal < n.Bindings[j].Ordinal })
	nodes := map[forest.TreeID]Tree{}
	for _, t := range n.Trees {
		if !validID(string(t.ID)) || (t.ParentID != "" && !validID(string(t.ParentID))) || !validID(t.Definition.TypeID) || !validID(t.Definition.Version) || !validID(t.ContractKey) || !validDigest(t.ImplementationDigest) {
			return Manifest{}, fmt.Errorf("%w: tree %q", ErrInvalidManifest, t.ID)
		}
		if _, ok := nodes[t.ID]; ok {
			return Manifest{}, ErrInvalidManifest
		}
		nodes[t.ID] = t
	}
	// A color walk is iterative so valid deep forests do not consume the Go stack.
	done := map[forest.TreeID]bool{}
	for _, t := range n.Trees {
		path := map[forest.TreeID]bool{}
		for id := t.ID; id != "" && !done[id]; id = nodes[id].ParentID {
			if _, ok := nodes[id]; !ok {
				return Manifest{}, fmt.Errorf("%w: missing parent %q", ErrInvalidManifest, id)
			}
			if path[id] {
				return Manifest{}, fmt.Errorf("%w: Nest cycle", ErrInvalidManifest)
			}
			path[id] = true
		}
		for id := range path {
			done[id] = true
		}
	}
	ids := map[string]bool{}
	for i, b := range n.Bindings {
		if !validID(b.ID) || !validID(string(b.Consumer)) || !validID(string(b.Provider)) || !validID(string(b.Port)) || !validID(string(b.Capability)) || ids[b.ID] || b.Ordinal != uint64(i+1) || (b.Kind != "link" && b.Kind != "nest") {
			return Manifest{}, ErrInvalidManifest
		}
		if _, ok := nodes[b.Consumer]; !ok {
			return Manifest{}, ErrInvalidManifest
		}
		if _, ok := nodes[b.Provider]; !ok {
			return Manifest{}, ErrInvalidManifest
		}
		ids[b.ID] = true
	}
	data, err := json.Marshal(n)
	if err != nil || len(data) > MaxManifestBytes {
		return Manifest{}, ErrInvalidManifest
	}
	return n, nil
}
func Encode(m Manifest) ([]byte, error) {
	n, e := Normalize(m)
	if e != nil {
		return nil, e
	}
	return json.Marshal(n)
}
func Decode(data []byte) (Manifest, error) {
	var m Manifest
	if err := strict(data, &m, MaxManifestBytes); err != nil {
		return m, fmt.Errorf("%w: %v", ErrInvalidManifest, err)
	}
	return Normalize(m)
}
func Digest(m Manifest) (string, error) {
	data, e := Encode(m)
	if e != nil {
		return "", e
	}
	return hash("forest-topology/manifest/v1", data), nil
}

// strict rejects duplicate object keys as well as unknown fields and trailing
// values. Neither encoding/json's last-key-wins behavior nor arbitrary nesting
// is appropriate for durable operation identity.
func strict(data []byte, target any, limit int) error {
	if len(data) > limit || !utf8.Valid(data) {
		return fmt.Errorf("invalid size or UTF-8")
	}
	d := json.NewDecoder(bytes.NewReader(data))
	d.UseNumber()
	var walk func(int) error
	walk = func(depth int) error {
		if depth > 32 {
			return fmt.Errorf("JSON nesting limit")
		}
		tok, e := d.Token()
		if e != nil {
			return e
		}
		del, ok := tok.(json.Delim)
		if !ok {
			return nil
		}
		switch del {
		case '{':
			seen := map[string]bool{}
			for d.More() {
				key, e := d.Token()
				if e != nil {
					return e
				}
				s, ok := key.(string)
				if !ok || seen[s] {
					return fmt.Errorf("duplicate or invalid key")
				}
				seen[s] = true
				if e = walk(depth + 1); e != nil {
					return e
				}
			}
		case '[':
			for d.More() {
				if e := walk(depth + 1); e != nil {
					return e
				}
			}
		default:
			return fmt.Errorf("unexpected delimiter")
		}
		_, e = d.Token()
		return e
	}
	if e := walk(0); e != nil {
		return e
	}
	if _, e := d.Token(); e != io.EOF {
		return fmt.Errorf("trailing JSON")
	}
	d = json.NewDecoder(bytes.NewReader(data))
	d.DisallowUnknownFields()
	if e := d.Decode(target); e != nil {
		return e
	}
	return nil
}
