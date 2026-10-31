package topology

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
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
	n, e := canonicalize(m)
	if e != nil {
		return Manifest{}, e
	}
	return n.value, nil
}
func Encode(m Manifest) ([]byte, error) {
	n, e := canonicalize(m)
	if e != nil {
		return nil, e
	}
	return n.encoded, nil
}
func Decode(data []byte) (Manifest, error) {
	var m Manifest
	if err := strict(data, &m, MaxManifestBytes); err != nil {
		return m, fmt.Errorf("%w: %v", ErrInvalidManifest, err)
	}
	return Normalize(m)
}
func Digest(m Manifest) (string, error) {
	n, e := canonicalize(m)
	if e != nil {
		return "", e
	}
	return n.sum(), nil
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
