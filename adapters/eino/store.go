package einoforest

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"unicode/utf8"
)

var ErrCorrupt = errors.New("checkpoint corrupt")
var ErrProfile = errors.New("checkpoint engine or execution profile incompatible")

type Profile struct{ Engine, Namespace, Schema string }
type envelope struct {
	Format  int
	Profile Profile
	Key     string
	Digest  string
	Payload []byte
}

// FileStore holds unchanged native bytes and profile in immutable revisions.
// Exactly one process/Owner manages the directory. No distributed lease or
// power-loss durability is claimed; publication needs atomic local hard links.
type FileStore struct {
	mu        sync.Mutex
	directory string
	profile   Profile
}

func NewFileStore(directory string, p Profile) (*FileStore, error) {
	if directory == "" || p.Engine == "" || p.Namespace == "" || p.Schema == "" || len(p.Engine) > 128 || len(p.Namespace) > 256 || len(p.Schema) > 256 {
		return nil, ErrInvalid
	}
	path, err := filepath.Abs(directory)
	if err != nil {
		return nil, err
	}
	if err = os.MkdirAll(path, 0700); err != nil {
		return nil, err
	}
	return &FileStore{directory: path, profile: p}, nil
}
func (s *FileStore) path(key string) string {
	sum := sha256.Sum256([]byte(s.profile.Namespace + "\x00" + key))
	return filepath.Join(s.directory, hex.EncodeToString(sum[:]))
}
func validKey(key string) bool {
	return key != "" && len(key) <= 256 && utf8.ValidString(key) && !strings.ContainsRune(key, 0)
}
func (s *FileStore) latest(key string) (string, uint64, error) {
	entries, err := os.ReadDir(s.path(key))
	if errors.Is(err, os.ErrNotExist) {
		return "", 0, nil
	}
	if err != nil {
		return "", 0, err
	}
	if len(entries) > 10000 {
		return "", 0, ErrBudget
	}
	var names []string
	for _, e := range entries {
		if strings.HasPrefix(e.Name(), ".tmp-") {
			continue
		}
		if e.IsDir() || !strings.HasSuffix(e.Name(), ".json") {
			return "", 0, ErrCorrupt
		}
		n, err := strconv.ParseUint(strings.TrimSuffix(e.Name(), ".json"), 10, 64)
		if err != nil || n == 0 || e.Name() != fmt.Sprintf("%020d.json", n) {
			return "", 0, ErrCorrupt
		}
		names = append(names, e.Name())
	}
	if len(names) == 0 {
		return "", 0, nil
	}
	sort.Strings(names)
	name := names[len(names)-1]
	n, _ := strconv.ParseUint(strings.TrimSuffix(name, ".json"), 10, 64)
	return filepath.Join(s.path(key), name), n, nil
}
func (s *FileStore) get(key string) ([]byte, bool, error) {
	path, _, err := s.latest(key)
	if err != nil {
		return nil, false, err
	}
	if path == "" {
		return nil, false, nil
	}
	f, err := os.Open(path)
	if err != nil {
		return nil, false, err
	}
	defer f.Close()
	data, err := io.ReadAll(io.LimitReader(f, (24<<20)+1))
	if err != nil {
		return nil, false, err
	}
	if len(data) > 24<<20 {
		return nil, false, ErrBudget
	}
	var e envelope
	if json.Unmarshal(data, &e) != nil || e.Format != 1 || e.Key != key || len(e.Payload) > 16<<20 {
		return nil, false, ErrCorrupt
	}
	digest := sha256.Sum256(e.Payload)
	if hex.EncodeToString(digest[:]) != e.Digest {
		return nil, false, ErrCorrupt
	}
	if e.Profile != s.profile {
		return nil, false, ErrProfile
	}
	return e.Payload, true, nil
}
func (s *FileStore) Get(ctx context.Context, key string) ([]byte, bool, error) {
	if !validKey(key) {
		return nil, false, ErrInvalid
	}
	if err := ctx.Err(); err != nil {
		return nil, false, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.get(key)
}
func (s *FileStore) Set(ctx context.Context, key string, payload []byte) error {
	if !validKey(key) || len(payload) > 16<<20 {
		return ErrInvalid
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, _, err := s.get(key); err != nil {
		return err
	}
	directory := s.path(key)
	if err := os.MkdirAll(directory, 0700); err != nil {
		return err
	}
	_, n, err := s.latest(key)
	if err != nil {
		return err
	}
	if n == ^uint64(0) {
		return ErrBudget
	}
	sum := sha256.Sum256(payload)
	data, err := json.Marshal(envelope{Format: 1, Profile: s.profile, Key: key, Digest: hex.EncodeToString(sum[:]), Payload: payload})
	if err != nil {
		return err
	}
	f, err := os.CreateTemp(directory, ".tmp-")
	if err != nil {
		return err
	}
	name := f.Name()
	defer os.Remove(name)
	if _, err = f.Write(data); err != nil {
		f.Close()
		return err
	}
	if err = f.Sync(); err != nil {
		f.Close()
		return err
	}
	if err = f.Close(); err != nil {
		return err
	}
	if err = ctx.Err(); err != nil {
		return err
	}
	return os.Link(name, filepath.Join(directory, fmt.Sprintf("%020d.json", n+1)))
}
