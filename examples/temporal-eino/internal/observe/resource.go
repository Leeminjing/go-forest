package observe

import (
	"os"
	"sync"
)

// Handle is a real owned resource, probed by calls and lifecycle acceptance.
type Handle struct {
	mu     sync.Mutex
	file   *os.File
	closes int
}

func (h *Handle) Open(directory string) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	if e := os.MkdirAll(directory, 0700); e != nil {
		return e
	}
	f, e := os.CreateTemp(directory, "native-")
	h.file = f
	return e
}
func (h *Handle) Check() error {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.file == nil {
		return os.ErrClosed
	}
	_, e := h.file.Stat()
	return e
}
func (h *Handle) Close() error {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.file == nil {
		return nil
	}
	e := h.file.Close()
	if e != nil {
		return e
	}
	h.closes++
	e = os.Remove(h.file.Name())
	h.file = nil
	return e
}
func (h *Handle) Report() (bool, int) {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.file != nil, h.closes
}
