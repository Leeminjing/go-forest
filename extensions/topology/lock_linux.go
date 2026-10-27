//go:build linux

package topology

import (
	"errors"
	"os"
	"syscall"
)

func lockFile(f *os.File) (func() error, error) {
	if e := syscall.Flock(int(f.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); e != nil {
		return nil, errors.Join(ErrStoreBusy, e)
	}
	return func() error { return syscall.Flock(int(f.Fd()), syscall.LOCK_UN) }, nil
}
func syncDirectory(path string) error {
	f, e := os.Open(path)
	if e != nil {
		return e
	}
	return errors.Join(f.Sync(), f.Close())
}
func isReparse(os.FileInfo) bool { return false }
