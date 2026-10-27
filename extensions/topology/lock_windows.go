//go:build windows

package topology

import (
	"errors"
	"os"
	"strings"
	"syscall"
	"unsafe"
)

var kernel = syscall.NewLazyDLL("kernel32.dll")
var lockEx = kernel.NewProc("LockFileEx")
var unlockEx = kernel.NewProc("UnlockFileEx")

func lockFile(f *os.File) (func() error, error) {
	if strings.HasPrefix(f.Name(), `\\`) {
		return nil, ErrUnsupportedPlatform
	}
	overlapped := new(syscall.Overlapped)
	result, _, e := lockEx.Call(f.Fd(), 3, 0, 1, 0, uintptr(unsafe.Pointer(overlapped)))
	if result == 0 {
		return nil, errors.Join(ErrStoreBusy, e)
	}
	return func() error {
		result, _, e := unlockEx.Call(f.Fd(), 0, 1, 0, uintptr(unsafe.Pointer(overlapped)))
		if result == 0 {
			return e
		}
		return nil
	}, nil
}

// File.Sync flushes journal contents on Windows. This does not claim a portable
// directory-entry or device-cache power-loss guarantee.
func syncDirectory(string) error { return nil }
func isReparse(info os.FileInfo) bool {
	attributes, ok := info.Sys().(*syscall.Win32FileAttributeData)
	return ok && attributes.FileAttributes&syscall.FILE_ATTRIBUTE_REPARSE_POINT != 0
}
