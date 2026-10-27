//go:build !windows && !linux

package topology

import "os"

func lockFile(*os.File) (func() error, error) { return nil, ErrUnsupportedPlatform }
func syncDirectory(string) error              { return ErrUnsupportedPlatform }
func isReparse(os.FileInfo) bool              { return false }
