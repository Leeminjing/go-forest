//go:build windows

package ecosystem

import (
	"errors"
	"os/exec"
	"syscall"
)

func confHide(cmd *exec.Cmd) { cmd.SysProcAttr = &syscall.SysProcAttr{HideWindow: true} }

// Antivirus/indexer handles may briefly deny reads of a fully renamed report.
// Only the two native sharing errors are retried, under the caller's deadline.
func confReportBusy(err error) bool {
	return errors.Is(err, syscall.Errno(32)) || errors.Is(err, syscall.Errno(33))
}
