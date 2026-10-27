//go:build windows

package topology

import (
	"os/exec"
	"syscall"
)

func hideWorker(cmd *exec.Cmd) { cmd.SysProcAttr = &syscall.SysProcAttr{HideWindow: true} }
