//go:build windows

package einodemo_test

import (
	"os/exec"
	"syscall"
)

func hideCommand(cmd *exec.Cmd) { cmd.SysProcAttr = &syscall.SysProcAttr{HideWindow: true} }
