//go:build windows

package statemachine

import (
	"os/exec"
	"syscall"
)

func hideWorker(c *exec.Cmd) { c.SysProcAttr = &syscall.SysProcAttr{HideWindow: true} }
