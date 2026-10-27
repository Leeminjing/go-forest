//go:build !windows

package statemachine

import "os/exec"

func hideWorker(*exec.Cmd) {}
