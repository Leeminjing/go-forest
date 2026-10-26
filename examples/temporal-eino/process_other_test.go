//go:build !windows

package einodemo_test

import "os/exec"

func hideCommand(*exec.Cmd) {}
