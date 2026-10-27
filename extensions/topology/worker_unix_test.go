//go:build !windows

package topology

import "os/exec"

func hideWorker(*exec.Cmd) {}
