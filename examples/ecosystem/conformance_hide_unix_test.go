//go:build !windows

package ecosystem

import "os/exec"

func confHide(*exec.Cmd) {}

func confReportBusy(error) bool { return false }
