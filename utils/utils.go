package utils

import (
	"os/exec"
)

func Execute(name string, arg ...string) (string, error) {
	cmd := exec.Command(name, arg...)
	stdoutStderr, err := cmd.CombinedOutput()
	return string(stdoutStderr), err
}
