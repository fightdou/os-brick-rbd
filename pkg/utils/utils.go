package utils

import (
	"fmt"
	"os/exec"
	"strconv"
)

// Execute a shell command
func Execute(command string, arg ...string) (string, error) {
	cmd := exec.Command(command, arg...)
	stdoutStderr, err := cmd.CombinedOutput()
	return string(stdoutStderr), err
}

func ExecIscsiadm(portalIP string, iqn string, args []string) (string, error) {
	var cmd []string
	baseArgs := []string{"-m", "node"}
	cmd = append(baseArgs, []string{"-T", iqn}...)
	cmd = append(cmd, []string{"-p", portalIP}...)
	cmd = append(cmd, args...)

	out, err := Execute("iscsiadm", cmd...)
	if err != nil {
		return "", fmt.Errorf("failed to execute iscsiadm command: %w", err)
	}

	return out, nil
}

func UpdateIscsiadm(portalIP, targetIQN, key, value string, args []string) (string, error) {
	a := []string{"--op", "update", "-n", key, "-v", value}
	a = append(a, args...)
	return ExecIscsiadm(portalIP, targetIQN, a)
}

func EchoScsiCommand(path, content string) error {
	// write content to path (sysfs)
	logger.Info("write scsi file [path: %s content: %s]", path, content)

	f, err := os.OpenFile(path, os.O_WRONLY, 0400)
	if err != nil {
		return fmt.Errorf("failed to open file: %w", err)
	}
	defer f.Close()

	if _, err := f.WriteString(content); err != nil {
		return fmt.Errorf("failed to write file: %w", err)
	}

	return nil
}

func ToBool(i interface{}) bool {
	switch res := i.(type) {
	case bool:
		return res
	case int, int32, int64, uint, uint32, uint64:
		return res != 0
	case string:
		result, err := strconv.ParseBool(res)
		if err != nil {
			panic(err.Error())
		}
		return result
	default:
		panic(fmt.Sprintf("Can not convert %T to bool.", res))
	}
}

func ToInt(i interface{}) int {
	var res int
	switch e := i.(type) {
	case int:
		res = e
	}
	return res
}

func ToIntSlice(i interface{}) []int {
	resSlice := i.([]interface{})
	result := make([]int, len(resSlice))
	for i, v := range resSlice {
		result[i] = v.(int)
	}
	return result
}

func ToString(i interface{}) string {
	res := fmt.Sprint(i)
	return res
}

func ToStringSlice(i interface{}) []string {
	resSlice := i.([]interface{})
	result := make([]string, len(resSlice))
	for i, v := range resSlice {
		result[i] = ToString(v)
	}
	return result
}
