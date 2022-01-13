package utils

import (
	"fmt"
	"os/exec"
	"strconv"
)

func Execute(name string, arg ...string) (string, error) {
	cmd := exec.Command(name, arg...)
	stdoutStderr, err := cmd.CombinedOutput()
	return string(stdoutStderr), err
}

func IsBool(args interface{}) bool {
	temp := fmt.Sprint(args)
	var res bool
	switch args.(type) {
	case bool:
		res, _ := strconv.ParseBool(temp)
		return res
	default:
		return res
	}
}

func IsString(args interface{}) string {
	temp := fmt.Sprint(args)
	return temp
}

func IsStringList(args interface{}) []string {
	argsList := args.([]interface{})
	result := make([]string, len(argsList))
	for i, v := range argsList {
		result[i] = v.(string)
	}
	return result
}
