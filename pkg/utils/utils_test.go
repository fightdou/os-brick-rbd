package utils

import (
	"reflect"
	"testing"
)

func TestExecute(t *testing.T) {
	t.Parallel()
	res, _ := Execute("echo", "-n", "aaa")
	if res != "aaa" {
		t.Error("Error echo value.")
	}
	_, err := Execute("err_cmd")
	if err == nil {
		t.Error("Error command expect get a error output.")
	}
}

func assertToBoolPanic(t *testing.T, f func(interface{}) bool, v interface{}) {
	defer func() {
		if r := recover(); r == nil {
			t.Error("The code did not panic")
		}
	}()
	f(v)
}

func TestToBool(t *testing.T) {
	t.Parallel()
	assertToBoolPanic(t, ToBool, "abc")
	assertToBoolPanic(t, ToBool, [1]int{1})
	if ToBool(0) {
		t.Error("The integer number 0 shoule be converted to false.")
	}
	if !ToBool(1) {
		t.Error("The integer number 1 should be converted to true")
	}
	trueStrings := []string{"1", "t", "T", "true", "TRUE", "True"}
	for _, v := range trueStrings {
		if !ToBool(v) {
			t.Error("Expected true")
		}
	}
	falseStrings := []string{"0", "f", "F", "false", "FALSE", "False"}
	for _, v := range falseStrings {
		if ToBool(v) {
			t.Error("Expected false")
		}
	}
}

func TestToString(t *testing.T) {
	t.Parallel()
	resType := reflect.TypeOf(ToString(1)).String()
	if resType != "string" {
		t.Errorf("Error type")
	}
	resType = reflect.TypeOf(ToString("1")).String()
	if resType != "string" {
		t.Error("Error type")
	}
}

func TestToStringSlice(t *testing.T) {
	t.Parallel()
	data := []interface{}{"a", 1}
	res := ToStringSlice(data)
	if res[0] != "a" {
		t.Error("Error value!!!")
	}
	if res[1] != "1" {
		t.Error("Error value!!!")
	}
}
