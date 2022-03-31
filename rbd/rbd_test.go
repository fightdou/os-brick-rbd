package rbd

import (
	"errors"
	"fmt"
	"reflect"
	"strings"
	"testing"

	"github.com/fightdou/os-brick-rbd/pkg/utils"
)

var callRecords []string
var (
	fakePool     = "fake_pool"
	fakeVolume   = "fake_volume"
	fakeCluster  = "fake_cluster"
	fakeUser     = "fake_user"
	fakeVolumeId = "fake_volume_id"
	fakeHost1    = "host1"
	fakePort1    = "1"
	fakeHost2    = "host2"
	fakePort2    = "2"
	fakeDevice   = "/dev/rbd1"
)

var rbdConnector = NewRBDConnector(map[string]interface{}{
	"data": map[string]interface{}{
		"name":          fmt.Sprintf("%s/%s", fakePool, fakeVolume),
		"hosts":         []interface{}{fakeHost1, fakeHost2},
		"ports":         []interface{}{fakePort1, fakePort2},
		"cluster_name":  fakeCluster,
		"auth_enabled":  "True",
		"auth_username": fakeUser,
		"volume_id":     fakeVolumeId,
		"discard":       false,
		"qos_specs":     "fake_qos",
		"access_mode":   "rw",
		"encrypted":     1,
	},
	"do_local_attach": true,
})

func fakeExecute(command string, arg ...string) (string, error) {
	cmdArg := strings.Join(arg, " ")
	callRecords = append(callRecords, strings.Join([]string{command, cmdArg}, " "))
	switch command {
	case "which":
		return fmt.Sprintf("/usr/bin/%s\n", cmdArg), nil
	case "rbd":
		if strings.HasPrefix(cmdArg, "map") {
			return fakeDevice, nil
		} else if strings.HasPrefix(cmdArg, "unmap") {
			return "", nil
		} else if strings.HasPrefix(cmdArg, "showmapped") {
			fakeRes := fmt.Sprintf("[{\"name\": \"%s\", \"device\": \"%s\"}]", fakeVolume, fakeDevice)
			return fakeRes, nil
		}
		return "", errors.New("Unexpected arg  for ceph")
	default:
		return "Unexpected command", errors.New("Unexpected command")
	}
}

func TestConnectVolume(t *testing.T) {
	utilsExecute = fakeExecute
	defer func() {
		utilsExecute = utils.Execute
		callRecords = []string{}
	}()
	_, err := rbdConnector.ConnectVolume()
	if err != nil {
		t.Error("Volume connection encounter error.")
	}
	expected_cmds := []string{
		"which rbd",
		fmt.Sprintf("rbd map %s --pool %s --id %s --mon_host %s:%s,%s:%s", fakeVolume, fakePool, fakeUser, fakeHost1, fakePort1, fakeHost2, fakePort2),
	}
	if !reflect.DeepEqual(expected_cmds, callRecords) {
		t.Errorf("\nExpected calls:\n%s\nActual calls:\n%s", strings.Join(expected_cmds, "\n"), strings.Join(callRecords, "\n"))
	}
}

func TestDisConnectVolume(t *testing.T) {
	utilsExecute = fakeExecute
	defer func() {
		utilsExecute = utils.Execute
		callRecords = []string{}
	}()
	err := rbdConnector.DisConnectVolume()
	if err != nil {
		t.Error("Volume disconnection encounter error.")
	}
	expected_cmds := []string{
		"rbd showmapped --format=json --id fake_user",
		fmt.Sprintf("rbd unmap %s", fakeDevice),
	}
	if !reflect.DeepEqual(expected_cmds, callRecords) {
		t.Errorf("\nExpected calls:\n%s\nActual calls:\n%s", strings.Join(expected_cmds, "\n"), strings.Join(callRecords, "\n"))
	}
}

func TestGetDevicePath(t *testing.T) {
	utilsExecute = fakeExecute
	defer func() {
		utilsExecute = utils.Execute
		callRecords = []string{}
	}()
	expected_path := fmt.Sprintf("/dev/rbd/%s/%s", fakePool, fakeVolume)
	path := rbdConnector.GetDevicePath()
	if path != expected_path {
		t.Errorf("\nExpected path:\n%s\nActula path:\n%s", expected_path, path)
	}
}
