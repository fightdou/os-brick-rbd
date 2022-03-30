package iscsi

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/fightdou/os-brick-rbd/utils"
)

type Target struct {
	Portal string
	Iqn    string
	Lun    int
}

// DiscoverIscsiPortals get iscsi connection information
func DiscoverIscsiPortals(portal string, iqn string, luns int) []Target {
	var target []Target
	var portals []string
	var iqns []string
	args := []string{"-m", "discovery", "-t", "sendtargets", "-p", portal}
	out, err := utils.Execute("iscsiadm", args...)
	if err != nil {
		return nil
	}
	entries := strings.Split(out, "\n")
	for _, entry := range entries {
		data := strings.Split(entry, " ")
		if !strings.Contains(data[1], iqn) {
			continue
		}
		p := strings.Split(data[0], ",")[0]
		portals = append(portals, p)
		iqns = append(iqns, data[1])
	}

	for i, por := range portals {
		t := NewTarget(por, iqns[i], luns)
		target = append(target, t)
	}

	return target
}

func NewTarget(portals string, iqns string, luns int) Target {
	p := Target{
		Portal: portals,
		Iqn:    iqns,
		Lun:    luns,
	}
	return p
}

//FindSysfsMultipathDM Find the dm device name given a list of device names
func FindSysfsMultipathDM(deviceName string) (dmDeviceName string, err error) {
	globStr := fmt.Sprintf("/sys/block/%s/holders/dm-*", deviceName)
	paths, err := filepath.Glob(globStr)
	if err != nil {
		return "", fmt.Errorf("failed to glob dm device filepath: %w", err)
	}
	if len(paths) == 0 {
		return "", fmt.Errorf("dm device is not found")
	}

	_, name := filepath.Split(paths[0])
	return name, nil
}

func DisconnectConnection(targets []Target) error {
	for _, p := range targets {
		err := disconnectFromIscsiPortal(p.Portal, p.Iqn)
		if err != nil {
			return fmt.Errorf("failed to disconnect from iSCSI portal: %w", err)
		}
	}
	return nil
}

func flushMultipathDevice(targetMultipathPath string) error {
	args := []string{"-f", targetMultipathPath}
	_, err := utils.Execute("multipath", args...)
	if err != nil {
		return fmt.Errorf("failed to execute multipath device flush command: %w", err)
	}

	return nil
}

func flushDeviceIO(devicePath string) error {
	_, err := os.Stat(devicePath)
	if err != nil {
		return fmt.Errorf("failed to stat device path: %w", err)
	}
	args := []string{"--flushbufs", devicePath}
	if _, err := utils.Execute("blockdev", args...); err != nil {
		return fmt.Errorf("failed to execute blockdev command: %s", err)
	}

	return nil
}
