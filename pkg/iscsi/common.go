package iscsi

import (
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/fightdou/os-brick-rbd/pkg/utils"
	"github.com/wonderivan/logger"
)

// Hctl is IDs of SCSI
type Hctl struct {
	HostID    int
	ChannelID int
	TargetID  int
	HostLUNID int
}

//GetHctl Given an iSCSI session return the host, channel, target, and lun
func GetHctl(id int, lun int) (*Hctl, error) {
	globStr := fmt.Sprintf("/sys/class/iscsi_host/host*/device/session%d/target*", id)
	paths, err := filepath.Glob(globStr)
	if err != nil {
		logger.Error("Failed to get session path", err)
		return nil, err
	}
	if len(paths) != 1 {
		logger.Error("target fail is not found", err)
		return nil, err
	}
	_, fileName := filepath.Split(paths[0])
	ids := strings.Split(fileName, ":")
	if len(ids) != 3 {
		return nil, fmt.Errorf("failed to parse iSCSI session filename")
	}
	channelID, err := strconv.Atoi(ids[1])
	if err != nil {
		return nil, fmt.Errorf("failed to parse channel ID: %w", err)
	}
	targetID, err := strconv.Atoi(ids[2])
	if err != nil {
		return nil, fmt.Errorf("failed to parse target ID: %w", err)
	}

	names := strings.Split(paths[0], "/")
	hostIDstr := strings.TrimPrefix(searchHost(names), "host")
	hostID, err := strconv.Atoi(hostIDstr)
	if err != nil {
		return nil, fmt.Errorf("failed to parse host ID: %w", err)
	}

	hctl := &Hctl{
		HostID:    hostID,
		ChannelID: channelID,
		TargetID:  targetID,
		HostLUNID: lun,
	}

	return hctl, nil
}

//searchHost search param
// return "host"+id
func searchHost(names []string) string {
	for _, v := range names {
		if strings.HasPrefix(v, "host") {
			return v
		}
	}

	return ""
}

//ScanISCSI
func ScanISCSI(hctl *Hctl) error {
	path := fmt.Sprintf("/sys/class/scsi_host/host%d/scan", hctl.HostID)
	content := fmt.Sprintf("%d %d %d",
		hctl.ChannelID,
		hctl.TargetID,
		hctl.HostLUNID)

	return utils.EchoScsiCommand(path, content)
}

func GetDeviceName(id int, hctl *Hctl) (string, error) {
	p := fmt.Sprintf(
		"/sys/class/iscsi_host/host%d/device/session%d/target%d:%d:%d/%d:%d:%d:%d/block/*",
		hctl.HostID,
		id,
		hctl.HostID, hctl.ChannelID, hctl.TargetID,
		hctl.HostID, hctl.ChannelID, hctl.TargetID, hctl.HostLUNID)

	paths, err := filepath.Glob(p)
	if err != nil {
		return "", fmt.Errorf("failed to parse iSCSI block device filepath: %w", err)
	}
	if len(paths) == 0 {
		return "", fmt.Errorf("device filepath is not found")
	}

	_, deviceName := filepath.Split(paths[0])

	return deviceName, nil
}

func removeScsiDevice(devicePath string) error {
	deviceName := strings.TrimPrefix(devicePath, "/dev/")
	deletePath := fmt.Sprintf("/sys/block/%s/device/delete", deviceName)
	_, err := os.Stat(deletePath)
	if err != nil {
		return fmt.Errorf("failed to stat device delete path: %w", err)
	}

	err = flushDeviceIO(devicePath)
	if err != nil {
		return fmt.Errorf("failed to flush device I/O: %w", err)
	}

	err = utils.EchoScsiCommand(deletePath, "1")
	if err != nil {
		return fmt.Errorf("failed to write to delete path: %w", err)
	}

	return nil
}

func waitForVolumesRemoval(targetDevicePaths []string) bool {
	exist := false
	for _, devicePath := range targetDevicePaths {
		_, err := os.Stat(devicePath)
		if err == nil {
			logger.Info("found not deleted volume: %s", devicePath)
			exist = true
			break
		}
	}
	return exist
}

// GetConnectionDevices get volumes in paths
func GetConnectionDevices(targets []Target) ([]string, error) {
	var devices []string
	sessions, err := GetSessions()
	if err != nil {
		return nil, fmt.Errorf("failed to get iSCSI sessions: %w", err)
	}
	for _, target := range targets {
		for _, session := range sessions {
			if session.TargetPortal != target.Portal || session.IQN != target.Iqn {
				continue
			}

			hctl, err := GetHctl(session.SessionID, target.Lun)
			if err != nil {
				return nil, fmt.Errorf("failed to get hctl info: %w", err)
			}
			deviceName, err := GetDeviceName(session.SessionID, hctl)
			if err != nil {
				return nil, fmt.Errorf("failed to get device name: %w", err)
			}
			if hctl.HostLUNID == target.Lun {
				devices = append(devices, deviceName)
			}
		}
	}
	return devices, nil
}

func RemoveConnection(targetDeviceNames []string, isMultiPath bool) error {
	var devicePaths []string
	for _, dn := range targetDeviceNames {
		devicePaths = append(devicePaths, "/dev/"+dn)
	}
	if isMultiPath {
		multiPathDeviceName, err := FindSysfsMultipathDM(targetDeviceNames[0])
		if err != nil {
			logger.Error("Find dm device failed", err)
			return err
		}
		logger.Debug("Removing devices %v", devicePaths)
		multiPathDevicePath := "/dev/" + multiPathDeviceName
		err = flushMultipathDevice(multiPathDevicePath)
		logger.Debug("Flush multipath devices %v", devicePaths)
		if err != nil {
			logger.Error("Flush %s failed", multiPathDevicePath)
		}
	}

	for _, devicePath := range devicePaths {
		err := removeScsiDevice(devicePath)
		if err != nil {
			return fmt.Errorf("failed to remove iSCSI device: %w", err)
		}
	}

	timeoutSecond := 10
	for i := 0; waitForVolumesRemoval(targetDeviceNames); i++ {
		// until exist target volume.
		logger.Info("wait removed target volume...")
		time.Sleep(1 * time.Second)

		if i == timeoutSecond {
			return fmt.Errorf("timeout exceeded wait for volume removal")
		}
	}

	err := removeScsiSymlinks(devicePaths)
	if err != nil {
		return fmt.Errorf("failed to remove scsi symlinks: %w", err)
	}
	return nil
}

func removeScsiSymlinks(devicePaths []string) error {
	links, err := filepath.Glob("/dev/disk/by-id/scsi-*")
	if err != nil {
		return fmt.Errorf("failed to get scsi link")
	}

	var removeTarget []string
	for _, link := range links {
		realpath, err := filepath.EvalSymlinks(link)
		if err != nil {
			logger.Info("failed to get realpath: %v", err)
		}

		for _, devicePath := range devicePaths {
			if realpath == devicePath {
				removeTarget = append(removeTarget, link)
				break
			}
		}
	}

	for _, l := range removeTarget {
		err = os.Remove(l)
		if err != nil {
			return fmt.Errorf("failed to delete symlink: %w", err)
		}
	}

	return nil
}

//disconnectFromIscsiPortal login iscsi partal
func disconnectFromIscsiPortal(portal string, iqn string) error {
	_, err := utils.UpdateIscsiadm(portal, iqn, "node.startup", "manual", nil)
	if err != nil {
		return fmt.Errorf("failed to update node.startup to manual: %w", err)
	}
	_, err = utils.ExecIscsiadm(portal, iqn, []string{"--logout"})
	if err != nil {
		logger.Error("Exec iscsiadm login command failed", err)
		return err
	}
	_, err = utils.ExecIscsiadm(portal, iqn, []string{"--op", "delete"})
	if err != nil {
		return fmt.Errorf("failed to execute --op delete: %w", err)
	}
	logger.Info("iscsiadm portal %s logout success", portal)
	return nil
}