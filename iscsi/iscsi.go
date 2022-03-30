package iscsi

import (
	"fmt"
	"path/filepath"
	"sync"

	"github.com/fightdou/os-brick-rbd/pkg/iscsi"
	"github.com/fightdou/os-brick-rbd/pkg/utils"
	"github.com/wonderivan/logger"
)

// ConnISCSI contains iscsi volume info
type ConnISCSI struct {
	targetDiscovered bool
	targetPortal     string
	targetPortals    []string
	targetIqn        string
	targetIqns       []string
	targetLun        int
	targetLuns       []int
	volumeID         bool
	authMethod       string
	authUsername     string
	authPassword     string
	QosSpecs         string
	AccessMode       string
	Encrypted        bool
}

// NewISCSIConnector Return ConnRbd Pointer to the object
func NewISCSIConnector(connInfo map[string]interface{}) *ConnISCSI {
	data := connInfo["data"].(map[string]interface{})
	conn := &ConnISCSI{}
	conn.targetDiscovered = utils.ToBool(data["target_discovered"])
	conn.targetPortal = utils.ToString(data["target_portal"])
	conn.targetPortals = utils.ToStringSlice(data["target_portal"])
	conn.targetIqn = utils.ToString(data["target_iqn"])
	conn.targetIqns = utils.ToStringSlice(data["target_iqns"])
	conn.targetLun = utils.ToInt(data["target_lun"])
	conn.targetLuns = utils.ToIntSlice(data["target_luns"])
	conn.volumeID = utils.ToBool(data["volume_id"])
	conn.authMethod = utils.ToString(data["auth_method"])
	conn.authUsername = utils.ToString(data["auth_username"])
	conn.authPassword = utils.ToString(data["auth_password"])
	conn.QosSpecs = utils.ToString(data["qos_specs"])
	conn.AccessMode = utils.ToString(data["access_mode"])
	conn.Encrypted = utils.ToBool(data["encrypted"])
	return conn
}
func (c *ConnISCSI) ConnectVolume() (map[string]string, error) {
	var res map[string]string
	if len(c.targetIqns) != 1 {
		device, err := c.connectMultiPathVolume()
		if err != nil {
			return nil, err
		}
		res["path"] = device
	} else {
		device, err := c.connectSinglePathVolume()
		if err != nil {
			return nil, err
		}
		res["path"] = device
	}
	return res, nil
}

func (c *ConnISCSI) DisConnectVolume() {
	err := c.cleanupConnection()
	if err != nil {
		logger.Error("Disconnect volume failed", err)
	}
}

func (c *ConnISCSI) ExtendVolume() (int64, error) {
	return 0, nil
}

func (c *ConnISCSI) GetDevicePath() string {
	target := c.getAllTargets()
	var devicePath string
	for _, i := range target {
		devicePath = fmt.Sprintf("/dev/disk/by-path/ip-%s-iscsi-%s-lun-%d", i.Portal, i.Iqn, i.Lun)
	}
	return devicePath
}

func (c *ConnISCSI) cleanupConnection() error {
	target := c.getAllTargets()
	deviceMap, err := iscsi.GetConnectionDevices(target)
	if err != nil {
		return err
	}
	var devicePaths []string
	for _, dn := range deviceMap {
		devicePaths = append(deviceMap, "/dev/"+dn)
	}
	isMultiPath := false
	if len(devicePaths) > 1 {
		isMultiPath = true
	}

	rErr := iscsi.RemoveConnection(deviceMap, isMultiPath)
	if rErr != nil {
		return rErr
	}
	attachedDevices, err := iscsi.GetAttachedSCSIDevices()
	if err != nil {
		return fmt.Errorf("failed to get attached devices: %w", err)
	}

	if len(attachedDevices) == 0 {
		// call logout when No action session
		if err := iscsi.DisconnectConnection(target); err != nil {
			return fmt.Errorf("failed to disconnet iSCSI connection: %w", err)
		}
	}
	return nil
}

func (c *ConnISCSI) connectMultiPathVolume() (string, error) {
	var err error
	target := c.getIpsIqnsLuns()
	var wg sync.WaitGroup
	var devices []string
	for _, p := range target {
		wg.Add(1)
		device, err := iscsi.ConVolume(p.Portal, p.Iqn, p.Lun)
		if err != nil {
			return "", fmt.Errorf("failed to connect volume: %w", err)
		}
		devices = append(devices, device)
		wg.Done()
	}
	wg.Wait()

	var dm string
	for _, d := range devices {
		dm, err = iscsi.FindSysfsMultipathDM(d)
		if err == nil {
			logger.Info("found dm device: %v", dm)
			break
		}

		logger.Error("found err, continue... [device: %s] [err: %s]", d, err.Error())
		continue
	}
	return filepath.Join("/dev", dm), nil
}

func (c *ConnISCSI) connectSinglePathVolume() (string, error) {
	target := c.getAllTargets()
	if len(target) != 1 {
		return "", fmt.Errorf("found multipath but call ConnectSinglePathVolume")
	}
	p := target[0]
	device, err := iscsi.ConVolume(p.Portal, p.Iqn, c.targetLun)
	if err != nil {
		return "", err
	}
	return filepath.Join("/dev/", device), nil
}

func (c *ConnISCSI) getIpsIqnsLuns() []iscsi.Target {
	if c.targetPortals != nil && c.targetIqns != nil {
		ipsIqnsLuns := c.getAllTargets()
		return ipsIqnsLuns
	} else {
		target := iscsi.DiscoverIscsiPortals(c.targetPortal, c.targetIqn, c.targetLun)
		return target
	}
}

func (c *ConnISCSI) getAllTargets() []iscsi.Target {
	var allTarget []iscsi.Target
	if len(c.targetPortals) > 1 && len(c.targetIqns) > 1 {
		for i, portalIP := range c.targetPortals {
			ips := iscsi.NewTarget(portalIP, c.targetIqns[i], c.targetLun)
			allTarget = append(allTarget, ips)
		}
		return allTarget
	}
	ips := iscsi.NewTarget(c.targetPortal, c.targetIqn, c.targetLun)
	allTarget = append(allTarget, ips)
	return allTarget
}
