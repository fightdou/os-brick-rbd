package iscsi

import (
	"fmt"
	"path/filepath"
	"sync"

	"github.com/fightdou/os-brick-rbd/pkg/iscsi"
	"github.com/fightdou/os-brick-rbd/pkg/utils"
	"github.com/wonderivan/logger"
)

var RetryCount int = 10

// ConnISCSI contains iscsi volume info
type ConnISCSI struct {
	targetDiscovered bool
	targetPortal     string
	targetPortals    []string
	targetIqn        string
	targetIqns       []string
	targetLun        int
	targetLuns       []int
	volumeID         string
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
	if data["target_portals"] != nil && data["target_iqns"] != nil && data["target_luns"] != nil {
		conn.targetPortals = utils.ToStringSlice(data["target_portals"])
		conn.targetIqns = utils.ToStringSlice(data["target_iqns"])
		conn.targetLuns = utils.ToIntSlice(data["target_luns"])
	}
	conn.targetIqn = utils.ToString(data["target_iqn"])
	conn.targetLun = utils.ToInt(data["target_lun"])
	conn.volumeID = utils.ToString(data["volume_id"])
	conn.authMethod = utils.ToString(data["auth_method"])
	conn.authUsername = utils.ToString(data["auth_username"])
	conn.authPassword = utils.ToString(data["auth_password"])
	conn.QosSpecs = utils.ToString(data["qos_specs"])
	conn.AccessMode = utils.ToString(data["access_mode"])
	conn.Encrypted = utils.ToBool(data["encrypted"])
	return conn
}

//ConnectVolume Attach the volume to pod
func (c *ConnISCSI) ConnectVolume() (map[string]string, error) {
	res := map[string]string{}
	if len(c.targetIqns) >= 1 {
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

//DisConnectVolume Detach the volume from pod
func (c *ConnISCSI) DisConnectVolume() error {
	err := c.cleanupConnection()
	if err != nil {
		logger.Error("Disconnect volume failed", err)
		return err
	}
	return nil
}

//ExtendVolume Update the local kernel's size information
func (c *ConnISCSI) ExtendVolume() (int64, error) {
	return 0, nil
}

//GetDevicePath Get mount device local path
func (c *ConnISCSI) GetDevicePath() string {
	target := c.getAllTargets()
	var devicePath string
	for _, i := range target {
		devicePath = fmt.Sprintf("/dev/disk/by-path/ip-%s-iscsi-%s-lun-%d", i.Portal, i.Iqn, i.Lun)
	}
	return devicePath
}

//connectMultiPathVolume Connect to a multipathed volume launching parallel login requests
func (c *ConnISCSI) connectMultiPathVolume() (string, error) {
	var err error
	target := c.getIpsIqnsLuns()
	var wg sync.WaitGroup
	var devices []string
	for _, p := range target {
		wg.Add(1)
		device, err := c.connVolume(p.Portal, p.Iqn, p.Lun)
		if err != nil {
			logger.Error("Failed to connect volume", err)
			return "", err
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

//connectSinglePathVolume Connect to a volume using a single path.
func (c *ConnISCSI) connectSinglePathVolume() (string, error) {
	var device string
	var err error
	target := c.getAllTargets()
	for i := range target {
		device, err = c.connVolume(target[i].Portal, target[i].Iqn, target[i].Lun)
		if err != nil {
			logger.Error("Request connect iscsi singlepath volume failed", err)
			return "", err
		}
	}
	return filepath.Join("/dev/", device), nil
}

//getIpsIqnsLuns Build a list of ips, iqns, and luns, use iSCSI discovery to get the information
func (c *ConnISCSI) getIpsIqnsLuns() []iscsi.Target {
	if c.targetPortals != nil && c.targetIqns != nil {
		ipsIqnsLuns := c.getAllTargets()
		return ipsIqnsLuns
	} else {
		target := iscsi.DiscoverIscsiPortals(c.targetPortal, c.targetIqn, c.targetLun)
		return target
	}
}

//getAllTargets Get target include ips, iqns, and luns
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

//connVolume Make a connection to a volume, send scans and wait for the device.
func (c *ConnISCSI) connVolume(portal string, iqn string, lun int) (string, error) {
	sessionId, err := c.connectToIscsiPortal(portal, iqn)
	if err != nil {
		logger.Error("Failed get iscsi session failed", err)
		return "", err
	}
	hctl, err := iscsi.GetHctl(sessionId, lun)
	if err != nil {
		logger.Error("Failed get volume hctl ", err)
		return "", err
	}
	if err := iscsi.ScanISCSI(hctl); err != nil {
		logger.Error("Failed to rescan target", err)
		return "", err
	}
	device, err := iscsi.GetDeviceName(sessionId, hctl)
	if err != nil {
		logger.Error("Failed to get device name", err)
		return "", err
	}
	logger.Debug("Connect volume [portal %s, iqn %s] success", portal, iqn)
	return device, nil
}

//connectToIscsiPortal Connect to iSCSI portal-target and return the session id
func (c *ConnISCSI) connectToIscsiPortal(portal string, iqn string) (int, error) {
	var err error
	if err := c.loginPortal(portal, iqn); err != nil {
		logger.Error("Iscsi login portal failed", err)
		return -1, err
	}
	for i := 0; i < RetryCount; i++ {
		sessions, err := iscsi.GetSessions()
		if err != nil {
			logger.Error("Get iscsi session failed", err)
			return 0, err
		}
		for _, session := range sessions {
			if session.TargetPortal == portal && session.IQN == iqn {
				return session.SessionID, nil
			}
		}
	}
	return -1, err
}

//loginPortal login iscsi partal
func (c *ConnISCSI) loginPortal(portal string, iqn string) error {
	var err error
	_, err = utils.ExecIscsiadm(portal, iqn, []string{})
	if err != nil {
		_, err := utils.ExecIscsiadm(portal, iqn, []string{"--interface", "default", "--op", "new"})
		if err != nil {
			logger.Error("Encountered database failure for %s", portal)
			return err
		}
	}

	_, err = utils.UpdateIscsiadm(portal, iqn, "node.session.scan", "manual", nil)
	if err != nil {
		logger.Error("Exec iscsiadm update node.session.scan manual failed", err)
		return err
	}

	if c.authMethod == "CHAP" {
		_, _ = utils.UpdateIscsiadm(portal, iqn, "node.session.auth.authmethod", c.authMethod, nil)
		_, _ = utils.UpdateIscsiadm(portal, iqn, "node.session.auth.username", c.authUsername, nil)
		_, _ = utils.UpdateIscsiadm(portal, iqn, "node.session.auth.password", c.authPassword, nil)
	}

	_, err = utils.ExecIscsiadm(portal, iqn, []string{"--login"})
	if err != nil {
		logger.Error("Exec iscsiadm login %s %s command failed", portal, iqn, err)
		return err
	}

	_, err = utils.ExecIscsiadm(portal, iqn, []string{"--rescan"})
	if err != nil {
		logger.Error("Exec iscsiadm rescan %s %s command failed", portal, iqn, err)
		return err
	}

	_, err = utils.UpdateIscsiadm(portal, iqn, "node.startup", "automatic", nil)
	if err != nil {
		logger.Error("Exec iscsiadm update command failed", err)
		return err
	}
	logger.Debug("iscsiadm portal %s login success", portal)
	return nil
}

//cleanupConnection Cleans up connection flushing and removing devices and multipath
func (c *ConnISCSI) cleanupConnection() error {
	var err error
	target := c.getAllTargets()
	deviceMap, err := iscsi.GetConnectionDevices(target)
	if err != nil {
		logger.Error("Get iscsi connection device failed", err)
		return err
	}

	isMultiPath := false
	if len(deviceMap) > 1 {
		isMultiPath = true
	}

	err = iscsi.RemoveConnection(deviceMap, isMultiPath)
	if err != nil {
		logger.Error("Remove iscsi connection failed", err)
		return err
	}

	if err = iscsi.DisconnectConnection(target); err != nil {
		logger.Error("failed to disconnet iSCSI connection", err)
		return err
	}
	logger.Info("Cleanup iscsi connection success!")
	return nil
}
