package rbd

import (
	"bufio"
	"encoding/json"
	"fmt"
	"github.com/fightdou/os-brick-rbd/initiator"
	osBrick "github.com/fightdou/os-brick-rbd/utils"
	"github.com/wonderivan/logger"
	"io/ioutil"
	"os"
	"path"
	"strconv"
	"strings"
)

// ConnRbd contains rbd volume info
type ConnRbd struct {
	Name          string
	Hosts         []string
	Ports         []string
	ClusterName   string
	AuthEnabled   bool
	AuthUserName  string
	VolumeID      string
	Discard       bool
	QosSpecs      string
	Keyring       string
	AccessMode    string
	Encrypted     bool
	DoLocalAttach bool
}

// NewRBDConnector Return ConnRbd Pointer to the object
func NewRBDConnector(connInfo map[string]interface{}) *ConnRbd {
	data := connInfo["data"].(map[string]interface{})
	conn := &ConnRbd{}
	conn.Name = osBrick.IsString(data["name"])
	conn.Hosts = osBrick.IsStringList(data["hosts"])
	conn.Ports = osBrick.IsStringList(data["ports"])
	conn.ClusterName = osBrick.IsString(data["cluster_name"])
	conn.AuthEnabled = osBrick.IsBool(data["auth_enabled"])
	conn.AuthUserName = osBrick.IsString(data["auth_username"])
	conn.VolumeID = osBrick.IsString(data["volume_id"])
	conn.Discard = osBrick.IsBool(data["discard"])
	conn.QosSpecs = osBrick.IsString(data["qos_specs"])
	conn.AccessMode = osBrick.IsString(data["access_mode"])
	conn.Encrypted = osBrick.IsBool(data["encrypted"])
	conn.DoLocalAttach = osBrick.IsBool(connInfo["do_local_attach"])
	return conn
}

// GetVolumePaths Return the list of existing paths for a volume
func (c *ConnRbd) GetVolumePaths() []interface{} {
	return nil
}

// GetSearchPath Return the directory where a Connector looks for volumes
func (c *ConnRbd) GetSearchPath() interface{} {
	return nil
}

// GetALLAvailableVolumes Return all volumes that exist in the search directory
func (c *ConnRbd) GetALLAvailableVolumes() interface{} {
	return nil
}

// CheckIOHandlerValid Check IO handle has correct data type
func (c *ConnRbd) CheckIOHandlerValid() error {
	return nil
}

// CheckVailDevice Test to see if the device path is a real device
func (c *ConnRbd) CheckVailDevice(path interface{}, rootAccess bool) bool {
	if path == nil {
		return false
	}
	switch path.(type) {
	case string:
		if rootAccess {
			res := checkVailDevice(path.(string))
			return res
		}
	default:
		return false
	}
	return true
}

// ConnectVolume Connect to a volume
func (c *ConnRbd) ConnectVolume() (map[string]string, error) {
	var err error
	result := map[string]string{}
	if c.DoLocalAttach {
		result, err = c.localAttachVolume()
		if err != nil {
			logger.Error("Do local attach volume failed", err)
			return nil, err
		}
		return result, nil
	}
	return nil, err
}

// DisConnectVolume Disconnect a volume
func (c *ConnRbd) DisConnectVolume(deviceInfo map[string]string) {
	if c.DoLocalAttach {
		var conf string
		if deviceInfo != nil {
			conf = deviceInfo["conf"]
		}
		rootDevice := c.findRootDevice(conf)
		if rootDevice != "" {
			cmd := []string{"unmap", rootDevice}
			args := c.getRbdArgs(conf)
			cmd = append(cmd, args...)
			osBrick.Execute("rbd", cmd...)
			if conf != "" {
				os.Remove(conf)
			}
			logger.Info("Exec rbd unmap command success")
		}
	}
}

// ExtendVolume Refresh local volume view and return current size in bytes
// Nothing to do, RBD attached volumes are automatically refreshed, but
// we need to return the new size for compatibility
func (c *ConnRbd) ExtendVolume() (int64, error) {
	var err error
	if c.DoLocalAttach {
		var conf string
		device := c.findRootDevice(conf)
		if device == "" {
			logger.Error("device is not exist", err)
			return 0, err
		}
		deviceName := path.Base(device)
		deviceNumber := deviceName[3:]
		size, err := ioutil.ReadFile("/sys/devices/rbd/" + deviceNumber + "/size")
		if err != nil {
			logger.Error("Read /sys/devices/rbd/?/size failed", err)
			return 0, err
		}
		strSize := string(size)
		vSize := strings.Replace(strSize, "'", "", -1)
		iSize, _ := strconv.ParseInt(vSize, 10, 64)
		logger.Info("extend volume to %s is success", iSize)
		return iSize, nil
	} else {
		handle := c.getRbdHandle()
		handle.Seek(0, 2)
		return handle.Tell(), err
	}
	return 0, err
}

// createCephConf Create ceph config file
func (c *ConnRbd) createCephConf() (string, error) {
	monitors := c.generateMonitorHost()
	monHosts := fmt.Sprintf("mon_host = %s", monitors)
	userKeyring := checkOrGetKeyringContents(c.Keyring, c.ClusterName, c.AuthUserName)

	data := "[global]"
	data = data + "\n" + monHosts + "\n" + fmt.Sprintf("[client.%s]", c.AuthUserName) + "\n" +
		fmt.Sprintf("keyring = %s", userKeyring)

	tmpFile, err := ioutil.TempFile("/tmp", "keyfile-")
	if err != nil {
		logger.Error("error creating a temporary keyfile", err)
		return "", err
	}
	defer func() {
		if err != nil {
			// don't complain about unhandled error
			_ = os.Remove(tmpFile.Name())
		}
	}()

	_, err = tmpFile.WriteString(data)
	if err != nil {
		logger.Error("failed to write temporary file", err)
		return "", err
	}
	tmpFile.Close()
	return tmpFile.Name(), nil
}

// createNonOpenstackConfig Get Ceph's .conf file for non OpenStack usage
func (c *ConnRbd) createNonOpenstackConfig() (string, string) {
	monHost := c.generateMonitorHost()
	if c.Keyring == "" {
		return "", monHost
	}
	conf, err := c.createCephConf()
	if err != nil {
		logger.Error("Create ceph conf file failed", err)
		return "", monHost
	}
	return conf, monHost
}

// findRootDevice Find the underlying /dev/rbd* device for a mapping
// Use the showmapped command to list all acive mappings and find the
// underlying /dev/rbd* device that corresponds to our pool and volume
func (c *ConnRbd) findRootDevice(conf string) string {
	volume := strings.Split(c.Name, "/")
	poolVolume := volume[1]
	cmd := []string{"showmapped", "--format=json"}
	args := c.getRbdArgs(conf)
	cmd = append(cmd, args...)
	res, err := osBrick.Execute("rbd", cmd...)
	logger.Debug("Exec rbd showmapped command success", res)
	if err != nil {
		logger.Error("Exec rbd showmapped failed", err)
		return ""
	}
	var result []map[string]string
	err = json.Unmarshal([]byte(res), &result)
	if err != nil {
		logger.Error("conversion json failed")
		return ""
	}
	for _, mapping := range result {
		if mapping["name"] == poolVolume {
			return mapping["device"]
		}
	}
	return ""
}

// generateMonitorHost generate monitor host
func (c *ConnRbd) generateMonitorHost() string {
	var monHosts []string
	for i, _ := range c.Hosts {
		host := fmt.Sprintf("%s:%s", c.Hosts[i], c.Ports[i])
		monHosts = append(monHosts, host)
	}
	monHost := strings.Join(monHosts, ",")
	return monHost
}

// getRbdHandle return RBDVolumeIOWrapper
func (c *ConnRbd) getRbdHandle() *initiator.RBDVolumeIOWrapper {
	conf, err := c.createCephConf()
	if err != nil {
		logger.Error("Create ceph conf failed", err)
		return nil
	}
	poolName := strings.Split(c.Name, "/")[0]
	rbdClient, err := initiator.NewRBDClient(c.AuthUserName, poolName, conf, c.ClusterName)
	if err != nil {
		logger.Error("Get rbd client failed", err)
		return nil
	}
	image, err := initiator.RBDVolume(rbdClient, c.VolumeID)
	if err != nil {
		logger.Error("Get rbd volume failed", err)
		return nil
	}
	metadata := initiator.NewRBDImageMetadata(image, poolName, c.AuthUserName, conf)
	ioWrapper := initiator.NewRBDVolumeIOWrapper(metadata)
	return ioWrapper
}

// getRbdArgs Return rbd command args
func (c *ConnRbd) getRbdArgs(conf string) []string {
	var args []string
	if conf != "" {
		args = append(args, "--conf")
		args = append(args, conf)
	}
	args = append(args, "--id")
	args = append(args, c.AuthUserName)

	monHost := c.generateMonitorHost()
	args = append(args, "--mon_host")
	args = append(args, monHost)
	logger.Debug("Get Rbd command Args", args)
	return args
}

// localAttachVolume Exec local attach volume process
func (c *ConnRbd) localAttachVolume() (map[string]string, error) {
	res := map[string]string{}
	out, err := osBrick.Execute("which", "rbd")
	if err != nil {
		logger.Error("Exec which rbd command failed", err)
		return nil, err
	}
	if out == "" {
		logger.Error("ceph-common package is not installed")
		return nil, err
	}

	volume := strings.Split(c.Name, "/")
	poolName := volume[0]
	poolVolume := volume[1]
	rbdDevPath := getRbdDeviceName(poolName, poolVolume)
	conf, monHosts := c.createNonOpenstackConfig()
	_, err = os.Readlink(rbdDevPath)
	if err != nil {
		cmd := []string{"map", poolVolume, "--pool", poolName, "--id", c.AuthUserName,
			"--mon_host", monHosts}
		if conf != "" {
			cmd = append(cmd, "--conf")
			cmd = append(cmd, conf)
		}
		result, err := osBrick.Execute("rbd", cmd...)
		if err != nil {
			logger.Info("command succeeded: rbd map path is %s", result)
			return nil, err
		}
	} else {
		logger.Info("Volume %s is already mapped to local device %s", poolVolume, rbdDevPath)
		return nil, err
	}

	res["path"] = rbdDevPath
	res["type"] = "block"
	if conf != "" {
		res["conf"] = conf
	}
	return res, nil
}

// getRbdDeviceName Return device name which will be generated by RBD kernel module
func getRbdDeviceName(pool string, volume string) string {
	return fmt.Sprintf("/dev/rbd/%s/%s", pool, volume)
}

// checkVailDevice Verify an existing RBD handle is connected and valid
func checkVailDevice(path string) bool {
	rp, err := os.Open(path)
	if err != nil {
		logger.Error("Open device path failed", err)
		return false
	}
	defer rp.Close()
	r := bufio.NewReader(rp)
	_, err = r.ReadString('\n')
	if err != nil {
		logger.Error("Read device path file failed", err)
		return false
	}
	return true
}

// checkOrGetKeyringContents Return user keyring
func checkOrGetKeyringContents(keyring string, clusterName string, user string) string {
	if keyring == "" {
		if user != "" {
			keyringPath := fmt.Sprintf("/etc/ceph/%s.client.%s.keyring", clusterName, user)
			rp, err := os.Open(keyringPath)
			if err != nil {
				logger.Error("Open keyring path failed", err)
				return ""
			}
			defer rp.Close()
			r := bufio.NewReader(rp)
			userKeyring, err := r.ReadString('\n')
			if err != nil {
				logger.Error("Read ceph keyring file failed", err)
				return ""
			}
			return userKeyring
		}
	}
	return ""
}
