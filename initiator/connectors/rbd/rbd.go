package rbd

import (
	"bufio"
	"encoding/json"
	"fmt"
	"github.com/fightdou/os-brick-rbd/initiator"
	osBrick "github.com/fightdou/os-brick-rbd/utils"
	"io/ioutil"
	"log"
	"os"
	"path"
	"strconv"
	"strings"
)

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

func (c *ConnRbd) GetVolumePaths() []interface{} {
	return nil
}

func (c *ConnRbd) GetSearchPath() interface{} {
	return nil
}

func (c *ConnRbd) GetALLAvailableVolumes() interface{} {
	return nil
}

func (c *ConnRbd) CheckIOHandlerValid() error {
	return nil
}

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

func (c *ConnRbd) getRbdHandle() *initiator.RBDVolumeIOWrapper {
	conf, err := c.createCephConf()
	if err != nil {
		return nil
	}
	poolName := strings.Split(c.Name, "/")[0]
	rbdClient, err := initiator.NewRBDClient(c.AuthUserName, poolName, conf, c.ClusterName)
	if err != nil {
		return nil
	}
	image, err := initiator.RBDVolume(rbdClient, c.VolumeID)
	if err != nil {
		return nil
	}
	metadata := initiator.NewRBDImageMetadata(image, poolName, c.AuthUserName, conf)
	ioWrapper := initiator.NewRBDVolumeIOWrapper(metadata)
	return ioWrapper
}

func (c *ConnRbd) createCephConf() (string, error) {
	monitors := c.generateMonitorHost()
	monHosts := fmt.Sprintf("mon_host = %s", monitors)
	userKeyring := checkOrGetKeyringContents(c.Keyring, c.ClusterName, c.AuthUserName)

	data := "[global]"
	data = data + "\n" + monHosts + "\n" + fmt.Sprintf("[client.%s]", c.AuthUserName) + "\n" + fmt.Sprintf("keyring = %s", userKeyring)

	tmpFile, err := ioutil.TempFile("/tmp", "keyfile-")
	if err != nil {
		return "", fmt.Errorf("error creating a temporary keyfile: %w", err)
	}
	defer func() {
		if err != nil {
			// don't complain about unhandled error
			_ = os.Remove(tmpFile.Name())
		}
	}()

	_, err = tmpFile.WriteString(data)
	if err != nil {
		return "", fmt.Errorf("failed to write temporary file: %w", err)
	}
	tmpFile.Close()
	return tmpFile.Name(), nil
}

func checkOrGetKeyringContents(keyring string, clusterName string, user string) string {
	if keyring == "" {
		if user != "" {
			keyringPath := fmt.Sprintf("/etc/ceph/%s.client.%s.keyring", clusterName, user)
			rp, err := os.Open(keyringPath)
			if err != nil {
				return ""
			}
			defer rp.Close()
			r := bufio.NewReader(rp)
			userKeyring, err := r.ReadString('\n')
			if err != nil {
				return ""
			}
			return userKeyring
		}
	}
	return ""
}

func checkVailDevice(path string) bool {
	rp, err := os.Open(path)
	if err != nil {
		return false
	}
	defer rp.Close()
	r := bufio.NewReader(rp)
	_, err = r.ReadBytes('\n')
	if err != nil {
		return false
	}
	return true
}

func (c *ConnRbd) GetConnectorProperties() map[string]interface{} {
	res := map[string]interface{}{}
	if c.DoLocalAttach {
		res["do_local_attach"] = true
		return res
	}
	return res
}

func (c *ConnRbd) ConnectVolume() (map[string]string, error) {
	var err error
	result := map[string]string{}
	if c.DoLocalAttach {
		result, err = c.localAttachVolume()
		if err != nil {
			return nil, err
		}
		return result, nil
	}
	return nil, err
}

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
		}
	}
}

func (c *ConnRbd) ExtendVolume() (int64, error) {
	var err error
	if c.DoLocalAttach {
		var conf string
		device := c.findRootDevice(conf)
		if device == "" {
			return 0, err
		}
		deviceName := path.Base(device)
		deviceNumber := deviceName[3:]
		size, err := ioutil.ReadFile("/sys/devices/rbd/" + deviceNumber + "/size")
		if err != nil {
			return 0, err
		}
		strSize := string(size)
		vSize := strings.Replace(strSize, "'", "", -1)
		iSize, _ := strconv.ParseInt(vSize, 10, 64)
		return iSize, nil
	} else {
		handle := c.getRbdHandle()
		handle.Seek(0, 2)
		return handle.Tell(), err
	}
	return 0, err
}

func (c *ConnRbd) findRootDevice(conf string) string {
	volume := strings.Split(c.Name, "/")
	poolVolume := volume[1]
	cmd := []string{"showmapped", "--format=json"}
	args := c.getRbdArgs(conf)
	cmd = append(cmd, args...)
	res, err := osBrick.Execute("rbd", cmd...)
	if err != nil {
		return ""
	}
	var result []map[string]string
	err = json.Unmarshal([]byte(res), &result)
	if err != nil {
		log.Print("conversion json failed")
		return ""
	}
	for _, mapping := range result {
		if mapping["name"] == poolVolume {
			return mapping["device"]
		}
	}
	return ""
}

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
	return args
}

func (c *ConnRbd) localAttachVolume() (map[string]string, error) {
	res := map[string]string{}
	out, err := osBrick.Execute("which", "rbd")
	if err != nil {
		return nil, err
	}
	if out == "" {
		log.Printf("ceph-common package is not installed")
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
			log.Printf("command succeeded: rbd map path is %s", result)
			return nil, err
		}
	} else {
		log.Printf("Volume %s is already mapped to local device %s", poolVolume, rbdDevPath)
		return nil, err
	}

	res["path"] = rbdDevPath
	res["type"] = "block"
	if conf != "" {
		res["conf"] = conf
	}
	return res, nil
}

func (c *ConnRbd) createNonOpenstackConfig() (string, string) {
	monHost := c.generateMonitorHost()
	if c.Keyring == "" {
		return "", monHost
	}
	keyFile, err := c.rootCreateCephKeyring()
	if err != nil {
		return "", monHost
	}
	conf, err := c.rootCreateCephConf(keyFile, monHost)
	if err != nil {
		return "", monHost
	}
	return conf, monHost
}

func (c *ConnRbd) generateMonitorHost() string {
	var monHosts []string
	for i, _ := range c.Hosts {
		host := fmt.Sprintf("%s:%s", c.Hosts[i], c.Ports[i])
		monHosts = append(monHosts, host)
	}
	monHost := strings.Join(monHosts, ",")
	return monHost
}

func (c *ConnRbd) rootCreateCephKeyring() (string, error) {
	var keyfileInfo string
	keyfileInfo = fmt.Sprintf("[client.%s]", c.AuthUserName) + "\n" + fmt.Sprintf("key = %s", c.Keyring)

	tmpfile, err := ioutil.TempFile("/tmp", "keyfile-")
	if err != nil {
		return "", fmt.Errorf("error creating a temporary keyfile: %w", err)
	}
	defer func() {
		if err != nil {
			// don't complain about unhandled error
			_ = os.Remove(tmpfile.Name())
		}
	}()

	if _, err = tmpfile.WriteString(keyfileInfo); err != nil {
		return "", fmt.Errorf("error writing key to temporary keyfile: %w", err)
	}

	keyFile := tmpfile.Name()
	if keyFile == "" {
		err = fmt.Errorf("error reading temporary filename for key: %w", err)

		return "", err
	}

	if err = tmpfile.Close(); err != nil {
		return "", fmt.Errorf("error closing temporary filename: %w", err)
	}
	return keyFile, nil
}

func (c *ConnRbd) rootCreateCephConf(keyFile string, monHost string) (string, error) {
	data := "[global]"
	data = data + "\n" + monHost + "\n" + fmt.Sprintf("[client.%s]", c.AuthUserName) + "\n" +
		fmt.Sprintf("keyring = %s", keyFile)

	file, err := ioutil.TempFile("/tmp", "brickrbd_")
	if err != nil {
		return "", fmt.Errorf("failed to create temporary file: %w", err)
	}
	defer func() {
		if err != nil {
			_ = os.Remove(file.Name())
		}
	}()
	_, err = file.WriteString(data)
	if err != nil {
		return "", fmt.Errorf("failed to write temporary file: %w", err)
	}
	err = file.Close()
	if err != nil {
		return "", fmt.Errorf("failed to close temporary file: %w", err)
	}
	return file.Name(), nil
}

func getRbdDeviceName(pool string, volume string) string {
	return fmt.Sprintf("/dev/rbd/%s/%s", pool, volume)
}

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
