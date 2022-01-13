package rbd

import (
	"bufio"
	"encoding/json"
	"fmt"
	osBrick "github.com/fightdou/os-brick-rbd"
	"github.com/fightdou/os-brick-rbd/initiator"
	"io/ioutil"
	"log"
	"os"
	"path"
	"strconv"
	"strings"
)

type ConnRbd struct {
	ConnectionProperties map[string]interface{}
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

func getRbdHandle(data map[string]interface{}) map[string]interface{} {
	result := map[string]interface{}{}
	user := IsString(data["auth_username"])
	volumeInfo := IsString(data["name"])
	volume := strings.Split(volumeInfo, "/")
	poolName := volume[0]
	poolVolume := volume[1]
	clusterName := IsString(data["cluster_name"])
	monitorIps := IsStringList(data["hosts"])
	monitorPorts := IsStringList(data["ports"])
	keyring := IsString(data["keyring"])
	conf, err := createCephConf(monitorIps, monitorPorts, clusterName, user, keyring)
	if err != nil {
		return nil
	}
	rbdClient, err := initiator.NewRBDClient(user, poolName, conf, clusterName)
	if err != nil {
		return nil
	}
	image, err := initiator.RBDVolume(rbdClient, poolVolume)
	if err != nil {
		return nil
	}
	metadata := initiator.NewRBDImageMetadata(image, poolName, user, conf)
	ioWrapper := initiator.NewRBDVolumeIOWrapper(metadata)
	result["path"] = ioWrapper
	return result
}

func createCephConf(monIP []string, monPort []string, clName string, user string, keyring string) (string,error) {
	var monitors []string
	for i, _ := range monIP {
		host := fmt.Sprintf("%s:%s", monIP[i], monPort[i])
		monitors = append(monitors, host)
	}
	monHosts := strings.Join(monitors, ",")
	monHosts = fmt.Sprintf("mon_host = %s", monHosts)
	userKeyring := checkOrGetKeyringContents(keyring, clName, user)

	data := "[global]"
	data = data + "\n" + monHosts + "\n" + fmt.Sprintf("[client.%s]", IsString(user)) + "\n" +
		fmt.Sprintf("keyring = %s", userKeyring)

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
			userKeyring, err := r.ReadString(4096)
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
	_, err = r.ReadBytes(4096)
	if err != nil {
		return false
	}
	return true
}

func (c *ConnRbd) GetConnectorProperties() map[string]interface{} {
	for k, _ := range c.ConnectionProperties {
		if k == "do_local_attach" {
			return c.ConnectionProperties
		} else {
			c.ConnectionProperties["do_local_attach"] = false
		}
	}
	return c.ConnectionProperties
}

func (c *ConnRbd) ConnectVolume() (map[string]string, map[string]interface{}, error) {
	var err error
	result := map[string]string{}
	localAttach := c.ConnectionProperties["do_local_attach"]
	data := c.ConnectionProperties["data"].(map[string]interface{})
	if IsBool(localAttach) {
		result, err = localAttachVolume(data)
		if err != nil {
			return nil, nil, err
		}
		return result, nil, nil
	}
	rbdHandle := getRbdHandle(data)
	return nil, rbdHandle, nil
}

func (c *ConnRbd) DisConnectVolume(deviceInfo map[string]string) {
	localAttach := c.ConnectionProperties["do_local_attach"]
	if IsBool(localAttach) {
		var conf string
		if deviceInfo != nil {
			conf = deviceInfo["conf"]
		}
		rootDevice := findRootDevice(c.ConnectionProperties["data"].(map[string]interface{}), conf)
		if rootDevice != "" {
			cmd := []string{"unmap", rootDevice}
			args := getRbdArgs(c.ConnectionProperties["data"].(map[string]interface{}), conf)
			cmd = append(cmd, args...)
			osBrick.Execute("rbd", cmd...)
			if conf != "" {
				os.Remove(conf)
			}
		}
	}
}
func (c *ConnRbd) ExtendVolume() (int, error) {
	var err error
	localAttach := c.ConnectionProperties["do_local_attach"]
	if IsBool(localAttach) {
		var conf string
		device := findRootDevice(c.ConnectionProperties["data"].(map[string]interface{}), conf)
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
		iSize, _ := strconv.Atoi(vSize)
		return iSize, nil
	}
	return 0, err
}

func findRootDevice(connProperties map[string]interface{}, conf string) string {
	volumeInfo := IsString(connProperties["name"])
	volume := strings.Split(volumeInfo, "/")
	poolVolume := volume[1]
	cmd := []string{"showmapped", "--format=json"}
	args := getRbdArgs(connProperties, conf)
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

func getRbdArgs(connProperties map[string]interface{}, conf string) []string {
	var args []string
	if conf != "" {
		args = append(args, "--conf")
		args = append(args, conf)
	}
	user := connProperties["auth_username"]
	args =append(args, "--id")
	args = append(args, IsString(user))
	monitorIps := connProperties["hosts"]
	monitorPorts := connProperties["ports"]
	monHost := generateMonitorHost(monitorIps, monitorPorts)
	args =append(args, "--mon_host")
	args = append(args, monHost)
	return args

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

func localAttachVolume(connProperties map[string]interface{}) (map[string]string, error){
	var res map[string]string
	res = make(map[string]string)
	out, err := osBrick.Execute("which", "rbd")
	if err != nil {
		return nil, err
	}
	if out == "" {
		log.Printf("ceph-common package is not installed")
		return nil, err
	}
	volumeInfo := IsString(connProperties["name"])
	volume := strings.Split(volumeInfo, "/")
	poolName := volume[0]
	poolVolume := volume[1]
	rbdDevPath := getRbdDeviceName(poolName, poolVolume)
	conf, monHosts := createNonOpenstackConfig(connProperties)
	fmt.Println(monHosts)
	user := connProperties["auth_username"]
	_, err = os.Readlink(rbdDevPath)
	if err != nil {
		cmd := []string{"map", poolVolume, "--pool", poolName, "--id", IsString(user),
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

func createNonOpenstackConfig(connProperties map[string]interface{}) (string,string) {
	monitorIps := connProperties["hosts"]
	monitorPorts := connProperties["ports"]

	monHost := generateMonitorHost(monitorIps, monitorPorts)
	keyring := connProperties["keyring"]
	if keyring == nil {
		return "", monHost
	}

	user := connProperties["auth_username"]

	keyFile, err := rootCreateCephKeyring(keyring, user)
	if err != nil {
		return "", monHost
	}
	conf, err := rootCreateCephConf(keyFile, monHost, user)
	if err != nil {
		return "", monHost
	}
	return conf, monHost
}

func generateMonitorHost(monitorIps interface{}, monitorPorts interface{}) string {
	var monIPs []string
	var monPorts []string
	monIPs = IsStringList(monitorIps)
	monPorts = IsStringList(monitorPorts)
	var monHosts []string
	for i, _ := range monIPs {
		host := fmt.Sprintf("%s:%s", monIPs[i], monPorts[i])
		monHosts = append(monHosts, host)
	}
	monHost := strings.Join(monHosts, ",")
	return monHost
}

func rootCreateCephKeyring(keyring interface{}, user interface{}) (string, error){
	keyrings := IsString(keyring)
	users := IsString(user)

	var keyfileInfo string
	keyfileInfo = fmt.Sprintf("[client.%s]", users) + "\n" + fmt.Sprintf("key = %s", keyrings)

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

func rootCreateCephConf(keyFile string, monHost string, user interface{}) (string, error) {
	data := "[global]"
	data = data + "\n" + monHost + "\n" + fmt.Sprintf("[client.%s]", IsString(user)) + "\n" +
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

