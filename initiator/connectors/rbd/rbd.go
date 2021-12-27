package rbd

import (
	"encoding/json"
	"fmt"
	osBrick "github.com/fightdou/os-brick-rbd"
	"io/ioutil"
	"log"
	"os"
	"strconv"
	"strings"
)

func ConnectVolume(connectionProperties map[string]interface{}) (map[string]string, error) {
	var err error
	result := map[string]string{}
	localAttach := connectionProperties["do_local_attach"]
	if IsBool(localAttach) {
		result, err = localAttachVolume(connectionProperties["data"].(map[string]interface{}))
		if err != nil {
			return nil, err
		}
		return result, nil
	}
	return result, nil
}

func DisConnectVolume(connectionProperties map[string]interface{}, deviceInfo map[string]string) {
	localAttach := connectionProperties["do_local_attach"]
	if IsBool(localAttach) {
		var conf string
		if deviceInfo != nil {
			conf = deviceInfo["conf"]
		}
		rootDevice := findRootDevice(connectionProperties["data"].(map[string]interface{}), conf)
		if rootDevice != "" {
			cmd := []string{"unmap", rootDevice}
			args := getRbdArgs(connectionProperties["data"].(map[string]interface{}), conf)
			cmd = append(cmd, args...)
			osBrick.Execute("rbd", cmd...)
			if conf != "" {
				os.Remove(conf)
			}
		}
	}
}

func findRootDevice(dataProperties map[string]interface{}, conf string) string {
	volumeInfo := IsString(dataProperties["name"])
	volume := strings.Split(volumeInfo, "/")
	poolVolume := volume[1]
	cmd := []string{"showmapped", "--format=json"}
	args := getRbdArgs(dataProperties, conf)
	cmd = append(cmd, args...)
	res, err := osBrick.Execute("rbd", cmd...)
	if err != nil {
		return ""
	}
	var result []map[string]string
	err = json.Unmarshal([]byte(res), &result)
	if err != nil {
		log.Fatalf("conversion json failed")
		return ""
	}
	for _, mapping := range result {
		if mapping["name"] == poolVolume {
			return mapping["device"]
		}
	}
	return ""
}

func getRbdArgs(dataProperties map[string]interface{}, conf string) []string {
	var args []string
	if conf != "" {
		args = append(args, "--conf")
		args = append(args, conf)
	}
	user := dataProperties["auth_username"]
	args =append(args, "--id")
	args = append(args, IsString(user))
	monitorIps := dataProperties["hosts"]
	monitorPorts := dataProperties["ports"]
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

func localAttachVolume(dataProperties map[string]interface{}) (map[string]string, error){
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
	volumeInfo := IsString(dataProperties["name"])
	volume := strings.Split(volumeInfo, "/")
	poolName := volume[0]
	poolVolume := volume[1]
	rbdDevPath := getRbdDeviceName(poolName, poolVolume)
	conf, monHosts := createNonOpenstackConfig(dataProperties)
	fmt.Println(monHosts)
	user := dataProperties["auth_username"]
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

func createNonOpenstackConfig(dataProperties map[string]interface{}) (string,string) {
	monitorIps := dataProperties["hosts"]
	monitorPorts := dataProperties["ports"]

	monHost := generateMonitorHost(monitorIps, monitorPorts)
	keyring := dataProperties["keyring"]
	if keyring == nil {
		return "", monHost
	}

	user := dataProperties["auth_username"]

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

