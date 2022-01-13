package common

func ParseParameter(connInfo map[string]interface{}) *rbd.ConnRbd {
	data := connInfo["data"].(map[string]interface{})
	conn := &rbd.ConnRbd{}
	conn.Name = IsString(data["name"])
	conn.Hosts = IsStringList(data["hosts"])
	conn.Ports = IsStringList(data["ports"])
	conn.ClusterName = IsString(data["cluster_name"])
	conn.AuthEnabled = IsBool(data["auth_enabled"])
	conn.AuthUserName = IsString(data["auth_username"])
	conn.VolumeID = IsString(data["volume_id"])
	conn.Discard = IsBool(data["discard"])
	conn.QosSpecs = IsString(data["qos_specs"])
	conn.AccessMode = IsString(data["access_mode"])
	conn.Encrypted = IsBool(data["encrypted"])
	conn.DoLocalAttach = IsBool(connInfo["do_local_attach"])
	return conn
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
