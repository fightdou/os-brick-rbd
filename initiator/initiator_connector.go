package initiator

type ConnProperties interface {
	GetConnectorProperties() map[string]interface{}
	CheckVailDevice(interface{}, bool) bool
	ConnectVolume() (map[string]string, error)
	DisConnectVolume(map[string]string)
	GetVolumePaths() []interface{}
	GetSearchPath() interface{}
	ExtendVolume() (int64, error)
	GetALLAvailableVolumes() interface{}
	CheckIOHandlerValid() error
}
