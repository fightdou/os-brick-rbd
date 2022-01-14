package initiator

// ConnProperties is base class interface
type ConnProperties interface {
	CheckVailDevice(interface{}, bool) bool
	ConnectVolume() (map[string]string, error)
	DisConnectVolume(map[string]string)
	GetVolumePaths() []interface{}
	GetSearchPath() interface{}
	ExtendVolume() (int64, error)
	GetALLAvailableVolumes() interface{}
	CheckIOHandlerValid() error
}
