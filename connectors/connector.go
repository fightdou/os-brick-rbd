package connectors

import (
	"github.com/fightdou/os-brick-rbd/connectors/rbd"
	"strings"
)

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
	GetDevicePath() string
}

// NewConnector Build a Connector object based upon protocol and architecture
func NewConnector(protocol string, connInfo map[string]interface{}) ConnProperties {
	switch strings.ToUpper(protocol) {
	case "RBD":
		connInfo["do_local_attach"] = true
		connRbd := rbd.NewRBDConnector(connInfo)
		return connRbd
	}
	return nil
}
