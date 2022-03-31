package connectors

import (
	"strings"

	"github.com/fightdou/os-brick-rbd/iscsi"
	"github.com/fightdou/os-brick-rbd/rbd"
)

// ConnProperties is base class interface
type ConnProperties interface {
	ConnectVolume() (map[string]string, error)
	DisConnectVolume() error
	ExtendVolume() (int64, error)
	GetDevicePath() string
}

// NewConnector Build a Connector object based upon protocol and architecture
func NewConnector(protocol string, connInfo map[string]interface{}) ConnProperties {
	switch strings.ToUpper(protocol) {
	case "RBD":
		// Only supported local attach volume
		connInfo["do_local_attach"] = true
		return rbd.NewRBDConnector(connInfo)
	case "ISCSI":
		return iscsi.NewISCSIConnector(connInfo)
	}
	return nil
}
