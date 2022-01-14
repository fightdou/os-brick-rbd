package common

import (
	"github.com/fightdou/os-brick-rbd/initiator"
	"github.com/fightdou/os-brick-rbd/initiator/connectors/rbd"
	"strings"
)

func NewConnector(protocol string, connInfo map[string]interface{}) initiator.ConnProperties {
	switch strings.ToUpper(protocol) {
	case "RBD":
		connInfo["do_local_attach"] = true
		connRbd := rbd.NewRBDConnector(connInfo)
		return connRbd
	}
	return nil
}
