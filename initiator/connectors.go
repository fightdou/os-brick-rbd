package initiator

import (
	"github.com/fightdou/os-brick-rbd/common"
	"strings"
)

func NewConnector(protocol string, connInfo map[string]interface{}) ConnProperties {
	switch strings.ToUpper(protocol) {
	case "RBD":
		connRbd := common.ParseParameter(connInfo)
		return connRbd
	}
	return nil
}
