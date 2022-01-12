package initiator

import (
	"github.com/test/os-brick-rbd/initiator/connectors/rbd"
	"strings"
)

type ConnFactory struct {
	Protocol string
	ConnectionProperties map[string]interface{}
}

func NewConnectorFactory(protocol string, connectionProperties map[string]interface{}) *ConnFactory {
	return &ConnFactory{
		Protocol: protocol,
		ConnectionProperties: connectionProperties,
	}
}

func (c *ConnFactory) NewFactory() ConnProperties {
	switch strings.ToUpper(c.Protocol) {
	case "RBD":
		return &rbd.ConnRbd{ConnectionProperties: c.ConnectionProperties}
	}
	return nil
}
