package connectors

import (
	"testing"

	"github.com/fightdou/os-brick-rbd/rbd"
)

func TestNewConnector(t *testing.T) {
	t.Parallel()
	connInfo := make(map[string]interface{})
	data := make(map[string]interface{})
	data["name"] = "fake"
	data["hosts"] = []interface{}{"host1", "host2"}
	data["ports"] = []interface{}{"1", "2"}
	data["cluster_name"] = "fake_cluster"
	data["auth_enabled"] = "True"
	data["auth_username"] = "user"
	data["volume_id"] = "fake_id"
	data["discard"] = false
	data["qos_specs"] = "fake_qos"
	data["access_mode"] = "rw"
	data["encrypted"] = "1"
	connInfo["data"] = data
	conn := NewConnector("RBD", connInfo)
	_, ok := conn.(*rbd.ConnRbd)
	if !ok {
		t.Error("Expected a *rbd.ConnRbd value.")
	}
	conn = NewConnector("FakeProcotol", connInfo)
	if conn != nil {
		t.Error("Expected nil value for not supported protocol.")
	}
}
