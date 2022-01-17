# os-brick-rbd

## Demo

- 调用 openstack go gophercloud 创建一个volume
- 调用 getConnectionInfo 获取 volume 的初始化信息
- 有了信息之后就执行连接卷、卸载卷等操作

```go
package main

import (
        "fmt"
        "github.com/gophercloud/gophercloud"
        "github.com/gophercloud/gophercloud/openstack"
        "github.com/gophercloud/gophercloud/openstack/blockstorage/extensions/volumeactions"
        "github.com/gophercloud/gophercloud/openstack/blockstorage/v3/volumes"
	"github.com/fightdou/os-brick-rbd/connectors"
)

func getConnectionInfo(blockstorageClient *gophercloud.ServiceClient, volumeID string)  map[string]interface{} {
        options := &volumeactions.InitializeConnectionOpts{}
        connInfo := volumeactions.InitializeConnection(blockstorageClient, volumeID, options)
        res, _ := connInfo.Extract()
        return res
}

func main() {
        opts := gophercloud.AuthOptions{
                IdentityEndpoint: "http://keystone.openstack.svc.cluster.local",
                Username: "admin",
                Password: "password",
                TenantName: "admin",
                DomainName: "Default",
        }

        provider, err := openstack.AuthenticatedClient(opts)
        if err != nil {
                fmt.Println(err)
        }
        opt := gophercloud.EndpointOpts{Region: "RegionOne"}

        blockstorageClient, err := openstack.NewBlockStorageV3(provider, opt)
        if err != nil {
                fmt.Println(err)
        }

        volume_opts := &volumes.CreateOpts{
		Name:             "os-brick1",
		Size:             1,
		AvailabilityZone: "nova",
	}
	_, err = volumes.Create(blockstorageClient, volume_opts).Extract()

        allPages, err := volumes.List(blockstorageClient, &volumes.ListOpts{Name: "os-brick1"}).AllPages()
        actual, err := volumes.ExtractVolumes(allPages)
        var volumeId string

        for _, volume := range actual {
                if volume.Name == "os-brick1" {
                        volumeId = volume.ID
                }
        }

        err = volumeactions.Attach(blockstorageClient, volumeId, &volumeactions.AttachOpts{
                MountPoint: "None",
                HostName: "None",
        }).ExtractErr()

        if err != nil {
                fmt.Println(err)
        }
        result := getConnectionInfo(blockstorageClient, volumeId)
        protocol := result["driver_volume_type"]
        strProtocol := fmt.Sprint(protocol)
	// 连接卷
	conn := connectors.NewConnector(strProtocol, result)
	conn.ConnectVolume()
	// 卸载卷
	conn.DisConnectVolume(nil)
}

```

## 执行结果

```
openstack volume list
+--------------------------------------+--------+-----------+------+------------------------------+
| ID                                   | Name   | Status    | Size | Attached to                  |
+--------------------------------------+--------+-----------+------+------------------------------+
| 002e5f6f-6daf-4f58-9b82-69c598c9da34 | brick1 | in-use    |    1 | Attached to None on None     |
+--------------------------------------+--------+-----------+------+------------------------------+

lsblk
NAME   MAJ:MIN RM  SIZE RO TYPE MOUNTPOINT
rbd0   251:0    0  256M  0 disk /var/lib/kubelet/pods/a40baddd-1672-4f17-ba65-45693c4482dd/volumes/kubernetes.io~csi/pvc-01c42bd3-a46a-4b08-97f1-4cde7e32729c/mount
rbd1   251:16   0    5G  0 disk /var/lib/kubelet/pods/05687bec-854a-40f0-9a4f-d79c4a277157/volumes/kubernetes.io~csi/pvc-978da417-53e4-42e0-82d8-84b22615d4fc/mount
rbd2   251:32   0    1G  0 disk

```

