package initiator

import (
	"github.com/go-ceph/rados"
	"github.com/go-ceph/rbd"
	"io"
)

type RBDClient struct {
	Conn *rados.Conn
	IOContext *rados.IOContext
}

func NewRBDClient(user string, pool string, confFile string, rbdClusterName string) (*RBDClient, error) {
	var err error
	rbdConn := &RBDClient{}
	conn, err := rados.NewConnWithClusterAndUser(rbdClusterName, user)
	if err != nil {
		return rbdConn, err
	}
	err = conn.ReadConfigFile(confFile)
	if err != nil {
		return rbdConn, err
	}
	err = conn.Connect()
	if err != nil {
		return rbdConn, err
	}
	ioctx, err := conn.OpenIOContext(pool)
	if err != nil {
		return rbdConn, err
	}
	rbdConn = &RBDClient{conn, ioctx}
	return rbdConn, nil
}

func RBDVolume(client *RBDClient, volume string) *rbd.Image {
	image, err := rbd.OpenImage(client.IOContext, volume, "")
	if err != nil {
		return nil
	}
	return image
}

type RBDImageMetadata struct {
	Image *rbd.Image
	Pool string
	User string
	Conf string
}

func NewRBDImageMetadata(image *rbd.Image, pool string, user string, conf string) *RBDImageMetadata {
	rbdImageMetadata := &RBDImageMetadata{
		Image: image,
		Pool: pool,
		User: user,
		Conf: conf,
	}
	return rbdImageMetadata
}

type RBDVolumeIOWrapper struct {
	*RBDImageMetadata
	offset int
}

func NewRBDVolumeIOWrapper(imageMetadata *RBDImageMetadata) *RBDVolumeIOWrapper {
	ioWrapper := &RBDVolumeIOWrapper{imageMetadata, 0}
	return ioWrapper
}

func (r *RBDVolumeIOWrapper) rbdIMage() *rbd.Image{
	return r.RBDImageMetadata.Image
}

func (r *RBDVolumeIOWrapper) rbdUser() string {
	return r.RBDImageMetadata.User
}

func (r *RBDVolumeIOWrapper) rbdPool() string {
	return r.RBDImageMetadata.Pool
}

func (r *RBDVolumeIOWrapper) rbdConf() string {
	return r.RBDImageMetadata.Conf
}

func (r *RBDVolumeIOWrapper) Read() {

}

func (r *RBDVolumeIOWrapper) Write() {

}

func (r *RBDVolumeIOWrapper) Seekable() bool {
	return true
}

func (r *RBDVolumeIOWrapper) Seek() {

}

func (r *RBDVolumeIOWrapper) Tell() int {
	return r.offset
}

func (r *RBDVolumeIOWrapper) Flash() {
	r.RBDImageMetadata.Image.Flush()
}

func (r *RBDVolumeIOWrapper) FileNo() {
	panic(io.ErrClosedPipe)
}

func (r *RBDVolumeIOWrapper) Close() {
	r.RBDImageMetadata.Image.Close()
}
