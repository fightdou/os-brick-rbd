package initiator

import (
	"github.com/ceph/go-ceph/rados"
	"github.com/ceph/go-ceph/rbd"
	"io"
)

type RBDClient struct {
	Conn      *rados.Conn
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

func RBDVolume(client *RBDClient, volume string) (*rbd.Image, error) {
	image, err := rbd.OpenImage(client.IOContext, volume, "")
	if err != nil {
		return nil, err
	}
	return image, nil
}

type RBDImageMetadata struct {
	Image *rbd.Image
	Pool  string
	User  string
	Conf  string
}

func NewRBDImageMetadata(image *rbd.Image, pool string, user string, conf string) *RBDImageMetadata {
	rbdImageMetadata := &RBDImageMetadata{
		Image: image,
		Pool:  pool,
		User:  user,
		Conf:  conf,
	}
	return rbdImageMetadata
}

type RBDVolumeIOWrapper struct {
	*RBDImageMetadata
	offset int64
}

func NewRBDVolumeIOWrapper(imageMetadata *RBDImageMetadata) *RBDVolumeIOWrapper {
	ioWrapper := &RBDVolumeIOWrapper{imageMetadata, 0}
	return ioWrapper
}

func (r *RBDVolumeIOWrapper) rbdIMage() *rbd.Image {
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

func (r *RBDVolumeIOWrapper) Read(length int64) (int, error) {
	offset := r.offset
	total, err := r.RBDImageMetadata.Image.GetSize()
	if offset >= int64(total) {
		return 0, err
	}
	if length == 0 {
		length = int64(total)
	}

	if (offset + length) > int64(total) {
		length = int64(total) - offset
	}
	dataIn := make([]byte, length)
	data, err := r.RBDImageMetadata.Image.ReadAt(dataIn, offset)
	if err != nil {
		return 0, err
	}
	r.incOffset(length)
	return data, nil
}

func (r *RBDVolumeIOWrapper) incOffset(length int64) int64 {
	r.offset += length
	return r.offset
}

func (r *RBDVolumeIOWrapper) Write(data string, offset int64) {
	dataOut := make([]byte, 0)
	dataOut = []byte(data)
	r.RBDImageMetadata.Image.WriteAt(dataOut, offset)
	r.incOffset(offset)
}

func (r *RBDVolumeIOWrapper) Seekable() bool {
	return true
}

func (r *RBDVolumeIOWrapper) Seek(offset int64, whence int64) {
	var newOffset int64
	if whence == 0 {
		newOffset = offset
	} else if whence == 1 {
		newOffset = r.offset + offset
	} else if whence == 2 {
		size, _ := r.RBDImageMetadata.Image.GetSize()
		newOffset = int64(size)
		newOffset += offset
	}
	if (newOffset) < 0 {
		panic(io.ErrClosedPipe)
	}
	r.offset = newOffset
}

func (r *RBDVolumeIOWrapper) Tell() int64 {
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
