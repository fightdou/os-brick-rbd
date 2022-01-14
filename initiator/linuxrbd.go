package initiator

import (
	"github.com/ceph/go-ceph/rados"
	"github.com/ceph/go-ceph/rbd"
	"github.com/wonderivan/logger"
)

// RBDClient Get Rados Conn and IOContext
type RBDClient struct {
	Conn      *rados.Conn
	IOContext *rados.IOContext
}

// NewRBDClient Return RBDClient
func NewRBDClient(user string, pool string, confFile string, rbdClusterName string) (*RBDClient, error) {
	var err error
	rbdConn := &RBDClient{}
	conn, err := rados.NewConnWithClusterAndUser(rbdClusterName, user)
	if err != nil {
		logger.Error("Get ceph connector failed", err)
		return rbdConn, err
	}
	err = conn.ReadConfigFile(confFile)
	if err != nil {
		logger.Error("Read ceph config file failed", err)
		return rbdConn, err
	}
	err = conn.Connect()
	if err != nil {
		logger.Error("Connect ceph cluster failed", err)
		return rbdConn, err
	}
	ioctx, err := conn.OpenIOContext(pool)
	if err != nil {
		logger.Error("Open pool ioctx failed", err)
		return rbdConn, err
	}
	rbdConn = &RBDClient{conn, ioctx}
	return rbdConn, nil
}

// RBDVolume Context manager for dealing with an existing rbd volume
func RBDVolume(client *RBDClient, volume string) (*rbd.Image, error) {
	image, err := rbd.OpenImage(client.IOContext, volume, "")
	if err != nil {
		logger.Error("Open ceph image failed", err)
		return nil, err
	}
	return image, nil
}

// RBDImageMetadata RBD image metadata to be used with RBDVolumeIOWrapper
type RBDImageMetadata struct {
	Image *rbd.Image
	Pool  string
	User  string
	Conf  string
}

// NewRBDImageMetadata Return NewRBDImageMetadata
func NewRBDImageMetadata(image *rbd.Image, pool string, user string, conf string) *RBDImageMetadata {
	rbdImageMetadata := &RBDImageMetadata{
		Image: image,
		Pool:  pool,
		User:  user,
		Conf:  conf,
	}
	return rbdImageMetadata
}

// RBDVolumeIOWrapper Enables LibRBD.Image objects to be treated as Python IO objects.
type RBDVolumeIOWrapper struct {
	*RBDImageMetadata
	offset int64
}

// NewRBDVolumeIOWrapper Return NewRBDImageMetadata
func NewRBDVolumeIOWrapper(imageMetadata *RBDImageMetadata) *RBDVolumeIOWrapper {
	ioWrapper := &RBDVolumeIOWrapper{imageMetadata, 0}
	return ioWrapper
}

// rbdIMage Return rbd Image object
func (r *RBDVolumeIOWrapper) rbdIMage() *rbd.Image {
	return r.RBDImageMetadata.Image
}

// rbdUser Return rbd user
func (r *RBDVolumeIOWrapper) rbdUser() string {
	return r.RBDImageMetadata.User
}

// rbdPool Return rbd pool
func (r *RBDVolumeIOWrapper) rbdPool() string {
	return r.RBDImageMetadata.Pool
}

// rbdConf Return rbd conf
func (r *RBDVolumeIOWrapper) rbdConf() string {
	return r.RBDImageMetadata.Conf
}

// Read copies data from the image into the supplied buffer
func (r *RBDVolumeIOWrapper) Read(length int64) (int, error) {
	offset := r.offset
	total, err := r.RBDImageMetadata.Image.GetSize()
	if err != nil {
		logger.Error("Get image size failed", err)
		return 0, nil
	}
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
		logger.Error("Read image failed", err)
		return 0, err
	}
	r.incOffset(length)
	return data, nil
}

// incOffset Return offset
func (r *RBDVolumeIOWrapper) incOffset(length int64) int64 {
	r.offset += length
	return r.offset
}

// Write copies data from the supplied buffer to the image
func (r *RBDVolumeIOWrapper) Write(data string, offset int64) {
	dataOut := make([]byte, 0)
	dataOut = []byte(data)
	r.RBDImageMetadata.Image.WriteAt(dataOut, offset)
	r.incOffset(offset)
}

// Seekable Return true
func (r *RBDVolumeIOWrapper) Seekable() bool {
	return true
}

// Seek updates the internal file position for the current image
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
		logger.Error("Invalid argument - whence=%s not supported", whence)
	}
	r.offset = newOffset
}

// Tell Return offset
func (r *RBDVolumeIOWrapper) Tell() int64 {
	return r.offset
}

// Flash Flush all cached writes to storage
func (r *RBDVolumeIOWrapper) Flash() {
	r.RBDImageMetadata.Image.Flush()
}

// Close an open rbd image
func (r *RBDVolumeIOWrapper) Close() {
	r.RBDImageMetadata.Image.Close()
}
