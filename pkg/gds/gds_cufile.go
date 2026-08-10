//go:build cufile

package gds

/*
#cgo LDFLAGS: -lcufile
#include <stdlib.h>
#include <cufile.h>

// cuFileError_t is a struct, not an int, so it cannot cross into Go as a
// scalar. These shims flatten it to the two ints Go actually needs,
// keeping all struct handling on the C side.
static int atlas_driver_open(int *cu_err) {
    CUfileError_t s = cuFileDriverOpen();
    *cu_err = (int)s.cu_err;
    return (int)s.err;
}

static int atlas_driver_close(int *cu_err) {
    CUfileError_t s = cuFileDriverClose();
    *cu_err = (int)s.cu_err;
    return (int)s.err;
}

static int atlas_handle_register(CUfileHandle_t *fh, int fd, int *cu_err) {
    CUfileDescr_t descr;
    memset(&descr, 0, sizeof(descr));
    descr.handle.fd = fd;
    descr.type = CU_FILE_HANDLE_TYPE_OPAQUE_FD;
    CUfileError_t s = cuFileHandleRegister(fh, &descr);
    *cu_err = (int)s.cu_err;
    return (int)s.err;
}

static int atlas_buf_register(const void *base, size_t size, int *cu_err) {
    CUfileError_t s = cuFileBufRegister(base, size, 0);
    *cu_err = (int)s.cu_err;
    return (int)s.err;
}

static int atlas_buf_deregister(const void *base, int *cu_err) {
    CUfileError_t s = cuFileBufDeregister(base);
    *cu_err = (int)s.cu_err;
    return (int)s.err;
}
*/
import "C"

import (
	"context"
	"fmt"
	"sync"
	"unsafe"
)

// UNVERIFIED: this file has never been compiled. See the package doc.
// It is written against NVIDIA's published cuFile API reference; the
// first person with a GPU should expect to fix compile errors here
// before expecting it to run.

var driverMu sync.Mutex

// Available reports whether this binary can do GPUDirect I/O.
func Available() bool { return true }

func status(errCode, cuErr C.int) error {
	if errCode == 0 {
		return nil
	}
	return fmt.Errorf("gds: cuFile error %d (cuda %d)", int(errCode), int(cuErr))
}

// OpenDriver calls cuFileDriverOpen. It is process-wide and refcounted
// by the driver itself; AtlasFS opens it once and keeps it for the life
// of the process.
func OpenDriver() (*Driver, error) {
	driverMu.Lock()
	defer driverMu.Unlock()
	var cuErr C.int
	if err := status(C.atlas_driver_open(&cuErr), cuErr); err != nil {
		return nil, err
	}
	return &Driver{opened: true}, nil
}

func (d *Driver) Close() error {
	driverMu.Lock()
	defer driverMu.Unlock()
	if !d.opened {
		return nil
	}
	var cuErr C.int
	err := status(C.atlas_driver_close(&cuErr), cuErr)
	d.opened = false
	return err
}

// RegisterFile calls cuFileHandleRegister for fd.
//
// The fd should have been opened with O_DIRECT for the direct path;
// without it GDS uses compatibility mode (POSIX reads) when
// allow_compat_mode is enabled in /etc/cufile.json, which works but
// removes the benefit. AtlasFS opens container objects O_DIRECT when
// this package is in use — see the gdslocal backend.
func (d *Driver) RegisterFile(fd int) (*FileHandle, error) {
	if !d.opened {
		return nil, ErrUnavailable
	}
	var h C.CUfileHandle_t
	var cuErr C.int
	if err := status(C.atlas_handle_register(&h, C.int(fd), &cuErr), cuErr); err != nil {
		return nil, err
	}
	return &FileHandle{fd: fd, handle: uintptr(unsafe.Pointer(h)), valid: true}, nil
}

func (h *FileHandle) Close() error {
	if !h.valid {
		return nil
	}
	C.cuFileHandleDeregister(C.CUfileHandle_t(unsafe.Pointer(h.handle)))
	h.valid = false
	return nil
}

// ReadInto calls cuFileRead, reading size bytes from fileOffset into the
// registered device buffer based at devPtr, at devOffset within it.
//
// Note the argument order: cuFileRead takes the registration *base* and
// a separate offset, which is why store.Dest carries them separately
// instead of handing around a pre-offset pointer.
//
// A negative return from cuFileRead is an error code, not a short read.
func (h *FileHandle) ReadInto(ctx context.Context, fileOffset int64, devPtr uintptr, devOffset, size int64) (int64, error) {
	if !h.valid {
		return 0, ErrUnavailable
	}
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	n := C.cuFileRead(
		C.CUfileHandle_t(unsafe.Pointer(h.handle)),
		unsafe.Pointer(devPtr),
		C.size_t(size),
		C.off_t(fileOffset),
		C.off_t(devOffset),
	)
	if n < 0 {
		return 0, fmt.Errorf("gds: cuFileRead failed with %d", int64(n))
	}
	if int64(n) != size {
		// Partial reads are not something AtlasFS can use: a chunk is
		// verified as a whole, so a short read is a failed read.
		return int64(n), fmt.Errorf("gds: short cuFileRead: got %d of %d bytes", int64(n), size)
	}
	return int64(n), nil
}

// RegisterBuffer calls cuFileBufRegister. The range must stay registered
// for the duration of every I/O that targets it.
func RegisterBuffer(devPtr uintptr, size int64) error {
	var cuErr C.int
	return status(C.atlas_buf_register(unsafe.Pointer(devPtr), C.size_t(size), &cuErr), cuErr)
}

// DeregisterBuffer calls cuFileBufDeregister.
func DeregisterBuffer(devPtr uintptr) error {
	var cuErr C.int
	return status(C.atlas_buf_deregister(unsafe.Pointer(devPtr), &cuErr), cuErr)
}
