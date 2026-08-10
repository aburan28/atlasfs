package store

import (
	"context"
	"errors"
	"fmt"
)

// Dest is where a read's bytes should land. It exists because a []byte
// cannot name GPU memory, and GPUDirect Storage's entire value is
// putting bytes there without a CPU bounce (see getinto.go for the
// host-side seam this completes).
//
// A Dest is either host memory — an ordinary Go slice — or device
// memory, named by the base pointer of a buffer registered with
// cuFileBufRegister plus an offset into it. Nothing in this package
// dereferences a device pointer; it is an opaque token that only a
// backend built with cuFile support (pkg/gds) knows how to use, which is
// what keeps CUDA out of every other build.
type Dest struct {
	host []byte

	// devPtr is the *base* of a registered device allocation, and
	// devOffset is where in it to write. Keeping them separate is not
	// fussiness: cuFileRead's signature is
	// (handle, bufPtr_base, size, file_offset, bufPtr_offset), and it
	// wants the registration base, not a pre-offset pointer. Collapsing
	// them here would force every caller to un-collapse them there.
	devPtr    uintptr
	devOffset int64
	devLen    int64
}

// ErrNotHostMemory is returned by Bytes for a device destination.
var ErrNotHostMemory = errors.New("store: destination is device memory, not a []byte")

// HostDest returns a Dest writing into p.
func HostDest(p []byte) Dest { return Dest{host: p} }

// DeviceDest returns a Dest writing length bytes at offset into the
// registered device allocation based at ptr.
//
// Callers are responsible for the registration lifetime: cuFile requires
// the range to have been passed to cuFileBufRegister and to stay
// registered for the duration of the read. This package cannot check
// that — it has no CUDA context — which is precisely why the constructor
// says so rather than pretending to validate it.
func DeviceDest(ptr uintptr, offset, length int64) Dest {
	return Dest{devPtr: ptr, devOffset: offset, devLen: length}
}

// IsDevice reports whether d names device memory.
func (d Dest) IsDevice() bool { return d.host == nil && d.devPtr != 0 }

// Len is how many bytes the destination can take.
func (d Dest) Len() int64 {
	if d.IsDevice() {
		return d.devLen
	}
	return int64(len(d.host))
}

// Bytes returns the host slice, or ErrNotHostMemory for a device
// destination. Verification paths call this — see pack.FetchInto's doc
// comment on why a destination the CPU cannot read back changes what
// DESIGN.md §24.4 can promise.
func (d Dest) Bytes() ([]byte, error) {
	if d.IsDevice() {
		return nil, ErrNotHostMemory
	}
	return d.host, nil
}

// Device returns the registration base, offset, and length of a device
// destination.
func (d Dest) Device() (ptr uintptr, offset, length int64, err error) {
	if !d.IsDevice() {
		return 0, 0, 0, fmt.Errorf("store: destination is host memory")
	}
	return d.devPtr, d.devOffset, d.devLen, nil
}

// GetterIntoDest is the device-aware sibling of GetterInto: a backend
// that can put a byte range into either host or device memory.
//
// It is deliberately a separate, optional interface rather than a
// replacement for GetterInto. Every backend in this repo serves host
// memory and none of them serve device memory, so folding Dest into the
// common path would make four implementations carry a parameter they
// cannot honor, in order to serve one that does not exist yet.
type GetterIntoDest interface {
	GetIntoDest(ctx context.Context, key string, off int64, d Dest) error
}

// GetIntoDest reads into d, using the backend's device-aware path when
// it has one. For a host destination it falls through to GetInto, so a
// caller can write one code path and let the backend decide how capable
// it is. A device destination against a backend that cannot serve one is
// an error rather than a silent host copy — quietly staging through host
// memory would give back exactly the bounce buffer the caller asked to
// avoid, while looking like it worked.
func GetIntoDest(ctx context.Context, b Backend, key string, off int64, d Dest) error {
	if gd, ok := b.(GetterIntoDest); ok {
		return gd.GetIntoDest(ctx, key, off, d)
	}
	if d.IsDevice() {
		return fmt.Errorf("store: backend %T cannot read into device memory (no GPUDirect support built in)", b)
	}
	p, err := d.Bytes()
	if err != nil {
		return err
	}
	return GetInto(ctx, b, key, off, p)
}
