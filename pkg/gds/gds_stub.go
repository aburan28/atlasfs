//go:build !cufile

package gds

import "context"

// The no-GPU build. Every entry point fails with ErrUnavailable rather
// than degrading to a host read, because a silent fallback is the one
// behavior a caller asking for GPUDirect must not get: it would hand
// back exactly the CPU bounce buffer they were trying to eliminate,
// while reporting success.

// Available reports whether this binary can do GPUDirect I/O.
func Available() bool { return false }

// OpenDriver would call cuFileDriverOpen.
func OpenDriver() (*Driver, error) { return nil, ErrUnavailable }

// Close would call cuFileDriverClose.
func (d *Driver) Close() error { return ErrUnavailable }

// RegisterFile would call cuFileHandleRegister on fd.
func (d *Driver) RegisterFile(fd int) (*FileHandle, error) { return nil, ErrUnavailable }

// Close would call cuFileHandleDeregister.
func (h *FileHandle) Close() error { return ErrUnavailable }

// ReadInto would call cuFileRead.
func (h *FileHandle) ReadInto(ctx context.Context, fileOffset int64, devPtr uintptr, devOffset, size int64) (int64, error) {
	return 0, ErrUnavailable
}

// RegisterBuffer would call cuFileBufRegister.
func RegisterBuffer(devPtr uintptr, size int64) error { return ErrUnavailable }

// DeregisterBuffer would call cuFileBufDeregister.
func DeregisterBuffer(devPtr uintptr) error { return ErrUnavailable }
