// Package gds is AtlasFS's NVIDIA GPUDirect Storage (cuFile) support.
//
// Build tags decide what you get:
//
//	go build -tags cufile ./...   cgo binding to libcufile (needs CUDA
//	                              headers, libcufile.so, and a GDS-capable
//	                              driver at runtime)
//	go build ./...                the stub in gds_stub.go, which reports
//	                              Available() == false and fails every
//	                              call with ErrUnavailable
//
// The split exists so that the rest of AtlasFS can be written against
// one API and still build, test, and ship on machines with no GPU — the
// overwhelmingly common case, including every machine this repo's CI
// runs on.
//
// # Honest status
//
// The cufile-tagged path has never been compiled or executed. The
// environment this was written in has no NVIDIA device, no CUDA toolkit,
// and no libcufile, so the cgo file is written against NVIDIA's
// published API reference and is unverified in the strongest sense: it
// has not been through a compiler. Treat it as a starting point that
// still needs a machine with a GPU to become real, not as working code.
// The stub path, the Dest abstraction it consumes (pkg/store), and the
// 4 KiB container alignment it requires (pkg/pack) are all tested.
//
// # What the API reference pins down
//
// From NVIDIA's GPUDirect Storage API reference and best-practices
// guide, the parts that shape this design:
//
//   - cuFileRead(fh, bufPtr_base, size, file_offset, bufPtr_offset)
//     takes the *registered base* of a device buffer plus an offset into
//     it, not a pre-offset pointer. store.Dest keeps those separate for
//     exactly this reason.
//   - A handle is registered per file descriptor with
//     cuFileHandleRegister over a CUfileDescr_t whose type is
//     CU_FILE_HANDLE_TYPE_OPAQUE_FD on Linux.
//   - Device buffers are registered with cuFileBufRegister and must stay
//     registered for the life of the I/O.
//   - An I/O is "unaligned" if the file offset, the size, the device
//     pointer, or the device buffer offset is not 4 KiB aligned. GDS
//     still serves it — through internal GPU bounce buffers, which is
//     the copy the whole feature exists to eliminate. Hence
//     pack.GDSAlignment.
//   - The direct path wants the file opened O_DIRECT; without it (or
//     when bounce-buffer allocation fails) GDS falls back to POSIX reads
//     if allow_compat_mode is set in /etc/cufile.json.
package gds

import "errors"

// ErrUnavailable is returned by every operation when the binary was
// built without the cufile tag, or with it but on a machine where the
// driver could not be opened.
var ErrUnavailable = errors.New("gds: GPUDirect Storage not available in this build")

// Driver is the process-wide cuFile driver handle (cuFileDriverOpen /
// cuFileDriverClose). One per process; the zero value is unopened.
type Driver struct {
	opened bool
}

// FileHandle wraps a cuFile-registered file descriptor
// (cuFileHandleRegister / cuFileHandleDeregister).
type FileHandle struct {
	fd     int
	handle uintptr // CUfileHandle_t
	valid  bool
}

// Aligned reports whether a read of size bytes at fileOffset into a
// device buffer based at devPtr+devOffset satisfies GDS's 4 KiB
// alignment on all four quantities. An unaligned read is not an error —
// GDS serves it via bounce buffers — so this exists for callers that
// want to know they are paying that cost, and for tests, rather than to
// gate anything.
func Aligned(fileOffset, size int64, devPtr uintptr, devOffset int64) bool {
	const a = 4096
	return fileOffset%a == 0 &&
		size%a == 0 &&
		uintptr(devPtr)%a == 0 &&
		devOffset%a == 0
}
