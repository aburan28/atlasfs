package store

import (
	"context"
	"fmt"
	"io"
)

// The caller-supplied-destination read path.
//
// Backend.Get hands back an io.ReadCloser, which means every caller that
// wants bytes ends up doing io.ReadAll and letting the runtime size the
// buffer by doubling. For AtlasFS that is not a small tax: reading a
// 64 MiB file through pkg/repo allocated ~2x the file size in transient
// buffers before this path existed (pkg/repo/bench_test.go's
// ReadColdSequential/64MiB reported it).
//
// It is also structurally in the way of GPUDirect Storage (NVIDIA
// cuFile). The entire point of GDS is that storage DMAs into GPU memory
// without a CPU bounce buffer; an API whose only shape is "here is a
// stream, you allocate somewhere to put it" cannot express that. The
// destination has to be the caller's to choose before a GDS backend can
// exist at all.
//
// So: GetterInto is an *optional* capability. Backends that can fill a
// caller's buffer directly implement it; everything else keeps working
// through GetInto's fallback, which is still a real improvement because
// io.ReadFull into a right-sized buffer beats io.ReadAll's doubling.
// Nothing about Backend changes, so no existing implementation breaks.

// GetterInto is implemented by backends that can read a byte range
// straight into a caller-provided buffer.
//
// Contract: read exactly len(p) bytes at off, or return an error.
// Partial success is not a thing AtlasFS wants here — a chunk is
// verified as a whole (DESIGN.md §24.4), so a short read is a failed
// read.
type GetterInto interface {
	GetInto(ctx context.Context, key string, off int64, p []byte) error
}

// GetInto reads len(p) bytes of key starting at off into p, using the
// backend's own zero-copy path when it has one and a streaming fallback
// otherwise. The fallback still avoids io.ReadAll: p is already the
// right size, so io.ReadFull neither grows nor reallocates.
func GetInto(ctx context.Context, b Backend, key string, off int64, p []byte) error {
	if len(p) == 0 {
		return nil
	}
	if gi, ok := b.(GetterInto); ok {
		return gi.GetInto(ctx, key, off, p)
	}
	rc, err := b.Get(ctx, key, off, int64(len(p)))
	if err != nil {
		return err
	}
	defer rc.Close()
	if _, err := io.ReadFull(rc, p); err != nil {
		if err == io.ErrUnexpectedEOF || err == io.EOF {
			return fmt.Errorf("store: short read for %s at %d: %w", key, off, err)
		}
		return err
	}
	return nil
}

// SupportsGetInto reports whether b fills caller buffers natively rather
// than through GetInto's streaming fallback. Only useful for diagnostics
// and tests — callers should just use GetInto, which is correct either
// way.
func SupportsGetInto(b Backend) bool {
	_, ok := b.(GetterInto)
	return ok
}

// Where a GPUDirect Storage backend plugs in, and what is still missing.
//
// What exists now: the read path from repo.FileReader.ReadAt down through
// pack.FetchInto to GetInto never allocates a buffer of its own on the
// hot path. The destination belongs to the caller, and a chunk-aligned
// read lands in it directly. Measured on a 64 MiB sequential read, that
// took transient allocation from ~405 MB to ~68 MB — the latter being the
// caller's own destination and essentially nothing else.
//
// What is still missing, stated precisely so nobody mistakes this for a
// finished GDS integration: GetterInto's destination is a []byte, which
// is host memory. Real cuFile reads into a CUdeviceptr registered with
// cuFileBufRegister, and no []byte can name that. Closing the gap needs
// one more step — a destination type that is either host or device
// memory, with GetterInto growing a device-aware sibling — and that step
// should be designed against a real cuFile binding rather than guessed
// at, because the registration and alignment rules (cuFileHandleRegister
// on the fd, 4 KiB alignment on offsets and sizes, GPU-side buffer
// lifetime) are what determine whether the API shape is right.
//
// The other unresolved question a GDS backend must answer is
// verification: see pack.FetchInto's doc comment, which spells out why
// DESIGN.md §24.4's self-verifying reads and a CPU-invisible DMA
// destination are in tension, and what the two honest resolutions are.
//
// Two smaller notes for whoever builds it:
//
//   - The S3/GCS/Azure backends deliberately do not implement GetterInto.
//     Their transport hands back an HTTP body that has to be drained
//     either way, so the fallback's io.ReadFull into a right-sized buffer
//     already captures the available win; a native implementation would
//     add code without removing a copy. Local disk is different — it has
//     pread(2) — which is why only that one implements it.
//   - GDS also requires the file to be on a filesystem its driver
//     supports, which for AtlasFS means the container objects, not the
//     FUSE mount. That points a GDS backend at pkg/store, exactly where
//     this seam is, rather than at pkg/fuseserver.
