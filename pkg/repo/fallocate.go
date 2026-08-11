package repo

import "syscall"

// fallocate(2) for a filesystem that buffers a file's whole content and
// commits it as a unit (DESIGN.md §16.1). There is no block allocator to
// talk to, so "preallocate" has no meaning here and every mode reduces
// to arranging bytes in that buffer:
//
//   - default (allocate): the range must exist and read as zeros, and
//     the file grows to cover it. Bytes already there are untouched.
//   - FALLOC_FL_KEEP_SIZE: same, but the length does not grow. A
//     buffered file draws no distinction between allocated and written,
//     so past EOF this is a no-op — which must still report success.
//   - FALLOC_FL_PUNCH_HOLE: the range reads as zeros, size unchanged.
//     Linux requires KEEP_SIZE alongside it.
//   - FALLOC_FL_ZERO_RANGE: the range reads as zeros, and the file may
//     grow to cover it.
//
// Zeroing rather than sparse-allocating is the honest implementation
// rather than a lazy one: the observable contract of all four is "these
// bytes read as zeros", and this build has no sparse representation to
// exploit. What it does not do is save space, which is the usual reason
// to punch a hole — so a caller punching holes to reclaim storage gets
// correct data and no reclaim. That is a real limitation, and it is why
// COLLAPSE_RANGE and INSERT_RANGE, which change a file's *shape* rather
// than its contents, are refused instead of faked.
//
// This lives in pkg/repo rather than in either mount because both mounts
// implement fallocate and they must not disagree about it; a caller
// should not get different semantics from `atlas mount` and
// `atlas mount -mds`.
const (
	FallocKeepSize    = 0x01 // FALLOC_FL_KEEP_SIZE
	FallocPunchHole   = 0x02 // FALLOC_FL_PUNCH_HOLE
	FallocNoHideStale = 0x04 // FALLOC_FL_NO_HIDE_STALE
	FallocCollapse    = 0x08 // FALLOC_FL_COLLAPSE_RANGE
	FallocZeroRange   = 0x10 // FALLOC_FL_ZERO_RANGE
	FallocInsert      = 0x20 // FALLOC_FL_INSERT_RANGE
	FallocUnshare     = 0x40 // FALLOC_FL_UNSHARE_RANGE
)

// CheckFallocate rejects the argument combinations Linux rejects, and
// the modes this filesystem cannot honour.
//
// Refusing beats pretending: a caller that asked to remove a range and
// got EOPNOTSUPP can fall back, while one that got success has silently
// lost the operation.
func CheckFallocate(off, size uint64, mode uint32) syscall.Errno {
	if int64(off) < 0 || int64(size) < 0 || size == 0 {
		return syscall.EINVAL
	}
	if off+size < off {
		return syscall.EFBIG // the addition wraps
	}
	if mode&(FallocCollapse|FallocInsert) != 0 {
		// The buffered write path could imitate these, but both are
		// defined to operate on filesystem block boundaries and this
		// build has no block size to expose.
		return syscall.EOPNOTSUPP
	}
	if mode&FallocPunchHole != 0 && mode&FallocKeepSize == 0 {
		return syscall.EOPNOTSUPP // as Linux requires
	}
	known := uint32(FallocKeepSize | FallocPunchHole | FallocNoHideStale | FallocZeroRange | FallocUnshare)
	if mode&^known != 0 {
		return syscall.EOPNOTSUPP
	}
	return 0
}

// ApplyFallocate applies mode to buf, returning the new buffer and
// whether anything changed (the caller uses that to decide whether the
// handle became dirty — a punched hole that is never written must still
// reach the commit).
func ApplyFallocate(buf []byte, off, size uint64, mode uint32) ([]byte, bool, syscall.Errno) {
	if errno := CheckFallocate(off, size, mode); errno != 0 {
		return buf, false, errno
	}
	end := off + size
	changed := false

	if int64(end) > int64(len(buf)) {
		// PUNCH_HOLE carries KEEP_SIZE but still has to zero what is
		// inside the file; past EOF there is nothing to zero and nothing
		// to grow, so it and a plain KEEP_SIZE both stop here.
		grows := mode&FallocKeepSize == 0 || mode&FallocZeroRange != 0
		if !grows {
			return buf, false, 0
		}
		if int64(end) > MaxFileSize {
			return buf, false, syscall.EFBIG
		}
		buf = append(buf, make([]byte, int64(end)-int64(len(buf)))...)
		changed = true
	}

	// PUNCH_HOLE and ZERO_RANGE both mean "this range reads as zeros".
	// Plain allocate leaves data that is already there alone.
	if mode&(FallocPunchHole|FallocZeroRange) != 0 {
		clear(buf[off:end])
		changed = true
	}
	return buf, changed, 0
}
