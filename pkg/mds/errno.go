package mds

import (
	"strconv"
	"strings"
	"syscall"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// gRPC's code space is coarser than errno's: ENOTDIR, EISDIR and
// ENOTEMPTY all collapse onto FailedPrecondition, so a caller that
// translated the code alone would report "directory not empty" for a
// rename that actually hit a type mismatch. The status code stays
// meaningful for a non-filesystem caller; the errno rides along in the
// message under a fixed prefix.
//
// Scope, measured rather than assumed: through pkg/mdsfuse this is
// defence in depth, not the load-bearing path. Linux's VFS resolves both
// ends of a rename/unlink/rmdir from its own dentry cache and rejects
// every type mismatch itself (EISDIR, ENOTDIR) before the request ever
// reaches a FUSE server — verified by stripping the tagging and watching
// pkg/mdsfuse's refusal tests still pass. What the tagging does buy is a
// correct errno for a direct API caller, which has no VFS in front of it
// and would otherwise see one code for three distinct failures.
const errnoPrefix = "errno="

// withErrno tags err's status with the exact errno a local filesystem
// would have returned.
func withErrno(code codes.Code, e syscall.Errno, err error) error {
	return status.Errorf(code, "%s%d: %s", errnoPrefix, uintptr(e), err.Error())
}

// ErrnoOf recovers the errno a tagged status carries. It reports false
// for an untagged error, which is the signal to fall back to mapping the
// gRPC code — an authority older than this tagging, or a transport-level
// failure, produces one.
func ErrnoOf(err error) (syscall.Errno, bool) {
	if err == nil {
		return 0, false
	}
	msg := status.Convert(err).Message()
	if !strings.HasPrefix(msg, errnoPrefix) {
		return 0, false
	}
	rest := msg[len(errnoPrefix):]
	end := strings.IndexByte(rest, ':')
	if end < 0 {
		return 0, false
	}
	n, convErr := strconv.ParseUint(rest[:end], 10, 32)
	if convErr != nil || n == 0 {
		return 0, false
	}
	return syscall.Errno(n), true
}
