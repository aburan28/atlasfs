package repo

// statfs(2) for an object-backed filesystem, which is a question the
// backend cannot really answer: S3, GCS and Azure have no capacity, and
// a local-disk backend's free space is the host's, not this repo's. The
// one number that *is* meaningful is the subtree's quota (DESIGN.md
// §18.3), so that is what df reports when one is set.
//
// With no quota, reporting the true "capacity" is impossible, and both
// honest-looking alternatives are worse than a synthetic one: reporting
// zero makes df show a full filesystem, and every tool that pre-checks
// free space (tar, rsync, cp --reflink, package managers) refuses to
// write. Reporting used-as-total says the same thing. So an unquotaed
// repo advertises UnboundedCapacity — the same convention s3fs and
// goofys use, for the same reason — and the comment here is the honest
// part: that number is a placeholder, not a measurement.

// UnboundedCapacity is the size a repo with no byte quota advertises to
// statfs(2). It is deliberately far larger than anything a caller will
// write and deliberately not a measurement of anything.
const UnboundedCapacity uint64 = 1 << 50 // 1 PiB

// UnboundedInodes is the same placeholder for the inode count.
const UnboundedInodes uint64 = 1 << 32

// StatfsInfo is what a mount needs to answer statfs(2).
type StatfsInfo struct {
	// Total and Used are bytes; Files and FilesUsed are inode counts.
	Total     uint64
	Used      uint64
	Files     uint64
	FilesUsed uint64
}

// Free reports the remaining bytes, clamped at zero — usage can exceed a
// limit that was lowered after the fact, and a negative free count would
// wrap into an enormous one.
func (s StatfsInfo) Free() uint64 {
	if s.Used > s.Total {
		return 0
	}
	return s.Total - s.Used
}

// FilesFree reports the remaining inodes, clamped the same way.
func (s StatfsInfo) FilesFree() uint64 {
	if s.FilesUsed > s.Files {
		return 0
	}
	return s.Files - s.FilesUsed
}

// StatfsInfoFrom builds the answer from a quota's limits and usage. A
// zero limit means unlimited for that dimension (§18.3), which is where
// the placeholder capacities come in.
func StatfsInfoFrom(bytesLimit, inodesLimit, bytesUsed, inodesUsed uint64) StatfsInfo {
	info := StatfsInfo{Total: bytesLimit, Used: bytesUsed, Files: inodesLimit, FilesUsed: inodesUsed}
	if bytesLimit == 0 {
		info.Total = UnboundedCapacity
	}
	if inodesLimit == 0 {
		info.Files = UnboundedInodes
	}
	return info
}

// Statfs reports this repo's capacity as df should see it.
func (r *Repo) Statfs() (StatfsInfo, error) {
	bytesLimit, inodesLimit, err := r.QuotaLimits()
	if err != nil {
		return StatfsInfo{}, err
	}
	bytesUsed, inodesUsed, err := r.QuotaUsage()
	if err != nil {
		return StatfsInfo{}, err
	}
	return StatfsInfoFrom(bytesLimit, inodesLimit, bytesUsed, inodesUsed), nil
}
