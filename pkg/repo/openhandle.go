package repo

import (
	"sync"

	"github.com/aburan28/atlasfs/pkg/metadb"
)

// DESIGN.md §19.3's open-but-unlinked guarantee: an unlinked inode stays
// reachable while any client holds an open handle, so a descriptor
// opened before the last name went away keeps reading. POSIX requires
// this, and it is what makes "create a temp file, unlink it immediately,
// keep using the fd" safe.
//
// The graveyard alone does not provide it. Grace buys a *bounded* window
// (§19.2's Invariant GC-1 sizes it against T_write_max, the maximum age
// of an uncommitted write session), but a descriptor may stay open for
// as long as its process lives — arbitrarily longer than any grace
// period. Without a pin, a long-lived reader plus a routine GC pass is a
// use-after-free: Sweep deletes the locators and the inode record while
// the reader is still entitled to both.
//
// The pin is a refcount rather than a flag because several descriptors
// may reference one inode; the first close must not un-pin it for the
// rest.
//
// Scope: this registry is per-process, and so covers the in-process
// mount (pkg/fuseserver) completely — that process is both the only
// holder of handles and the only caller of Sweep. §19.3 specifies the
// distributed form as *leased* open handles so a holder that dies cannot
// pin an inode forever; pkg/mds implements that on top of this type
// (see pkg/mds's open-handle lease registry), which is what extends the
// guarantee across the client/authority boundary.
type OpenHandles struct {
	mu sync.Mutex
	n  map[metadb.InodeID]int
}

// NewOpenHandles returns an empty registry. The zero value is not usable
// — Acquire needs the map — so every Repo gets one at open time.
func NewOpenHandles() *OpenHandles {
	return &OpenHandles{n: make(map[metadb.InodeID]int)}
}

// Acquire pins id until the returned release func is called. Release is
// idempotent: FUSE can deliver RELEASE more than once for a handle (an
// interrupted request is cancelled *and resent*), and a second decrement
// would un-pin an inode a different holder still has open.
func (o *OpenHandles) Acquire(id metadb.InodeID) (release func()) {
	o.mu.Lock()
	o.n[id]++
	o.mu.Unlock()

	var once sync.Once
	return func() {
		once.Do(func() {
			o.mu.Lock()
			defer o.mu.Unlock()
			if o.n[id] <= 1 {
				delete(o.n, id) // keep the map from growing without bound
				return
			}
			o.n[id]--
		})
	}
}

// Held reports whether any handle is currently open on id.
func (o *OpenHandles) Held(id metadb.InodeID) bool {
	if o == nil {
		return false
	}
	o.mu.Lock()
	defer o.mu.Unlock()
	return o.n[id] > 0
}

// Snapshot returns the set of currently-pinned inodes. Sweep's mark
// phase takes one snapshot per run rather than querying per entry, so a
// handle opened midway through a sweep cannot make the run see a
// half-pinned view.
//
// A handle opened *after* the snapshot is not a hazard: opening requires
// a resolvable inode record, and the sweep that is running can only
// delete records for inodes that were already unpinned and past grace —
// which the opener could not have resolved.
func (o *OpenHandles) Snapshot() map[metadb.InodeID]struct{} {
	if o == nil {
		return nil
	}
	o.mu.Lock()
	defer o.mu.Unlock()
	held := make(map[metadb.InodeID]struct{}, len(o.n))
	for id, n := range o.n {
		if n > 0 {
			held[id] = struct{}{}
		}
	}
	return held
}
