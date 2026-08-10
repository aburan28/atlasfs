// Package fuseserver mounts a repo.Repo over FUSE — read-only for the
// `immutable` class (DESIGN.md §8), read-write for `relaxed`/`session`
// (DESIGN.md §16.1, §22.2's non-ROX row). For `immutable`, there is no
// lease protocol to implement: every dentry and inode is fetched
// straight from metadb and is correct for the life of the mount by
// construction. For a mutable class, this package is the one real
// consumer this build has of pkg/coherence's per-holder leases and
// negative cache — every Node caches its own metadb.InodeRecord and
// negative-lookup results in memory, using a fixed holder ID ("local")
// because this mount talks to an in-process metadb rather than to a
// metadata authority.
//
// That is the mount's current limit, stated plainly: pkg/mds serves
// leases to real remote holders and implements §10.6's recall against
// them, but this mount does not go through it. `posix` is reachable by
// mounting through pkg/mdsfuse instead, where the mount is a genuine
// remote holder and a recall against it crosses a process boundary.
//
// This is a plain go-fuse mount (splice/passthrough tuning from
// DESIGN.md §21.2 is not implemented here — that is a later-phase
// performance pass, not a correctness requirement for the vertical
// slice). The mutable write model is buffer-then-commit-on-close
// (§16.1), not in-place random-access writes — see pkg/repo/write.go's
// package doc for the exact scope cut.
package fuseserver

import (
	"context"
	"errors"
	"io"
	"sync"
	"syscall"

	"github.com/hanwen/go-fuse/v2/fs"
	"github.com/hanwen/go-fuse/v2/fuse"

	"github.com/aburan28/atlasfs/pkg/metadb"
	"github.com/aburan28/atlasfs/pkg/repo"
)

// coherenceHolder is the single local holder ID this in-process mount
// registers as with a repo's coherence.Manager.
const coherenceHolder = "local"

// Node is one FUSE inode, backed by an AtlasFS inode in a Repo. parent
// and name are needed to commit an overwrite (writing to an existing
// file opened without O_CREATE) back through repo.CreateFile, which
// takes a (dir, name) pair, not an inode ID — the root node is the only
// one with no parent, and it is never opened for write (it's always a
// directory).
//
// cachedRec/stale/mu implement the coherence-aware read side: a Getattr
// (or anything else needing current attrs) trusts cachedRec while the
// repo's coherence.Manager still grants this holder a lease on the
// inode's key, refreshes from metadb on expiry, and refreshes
// immediately if markStale was called by a best-effort push (fired
// synchronously, in-process, by a Commit/Unlink/Mkdir/Rmdir elsewhere in
// this same process — see pkg/coherence's Bump doc comment on what
// "best-effort" means across a real network, which this single-process
// build doesn't have).
type Node struct {
	fs.Inode
	repo   *repo.Repo
	ino    metadb.InodeID
	parent metadb.InodeID
	name   string

	mu        sync.Mutex
	cachedRec metadb.InodeRecord
	stale     bool
	// activeWrite is the currently-open write handle for this node, if
	// any. Setattr needs it: go-fuse/the kernel does not reliably pass
	// the already-open handle as Setattr's fs.FileHandle parameter for
	// the O_TRUNC-on-open and ftruncate(fd) cases (observed empirically:
	// f arrives nil), so Setattr looks here instead of trusting f. See
	// Setattr's doc comment for why resizing the wrong buffer corrupts
	// data.
	activeWrite *writeFileHandle
}

var (
	_ fs.NodeLookuper   = (*Node)(nil)
	_ fs.NodeReaddirer  = (*Node)(nil)
	_ fs.NodeGetattrer  = (*Node)(nil)
	_ fs.NodeOpener     = (*Node)(nil)
	_ fs.NodeReadlinker = (*Node)(nil)
	_ fs.NodeCreater    = (*Node)(nil)
	_ fs.NodeUnlinker   = (*Node)(nil)
	_ fs.NodeMkdirer    = (*Node)(nil)
	_ fs.NodeRmdirer    = (*Node)(nil)
	_ fs.NodeSetattrer  = (*Node)(nil)
	_ fs.NodeRenamer    = (*Node)(nil)
	_ fs.NodeSymlinker  = (*Node)(nil)
	_ fs.NodeLinker     = (*Node)(nil)
	_ fs.NodeStatfser   = (*Node)(nil)
)

// binding reads the dentry this node is currently bound to. Rename
// rewrites it under mu, so a write path that captured the pair at open
// time must re-read it rather than close over the fields — committing to
// the pre-rename name would recreate the name the rename removed.
func (n *Node) binding() (metadb.InodeID, string) {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.parent, n.name
}

// currentRec returns the Node's current InodeRecord, consulting the
// repo's coherence.Manager (when non-nil) to decide whether the cached
// copy is still trustworthy before re-fetching from metadb.
func (n *Node) currentRec() (metadb.InodeRecord, syscall.Errno) {
	if n.repo.Coherence == nil {
		// immutable: content never changes, so whatever was cached at
		// Lookup/Mount time is permanently correct.
		return n.getCached(), 0
	}

	key := repo.InodeCoherenceKey(n.ino)
	n.mu.Lock()
	stale := n.stale
	n.mu.Unlock()

	if !stale {
		if _, ok := n.repo.Coherence.TrustedVersion(coherenceHolder, key); ok {
			return n.getCached(), 0
		}
	}

	rec, err := n.repo.DB.GetInode(n.ino)
	if err != nil {
		return metadb.InodeRecord{}, syscall.EIO
	}
	n.mu.Lock()
	n.cachedRec = rec
	n.stale = false
	n.mu.Unlock()
	n.repo.Coherence.Grant(coherenceHolder, key)
	n.repo.Coherence.Subscribe(coherenceHolder, key, n.markStale)
	return rec, 0
}

func (n *Node) getCached() metadb.InodeRecord {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.cachedRec
}

func (n *Node) setCached(rec metadb.InodeRecord) {
	n.mu.Lock()
	n.cachedRec = rec
	n.stale = false
	n.mu.Unlock()
}

// markStale is registered with the coherence.Manager as this Node's
// best-effort push-invalidation callback (DESIGN.md §10.2): it does not
// re-fetch anything itself, just marks the cache untrustworthy so the
// next currentRec() call does.
func (n *Node) markStale() {
	n.mu.Lock()
	n.stale = true
	n.mu.Unlock()
}

// fillAttrOut fills out from rec. mutable reflects the repo's class
// (DESIGN.md §8) — a regular file's permission bits are 0o644 on a
// mutable-class repo and 0o444 on immutable, matching what Open already
// enforces (a write syscall against an immutable mount fails with EROFS
// regardless of what these bits say, so this is presentation, not the
// actual access-control boundary).
func fillAttrOut(rec metadb.InodeRecord, mutable bool, out *fuse.Attr) {
	out.Size = rec.Size
	sec := uint64(0)
	if !rec.MTime.IsZero() {
		sec = uint64(rec.MTime.Unix())
	}
	out.Mtime = sec
	out.Atime = sec
	out.Ctime = sec
	switch {
	case rec.IsDir:
		out.Mode = syscall.S_IFDIR | 0o755
		out.Nlink = 2
	case rec.IsSymlink:
		out.Mode = syscall.S_IFLNK | 0o777
		out.Nlink = 1
	default:
		mode := uint32(0o444)
		if mutable {
			mode = 0o644
		}
		out.Mode = syscall.S_IFREG | mode
		// The stored count, not a constant: `ls -l` and `find -links`
		// read it, and a hard-linked file reporting 1 would tell a
		// caller it is safe to delete the last name when it is not.
		out.Nlink = uint32(rec.NLink)
		if out.Nlink == 0 {
			out.Nlink = 1
		}
	}
}

// direntMode is the raw type bits (S_IFDIR/S_IFLNK/S_IFREG) go-fuse
// needs for a StableAttr or DirEntry — the permission bits live in
// fillAttrOut instead, since directory listings and Lookup don't carry
// full attrs.
func direntMode(rec metadb.InodeRecord) uint32 {
	switch {
	case rec.IsDir:
		return syscall.S_IFDIR
	case rec.IsSymlink:
		return syscall.S_IFLNK
	default:
		return syscall.S_IFREG
	}
}

// Statfs answers df. See pkg/repo/statfs.go for why the numbers are the
// quota's and not the backend's.
func (n *Node) Statfs(ctx context.Context, out *fuse.StatfsOut) syscall.Errno {
	info, err := n.repo.Statfs()
	if err != nil {
		return syscall.EIO
	}
	fillStatfs(info, out)
	return 0
}

// statfsBlockSize is the unit df divides by. It is a reporting unit, not
// an allocation unit — this filesystem stores content-addressed chunks in
// packed containers (§5.4), so there is no on-disk block to match.
const statfsBlockSize = 4096

func fillStatfs(info repo.StatfsInfo, out *fuse.StatfsOut) {
	out.Bsize = statfsBlockSize
	out.Frsize = statfsBlockSize
	out.Blocks = info.Total / statfsBlockSize
	free := info.Free() / statfsBlockSize
	out.Bfree, out.Bavail = free, free
	out.Files = info.Files
	out.Ffree = info.FilesFree()
	out.NameLen = 255
}

func (n *Node) Getattr(ctx context.Context, f fs.FileHandle, out *fuse.AttrOut) syscall.Errno {
	rec, errno := n.currentRec()
	if errno != 0 {
		return errno
	}
	fillAttrOut(rec, n.repo.Class.Mutable(), &out.Attr)
	return 0
}

// Readlink returns a symlink's target. The kernel calls this instead of
// Open when resolving a symlink node.
func (n *Node) Readlink(ctx context.Context) ([]byte, syscall.Errno) {
	rec, errno := n.currentRec()
	if errno != 0 {
		return nil, errno
	}
	if !rec.IsSymlink {
		return nil, syscall.EINVAL
	}
	return []byte(rec.SymlinkTarget), 0
}

// Lookup consults negative caching (DESIGN.md §10.4) before ever
// touching metadb: a name known-absent as of the directory's last-seen
// version is rejected locally, and a fresh miss is recorded the same
// way. Any mutation under this directory (Create/Unlink/Mkdir/Rmdir)
// bumps the directory's coherence version, which invalidates every
// negative entry recorded against it in one step — no per-entry
// messages needed, which is the entire point of gating on dirver
// instead of push per name.
func (n *Node) Lookup(ctx context.Context, name string, out *fuse.EntryOut) (*fs.Inode, syscall.Errno) {
	rec, errno := n.currentRec()
	if errno != 0 {
		return nil, errno
	}
	if !rec.IsDir {
		return nil, syscall.ENOTDIR
	}

	dirKey := repo.DirCoherenceKey(n.ino)
	if n.repo.Coherence != nil && n.repo.Coherence.NegativeTrusted(coherenceHolder, dirKey, name) {
		return nil, syscall.ENOENT
	}

	childIno, err := n.repo.DB.Lookup(n.ino, name)
	if err != nil {
		if n.repo.Coherence != nil {
			n.repo.Coherence.GrantNegative(coherenceHolder, dirKey, name)
		}
		return nil, syscall.ENOENT
	}
	childRec, err := n.repo.DB.GetInode(childIno)
	if err != nil {
		return nil, syscall.EIO
	}
	child := &Node{repo: n.repo, ino: childIno, parent: n.ino, name: name}
	child.setCached(childRec)
	fillAttrOut(childRec, n.repo.Class.Mutable(), &out.Attr)
	stable := fs.StableAttr{Mode: direntMode(childRec), Ino: uint64(childIno)}
	inode := n.NewInode(ctx, child, stable)
	return inode, 0
}

func (n *Node) Readdir(ctx context.Context) (fs.DirStream, syscall.Errno) {
	rec, errno := n.currentRec()
	if errno != 0 {
		return nil, errno
	}
	if !rec.IsDir {
		return nil, syscall.ENOTDIR
	}
	entries, err := n.repo.Readdir(n.ino)
	if err != nil {
		return nil, syscall.EIO
	}
	list := make([]fuse.DirEntry, 0, len(entries))
	for _, e := range entries {
		childRec, err := n.repo.DB.GetInode(e.Inode)
		if err != nil {
			continue
		}
		list = append(list, fuse.DirEntry{Name: e.Name, Ino: uint64(e.Inode), Mode: direntMode(childRec)})
	}
	return fs.NewListDirStream(list), 0
}

func (n *Node) Open(ctx context.Context, flags uint32) (fs.FileHandle, uint32, syscall.Errno) {
	rec, errno := n.currentRec()
	if errno != 0 {
		return nil, 0, errno
	}
	if rec.IsDir {
		return nil, 0, syscall.EISDIR
	}
	if rec.IsSymlink {
		return nil, 0, syscall.EINVAL
	}

	if flags&(syscall.O_WRONLY|syscall.O_RDWR) != 0 {
		// Read-write mount: writes are rejected before they ever reach a
		// FileReader/write handle unless the repo's class permits them
		// (DESIGN.md §8 — `immutable` is EROFS by definition, not merely
		// unimplemented here).
		if !n.repo.Class.Mutable() {
			return nil, 0, syscall.EROFS
		}
		wh := &writeFileHandle{repo: n.repo, parent: n.parent, name: n.name, node: n}
		if flags&syscall.O_TRUNC != 0 {
			wh.dirty = true // truncate must commit even with zero further writes
		} else if rec.Size > 0 {
			fr, err := n.repo.OpenFile(ctx, rec)
			if err != nil {
				return nil, 0, syscall.EIO
			}
			data, err := fr.ReadAll()
			if err != nil {
				return nil, 0, syscall.EIO
			}
			wh.buf = append([]byte(nil), data...)
		}
		n.mu.Lock()
		n.activeWrite = wh
		n.mu.Unlock()
		return wh, fuse.FOPEN_KEEP_CACHE, 0
	}

	fr, err := n.repo.OpenFile(ctx, rec)
	if err != nil {
		return nil, 0, syscall.EIO
	}
	return &fileHandle{fr: fr}, fuse.FOPEN_KEEP_CACHE, 0
}

// Create makes a new, empty file and returns a writable handle for it.
// The empty file is committed immediately (visible in the namespace
// right away, matching ordinary create(2) semantics) — DESIGN.md
// §16.1's overwrite-in-place path then applies unchanged when the
// returned handle's Release commits real content into the same inode.
func (n *Node) Create(ctx context.Context, name string, flags uint32, mode uint32, out *fuse.EntryOut) (*fs.Inode, fs.FileHandle, uint32, syscall.Errno) {
	if !n.repo.Class.Mutable() {
		return nil, nil, 0, syscall.EROFS
	}
	parentRec, errno := n.currentRec()
	if errno != 0 {
		return nil, nil, 0, errno
	}
	if !parentRec.IsDir {
		return nil, nil, 0, syscall.ENOTDIR
	}

	h, err := n.repo.CreateFile(n.ino, name)
	if err != nil {
		return nil, nil, 0, errnoFor(err)
	}
	id, err := h.Commit(ctx)
	if err != nil {
		return nil, nil, 0, errnoFor(err)
	}
	rec, err := n.repo.DB.GetInode(id)
	if err != nil {
		return nil, nil, 0, syscall.EIO
	}

	child := &Node{repo: n.repo, ino: id, parent: n.ino, name: name}
	child.setCached(rec)
	fillAttrOut(rec, n.repo.Class.Mutable(), &out.Attr)
	stable := fs.StableAttr{Mode: direntMode(rec), Ino: uint64(id)}
	inode := n.NewInode(ctx, child, stable)

	fh := &writeFileHandle{repo: n.repo, parent: n.ino, name: name, node: child}
	child.activeWrite = fh // no lock needed: child isn't reachable by any other goroutine yet
	return inode, fh, fuse.FOPEN_KEEP_CACHE, 0
}

func (n *Node) Unlink(ctx context.Context, name string) syscall.Errno {
	return errnoFor(n.repo.Unlink(n.ino, name))
}

func (n *Node) Mkdir(ctx context.Context, name string, mode uint32, out *fuse.EntryOut) (*fs.Inode, syscall.Errno) {
	id, err := n.repo.Mkdir(n.ino, name)
	if err != nil {
		return nil, errnoFor(err)
	}
	rec, err := n.repo.DB.GetInode(id)
	if err != nil {
		return nil, syscall.EIO
	}
	child := &Node{repo: n.repo, ino: id, parent: n.ino, name: name}
	child.setCached(rec)
	fillAttrOut(rec, n.repo.Class.Mutable(), &out.Attr)
	stable := fs.StableAttr{Mode: direntMode(rec), Ino: uint64(id)}
	return n.NewInode(ctx, child, stable), 0
}

func (n *Node) Rmdir(ctx context.Context, name string) syscall.Errno {
	return errnoFor(n.repo.Rmdir(n.ino, name))
}

// Rename moves name out of this directory and into newParent under
// newName. The metadata move is one metadb transaction; what happens
// here on top of it is rebinding the moved Node, so an already-open
// write handle for the file commits to where the file now lives.
func (n *Node) Rename(ctx context.Context, name string, newParent fs.InodeEmbedder, newName string, flags uint32) syscall.Errno {
	if !n.repo.Class.Mutable() {
		return syscall.EROFS
	}
	if flags != 0 {
		// renameat2's RENAME_EXCHANGE and RENAME_NOREPLACE need atomicity
		// repo.Rename does not offer, and honouring the call while
		// ignoring the flag would turn a "don't clobber" request into a
		// clobber. EINVAL is what a filesystem without renameat2 support
		// returns, and what glibc's fallback path expects.
		return syscall.EINVAL
	}
	dst, ok := newParent.(*Node)
	if !ok {
		return syscall.EXDEV
	}
	child := n.GetChild(name)
	if err := n.repo.Rename(n.ino, name, dst.ino, newName); err != nil {
		return errnoFor(err)
	}
	if child != nil {
		if cn, ok := child.Operations().(*Node); ok {
			cn.mu.Lock()
			cn.parent, cn.name = dst.ino, newName
			cn.mu.Unlock()
		}
	}
	return 0
}

// Link binds name in this directory to an already-existing inode. The
// returned *fs.Inode is the existing node, not a new one — two names for
// one inode is the entire point, and handing back a fresh node would
// give the kernel two inode identities for the same file.
func (n *Node) Link(ctx context.Context, target fs.InodeEmbedder, name string, out *fuse.EntryOut) (*fs.Inode, syscall.Errno) {
	tn, ok := target.(*Node)
	if !ok {
		return nil, syscall.EXDEV
	}
	rec, err := n.repo.Link(n.ino, name, tn.ino)
	if err != nil {
		return nil, errnoFor(err)
	}
	tn.setCached(rec)
	fillAttrOut(rec, n.repo.Class.Mutable(), &out.Attr)
	return tn.EmbeddedInode(), 0
}

func (n *Node) Symlink(ctx context.Context, target, name string, out *fuse.EntryOut) (*fs.Inode, syscall.Errno) {
	id, err := n.repo.Symlink(n.ino, name, target)
	if err != nil {
		return nil, errnoFor(err)
	}
	rec, err := n.repo.DB.GetInode(id)
	if err != nil {
		return nil, syscall.EIO
	}
	child := &Node{repo: n.repo, ino: id, parent: n.ino, name: name}
	child.setCached(rec)
	fillAttrOut(rec, n.repo.Class.Mutable(), &out.Attr)
	return n.NewInode(ctx, child, fs.StableAttr{Mode: syscall.S_IFLNK, Ino: uint64(id)}), 0
}

// Setattr handles truncate (via truncate(2)/ftruncate(2), and the
// O_TRUNC-on-an-existing-file case that a plain open(2) also routes
// through this — not through Node.Open's flags — on Linux's FUSE
// implementation). Every other attribute change (mode, times, owner) is
// accepted but not persisted: this build's InodeRecord doesn't track a
// separate mode from the class-derived default (fillAttrOut), and there
// is no uid/gid model yet (DESIGN.md §20 is a later phase) — silently
// accepting rather than returning ENOSYS matches what most FUSE
// filesystems do for attributes they don't materially support, since
// returning an error here breaks a surprising amount of ordinary
// software (cp, rsync, tar) that always tries to restore them.
func (n *Node) Setattr(ctx context.Context, f fs.FileHandle, in *fuse.SetAttrIn, out *fuse.AttrOut) syscall.Errno {
	rec, errno := n.currentRec()
	if errno != 0 {
		return errno
	}

	if sz, ok := in.GetSize(); ok {
		if !n.repo.Class.Mutable() {
			return syscall.EROFS
		}
		if rec.IsDir {
			return syscall.EISDIR
		}

		// O_TRUNC on an existing file, and ftruncate(2) on an open fd,
		// both reach us here while a write handle for this node is
		// already open — but empirically, go-fuse/the kernel does not
		// reliably pass that handle as f (observed nil in exactly this
		// case: Open() runs first, sees no O_TRUNC bit by the time it
		// gets there and preloads the file's existing content, then
		// Setattr arrives with f == nil). Resizing independently here —
		// a fresh read-modify-commit disconnected from the handle's
		// in-memory buffer — would leave that buffer holding stale
		// pre-truncate bytes that a subsequent Write then partially
		// overwrites instead of replacing, corrupting the result.
		// n.activeWrite, not f, is what makes resizing the SAME buffer
		// Write/Flush operate on reliable.
		n.mu.Lock()
		wh := n.activeWrite
		n.mu.Unlock()
		if wh != nil {
			wh.resize(sz)
			rec.Size = sz
			fillAttrOut(rec, n.repo.Class.Mutable(), &out.Attr)
			return 0
		}

		var data []byte
		if rec.Size > 0 {
			fr, err := n.repo.OpenFile(ctx, rec)
			if err != nil {
				return syscall.EIO
			}
			data, err = fr.ReadAll()
			if err != nil {
				return syscall.EIO
			}
		}
		if uint64(len(data)) < sz {
			grown := make([]byte, sz)
			copy(grown, data)
			data = grown
		} else {
			data = data[:sz]
		}
		parent, name := n.binding()
		repoWH, err := n.repo.CreateFile(parent, name)
		if err != nil {
			return errnoFor(err)
		}
		if _, err := repoWH.Write(data); err != nil {
			return syscall.EIO
		}
		id, err := repoWH.Commit(ctx)
		if err != nil {
			return errnoFor(err)
		}
		if newRec, err := n.repo.DB.GetInode(id); err == nil {
			n.setCached(newRec)
			rec = newRec
		}
	}

	fillAttrOut(rec, n.repo.Class.Mutable(), &out.Attr)
	return 0
}

// errnoFor maps pkg/repo's sentinel errors to the errno a real syscall
// would return, rather than collapsing everything to EIO.
func errnoFor(err error) syscall.Errno {
	switch {
	case err == nil:
		return 0
	case errors.Is(err, repo.ErrReadOnly):
		return syscall.EROFS
	case errors.Is(err, metadb.ErrNotFound):
		return syscall.ENOENT
	case errors.Is(err, repo.ErrExists), errors.Is(err, metadb.ErrExists):
		return syscall.EEXIST
	case errors.Is(err, repo.ErrIsDirectory), errors.Is(err, metadb.ErrIsDirectory):
		return syscall.EISDIR
	case errors.Is(err, repo.ErrNotDir), errors.Is(err, metadb.ErrNotDir):
		return syscall.ENOTDIR
	case errors.Is(err, repo.ErrNotEmpty), errors.Is(err, metadb.ErrNotEmpty):
		return syscall.ENOTEMPTY
	case errors.Is(err, metadb.ErrInvalidRename):
		return syscall.EINVAL
	case errors.Is(err, repo.ErrQuotaExceeded), errors.Is(err, metadb.ErrQuotaExceeded):
		return syscall.EDQUOT
	default:
		return syscall.EIO
	}
}

// fileHandle is the read-only handle used for O_RDONLY opens (always,
// on an immutable repo; also on a mutable one when the caller didn't
// ask to write).
type fileHandle struct {
	fr *repo.FileReader
}

var (
	_ fs.FileReader  = (*fileHandle)(nil)
	_ fs.FileFsyncer = (*fileHandle)(nil)
)

// Fsync on a read-only handle has nothing to flush, but must still
// succeed: fsync(2) on an O_RDONLY fd is legal, and returning ENOSYS
// would surface as an error to callers that sync every fd they hold.
func (h *fileHandle) Fsync(ctx context.Context, flags uint32) syscall.Errno { return 0 }

func (h *fileHandle) Read(ctx context.Context, dest []byte, off int64) (fuse.ReadResult, syscall.Errno) {
	n, err := h.fr.ReadAt(dest, off)
	if err != nil && err != io.EOF {
		return nil, syscall.EIO
	}
	return fuse.ReadResultData(dest[:n]), 0
}

// writeFileHandle buffers a file's full content in memory across the
// life of an open-for-write session and commits it as a unit on
// Release — DESIGN.md §16.1's buffer-then-chunk-on-close model. Reads
// on a writable handle are served from the same buffer, so a session
// sees its own uncommitted writes (read-your-own-writes within one
// open), which a plain repo.FileReader (backed by the last-committed
// manifest) would not.
type writeFileHandle struct {
	repo   *repo.Repo
	parent metadb.InodeID
	name   string
	node   *Node // refreshed in place on commit so subsequent Getattr/Open reflect it immediately

	mu    sync.Mutex
	buf   []byte
	dirty bool
}

var (
	_ fs.FileWriter   = (*writeFileHandle)(nil)
	_ fs.FileReader   = (*writeFileHandle)(nil)
	_ fs.FileFlusher  = (*writeFileHandle)(nil)
	_ fs.FileReleaser = (*writeFileHandle)(nil)
	_ fs.FileFsyncer  = (*writeFileHandle)(nil)
)

// Fsync commits whatever is buffered, which is exactly DESIGN.md §16.2's
// contract for it: durable in the home region — chunks in the object
// store, manifest committed in metadata. It does not wait on any
// cross-region replication; §16.2 is explicit that a checkpoint writer
// calling fsync in a training loop must not stall for one, and a caller
// wanting that guarantee asks for it separately.
//
// Without this the kernel gets ENOSYS and stops sending FSYNC, so a
// process that wrote, fsynced and then died would lose the write even
// though fsync returned success — the buffer would still be waiting for
// a close(2) that never came.
func (h *writeFileHandle) Fsync(ctx context.Context, flags uint32) syscall.Errno {
	return h.commitIfDirty(ctx)
}

// target is the dentry this handle commits into. It prefers the node's
// current binding over the pair captured at Open time, because a rename
// while the file is open moves the dentry: committing to the captured
// name would recreate the name the rename just removed.
func (h *writeFileHandle) target() (metadb.InodeID, string) {
	if h.node != nil {
		if parent, name := h.node.binding(); name != "" {
			return parent, name
		}
	}
	return h.parent, h.name
}

// resize truncates or zero-extends the handle's buffer to sz bytes and
// marks it dirty, so a subsequent Flush commits the resized content —
// the direct fix for the O_TRUNC/ftruncate corruption described in
// Node.Setattr's comment above.
func (h *writeFileHandle) resize(sz uint64) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if uint64(len(h.buf)) < sz {
		grown := make([]byte, sz)
		copy(grown, h.buf)
		h.buf = grown
	} else {
		h.buf = h.buf[:sz]
	}
	h.dirty = true
}

func (h *writeFileHandle) Write(ctx context.Context, data []byte, off int64) (uint32, syscall.Errno) {
	h.mu.Lock()
	defer h.mu.Unlock()
	end := off + int64(len(data))
	if end > int64(len(h.buf)) {
		grown := make([]byte, end)
		copy(grown, h.buf)
		h.buf = grown
	}
	copy(h.buf[off:], data)
	h.dirty = true
	return uint32(len(data)), 0
}

func (h *writeFileHandle) Read(ctx context.Context, dest []byte, off int64) (fuse.ReadResult, syscall.Errno) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if off >= int64(len(h.buf)) {
		return fuse.ReadResultData(nil), 0
	}
	end := off + int64(len(dest))
	if end > int64(len(h.buf)) {
		end = int64(len(h.buf))
	}
	return fuse.ReadResultData(h.buf[off:end]), 0
}

// Flush commits the buffered content. This — not Release — is the FUSE
// hook that must do it: RELEASE is documented as asynchronous relative
// to close(2) (the kernel does not wait for it), while FLUSH is sent
// synchronously as part of close(2) and the caller does block on it.
// Committing only on Release would make writes racily invisible to a
// process that closes the file and immediately reopens it — exactly the
// read-your-writes property this build exists to get right.
func (h *writeFileHandle) Flush(ctx context.Context) syscall.Errno {
	return h.commitIfDirty(ctx)
}

// Release is a backstop for any caller that drops the last reference to
// this handle without ever calling close(2)/Flush (e.g. an abnormal
// exit some FUSE clients can trigger) — commitIfDirty's own guard makes
// calling it a second time here a no-op in the ordinary case.
func (h *writeFileHandle) Release(ctx context.Context) syscall.Errno {
	errno := h.commitIfDirty(ctx)
	if h.node != nil {
		h.node.mu.Lock()
		if h.node.activeWrite == h {
			h.node.activeWrite = nil
		}
		h.node.mu.Unlock()
	}
	return errno
}

// commitIfDirty commits the buffered content whenever there is writing
// pending since the last commit — NOT just once ever. FLUSH can fire
// more than once in a single open session (observed empirically: a
// truncate via Setattr triggers one, and the eventual close(2) triggers
// another), and a one-shot "already committed" latch would let the
// FIRST flush — which can land before all of a session's Write calls
// have happened — permanently block every later one from persisting
// anything, silently dropping data written after it. Guarding on "dirty
// since the last commit" instead of "ever committed" is what makes a
// second, third, etc. Flush in the same session correct: a no-op if
// nothing changed, a real commit if something did.
func (h *writeFileHandle) commitIfDirty(ctx context.Context) syscall.Errno {
	h.mu.Lock()
	defer h.mu.Unlock()
	if !h.dirty {
		return 0
	}
	h.dirty = false

	parent, name := h.target()
	wh, err := h.repo.CreateFile(parent, name)
	if err != nil {
		return errnoFor(err)
	}
	if _, err := wh.Write(h.buf); err != nil {
		return syscall.EIO
	}
	id, err := wh.Commit(ctx)
	if err != nil {
		return errnoFor(err)
	}
	if h.node != nil {
		if rec, err := h.repo.DB.GetInode(id); err == nil {
			h.node.setCached(rec)
		}
	}
	return 0
}

// Mount mounts r at mountpoint — read-only for an immutable repo,
// read-write otherwise — and blocks until unmounted. onMounted, if
// non-nil, is invoked with the *fuse.Server once mounted so the caller
// can wire up signal-triggered unmount.
func Mount(ctx context.Context, r *repo.Repo, mountpoint string, onMounted func(*fuse.Server)) error {
	_, rootRec, err := r.Resolve("/")
	if err != nil {
		return err
	}
	root := &Node{repo: r, ino: metadb.RootInode}
	root.setCached(rootRec)

	opts := []string{}
	if !r.Class.Mutable() {
		opts = append(opts, "ro")
	}
	// Let the kernel cache attrs and dentries for exactly this class's
	// D (repo.Class.KernelCacheTTL). Leaving these nil — go-fuse's
	// default — means a zero timeout, so every getattr and every lookup
	// becomes a userspace round trip even on an `immutable` mount where
	// nothing can change. That is not a stricter guarantee than §10
	// promises, just a slower way to provide the same one: measured at
	// 124µs per stat before this was set (pkg/fuseserver/bench_test.go).
	ttl := r.Class.KernelCacheTTL()
	server, err := fs.Mount(mountpoint, root, &fs.Options{
		EntryTimeout: &ttl,
		AttrTimeout:  &ttl,
		// NegativeTimeout is deliberately left at zero rather than set to
		// ttl. §10.4 validates a negative entry against a directory
		// version, which pkg/coherence implements and consults on every
		// Lookup; a kernel-side negative cache cannot participate in that
		// check, so a create-after-ENOENT on another holder would be
		// masked for up to ttl with no way to invalidate it. Paying the
		// round trip on misses is the cost of keeping §10.4's guarantee.
		MountOptions: fuse.MountOptions{
			FsName: "atlasfs",
			Name:   "atlasfs",
			// DirectMount: call mount(2) ourselves instead of shelling
			// out to fusermount, which is frequently absent (e.g.
			// minimal containers). Falls back to fusermount if
			// mount(2) fails, so this is strictly more portable, not
			// less, when we do have CAP_SYS_ADMIN.
			DirectMount: true,
			Options:     opts,
		},
	})
	if err != nil {
		return err
	}
	if onMounted != nil {
		onMounted(server)
	}
	server.Wait()
	return nil
}
