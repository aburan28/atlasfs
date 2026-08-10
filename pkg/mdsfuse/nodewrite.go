package mdsfuse

import (
	"context"
	"os"
	"sync"
	"syscall"

	"github.com/hanwen/go-fuse/v2/fs"
	"github.com/hanwen/go-fuse/v2/fuse"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/aburan28/atlasfs/pkg/mds"
	"github.com/aburan28/atlasfs/pkg/metadb"
	"github.com/aburan28/atlasfs/pkg/repo"
)

// callerOwner is the uid/gid to stamp on an inode this syscall is
// creating: the calling process's, which go-fuse carries on the request
// context (DESIGN.md §20). The authority cannot derive it — the RPC comes
// from the mount, not from the user — so the mount forwards it.
func callerOwner(ctx context.Context) (uid, gid uint32) {
	if c, ok := fuse.FromContext(ctx); ok {
		return c.Uid, c.Gid
	}
	return uint32(os.Getuid()), uint32(os.Getgid())
}

// The mutating half of the mount. Every operation here is a round trip
// to the authority, which is the point: this is where a write on one
// mount becomes an invalidation — or, for `posix`, a blocking recall —
// on every other mount holding the same inode.

var (
	_ fs.NodeCreater   = (*Node)(nil)
	_ fs.NodeUnlinker  = (*Node)(nil)
	_ fs.NodeMkdirer   = (*Node)(nil)
	_ fs.NodeRmdirer   = (*Node)(nil)
	_ fs.NodeSetattrer = (*Node)(nil)
	_ fs.NodeRenamer   = (*Node)(nil)
	_ fs.NodeSymlinker = (*Node)(nil)
	_ fs.NodeLinker    = (*Node)(nil)
	_ fs.NodeStatfser  = (*Node)(nil)
	_ fs.NodeMknoder   = (*Node)(nil)
)

// errnoFor maps an authority failure back to the errno a filesystem
// caller expects. Without this every failure would surface as EIO, which
// tells a user nothing about whether they hit a quota, a name collision,
// or a genuine fault.
//
// The authority tags its statuses with the exact errno (mds.ErrnoOf),
// because gRPC's code space is coarser than errno's: ENOTDIR, EISDIR and
// ENOTEMPTY share FailedPrecondition. The code-based switch below is the
// fallback for anything untagged — a transport failure, or a status that
// never passed through the authority's own error mapping.
func errnoFor(err error) syscall.Errno {
	if err == nil {
		return 0
	}
	if e, ok := mds.ErrnoOf(err); ok {
		return e
	}
	switch status.Code(err) {
	case codes.NotFound:
		return syscall.ENOENT
	case codes.AlreadyExists:
		return syscall.EEXIST
	case codes.FailedPrecondition:
		return syscall.ENOTEMPTY
	case codes.InvalidArgument:
		return syscall.EINVAL
	case codes.ResourceExhausted:
		return syscall.EDQUOT
	case codes.PermissionDenied:
		return syscall.EACCES
	default:
		return syscall.EIO
	}
}

// writeHandle is one open-for-write file. It holds the session that
// buffers content until flush.
type writeHandle struct {
	mu   sync.Mutex
	sess *writeSession
	node *Node
}

var (
	_ fs.FileWriter   = (*writeHandle)(nil)
	_ fs.FileReader   = (*writeHandle)(nil)
	_ fs.FileFlusher  = (*writeHandle)(nil)
	_ fs.FileReleaser = (*writeHandle)(nil)
	_ fs.FileFsyncer  = (*writeHandle)(nil)
)

// Fsync commits the buffered content — DESIGN.md §16.2's "durable in the
// home region": chunks in the object store, manifest committed at the
// authority. It never waits on cross-region replication, which §16.2
// reserves for an explicit publish.
//
// Without it the kernel gets ENOSYS and stops sending FSYNC, so a
// process that wrote, fsynced and then died would lose the write despite
// fsync having returned success.
func (h *writeHandle) Fsync(ctx context.Context, flags uint32) syscall.Errno {
	h.mu.Lock()
	defer h.mu.Unlock()
	return errnoFor(h.sess.commit(ctx))
}

func (h *writeHandle) Write(ctx context.Context, data []byte, off int64) (uint32, syscall.Errno) {
	h.mu.Lock()
	defer h.mu.Unlock()
	n, err := h.sess.writeAt(data, off)
	if err != nil {
		return 0, syscall.EIO
	}
	return uint32(n), 0
}

// Read serves from the in-flight buffer so a session sees its own
// uncommitted writes — read-your-own-writes within one open, which a
// reader built on the last-committed record would not give.
func (h *writeHandle) Read(ctx context.Context, dest []byte, off int64) (fuse.ReadResult, syscall.Errno) {
	h.mu.Lock()
	defer h.mu.Unlock()
	buf := h.sess.buf.Bytes()
	if off >= int64(len(buf)) {
		return fuse.ReadResultData(nil), 0
	}
	end := off + int64(len(dest))
	if end > int64(len(buf)) {
		end = int64(len(buf))
	}
	return fuse.ReadResultData(buf[off:end]), 0
}

// Flush is where the commit happens, not Release. RELEASE is documented
// as asynchronous relative to close(2) — the kernel does not wait for it
// — while FLUSH is sent synchronously as part of close(2) and the caller
// does block on it. Committing only on Release would make a write racily
// invisible to whatever the process does next.
func (h *writeHandle) Flush(ctx context.Context) syscall.Errno {
	h.mu.Lock()
	defer h.mu.Unlock()
	if err := h.sess.commit(ctx); err != nil {
		return errnoFor(err)
	}
	return 0
}

func (h *writeHandle) Release(ctx context.Context) syscall.Errno {
	h.mu.Lock()
	defer h.mu.Unlock()
	err := h.sess.commit(ctx)
	h.sess.closed = true
	if h.node != nil {
		h.node.clearActiveWrite(h)
	}
	return errnoFor(err)
}

func (n *Node) clearActiveWrite(h *writeHandle) {
	n.mu.Lock()
	if n.activeWrite == h {
		n.activeWrite = nil
	}
	n.mu.Unlock()
}

func (n *Node) Create(ctx context.Context, name string, flags uint32, mode uint32, out *fuse.EntryOut) (*fs.Inode, fs.FileHandle, uint32, syscall.Errno) {
	if n.cfg.ReadOnly {
		return nil, nil, 0, syscall.EROFS
	}
	// The kernel has already applied the caller's umask, so mode is what
	// the file should end up with.
	uid, gid := callerOwner(ctx)
	h := &writeHandle{sess: &writeSession{cfg: n.cfg, dir: n.ino, name: name, mode: mode, uid: uid, gid: gid}}
	// Commit immediately so the name exists as soon as create(2) returns,
	// which is what a caller that stats it straight afterwards expects.
	// The empty record is replaced on the first real flush.
	h.sess.dirty = true
	if err := h.sess.commit(ctx); err != nil {
		return nil, nil, 0, errnoFor(err)
	}
	resp, err := n.cfg.Client.Lookup(ctx, n.ino, name)
	if err != nil || !resp.Found {
		return nil, nil, 0, syscall.EIO
	}
	child := &Node{cfg: n.cfg, ino: resp.Inode, rec: resp.Record, parent: n.ino, name: name}
	child.activeWrite = h
	h.node = child
	h.sess.node = child
	fillAttrMode(resp.Record, n.cfg.ReadOnly, &out.Attr)
	inode := n.NewInode(ctx, child, fs.StableAttr{Mode: syscall.S_IFREG, Ino: uint64(resp.Inode)})
	return inode, h, 0, 0
}

func (n *Node) Unlink(ctx context.Context, name string) syscall.Errno {
	if n.cfg.ReadOnly {
		return syscall.EROFS
	}
	return errnoFor(n.cfg.Client.Unlink(ctx, n.ino, name))
}

func (n *Node) Mkdir(ctx context.Context, name string, mode uint32, out *fuse.EntryOut) (*fs.Inode, syscall.Errno) {
	if n.cfg.ReadOnly {
		return nil, syscall.EROFS
	}
	uid, gid := callerOwner(ctx)
	id, err := n.cfg.Client.Mkdir(ctx, n.ino, name, mode, uid, gid)
	if err != nil {
		return nil, errnoFor(err)
	}
	rec, err := n.cfg.Client.GetInode(ctx, id)
	if err != nil {
		return nil, syscall.EIO
	}
	child := &Node{cfg: n.cfg, ino: id, rec: rec, parent: n.ino, name: name}
	fillAttrMode(rec, n.cfg.ReadOnly, &out.Attr)
	return n.NewInode(ctx, child, fs.StableAttr{Mode: syscall.S_IFDIR, Ino: uint64(id)}), 0
}

func (n *Node) Rmdir(ctx context.Context, name string) syscall.Errno {
	if n.cfg.ReadOnly {
		return syscall.EROFS
	}
	return errnoFor(n.cfg.Client.Rmdir(ctx, n.ino, name))
}

// Rename moves name out of this directory and into newParent under
// newName. Both directories' versions bump at the authority, so a holder
// caching either listing — or a negative entry for newName — is told in
// the same round trip that performs the move.
func (n *Node) Rename(ctx context.Context, name string, newParent fs.InodeEmbedder, newName string, flags uint32) syscall.Errno {
	if n.cfg.ReadOnly {
		return syscall.EROFS
	}
	if flags != 0 {
		// renameat2's RENAME_EXCHANGE and RENAME_NOREPLACE both need
		// atomicity the authority's Rename does not offer, and silently
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
	if err := n.cfg.Client.Rename(ctx, n.ino, name, dst.ino, newName); err != nil {
		return errnoFor(err)
	}
	rebind(child, dst.ino, newName)
	return 0
}

// rebind points a moved Node at its new dentry. The (parent, name) pair
// is what a commit rebinds — Commit takes a dentry, not an inode ID — so
// leaving it on the pre-rename name would make the next write to an
// already-open file recreate the name the rename just removed.
func rebind(child *fs.Inode, newParent metadb.InodeID, newName string) {
	if child == nil {
		return
	}
	cn, ok := child.Operations().(*Node)
	if !ok {
		return
	}
	cn.mu.Lock()
	cn.parent, cn.name = newParent, newName
	cn.mu.Unlock()
}

// binding reads the dentry this node is currently bound to. Rename
// mutates it under mu, so a write path that captured it earlier must
// re-read rather than close over the fields.
func (n *Node) binding() (metadb.InodeID, string) {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.parent, n.name
}

// Statfs answers df from the authority's quota — the only capacity
// numbers that mean anything here (see pkg/repo/statfs.go).
func (n *Node) Statfs(ctx context.Context, out *fuse.StatfsOut) syscall.Errno {
	resp, err := n.cfg.Client.Statfs(ctx)
	if err != nil {
		return errnoFor(err)
	}
	info := repo.StatfsInfoFrom(resp.BytesLimit, resp.InodesLimit, resp.BytesUsed, resp.InodesUsed)
	out.Bsize, out.Frsize = statfsBlockSize, statfsBlockSize
	out.Blocks = info.Total / statfsBlockSize
	free := info.Free() / statfsBlockSize
	out.Bfree, out.Bavail = free, free
	out.Files = info.Files
	out.Ffree = info.FilesFree()
	out.NameLen = 255
	return 0
}

// statfsBlockSize is the unit df divides by — a reporting unit, not an
// allocation unit: content-addressed chunks live in packed containers
// (§5.4), so there is no on-disk block to match.
const statfsBlockSize = 4096

// Mknod creates a FIFO, socket or device node through the authority.
// S_IFREG is refused: a regular file arrives through Create, which has a
// content pointer and a write handle this path would not give it.
func (n *Node) Mknod(ctx context.Context, name string, mode, rdev uint32, out *fuse.EntryOut) (*fs.Inode, syscall.Errno) {
	if n.cfg.ReadOnly {
		return nil, syscall.EROFS
	}
	typ := mode & syscall.S_IFMT
	switch typ {
	case syscall.S_IFIFO, syscall.S_IFSOCK, syscall.S_IFCHR, syscall.S_IFBLK:
	default:
		return nil, syscall.EINVAL
	}
	uid, gid := callerOwner(ctx)
	id, err := n.cfg.Client.Mknod(ctx, n.ino, name, typ, rdev, mode&0o7777, uid, gid)
	if err != nil {
		return nil, errnoFor(err)
	}
	rec, err := n.cfg.Client.GetInode(ctx, id)
	if err != nil {
		return nil, syscall.EIO
	}
	child := &Node{cfg: n.cfg, ino: id, rec: rec, parent: n.ino, name: name}
	fillAttrMode(rec, n.cfg.ReadOnly, &out.Attr)
	return n.NewInode(ctx, child, fs.StableAttr{Mode: typ, Ino: uint64(id)}), 0
}

// Link binds name in this directory to an already-existing inode. It
// returns the existing node rather than a fresh one: two names for one
// inode is the point, and a second node would give the kernel two inode
// identities for the same file.
func (n *Node) Link(ctx context.Context, target fs.InodeEmbedder, name string, out *fuse.EntryOut) (*fs.Inode, syscall.Errno) {
	if n.cfg.ReadOnly {
		return nil, syscall.EROFS
	}
	tn, ok := target.(*Node)
	if !ok {
		return nil, syscall.EXDEV
	}
	rec, err := n.cfg.Client.Link(ctx, n.ino, name, tn.ino)
	if err != nil {
		return nil, errnoFor(err)
	}
	fillAttrMode(rec, n.cfg.ReadOnly, &out.Attr)
	return tn.EmbeddedInode(), 0
}

func (n *Node) Symlink(ctx context.Context, target, name string, out *fuse.EntryOut) (*fs.Inode, syscall.Errno) {
	if n.cfg.ReadOnly {
		return nil, syscall.EROFS
	}
	uid, gid := callerOwner(ctx)
	id, err := n.cfg.Client.Symlink(ctx, n.ino, name, target, uid, gid)
	if err != nil {
		return nil, errnoFor(err)
	}
	rec, err := n.cfg.Client.GetInode(ctx, id)
	if err != nil {
		return nil, syscall.EIO
	}
	child := &Node{cfg: n.cfg, ino: id, rec: rec, parent: n.ino, name: name}
	fillAttrMode(rec, n.cfg.ReadOnly, &out.Attr)
	return n.NewInode(ctx, child, fs.StableAttr{Mode: syscall.S_IFLNK, Ino: uint64(id)}), 0
}

// Setattr handles truncate. It reads the active write handle off the
// Node rather than trusting the f parameter: go-fuse/the kernel does not
// reliably pass the already-open handle for O_TRUNC-on-open or
// ftruncate(fd) — observed nil in both cases on the in-process mount —
// and resizing the wrong buffer silently corrupts the file.
func (n *Node) Setattr(ctx context.Context, f fs.FileHandle, in *fuse.SetAttrIn, out *fuse.AttrOut) syscall.Errno {
	if n.cfg.ReadOnly {
		return syscall.EROFS
	}
	// Mode and mtime are pure metadata and go straight to the authority.
	// Ownership does not: DESIGN.md §20's uid/gid model is a later phase,
	// and chown is accepted-and-ignored rather than refused because cp,
	// rsync and tar all restore ownership unconditionally.
	var mut metadb.AttrMutation
	if mode, ok := in.GetMode(); ok {
		mut.Mode = &mode
	}
	if uid, ok := in.GetUID(); ok {
		mut.Uid = &uid
	}
	if gid, ok := in.GetGID(); ok {
		mut.Gid = &gid
	}
	if mtime, ok := in.GetMTime(); ok {
		mut.MTime = &mtime
	}
	if mut.Mode != nil || mut.Uid != nil || mut.Gid != nil || mut.MTime != nil {
		rec, err := n.cfg.Client.SetAttr(ctx, n.ino, mut)
		if err != nil {
			return errnoFor(err)
		}
		fillAttrMode(rec, n.cfg.ReadOnly, &out.Attr)
	}

	size, ok := in.GetSize()
	if !ok {
		if mut.Mode == nil && mut.Uid == nil && mut.Gid == nil && mut.MTime == nil {
			rec, errno := n.current(ctx)
			if errno != 0 {
				return errno
			}
			fillAttrMode(rec, n.cfg.ReadOnly, &out.Attr)
		}
		return 0
	}

	n.mu.Lock()
	h := n.activeWrite
	n.mu.Unlock()

	if h != nil {
		h.mu.Lock()
		h.sess.resize(int64(size))
		err := h.sess.commit(ctx)
		h.mu.Unlock()
		if err != nil {
			return errnoFor(err)
		}
	} else {
		// Truncate with no open write handle: read the current content,
		// resize it, and commit as a fresh session.
		rec, errno := n.current(ctx)
		if errno != 0 {
			return errno
		}
		dir, name := n.binding()
		if name == "" {
			return syscall.EINVAL // root or an inode we cannot re-bind
		}
		sess := &writeSession{cfg: n.cfg, dir: dir, name: name}
		rd, err := newReader(ctx, n.cfg, rec)
		if err != nil {
			return syscall.EIO
		}
		cur := make([]byte, rec.Size)
		if rec.Size > 0 {
			if _, err := rd.ReadAt(ctx, cur, 0); err != nil {
				return syscall.EIO
			}
		}
		sess.buf.Write(cur)
		sess.resize(int64(size))
		if err := sess.commit(ctx); err != nil {
			return errnoFor(err)
		}
	}

	rec, errno := n.current(ctx)
	if errno != 0 {
		return errno
	}
	fillAttrMode(rec, n.cfg.ReadOnly, &out.Attr)
	return 0
}

// openForWrite starts a write session against an existing file,
// preloading its current content so a partial overwrite does not
// truncate the rest.
func (n *Node) openForWrite(ctx context.Context, truncate bool) (fs.FileHandle, syscall.Errno) {
	dir, name := n.binding()
	if name == "" {
		return nil, syscall.EINVAL
	}
	sess := &writeSession{cfg: n.cfg, dir: dir, name: name, node: n}
	if !truncate {
		rec, errno := n.current(ctx)
		if errno != 0 {
			return nil, errno
		}
		if rec.Size > 0 {
			rd, err := newReader(ctx, n.cfg, rec)
			if err != nil {
				return nil, syscall.EIO
			}
			cur := make([]byte, rec.Size)
			if _, err := rd.ReadAt(ctx, cur, 0); err != nil {
				return nil, syscall.EIO
			}
			sess.buf.Write(cur)
		}
	} else {
		sess.dirty = true
	}
	h := &writeHandle{sess: sess, node: n}
	n.mu.Lock()
	n.activeWrite = h
	n.mu.Unlock()
	return h, 0
}
