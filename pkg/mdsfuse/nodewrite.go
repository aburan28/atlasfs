package mdsfuse

import (
	"context"
	"sync"
	"syscall"

	"github.com/hanwen/go-fuse/v2/fs"
	"github.com/hanwen/go-fuse/v2/fuse"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

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
)

// errnoFor maps the authority's gRPC status codes back to the errnos a
// filesystem caller expects. Without this every failure would surface as
// EIO, which tells a user nothing about whether they hit a quota, a
// name collision, or a genuine fault.
func errnoFor(err error) syscall.Errno {
	if err == nil {
		return 0
	}
	switch status.Code(err) {
	case codes.NotFound:
		return syscall.ENOENT
	case codes.AlreadyExists:
		return syscall.EEXIST
	case codes.FailedPrecondition:
		// Covers both "not a directory" and "directory not empty"; the
		// latter is the common one and ENOTEMPTY is what shells expect
		// from rmdir.
		return syscall.ENOTEMPTY
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
)

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
	h := &writeHandle{sess: &writeSession{cfg: n.cfg, dir: n.ino, name: name}}
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
	id, err := n.cfg.Client.Mkdir(ctx, n.ino, name)
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

// Setattr handles truncate. It reads the active write handle off the
// Node rather than trusting the f parameter: go-fuse/the kernel does not
// reliably pass the already-open handle for O_TRUNC-on-open or
// ftruncate(fd) — observed nil in both cases on the in-process mount —
// and resizing the wrong buffer silently corrupts the file.
func (n *Node) Setattr(ctx context.Context, f fs.FileHandle, in *fuse.SetAttrIn, out *fuse.AttrOut) syscall.Errno {
	if n.cfg.ReadOnly {
		return syscall.EROFS
	}
	size, ok := in.GetSize()
	if !ok {
		// Nothing this mount tracks (mode/uid/gid/times are not stored
		// per-inode here beyond mtime), so report current attrs and move
		// on rather than failing a chmod a caller may not even care about.
		rec, errno := n.current(ctx)
		if errno != 0 {
			return errno
		}
		fillAttrMode(rec, n.cfg.ReadOnly, &out.Attr)
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
		sess := &writeSession{cfg: n.cfg, dir: n.parent, name: n.name}
		if n.name == "" {
			return syscall.EINVAL // root or an inode we cannot re-bind
		}
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
	if n.name == "" {
		return nil, syscall.EINVAL
	}
	sess := &writeSession{cfg: n.cfg, dir: n.parent, name: n.name}
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
