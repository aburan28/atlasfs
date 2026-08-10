// Package mdsfuse mounts a subtree whose metadata comes from a remote
// metadata authority (pkg/mds) over gRPC, while its file bytes come
// straight from object storage (pkg/store).
//
// This is the piece that turns the client/authority split into an actual
// filesystem. Before it, pkg/mds had real remote holders and working
// §10.6 recall — but only its own tests consumed them, so `posix` was a
// library capability rather than something you could mount. Here a
// mountpoint is a real lease holder: the kernel's reads become RPCs to
// the authority, the authority's pushes invalidate this client's cache,
// and a recall against this mount is a recall against a genuinely
// separate process.
//
// # The split, preserved through the mount
//
// DESIGN.md §11 has clients read chunk data from object storage
// directly, never through the metadata authority. That holds here:
//
//	kernel read -> Node -> mds.Client   (inode, dentry, chunk locator)
//	                    -> store.Backend (the bytes themselves)
//
// The authority is never in the data path, which is why a mount can sit
// behind a Dragonfly peer (pkg/store/dragonfly) for the bytes while
// still talking to one authority for the metadata.
//
// # Scope: read-only, deliberately
//
// This mount serves lookup, getattr, readdir, readlink, open, and read.
// It does not write. That is a real limit and not a stub: the write path
// through an authority is DESIGN.md §16.1's upload-then-commit — the
// client chunks and uploads to the backend itself, then calls Commit —
// and it needs its own packer, quota round trip, and failure handling on
// this side of the RPC. pkg/fuseserver's in-process mount remains the
// writable one. Unifying the two mounts behind one node implementation
// is follow-up work; duplicating only the read half was the smaller,
// safer increment.
package mdsfuse

import (
	"context"
	"fmt"
	"io"
	"sort"
	"sync"
	"syscall"
	"time"

	"github.com/hanwen/go-fuse/v2/fs"
	"github.com/hanwen/go-fuse/v2/fuse"

	"github.com/aburan28/atlasfs/pkg/chunk"
	"github.com/aburan28/atlasfs/pkg/manifest"
	"github.com/aburan28/atlasfs/pkg/mds"
	"github.com/aburan28/atlasfs/pkg/metadb"
	"github.com/aburan28/atlasfs/pkg/pack"
	"github.com/aburan28/atlasfs/pkg/store"
)

// Config describes what to mount.
type Config struct {
	Client  *mds.Client
	Backend store.Backend

	// KernelCacheTTL is how long the kernel may trust an attr or dentry,
	// and it must be the class's own D (repo.Class.KernelCacheTTL). For
	// `posix` that is zero: the kernel's cache cannot participate in
	// §10.6's recall, so letting it answer a getattr would serve a copy
	// the recall protocol believes was surrendered.
	KernelCacheTTL time.Duration
}

// Mount mounts cfg at mountpoint and blocks until unmounted.
func Mount(ctx context.Context, cfg Config, mountpoint string, onMounted func(*fuse.Server)) error {
	if cfg.Client == nil || cfg.Backend == nil {
		return fmt.Errorf("mdsfuse: Client and Backend are both required")
	}
	rootRec, err := cfg.Client.GetInode(ctx, metadb.RootInode)
	if err != nil {
		return fmt.Errorf("mdsfuse: read root inode from authority: %w", err)
	}
	root := &Node{cfg: cfg, ino: metadb.RootInode, rec: rootRec}

	ttl := cfg.KernelCacheTTL
	server, err := fs.Mount(mountpoint, root, &fs.Options{
		EntryTimeout: &ttl,
		AttrTimeout:  &ttl,
		// NegativeTimeout stays zero: §10.4 validates a negative entry
		// against a directory version the kernel cannot see, so a
		// kernel-side miss cache would mask a create on another holder
		// with no way to invalidate it.
		MountOptions: fuse.MountOptions{
			FsName:      "atlasfs-mds",
			Name:        "atlasfs",
			DirectMount: true,
			Options:     []string{"ro"},
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

// Node is one inode, resolved through the authority.
//
// It holds no lease state of its own: mds.Client already caches records
// under their leases and drops them on a push, so a second cache here
// would be a second thing to invalidate and a second way to be wrong.
// Node keeps only the record it was constructed with, and asks the
// client again whenever it needs current attrs — which is a cache hit
// inside the client whenever the lease still holds.
type Node struct {
	fs.Inode
	cfg Config
	ino metadb.InodeID
	rec metadb.InodeRecord
}

var (
	_ fs.NodeLookuper   = (*Node)(nil)
	_ fs.NodeGetattrer  = (*Node)(nil)
	_ fs.NodeReaddirer  = (*Node)(nil)
	_ fs.NodeOpener     = (*Node)(nil)
	_ fs.NodeReadlinker = (*Node)(nil)
)

func (n *Node) current(ctx context.Context) (metadb.InodeRecord, syscall.Errno) {
	rec, err := n.cfg.Client.GetInode(ctx, n.ino)
	if err != nil {
		return metadb.InodeRecord{}, syscall.EIO
	}
	return rec, 0
}

func fillAttr(rec metadb.InodeRecord, out *fuse.Attr) {
	out.Size = rec.Size
	sec := uint64(0)
	if !rec.MTime.IsZero() {
		sec = uint64(rec.MTime.Unix())
	}
	out.Mtime, out.Atime, out.Ctime = sec, sec, sec
	switch {
	case rec.IsDir:
		out.Mode, out.Nlink = syscall.S_IFDIR|0o555, 2
	case rec.IsSymlink:
		out.Mode, out.Nlink = syscall.S_IFLNK|0o777, 1
	default:
		// 0o444 throughout: this mount is read-only, so advertising write
		// bits would only invite an EROFS later.
		out.Mode, out.Nlink = syscall.S_IFREG|0o444, 1
	}
}

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

func (n *Node) Getattr(ctx context.Context, f fs.FileHandle, out *fuse.AttrOut) syscall.Errno {
	rec, errno := n.current(ctx)
	if errno != 0 {
		return errno
	}
	fillAttr(rec, &out.Attr)
	return 0
}

func (n *Node) Lookup(ctx context.Context, name string, out *fuse.EntryOut) (*fs.Inode, syscall.Errno) {
	resp, err := n.cfg.Client.Lookup(ctx, n.ino, name)
	if err != nil {
		return nil, syscall.EIO
	}
	if !resp.Found {
		return nil, syscall.ENOENT
	}
	child := &Node{cfg: n.cfg, ino: resp.Inode, rec: resp.Record}
	fillAttr(resp.Record, &out.Attr)
	return n.NewInode(ctx, child, fs.StableAttr{
		Mode: direntMode(resp.Record),
		Ino:  uint64(resp.Inode),
	}), 0
}

func (n *Node) Readdir(ctx context.Context) (fs.DirStream, syscall.Errno) {
	entries, err := n.cfg.Client.Readdir(ctx, n.ino)
	if err != nil {
		return nil, syscall.EIO
	}
	out := make([]fuse.DirEntry, 0, len(entries))
	for _, e := range entries {
		rec, err := n.cfg.Client.GetInode(ctx, e.Inode)
		if err != nil {
			return nil, syscall.EIO
		}
		out = append(out, fuse.DirEntry{
			Name: e.Name,
			Ino:  uint64(e.Inode),
			Mode: direntMode(rec),
		})
	}
	return fs.NewListDirStream(out), 0
}

func (n *Node) Readlink(ctx context.Context) ([]byte, syscall.Errno) {
	rec, errno := n.current(ctx)
	if errno != 0 {
		return nil, errno
	}
	if !rec.IsSymlink {
		return nil, syscall.EINVAL
	}
	return []byte(rec.SymlinkTarget), 0
}

func (n *Node) Open(ctx context.Context, flags uint32) (fs.FileHandle, uint32, syscall.Errno) {
	if flags&(syscall.O_WRONLY|syscall.O_RDWR|syscall.O_APPEND|syscall.O_CREAT|syscall.O_TRUNC) != 0 {
		return nil, 0, syscall.EROFS
	}
	rec, errno := n.current(ctx)
	if errno != 0 {
		return nil, 0, errno
	}
	rd, err := newReader(ctx, n.cfg, rec)
	if err != nil {
		return nil, 0, syscall.EIO
	}
	return &fileHandle{rd: rd}, 0, 0
}

type fileHandle struct{ rd *reader }

var _ fs.FileReader = (*fileHandle)(nil)

func (h *fileHandle) Read(ctx context.Context, dest []byte, off int64) (fuse.ReadResult, syscall.Errno) {
	n, err := h.rd.ReadAt(ctx, dest, off)
	if err != nil && err != io.EOF {
		return nil, syscall.EIO
	}
	return fuse.ReadResultData(dest[:n]), 0
}

// reader turns an inode record into bytes: chunk IDs come from the
// manifest (fetched from the backend, since a manifest is a
// content-addressed blob per §5.3, not a metadata row), locators come
// from the authority, and the bytes come from the backend.
type reader struct {
	cfg  Config
	size int64

	hasInline bool
	inline    chunk.ID

	m       *manifest.Manifest
	offsets []int64

	mu    sync.Mutex
	cache map[chunk.ID][]byte
	order []chunk.ID
}

const readerCacheEntries = 16

func newReader(ctx context.Context, cfg Config, rec metadb.InodeRecord) (*reader, error) {
	r := &reader{cfg: cfg, size: int64(rec.Size), cache: map[chunk.ID][]byte{}}
	switch {
	case rec.HasInline:
		r.hasInline, r.inline = true, rec.InlineChunk
	case rec.HasManifest:
		m, err := fetchManifest(ctx, cfg.Backend, rec.ManifestID)
		if err != nil {
			return nil, err
		}
		r.m = m
		r.offsets = make([]int64, len(m.Entries))
		var off int64
		for i, e := range m.Entries {
			r.offsets[i] = off
			off += int64(e.Length)
		}
	}
	return r, nil
}

func fetchManifest(ctx context.Context, b store.Backend, id manifest.ID) (*manifest.Manifest, error) {
	h := id.String()
	rc, err := b.Get(ctx, fmt.Sprintf("atlas/m/%s/%s", h[:2], h), 0, -1)
	if err != nil {
		return nil, err
	}
	defer rc.Close()
	data, err := io.ReadAll(rc)
	if err != nil {
		return nil, err
	}
	return manifest.DecodeBytes(data)
}

func (r *reader) chunkBytes(ctx context.Context, id chunk.ID) ([]byte, error) {
	r.mu.Lock()
	if b, ok := r.cache[id]; ok {
		r.mu.Unlock()
		return b, nil
	}
	r.mu.Unlock()

	loc, found, err := r.cfg.Client.GetLocator(ctx, id)
	if err != nil {
		return nil, err
	}
	if !found {
		return nil, fmt.Errorf("mdsfuse: chunk %s has no locator", id)
	}
	buf := make([]byte, loc.Length)
	// Hash-verified on arrival (DESIGN.md §24.4) — the authority told us
	// where to look, but it is the content hash that says we got the
	// right bytes.
	if err := pack.FetchInto(ctx, r.cfg.Backend, id, loc, buf); err != nil {
		return nil, err
	}

	r.mu.Lock()
	r.cache[id] = buf
	r.order = append(r.order, id)
	if len(r.order) > readerCacheEntries {
		delete(r.cache, r.order[0])
		r.order = r.order[1:]
	}
	r.mu.Unlock()
	return buf, nil
}

func (r *reader) ReadAt(ctx context.Context, p []byte, off int64) (int, error) {
	if off < 0 {
		return 0, fmt.Errorf("mdsfuse: negative offset")
	}
	if off >= r.size {
		return 0, io.EOF
	}
	if r.hasInline {
		data, err := r.chunkBytes(ctx, r.inline)
		if err != nil {
			return 0, err
		}
		if off >= int64(len(data)) {
			return 0, io.EOF
		}
		return copy(p, data[off:]), nil
	}
	if r.m == nil {
		return 0, io.EOF
	}

	total := 0
	for total < len(p) && off < r.size {
		idx := sort.Search(len(r.offsets), func(i int) bool {
			return r.offsets[i]+int64(r.m.Entries[i].Length) > off
		})
		if idx >= len(r.offsets) {
			break
		}
		entry := r.m.Entries[idx]
		data, err := r.chunkBytes(ctx, entry.ChunkID)
		if err != nil {
			return total, err
		}
		n := copy(p[total:], data[off-r.offsets[idx]:])
		if n == 0 {
			break
		}
		total += n
		off += int64(n)
	}
	if total == 0 {
		return 0, io.EOF
	}
	return total, nil
}
