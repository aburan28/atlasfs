// Package fuseserver mounts a repo.Repo read-only over FUSE. This is the
// `immutable`-class-only read path from DESIGN.md §21/§8: because the
// subtree is immutable, there is no lease protocol to implement here —
// every dentry and inode is fetched straight from metadb and is correct
// for the life of the mount by construction.
//
// This is a plain go-fuse mount (splice/passthrough tuning from
// DESIGN.md §21.2 is not implemented here — that is a later-phase
// performance pass, not a correctness requirement for the vertical
// slice).
package fuseserver

import (
	"context"
	"io"
	"syscall"

	"github.com/hanwen/go-fuse/v2/fs"
	"github.com/hanwen/go-fuse/v2/fuse"

	"github.com/aburan28/atlasfs/pkg/metadb"
	"github.com/aburan28/atlasfs/pkg/repo"
)

// Node is one FUSE inode, backed by an AtlasFS inode in a Repo.
type Node struct {
	fs.Inode
	repo *repo.Repo
	ino  metadb.InodeID
	rec  metadb.InodeRecord
}

var (
	_ fs.NodeLookuper  = (*Node)(nil)
	_ fs.NodeReaddirer = (*Node)(nil)
	_ fs.NodeGetattrer = (*Node)(nil)
	_ fs.NodeOpener    = (*Node)(nil)
)

func (n *Node) Getattr(ctx context.Context, f fs.FileHandle, out *fuse.AttrOut) syscall.Errno {
	n.fillAttrOut(&out.Attr)
	return 0
}

func (n *Node) fillAttrOut(out *fuse.Attr) {
	out.Size = n.rec.Size
	sec := uint64(0)
	if !n.rec.MTime.IsZero() {
		sec = uint64(n.rec.MTime.Unix())
	}
	out.Mtime = sec
	out.Atime = sec
	out.Ctime = sec
	if n.rec.IsDir {
		out.Mode = syscall.S_IFDIR | 0o555
		out.Nlink = 2
	} else {
		out.Mode = syscall.S_IFREG | 0o444
		out.Nlink = 1
	}
}

func (n *Node) Lookup(ctx context.Context, name string, out *fuse.EntryOut) (*fs.Inode, syscall.Errno) {
	if !n.rec.IsDir {
		return nil, syscall.ENOTDIR
	}
	childIno, err := n.repo.DB.Lookup(n.ino, name)
	if err != nil {
		return nil, syscall.ENOENT
	}
	rec, err := n.repo.DB.GetInode(childIno)
	if err != nil {
		return nil, syscall.EIO
	}
	child := &Node{repo: n.repo, ino: childIno, rec: rec}
	child.fillAttrOut(&out.Attr)
	mode := uint32(syscall.S_IFREG)
	if rec.IsDir {
		mode = syscall.S_IFDIR
	}
	stable := fs.StableAttr{Mode: mode, Ino: uint64(childIno)}
	inode := n.NewInode(ctx, child, stable)
	return inode, 0
}

func (n *Node) Readdir(ctx context.Context) (fs.DirStream, syscall.Errno) {
	if !n.rec.IsDir {
		return nil, syscall.ENOTDIR
	}
	entries, err := n.repo.Readdir(n.ino)
	if err != nil {
		return nil, syscall.EIO
	}
	list := make([]fuse.DirEntry, 0, len(entries))
	for _, e := range entries {
		rec, err := n.repo.DB.GetInode(e.Inode)
		if err != nil {
			continue
		}
		mode := uint32(syscall.S_IFREG)
		if rec.IsDir {
			mode = syscall.S_IFDIR
		}
		list = append(list, fuse.DirEntry{Name: e.Name, Ino: uint64(e.Inode), Mode: mode})
	}
	return fs.NewListDirStream(list), 0
}

func (n *Node) Open(ctx context.Context, flags uint32) (fs.FileHandle, uint32, syscall.Errno) {
	if n.rec.IsDir {
		return nil, 0, syscall.EISDIR
	}
	// Read-only mount: writes are rejected before they ever reach a
	// FileReader (DESIGN.md §8's `immutable` class — mutation is
	// EROFS by definition, not merely unimplemented here).
	if flags&(syscall.O_WRONLY|syscall.O_RDWR) != 0 {
		return nil, 0, syscall.EROFS
	}
	fr, err := n.repo.OpenFile(ctx, n.rec)
	if err != nil {
		return nil, 0, syscall.EIO
	}
	return &fileHandle{fr: fr}, fuse.FOPEN_KEEP_CACHE, 0
}

type fileHandle struct {
	fr *repo.FileReader
}

var _ fs.FileReader = (*fileHandle)(nil)

func (h *fileHandle) Read(ctx context.Context, dest []byte, off int64) (fuse.ReadResult, syscall.Errno) {
	n, err := h.fr.ReadAt(dest, off)
	if err != nil && err != io.EOF {
		return nil, syscall.EIO
	}
	return fuse.ReadResultData(dest[:n]), 0
}

// Mount mounts r read-only at mountpoint and blocks until unmounted.
// unmount(), if non-nil, is invoked with the *fuse.Server once mounted
// so the caller can wire up signal-triggered unmount.
func Mount(ctx context.Context, r *repo.Repo, mountpoint string, onMounted func(*fuse.Server)) error {
	_, rootRec, err := r.Resolve("/")
	if err != nil {
		return err
	}
	root := &Node{repo: r, ino: metadb.RootInode, rec: rootRec}

	server, err := fs.Mount(mountpoint, root, &fs.Options{
		MountOptions: fuse.MountOptions{
			FsName: "atlasfs",
			Name:   "atlasfs",
			// DirectMount: call mount(2) ourselves instead of shelling
			// out to fusermount, which is frequently absent (e.g.
			// minimal containers). Falls back to fusermount if
			// mount(2) fails, so this is strictly more portable, not
			// less, when we do have CAP_SYS_ADMIN.
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
