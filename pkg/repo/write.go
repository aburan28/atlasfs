// Mutable write path for non-immutable repos (DESIGN.md §16.1, §8).
//
// Writes buffer in memory and commit as a unit on Close — the same
// buffer-then-chunk-on-close model §16.1 describes for the general
// write path, not an in-place random-access byte-range writer. That
// scope is deliberate: arbitrary O_RDWR mid-file writes, O_APPEND, and
// byte-range locks are §16.3/§17's `posix`-class territory, explicitly
// out of scope for this build (see pkg/coherence's package doc).
//
// Overwriting an existing name reuses that name's existing inode ID and
// swaps its content pointer in one metadb transaction — DESIGN.md
// §16.1's "the manifest swap is atomic: a reader sees either the old
// manifest or the new one, never a partial file" — rather than
// allocating a fresh inode per write, which is what makes a cached
// lookup-by-inode-ID (pkg/coherence's per-object leases) a meaningful
// thing to invalidate on overwrite instead of silently going stale.
package repo

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/aburan28/atlasfs/pkg/metadb"
)

// Sentinel errors for the mutable write path, distinguished so a caller
// like pkg/fuseserver can map each one to the right errno instead of a
// generic EIO for everything.
var (
	ErrReadOnly      = errors.New("repo: repo is immutable-class, read-only")
	ErrExists        = errors.New("repo: name already exists")
	ErrIsDirectory   = errors.New("repo: is a directory")
	ErrNotDir        = errors.New("repo: not a directory")
	ErrNotEmpty      = errors.New("repo: directory not empty")
	ErrQuotaExceeded = errors.New("repo: quota exceeded")
)

func InodeCoherenceKey(id metadb.InodeID) string { return fmt.Sprintf("inode:%d", id) }
func DirCoherenceKey(id metadb.InodeID) string   { return fmt.Sprintf("dir:%d", id) }

// SetQuota sets this repo's byte and inode limits (DESIGN.md §18.3). A
// zero value means unlimited for that dimension. This build has exactly
// one subtree per repo (see pkg/metadb's package doc), so there is one
// quota scope rather than a per-subtree one.
func (r *Repo) SetQuota(bytesLimit, inodesLimit uint64) error {
	return r.DB.SetQuotaLimits(bytesLimit, inodesLimit)
}

// QuotaUsage reports current byte and inode usage against this repo's
// quota (DESIGN.md §18.3).
func (r *Repo) QuotaUsage() (bytesUsed, inodesUsed uint64, err error) {
	return r.DB.GetQuotaUsage()
}

// QuotaLimits reports this repo's configured quota limits; 0 means
// unlimited (DESIGN.md §18.3).
func (r *Repo) QuotaLimits() (bytesLimit, inodesLimit uint64, err error) {
	return r.DB.GetQuotaLimits()
}

// WriteHandle buffers a new or replacement file's content. Nothing is
// visible in the namespace until Commit.
type WriteHandle struct {
	repo *Repo
	dir  metadb.InodeID
	name string
	// mode/modeSet: a create(2) asking for mode 0 is a legitimate
	// request (an unreadable, unwritable file), so "no mode given" needs
	// its own flag rather than being spelled as zero.
	mode    uint32
	modeSet bool
	owner   Owner
	buf     bytes.Buffer
	done    bool
}

// Owner is the uid/gid a new inode is created with (DESIGN.md §20).
// The caller supplies it because only the mount knows who made the
// syscall — pkg/repo has no notion of a current user.
type Owner struct {
	Uid uint32
	Gid uint32
}

// SetOwner records the uid/gid a newly-created file gets. Like SetMode
// it applies only when Commit binds a new name; an overwrite leaves the
// existing inode's ownership alone, because writing to a file you do not
// own must not quietly transfer it to you.
func (h *WriteHandle) SetOwner(o Owner) { h.owner = o }

// SetMode records the permission bits a create(2) asked for. It applies
// only when Commit binds a *new* name: an overwrite keeps the existing
// inode's mode, because the caller is supplying content, not
// permissions (see metadb.CommitFile).
//
// Without this an open(2) with O_CREAT and mode 0755 produces a 0644
// file, and every program that creates an executable directly — install,
// tar restoring an archive, a build emitting a script — loses the bit.
func (h *WriteHandle) SetMode(mode uint32) { h.mode, h.modeSet = mode&0o7777, true }

// CreateFile opens a buffered write handle for name within dir. Fails
// with ErrReadOnly outside a mutable class. Does not touch the
// namespace or check for an existing name — that's resolved atomically
// at Commit, matching §16.1: nothing before Commit is observable to a
// reader.
func (r *Repo) CreateFile(dir metadb.InodeID, name string) (*WriteHandle, error) {
	if !r.Class.Mutable() {
		return nil, ErrReadOnly
	}
	return &WriteHandle{repo: r, dir: dir, name: name}, nil
}

// Write buffers p. Never fails except after Commit/Discard.
func (h *WriteHandle) Write(p []byte) (int, error) {
	if h.done {
		return 0, fmt.Errorf("repo: write to a committed or discarded handle")
	}
	return h.buf.Write(p)
}

// Discard abandons the handle; nothing is committed. Idempotent.
func (h *WriteHandle) Discard() { h.done = true }

// Commit chunks the buffered content (DESIGN.md §16.1), stores it via
// the same dedup-checked path publish uses (storeContent), and binds it
// into the namespace: an existing name's inode is updated in place, a
// new name gets a fresh inode. Fires the coherence invalidations that
// make the update visible to a lease-holding reader — bumping both the
// file's own object key (an existing cached attr/manifest lookup by
// inode ID is now stale) and the parent directory's version (a lookup
// or negative-cache entry for this name is now stale).
func (h *WriteHandle) Commit(ctx context.Context) (metadb.InodeID, error) {
	if h.done {
		return 0, fmt.Errorf("repo: Commit called on an already-committed or discarded handle")
	}
	h.done = true
	r := h.repo

	// Resolve the target name before doing any real storage work, both
	// to compute the byte delta an overwrite charges (new size minus old
	// size — an overwrite must not double-count the bytes it replaces)
	// and to reject an over-quota write via CheckQuota before chunking,
	// hashing, and packing content that would just be thrown away: a
	// rejection here leaves no dangling chunk locator. CommitFile below
	// re-resolves and re-checks atomically with the actual write, which
	// is what actually enforces the limit (DESIGN.md §18.3) — this is
	// only the fast, non-authoritative path that keeps the common case
	// cheap and trace-free.
	existing, lookupErr := r.DB.Lookup(h.dir, h.name)
	isNew := errors.Is(lookupErr, metadb.ErrNotFound)
	var oldSize uint64
	switch {
	case lookupErr == nil:
		existingRec, err := r.DB.GetInode(existing)
		if err != nil {
			return 0, err
		}
		if existingRec.IsDir {
			return 0, fmt.Errorf("%w: %q", ErrIsDirectory, h.name)
		}
		oldSize = existingRec.Size
	case isNew:
		// nothing bound yet: oldSize stays 0
	default:
		return 0, lookupErr
	}

	inodeDelta := int64(0)
	if isNew {
		inodeDelta = 1
	}
	byteDelta := int64(h.buf.Len()) - int64(oldSize)
	if err := r.DB.CheckQuota(byteDelta, inodeDelta); err != nil {
		return 0, ErrQuotaExceeded
	}

	content, err := r.storeContent(ctx, bytes.NewReader(h.buf.Bytes()), int64(h.buf.Len()))
	if err != nil {
		return 0, err
	}
	// Unlike PublishTree — which defers sealing across an entire walk
	// for packing efficiency and flushes once at the end — a standalone
	// Commit has no "end of walk" to defer to: its chunk must be
	// durable and locatable the moment Commit returns, so it flushes
	// its own (likely partial) container immediately. This trades some
	// packing efficiency on the mutable path for correctness; a busier
	// mutable workload batching multiple Commits before an explicit
	// flush is a real future optimization, not attempted here.
	if err := r.flushPacker(ctx); err != nil {
		return 0, err
	}

	mode := h.mode
	if !h.modeSet {
		mode = 0o644
	}
	rec := metadb.InodeRecord{Mode: mode, Uid: h.owner.Uid, Gid: h.owner.Gid, MTime: time.Now(), NLink: 1}
	content.apply(&rec)

	id, err := r.DB.CommitFile(h.dir, h.name, rec)
	if err != nil {
		switch {
		case errors.Is(err, metadb.ErrIsDirectory):
			return 0, fmt.Errorf("%w: %q", ErrIsDirectory, h.name)
		case errors.Is(err, metadb.ErrQuotaExceeded):
			return 0, ErrQuotaExceeded
		default:
			return 0, err
		}
	}

	if r.Coherence != nil {
		r.Coherence.Bump(InodeCoherenceKey(id))
		r.Coherence.BumpDir(DirCoherenceKey(h.dir))
	}
	return id, nil
}

// Unlink removes name from dir, moving the target inode into the
// graveyard (DESIGN.md §19.3) rather than reclaiming it immediately —
// pkg/repo.Sweep is what later collects its chunks, once past the grace
// period. This build has no open-file-handle tracking across process
// boundaries (single process only — see gc.go's doc comment), so unlike
// real §19.3, an inode is gravable immediately on unlink rather than
// only once its last open handle closes; that gap is stated, not
// hidden.
func (r *Repo) Unlink(dir metadb.InodeID, name string) error {
	if !r.Class.Mutable() {
		return ErrReadOnly
	}
	if err := r.DB.RemoveEntry(dir, name, r.Clock.Now()); err != nil {
		return err
	}
	if r.Coherence != nil {
		r.Coherence.BumpDir(DirCoherenceKey(dir))
	}
	return nil
}

// Mkdir creates a new, empty directory named name under dir. Fails with
// EEXIST-equivalent if name is already bound to anything — unlike
// WriteHandle.Commit, mkdir(2) never silently overwrites.
func (r *Repo) Mkdir(dir metadb.InodeID, name string, mode uint32, owner Owner) (metadb.InodeID, error) {
	if !r.Class.Mutable() {
		return 0, ErrReadOnly
	}
	id, err := r.DB.CommitMkdir(dir, name, metadb.InodeRecord{
		IsDir: true, Mode: mode & 0o7777, Uid: owner.Uid, Gid: owner.Gid, MTime: time.Now(), NLink: 2,
	})
	if err != nil {
		switch {
		case errors.Is(err, metadb.ErrExists):
			return 0, fmt.Errorf("%w: %q", ErrExists, name)
		case errors.Is(err, metadb.ErrQuotaExceeded):
			return 0, ErrQuotaExceeded
		default:
			return 0, err
		}
	}
	if r.Coherence != nil {
		r.Coherence.BumpDir(DirCoherenceKey(dir))
	}
	return id, nil
}

// Rmdir removes an empty directory named name under dir. Fails if the
// directory is not empty — the same restriction POSIX rmdir(2) imposes,
// kept here rather than silently recursing.
func (r *Repo) Rmdir(dir metadb.InodeID, name string) error {
	if !r.Class.Mutable() {
		return ErrReadOnly
	}
	id, err := r.DB.Lookup(dir, name)
	if err != nil {
		return err
	}
	rec, err := r.DB.GetInode(id)
	if err != nil {
		return err
	}
	if !rec.IsDir {
		return fmt.Errorf("%w: %q", ErrNotDir, name)
	}
	entries, err := r.DB.Readdir(id)
	if err != nil {
		return err
	}
	if len(entries) > 0 {
		return fmt.Errorf("%w: %q", ErrNotEmpty, name)
	}
	if err := r.DB.RemoveEntry(dir, name, r.Clock.Now()); err != nil {
		return err
	}
	if r.Coherence != nil {
		r.Coherence.BumpDir(DirCoherenceKey(dir))
	}
	return nil
}

// Rename moves oldName in oldDir to newName in newDir (DESIGN.md §16.1's
// namespace mutations). The metadata move is one transaction in metadb;
// what this adds is the coherence side effect — both directories'
// versions bump, since a name appeared in one and vanished from the
// other, and any holder caching either listing must be told.
func (r *Repo) Rename(oldDir metadb.InodeID, oldName string, newDir metadb.InodeID, newName string) error {
	if !r.Class.Mutable() {
		return ErrReadOnly
	}
	if err := r.DB.Rename(oldDir, oldName, newDir, newName, r.Clock.Now()); err != nil {
		return err
	}
	if r.Coherence != nil {
		r.Coherence.BumpDir(DirCoherenceKey(oldDir))
		r.Coherence.Bump(DirCoherenceKey(oldDir))
		if newDir != oldDir {
			r.Coherence.BumpDir(DirCoherenceKey(newDir))
			r.Coherence.Bump(DirCoherenceKey(newDir))
		}
	}
	return nil
}

// Link binds an additional name to an existing inode (DESIGN.md §19.3).
// Both leases move: the directory gains a name, and the inode's own
// nlink changed, which §10.5 puts in the inode's lease domain.
func (r *Repo) Link(dir metadb.InodeID, name string, target metadb.InodeID) (metadb.InodeRecord, error) {
	if !r.Class.Mutable() {
		return metadb.InodeRecord{}, ErrReadOnly
	}
	rec, err := r.DB.Link(dir, name, target)
	if err != nil {
		return metadb.InodeRecord{}, err
	}
	if r.Coherence != nil {
		r.Coherence.Bump(InodeCoherenceKey(target))
		r.Coherence.BumpDir(DirCoherenceKey(dir))
		r.Coherence.Bump(DirCoherenceKey(dir))
	}
	return rec, nil
}

// Mknod creates a special file — FIFO, socket, or device node — at
// (dir, name). typ is the S_IFMT bits and rdev the device number, which
// only S_IFCHR/S_IFBLK use.
func (r *Repo) Mknod(dir metadb.InodeID, name string, typ, rdev, mode uint32, owner Owner) (metadb.InodeID, error) {
	if !r.Class.Mutable() {
		return 0, ErrReadOnly
	}
	id, err := r.DB.CreateSpecial(dir, name, typ, rdev, mode, owner.Uid, owner.Gid)
	if err != nil {
		return 0, err
	}
	if r.Coherence != nil {
		r.Coherence.BumpDir(DirCoherenceKey(dir))
		r.Coherence.Bump(DirCoherenceKey(dir))
	}
	return id, nil
}

// Symlink creates a symlink at (dir, name) pointing at target.
func (r *Repo) Symlink(dir metadb.InodeID, name, target string, owner Owner) (metadb.InodeID, error) {
	if !r.Class.Mutable() {
		return 0, ErrReadOnly
	}
	id, err := r.DB.CreateSymlink(dir, name, target, owner.Uid, owner.Gid)
	if err != nil {
		return 0, err
	}
	if r.Coherence != nil {
		r.Coherence.BumpDir(DirCoherenceKey(dir))
		r.Coherence.Bump(DirCoherenceKey(dir))
	}
	return id, nil
}

// SetAttr changes an inode's mode and/or mtime (metadb.AttrMutation
// names which). It bumps the inode's own lease and not the directory's:
// §10.5 puts attributes in the inode's domain, and no name changed.
func (r *Repo) SetAttr(id metadb.InodeID, mut metadb.AttrMutation) (metadb.InodeRecord, error) {
	if !r.Class.Mutable() {
		return metadb.InodeRecord{}, ErrReadOnly
	}
	rec, err := r.DB.SetAttr(id, mut)
	if err != nil {
		return metadb.InodeRecord{}, err
	}
	if r.Coherence != nil {
		r.Coherence.Bump(InodeCoherenceKey(id))
	}
	return rec, nil
}
