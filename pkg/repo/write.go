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
	ErrReadOnly    = errors.New("repo: repo is immutable-class, read-only")
	ErrExists      = errors.New("repo: name already exists")
	ErrIsDirectory = errors.New("repo: is a directory")
	ErrNotDir      = errors.New("repo: not a directory")
	ErrNotEmpty    = errors.New("repo: directory not empty")
)

func InodeCoherenceKey(id metadb.InodeID) string { return fmt.Sprintf("inode:%d", id) }
func DirCoherenceKey(id metadb.InodeID) string   { return fmt.Sprintf("dir:%d", id) }

// WriteHandle buffers a new or replacement file's content. Nothing is
// visible in the namespace until Commit.
type WriteHandle struct {
	repo *Repo
	dir  metadb.InodeID
	name string
	buf  bytes.Buffer
	done bool
}

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

	existing, lookupErr := r.DB.Lookup(h.dir, h.name)
	var id metadb.InodeID
	isNew := errors.Is(lookupErr, metadb.ErrNotFound)
	switch {
	case lookupErr == nil:
		existingRec, err := r.DB.GetInode(existing)
		if err != nil {
			return 0, err
		}
		if existingRec.IsDir {
			return 0, fmt.Errorf("%w: %q", ErrIsDirectory, h.name)
		}
		id = existing
	case isNew:
		id, err = r.DB.AllocInode()
		if err != nil {
			return 0, err
		}
	default:
		return 0, lookupErr
	}

	rec := metadb.InodeRecord{Mode: 0o644, MTime: time.Now(), NLink: 1}
	content.apply(&rec)
	if err := r.DB.PutInode(id, rec); err != nil {
		return 0, err
	}
	if isNew {
		if err := r.DB.SetDentry(h.dir, h.name, id); err != nil {
			return 0, err
		}
	}

	if r.Coherence != nil {
		r.Coherence.Bump(InodeCoherenceKey(id))
		r.Coherence.BumpDir(DirCoherenceKey(h.dir))
	}
	return id, nil
}

// Unlink removes name from dir. DESIGN.md §19.3's graveyard/nlink
// bookkeeping (needed for POSIX's "open but unlinked" guarantee) is not
// implemented — this build has no reference-counted GC at all yet (see
// metadb.RemoveDentry's doc comment), so Unlink simply drops the
// binding; the inode record and any chunks it alone referenced become
// unreachable rather than reclaimed.
func (r *Repo) Unlink(dir metadb.InodeID, name string) error {
	if !r.Class.Mutable() {
		return ErrReadOnly
	}
	if err := r.DB.RemoveDentry(dir, name); err != nil {
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
func (r *Repo) Mkdir(dir metadb.InodeID, name string) (metadb.InodeID, error) {
	if !r.Class.Mutable() {
		return 0, ErrReadOnly
	}
	if _, err := r.DB.Lookup(dir, name); err == nil {
		return 0, fmt.Errorf("%w: %q", ErrExists, name)
	} else if !errors.Is(err, metadb.ErrNotFound) {
		return 0, err
	}
	id, err := r.DB.AllocInode()
	if err != nil {
		return 0, err
	}
	if err := r.DB.PutInode(id, metadb.InodeRecord{IsDir: true, Mode: 0o755, MTime: time.Now(), NLink: 2}); err != nil {
		return 0, err
	}
	if err := r.DB.SetDentry(dir, name, id); err != nil {
		return 0, err
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
	if err := r.DB.RemoveDentry(dir, name); err != nil {
		return err
	}
	if r.Coherence != nil {
		r.Coherence.BumpDir(DirCoherenceKey(dir))
	}
	return nil
}
