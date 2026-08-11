package mdsfuse

import (
	"bytes"
	"context"
	"fmt"
	"syscall"
	"time"

	"github.com/aburan28/atlasfs/pkg/chunk"
	"github.com/aburan28/atlasfs/pkg/manifest"
	"github.com/aburan28/atlasfs/pkg/metadb"
	"github.com/aburan28/atlasfs/pkg/pack"
	"github.com/aburan28/atlasfs/pkg/repo"
	"github.com/aburan28/atlasfs/pkg/store"
)

// The client half of DESIGN.md §16.1's write path, across the RPC
// boundary. The sequence matters and is the whole reason this is not
// simply "send the bytes to the authority":
//
//  1. Chunk the content locally and hash it.
//  2. Ask the authority which chunks it already has (dedup, §16.1
//     step 2) and upload only the rest — as sealed containers written
//     straight to object storage, never through the authority.
//  3. Register the new chunks' locators with the authority.
//  4. Commit the inode record, which is the moment the write becomes
//     visible and the moment other holders are invalidated or recalled.
//
// Data never crosses the authority (§11). What crosses is the content
// pointer, which is what makes the authority's cost independent of file
// size.
//
// Scope, stated rather than implied: this is buffer-then-commit-on-flush,
// exactly as pkg/repo/write.go is, not random-access byte-range writes.
// A file's whole content is assembled client-side and committed as a
// unit. §16.3's in-place partial writes are `posix`-class territory and
// are not implemented on either mount.

// writeSession accumulates a file's content until flush.
type writeSession struct {
	cfg  Config
	dir  metadb.InodeID
	name string
	// node, when set, is the authority on where this session commits. A
	// rename while the file is open moves the dentry out from under
	// dir/name, and committing to the captured pair would recreate the
	// name the rename just removed. dir/name remain the fallback for a
	// session that has no node yet (Create, before the child exists).
	node *Node
	// mode is what a create(2) asked for, carried to the first commit so
	// a file created executable is executable. Zero means "use the
	// default"; an overwrite keeps the stored mode regardless.
	// mode/modeSet: mode 0 is a legitimate create(2) request, so "not
	// given" needs its own flag rather than being spelled as zero.
	mode    uint32
	modeSet bool
	// uid/gid are the creating process's, stamped on a new inode. An
	// overwrite keeps the stored ownership — writing to a file you do not
	// own must not quietly transfer it to you (metadb.CommitFile).
	uid    uint32
	gid    uint32
	buf    bytes.Buffer
	dirty  bool
	closed bool
}

// target is the dentry this session commits into, re-read at commit time
// rather than captured at open time — see the node field.
func (w *writeSession) target() (metadb.InodeID, string) {
	if w.node != nil {
		if dir, name := w.node.binding(); name != "" {
			return dir, name
		}
	}
	return w.dir, w.name
}

func (w *writeSession) writeAt(p []byte, off int64) (int, error) {
	// Grow to cover a write past the current end, so an offset write into
	// a sparse region behaves rather than silently reordering bytes.
	if need := off + int64(len(p)); need > int64(w.buf.Len()) {
		w.buf.Write(make([]byte, need-int64(w.buf.Len())))
	}
	copy(w.buf.Bytes()[off:], p)
	w.dirty = true
	return len(p), nil
}

// allocate applies fallocate(2) to the buffered content. bytes.Buffer
// has no way to replace its contents in place, so the reassembled slice
// is written back through Reset+Write — cheap next to the commit that
// follows, and it keeps the one shared implementation of the semantics.
func (w *writeSession) allocate(off, size uint64, mode uint32) syscall.Errno {
	buf, changed, errno := repo.ApplyFallocate(w.buf.Bytes(), off, size, mode)
	if errno != 0 {
		return errno
	}
	if changed {
		w.buf.Reset()
		w.buf.Write(buf)
		w.dirty = true
	}
	return 0
}

func (w *writeSession) resize(n int64) {
	cur := int64(w.buf.Len())
	switch {
	case n < cur:
		w.buf.Truncate(int(n))
	case n > cur:
		w.buf.Write(make([]byte, n-cur))
	}
	w.dirty = true
}

// commit performs steps 1–4 above. It is idempotent across repeated
// flushes: FLUSH can fire more than once in a single open-for-write
// session (notably right after a kernel-issued truncate, before the real
// writes land), so the guard is "dirty since the last commit" rather
// than "ever committed" — the latter silently drops everything written
// after the first flush, a bug this codebase has already been bitten by
// once on the in-process mount.
func (w *writeSession) commit(ctx context.Context) error {
	if !w.dirty {
		return nil
	}
	w.dirty = false

	content := w.buf.Bytes()
	ref, err := storeContent(ctx, w.cfg, content)
	if err != nil {
		w.dirty = true // let a later flush retry rather than lose the write
		return err
	}

	mode := w.mode & 0o7777
	if !w.modeSet {
		mode = 0o644
	}
	rec := metadb.InodeRecord{
		Mode:  mode,
		Uid:   w.uid,
		Gid:   w.gid,
		Size:  uint64(len(content)),
		MTime: time.Now(),
		NLink: 1,
	}
	ref.apply(&rec)

	dir, name := w.target()

	// A file unlinked while this session was open must not come back:
	// Commit binds (dir, name) and would recreate the dentry the unlink
	// removed, so the content goes straight to the inode instead
	// (DESIGN.md §19.3). The same applies when the name has since been
	// taken over by a different inode — committing then would clobber a
	// file this session never opened.
	if id, detached := w.detached(ctx, dir, name); detached {
		if _, err := w.cfg.Client.CommitDetached(ctx, id, rec); err != nil {
			w.dirty = true
			return err
		}
		return nil
	}

	if _, err := w.cfg.Client.Commit(ctx, dir, name, rec); err != nil {
		w.dirty = true
		return err
	}
	return nil
}

// detached reports this session's inode and whether (dir, name) no
// longer resolves to it. The lookup goes to the authority rather than a
// cache: a stale "still bound" answer here recreates a name the user
// deleted, which is the failure this exists to prevent.
//
// A session with no node yet (Create, before its child exists) has just
// bound the name itself and is never detached.
func (w *writeSession) detached(ctx context.Context, dir metadb.InodeID, name string) (metadb.InodeID, bool) {
	if w.node == nil || w.node.ino == 0 {
		return 0, false
	}
	found, id, _, err := w.cfg.Client.LookupUncached(ctx, dir, name)
	if err != nil {
		// Cannot establish the binding — treat as attached, which is the
		// pre-existing behaviour and keeps a transient RPC failure from
		// silently turning a normal write into a detached one.
		return w.node.ino, false
	}
	if !found || id != w.node.ino {
		return w.node.ino, true
	}
	return w.node.ino, false
}

// contentRef is the chunk/manifest-shaped part of an inode record —
// either a single inlined chunk (§5.3) or a manifest reference.
type contentRef struct {
	size        uint64
	hasInline   bool
	inlineChunk chunk.ID
	hasManifest bool
	manifestID  manifest.ID
}

func (c contentRef) apply(rec *metadb.InodeRecord) {
	rec.Size = c.size
	rec.HasInline, rec.InlineChunk = c.hasInline, c.inlineChunk
	rec.HasManifest, rec.ManifestID = c.hasManifest, c.manifestID
}

// storeContent chunks data, uploads what the region does not already
// have, registers the new locators, and returns the content pointer.
func storeContent(ctx context.Context, cfg Config, data []byte) (contentRef, error) {
	ref := contentRef{size: uint64(len(data))}
	if len(data) == 0 {
		return ref, nil
	}

	chunkSize := cfg.ChunkSize
	if chunkSize <= 0 {
		chunkSize = chunk.DefaultSize
	}
	chunks := chunk.SplitBytes(data, chunkSize)

	packer := pack.NewPacker(cfg.Backend, regionOf(cfg), 0)
	if cfg.ChunkAlignment > 0 {
		packer.SetAlignment(cfg.ChunkAlignment)
	}
	pending := 0
	for _, c := range chunks {
		have, err := cfg.Client.HasLocator(ctx, c.ID)
		if err != nil {
			return ref, err
		}
		if have {
			continue // dedup: the region already has these bytes
		}
		packer.Add(c)
		pending++
	}
	if pending > 0 {
		locs, err := packer.Seal(ctx)
		if err != nil {
			return ref, err
		}
		for id, loc := range locs {
			if err := cfg.Client.PutLocator(ctx, id, loc); err != nil {
				return ref, err
			}
		}
	}

	if len(chunks) == 1 {
		// §5.3: a lone chunk is inlined in the inode, skipping the
		// manifest object entirely.
		ref.hasInline, ref.inlineChunk = true, chunks[0].ID
		return ref, nil
	}

	m := manifest.New(chunks, chunkSize)
	id, err := putManifest(ctx, cfg, m)
	if err != nil {
		return ref, err
	}
	ref.hasManifest, ref.manifestID = true, id
	return ref, nil
}

// putManifest writes the manifest blob to object storage. Manifests are
// content-addressed blobs, not metadata rows (§5.3), so they go to the
// backend like chunk data — the authority only ever learns the ID.
func putManifest(ctx context.Context, cfg Config, m *manifest.Manifest) (manifest.ID, error) {
	data := m.EncodeBytes()
	id := m.ID()
	h := id.String()
	key := fmt.Sprintf("atlas/m/%s/%s", h[:2], h)
	_, err := cfg.Backend.Put(ctx, key, bytes.NewReader(data), int64(len(data)), store.PutOpts{})
	return id, err
}
