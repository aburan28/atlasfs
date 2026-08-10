// Package libatlas implements DESIGN.md §21.3's escape hatch: a small
// client library, not a kernel-visible interface, that materializes a
// repo file to local disk and hands back an *os.File open on that local
// copy. The library exists because mmap over a network-backed FUSE file
// is the worst case for this architecture — synchronous page faults
// that serialize, kernel readahead with no notion of chunk boundaries,
// MAP_POPULATE turning a lazy load into a stall — and safetensors,
// torch.load, numpy.memmap, and Arrow/Parquet all mmap by design, so
// this is how most bytes in a training/inference workload actually
// arrive (§21.3). atlas_open_mapped's Go analog is OpenMapped: resolve,
// materialize with the client's real chunk-fetch concurrency (the thing
// it's good at), then let the caller mmap an ordinary local file with
// real page cache and real readahead — no FUSE involvement in the fault
// path at all.
//
// Materializer's cache is keyed by content, not by path: a repo file's
// manifest ID or inline chunk ID (DESIGN.md §5.2/§5.3) names its bytes,
// so two resolutions landing on the same ID are guaranteed byte-
// identical and a cache hit never needs revalidation, for either
// consistency class (DESIGN.md §8) — that is what content addressing
// buys for free here. What does need checking on every OpenMapped call
// is which content ID a path currently resolves to, since a
// `relaxed`/`session` repo's WriteHandle.Commit (pkg/repo/write.go) can
// repoint a path at a new manifest ID at any time. Materializer never
// caches that path→ID resolution itself: every OpenMapped re-resolves
// straight through repo.Repo.Resolve, the same direct metadb read
// pkg/fuseserver's Node falls back to once its own coherence-manager
// lease goes stale (see fuseserver.go's currentRec). There is
// consequently no path-keyed state in this package for a lease to ever
// need to invalidate — the only thing cached on disk is immutable,
// content-addressed bytes, and a stale mapping simply becomes a
// materialized file nothing points to anymore, reclaimed like any other
// cache entry under LRU pressure.
package libatlas

import (
	"container/list"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"

	"github.com/aburan28/atlasfs/pkg/metadb"
	"github.com/aburan28/atlasfs/pkg/repo"
)

// ErrNotRegularFile is returned by OpenMapped for a directory or
// symlink — mmap has no meaning for either, so materializing one would
// only paper over a caller bug.
var ErrNotRegularFile = errors.New("libatlas: not a regular file")

// materializeBufSize is the copy buffer OpenMapped streams chunk data
// through when writing a cache entry. Large enough to keep syscall
// count low for a multi-GB model file, and deliberately not "read the
// whole file into memory first" the way repo.FileReader.ReadAll is
// (fine for fuseserver's small buffered writes, wrong here where the
// whole point is files too big to want resident twice).
const materializeBufSize = 1 << 20 // 1 MiB

// cacheEntry is one materialized file: its content key, on-disk path,
// and byte size (for the LRU budget). Held as a list.Element.Value so
// the LRU is a plain doubly linked list, most-recently-used at Front.
type cacheEntry struct {
	key  string
	path string
	size int64
}

// inflight coalesces concurrent OpenMapped calls racing to materialize
// the same content key — without it, two callers opening the same
// freshly-published file at once would each fetch and write their own
// temp file, and whichever Rename lost would have done the fetch for
// nothing.
type inflight struct {
	wg   sync.WaitGroup
	path string
	size int64
	err  error
}

// Materializer owns a local cache directory that OpenMapped fills on
// demand and evicts from under LRU pressure, per DESIGN.md §21.3's
// materialize-then-passthrough design. In production cacheDir is local
// NVMe; a plain temp directory is fine for this build (§21.3 specifies
// the mechanism, not a particular filesystem). The zero value is not
// usable — construct with NewMaterializer.
type Materializer struct {
	cacheDir string
	maxBytes int64

	mu       sync.Mutex
	byKey    map[string]*list.Element
	lru      *list.List // *cacheEntry, most-recently-used at Front
	curBytes int64
	inflight map[string]*inflight
}

// NewMaterializer returns a Materializer caching materialized files
// under cacheDir (created if it does not exist), evicting least-
// recently-used entries once their total size would exceed maxBytes.
func NewMaterializer(cacheDir string, maxBytes int64) (*Materializer, error) {
	if maxBytes <= 0 {
		return nil, fmt.Errorf("libatlas: maxBytes must be positive, got %d", maxBytes)
	}
	if err := os.MkdirAll(cacheDir, 0o755); err != nil {
		return nil, fmt.Errorf("libatlas: create cache dir %q: %w", cacheDir, err)
	}
	return &Materializer{
		cacheDir: cacheDir,
		maxBytes: maxBytes,
		byKey:    map[string]*list.Element{},
		lru:      list.New(),
		inflight: map[string]*inflight{},
	}, nil
}

// contentKey names path's current bytes: DESIGN.md §5.2/§5.3's manifest
// ID for a multi-chunk file, or the lone chunk's ID for a file small
// enough to inline (repo.go's storeContent picks between the two the
// same way for every write path this build has). A zero-length file
// has neither — "empty" is a fine, stable stand-in, since every
// zero-length file is the same (zero) bytes by definition.
func contentKey(rec metadb.InodeRecord) string {
	switch {
	case rec.HasInline:
		return "inline-" + rec.InlineChunk.String()
	case rec.HasManifest:
		return "manifest-" + rec.ManifestID.String()
	default:
		return "empty"
	}
}

// OpenMapped resolves path in r, materializing its current content to
// local disk if not already cached, and returns an *os.File open on
// that local copy — an ordinary local file a caller can mmap for real
// page cache and readahead (DESIGN.md §21.3), with the FUSE mount out
// of the picture for the fault path entirely.
//
// path is re-resolved through r on every call rather than trusted from
// a previous OpenMapped — see the package doc for why that (not a
// coherence lease held by this package) is what keeps a
// `relaxed`/`session` repo's mutation from ever handing back stale
// bytes here.
func (m *Materializer) OpenMapped(ctx context.Context, r *repo.Repo, path string) (*os.File, error) {
	_, rec, err := r.Resolve(path)
	if err != nil {
		return nil, fmt.Errorf("libatlas: resolve %q: %w", path, err)
	}
	if rec.IsDir {
		return nil, fmt.Errorf("%w: %q is a directory", ErrNotRegularFile, path)
	}
	if rec.IsSymlink {
		return nil, fmt.Errorf("%w: %q is a symlink", ErrNotRegularFile, path)
	}

	key := contentKey(rec)
	localPath, err := m.materialize(ctx, r, key, rec)
	if err != nil {
		return nil, err
	}
	return os.Open(localPath)
}

// materialize returns key's cached local path, fetching it through r
// (or coalescing onto a fetch already in flight) if this is the first
// request for key, and recording it in the LRU on success.
func (m *Materializer) materialize(ctx context.Context, r *repo.Repo, key string, rec metadb.InodeRecord) (string, error) {
	m.mu.Lock()
	if el, ok := m.byKey[key]; ok {
		m.lru.MoveToFront(el)
		path := el.Value.(*cacheEntry).path
		m.mu.Unlock()
		return path, nil
	}
	if call, ok := m.inflight[key]; ok {
		m.mu.Unlock()
		call.wg.Wait()
		return call.path, call.err
	}
	call := &inflight{}
	call.wg.Add(1)
	m.inflight[key] = call
	m.mu.Unlock()

	path, size, err := m.fetch(ctx, r, key, rec)
	call.path, call.size, call.err = path, size, err
	call.wg.Done()

	m.mu.Lock()
	delete(m.inflight, key)
	if err == nil {
		el := m.lru.PushFront(&cacheEntry{key: key, path: path, size: size})
		m.byKey[key] = el
		m.curBytes += size
		// el is protected from its own insertion eviction pass: a file
		// larger than maxBytes on its own is still cached (nothing else
		// to evict it in favor of), matching ordinary LRU-cache
		// behavior — the alternative is a file too big to ever open.
		m.evictLocked(el)
	}
	m.mu.Unlock()
	return path, err
}

// fetch does the actual work: read path's content through r's normal
// read path (repo.FileReader — chunk resolution, locator lookup, and
// per-chunk hash verification per DESIGN.md §24.4, all reused rather
// than reimplemented here) and stream it into a temp file in cacheDir,
// then rename into place so a concurrent reader of the same key never
// observes a partial file.
func (m *Materializer) fetch(ctx context.Context, r *repo.Repo, key string, rec metadb.InodeRecord) (string, int64, error) {
	dest := filepath.Join(m.cacheDir, key)
	tmp, err := os.CreateTemp(m.cacheDir, ".materialize-*")
	if err != nil {
		return "", 0, fmt.Errorf("libatlas: create temp file: %w", err)
	}
	defer os.Remove(tmp.Name()) // no-op once Rename below has moved it

	if key != "empty" {
		fr, err := r.OpenFile(ctx, rec)
		if err != nil {
			tmp.Close()
			return "", 0, fmt.Errorf("libatlas: open %q: %w", key, err)
		}
		buf := make([]byte, materializeBufSize)
		if _, err := io.CopyBuffer(tmp, io.NewSectionReader(fr, 0, int64(rec.Size)), buf); err != nil {
			tmp.Close()
			return "", 0, fmt.Errorf("libatlas: materialize %q: %w", key, err)
		}
	}
	if err := tmp.Sync(); err != nil {
		tmp.Close()
		return "", 0, fmt.Errorf("libatlas: sync %q: %w", key, err)
	}
	if err := tmp.Close(); err != nil {
		return "", 0, fmt.Errorf("libatlas: close %q: %w", key, err)
	}
	if err := os.Rename(tmp.Name(), dest); err != nil {
		return "", 0, fmt.Errorf("libatlas: rename %q into cache: %w", key, err)
	}
	return dest, int64(rec.Size), nil
}

// evictLocked removes least-recently-used entries until curBytes is
// back within maxBytes or nothing is left to evict, never removing
// protect (the entry materialize just inserted — see its call site).
func (m *Materializer) evictLocked(protect *list.Element) {
	for m.curBytes > m.maxBytes {
		back := m.lru.Back()
		if back == nil || back == protect {
			return
		}
		m.removeElementLocked(back)
	}
}

func (m *Materializer) removeElementLocked(el *list.Element) {
	e := el.Value.(*cacheEntry)
	m.lru.Remove(el)
	delete(m.byKey, e.key)
	m.curBytes -= e.size
	os.Remove(e.path)
}

// Evict drops path's currently-materialized cache entry, if any, by
// resolving path to its current content key and removing that key
// immediately rather than waiting on LRU pressure. Returns whether an
// entry was removed. Never required for correctness — a materialized
// file that a path no longer resolves to is already unreachable, see
// the package doc — but useful for a caller that knows a large file
// will not be needed again soon and wants its budget back now.
func (m *Materializer) Evict(ctx context.Context, r *repo.Repo, path string) (bool, error) {
	_, rec, err := r.Resolve(path)
	if err != nil {
		return false, fmt.Errorf("libatlas: resolve %q: %w", path, err)
	}
	key := contentKey(rec)

	m.mu.Lock()
	defer m.mu.Unlock()
	el, ok := m.byKey[key]
	if !ok {
		return false, nil
	}
	m.removeElementLocked(el)
	return true, nil
}

// Stats reports the Materializer's current cache occupancy: the number
// of materialized entries and their total size in bytes. Meant for
// tests and operational visibility, not for a caller to branch on —
// exactly which entries survive under pressure is the LRU policy's
// business, not a documented contract.
func (m *Materializer) Stats() (files int, bytes int64) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.lru.Len(), m.curBytes
}
