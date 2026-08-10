// Package repo ties chunk/manifest/pack/metadb/coherence together into
// the publish, read, and (for mutable classes) write paths for one repo
// (DESIGN.md §8, §16). A Repo is a single-node, single-region AtlasFS
// repository: a metadb.DB for dentries/inodes/locators, a store.Backend
// for sealed container and manifest objects, and — for every class but
// `immutable` — a coherence.Manager wired into every mutation.
package repo

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/aburan28/atlasfs/pkg/chunk"
	"github.com/aburan28/atlasfs/pkg/coherence"
	"github.com/aburan28/atlasfs/pkg/manifest"
	"github.com/aburan28/atlasfs/pkg/metadb"
	"github.com/aburan28/atlasfs/pkg/pack"
	"github.com/aburan28/atlasfs/pkg/store"
	"github.com/aburan28/atlasfs/pkg/store/local"
)

// DefaultRegion is used for every locator and container key in this
// single-node build. DESIGN.md's locator keyspace is region-keyed
// (§7.5) so a later multi-region build adds a dimension here rather
// than changing the format.
const DefaultRegion = "local"

// Class is a repo's consistency class (DESIGN.md §8), fixed for the
// repo's lifetime once persisted by EnsureClass. `posix` is deliberately
// absent — see pkg/metadb's and pkg/coherence's package docs for why.
type Class string

const (
	ClassImmutable Class = "immutable"
	ClassRelaxed   Class = "relaxed"
	ClassSession   Class = "session"

	// ClassPosix is DESIGN.md §8's linearizable class. Its staleness
	// bound is D = 0, achieved by §10.6's blocking recall rather than by
	// a short lease — see LeaseDuration. It is served by cmd/atlas-mds,
	// which has real remote holders to recall from; the in-process
	// pkg/fuseserver path does not offer it, because a single holder
	// cannot demonstrate the property that distinguishes the class.
	ClassPosix Class = "posix"
)

// leaseDuration returns DESIGN.md §8's stated default lease duration
// for each class. ClassImmutable has none — nothing is ever invalidated
// because nothing ever changes once published, so no Manager is needed.
func (c Class) leaseDuration() time.Duration {
	switch c {
	case ClassSession:
		return 5 * time.Second
	case ClassRelaxed:
		return 30 * time.Second
	case ClassPosix:
		return posixLeaseDuration
	default:
		return 0
	}
}

// LeaseDuration exports leaseDuration for callers outside this package
// (cmd/atlas-mds configures a pkg/mds.Server from it).
func (c Class) LeaseDuration() time.Duration { return c.leaseDuration() }

// posixLeaseDuration is long on purpose, and it is not a contradiction
// of §8.1's "D = 0" for the `posix` class. D bounds *staleness*, and for
// posix that bound is enforced by §10.6's blocking recall — a mutation
// cannot commit until holders have given their copies up — not by
// waiting for a lease to lapse. A short lease would add revalidation
// traffic on every read while changing the staleness bound not at all,
// since recall already drives it to zero. The lease still exists so that
// a holder which loses contact with the authority eventually stops
// trusting its cache on its own clock (§10.7).
const posixLeaseDuration = time.Hour

// KernelCacheTTL is how long a kernel-side attribute or dentry cache
// entry may be trusted for this class — the `D` of DESIGN.md §10 applied
// to the one cache holder the design never names explicitly: the kernel
// itself. A FUSE mount that leaves the kernel's attr/entry timeouts at
// zero has not made itself more correct, only slower; it has moved every
// getattr into a userspace round trip while §10's lease machinery sits
// unused one layer down. Handing the kernel exactly the class's own D
// makes it a well-behaved holder under the same bound every other holder
// obeys.
//
// ClassImmutable gets a long but finite TTL rather than an infinite one,
// because §8.2 is explicit that `immutable` means "this content does not
// change", not "this path never rebinds" — republishing over a path is
// legal, and a client that cached the old binding forever would never
// see it.
func (c Class) KernelCacheTTL() time.Duration {
	switch c {
	case ClassImmutable, "":
		return immutableKernelCacheTTL
	case ClassPosix:
		// Zero, and not posixLeaseDuration. The kernel's attr cache
		// cannot participate in §10.6's recall — there is no way to ask
		// it to drop an entry and acknowledge — so any non-zero TTL here
		// would let the kernel answer a getattr from a copy the recall
		// protocol believes was surrendered. `posix` buys D = 0 by paying
		// a round trip per operation; caching in the one layer that
		// cannot be recalled would spend the guarantee to get the cost
		// back.
		return 0
	default:
		return c.leaseDuration()
	}
}

// immutableKernelCacheTTL bounds how stale an `immutable` binding may be
// after a republish (DESIGN.md §8.2). One minute is a judgement call, not
// a figure from the design: long enough that a tree walk over a published
// dataset is served from the kernel, short enough that a republish shows
// up without an unmount.
const immutableKernelCacheTTL = time.Minute

// Mutable reports whether this class permits Create/Write/Unlink/Mkdir
// after initial publish. Only ClassImmutable is not.
func (c Class) Mutable() bool { return c != ClassImmutable && c != "" }

type Repo struct {
	Dir     string
	DB      *metadb.DB
	Backend store.Backend
	Region  string
	Class   Class

	// Coherence is nil for ClassImmutable (nothing to invalidate — see
	// Class.leaseDuration) and non-nil for every mutable class, wired
	// into the write path (bumps on mutation) and available to readers
	// (fuseserver) to cache attrs/dentries without re-hitting metadb on
	// every call while a lease is still valid.
	Coherence *coherence.Manager

	ChunkSize int
	packer    *pack.Packer

	// SingleObjectThreshold overrides pack.SingleObjectThreshold
	// (defaults to it in Open). DESIGN.md §5.5 fixes this at 64 MiB in
	// production; it's exposed here so tests can exercise the dedicated-
	// container code path without allocating and hashing real 64 MiB
	// files.
	SingleObjectThreshold int64

	// Clock is the source of "now" for every graveyard timestamp this
	// repo records (Unlink, Rmdir) and for Sweep's grace-period
	// comparisons (DESIGN.md §19). Reuses coherence.Clock rather than a
	// second time-abstraction — same role, same interface, and tests can
	// override it exactly like they already do for a Manager's clock
	// (see pkg/coherence's tests) to exercise a grace period without
	// sleeping for real hours.
	Clock coherence.Clock
}

// Open opens (or initializes) a repo rooted at dir: dir/meta.db for
// metadata, dir/objects/ as the local backend's container/manifest
// object store. This is the local-backend, immutable-class convenience
// path — the one every pre-existing caller in this codebase used before
// classes existed, and its behavior is unchanged: a fresh dir becomes an
// `immutable` repo, and an existing repo opens as whatever class it was
// created with (EnsureClass — see OpenWithClass).
func Open(dir string) (*Repo, error) {
	backend, err := local.New(filepath.Join(dir, "objects"))
	if err != nil {
		return nil, err
	}
	return OpenRemote(dir, backend, DefaultRegion)
}

// OpenRemote opens a repo rooted at dir (dir/meta.db for metadata) using
// a caller-supplied Backend and region, instead of the local-disk
// default. Use this to point a repo at S3/GCS/Azure while keeping
// metadata local — the single-node metadb stand-in doesn't need to move
// for the backend to become real cloud storage. Defaults to the
// immutable class; see OpenWithClass to create a mutable repo.
func OpenRemote(dir string, backend store.Backend, region string) (*Repo, error) {
	return OpenWithClass(dir, backend, region, ClassImmutable)
}

// OpenWithClass is the general repo constructor. class is only a hint
// used when dir holds no repo yet — DESIGN.md §8's classes are fixed at
// creation, so reopening an existing repo silently returns whatever
// class EnsureClass finds already persisted, regardless of what the
// caller asked for here. That is what lets Open(dir), which always
// hints ClassImmutable, correctly reopen a `relaxed` repo as `relaxed`.
func OpenWithClass(dir string, backend store.Backend, region string, class Class) (*Repo, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	db, err := metadb.Open(filepath.Join(dir, "meta.db"))
	if err != nil {
		return nil, err
	}
	persisted, err := db.EnsureClass(string(class))
	if err != nil {
		db.Close()
		return nil, err
	}
	actualClass := Class(persisted)

	r := &Repo{
		Dir:                   dir,
		DB:                    db,
		Backend:               backend,
		Region:                region,
		Class:                 actualClass,
		ChunkSize:             chunk.DefaultSize,
		SingleObjectThreshold: pack.SingleObjectThreshold,
		Clock:                 coherence.RealClock{},
	}
	if d := actualClass.leaseDuration(); d > 0 {
		r.Coherence = coherence.New(coherence.RealClock{}, d)
	}
	r.packer = pack.NewPacker(backend, r.Region, pack.DefaultSealSize)
	return r, nil
}

// SetChunkAlignment makes every chunk this repo packs from now on start
// on an n-byte boundary within its container. Pass pack.GDSAlignment to
// make containers readable by GPUDirect Storage without falling into its
// bounce-buffer path; pass 0 for tight packing (the default).
//
// This is a per-deployment choice with a real cost, not a free
// improvement: measured at ~4x container inflation for 1 KiB files
// (pkg/pack's TestAlignmentPaddingCost), because DESIGN.md §14's
// small-file packing is exactly what padding undoes. Datasets read by
// GPUs want it; a repo full of small files read by CPUs does not.
//
// Existing chunks are unaffected — their locators already point at
// wherever they were written, and alignment changes nothing about how a
// locator is interpreted.
func (r *Repo) SetChunkAlignment(n int) { r.packer.SetAlignment(n) }

// ChunkAlignment reports the current chunk-start alignment.
func (r *Repo) ChunkAlignment() int { return r.packer.Alignment() }

func (r *Repo) Close() error { return r.DB.Close() }

func manifestKey(id manifest.ID) string {
	h := id.String()
	return fmt.Sprintf("atlas/m/%s/%s", h[:2], h)
}

// --- publish path (DESIGN.md §16.1, §14) --------------------------------

// PublishTree walks srcDir and publishes every regular file and
// directory under destPath (path components relative to the repo
// root). It is the CLI's `atlas publish` implementation. Packing
// preserves the walk order, which for a lexicographic directory walk is
// exactly the directory-locality-preserving order DESIGN.md §14.1 calls
// for.
func (r *Repo) PublishTree(ctx context.Context, srcDir string, destPath []string) (files, dirs int, bytesIn int64, err error) {
	rootInode, err := r.DB.EnsureDir(destPath)
	if err != nil {
		return 0, 0, 0, err
	}

	dirInodes := map[string]metadb.InodeID{"": rootInode}

	err = filepath.WalkDir(srcDir, func(p string, d fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		rel, err := filepath.Rel(srcDir, p)
		if err != nil {
			return err
		}
		if rel == "." {
			return nil
		}
		rel = filepath.ToSlash(rel)
		parent := path.Dir(rel)
		if parent == "." {
			parent = ""
		}
		parentInode, ok := dirInodes[parent]
		if !ok {
			return fmt.Errorf("repo: publish: parent dir not visited for %q", rel)
		}

		if d.IsDir() {
			info, err := d.Info()
			if err != nil {
				return err
			}
			rec := metadb.InodeRecord{IsDir: true, Mode: permOf(info.Mode(), 0o755), MTime: info.ModTime(), NLink: 2}
			id, err := r.DB.CommitMkdir(parentInode, d.Name(), rec)
			if errors.Is(err, metadb.ErrExists) {
				if id, err = r.DB.Lookup(parentInode, d.Name()); err != nil {
					return err
				}
			} else if err != nil {
				return err
			}
			dirInodes[rel] = id
			dirs++
			return nil
		}

		if d.Type()&fs.ModeSymlink != 0 {
			if err := r.publishSymlink(parentInode, d.Name(), p); err != nil {
				return fmt.Errorf("publish %s: %w", rel, err)
			}
			files++
			return nil
		}
		if !d.Type().IsRegular() {
			// devices, sockets, FIFOs: out of scope for this build's
			// read-only immutable slice.
			return nil
		}
		n, err := r.publishFile(ctx, parentInode, d.Name(), p)
		if err != nil {
			return fmt.Errorf("publish %s: %w", rel, err)
		}
		files++
		bytesIn += n
		return nil
	})
	if err != nil {
		return files, dirs, bytesIn, err
	}
	if err := r.flushPacker(ctx); err != nil {
		return files, dirs, bytesIn, err
	}
	return files, dirs, bytesIn, nil
}

// publishSymlink stores a symlink's target verbatim in the inode record.
// A symlink is never chunked or content-addressed: its "content" is the
// target string, stored directly, matching how a real filesystem
// handles fast symlinks.
func (r *Repo) publishSymlink(dir metadb.InodeID, name, srcPath string) error {
	target, err := os.Readlink(srcPath)
	if err != nil {
		return err
	}
	info, err := os.Lstat(srcPath)
	if err != nil {
		return err
	}
	rec := metadb.InodeRecord{
		Mode:          0o777,
		Size:          uint64(len(target)),
		MTime:         info.ModTime(),
		NLink:         1,
		IsSymlink:     true,
		SymlinkTarget: target,
	}
	if _, err := r.DB.PublishFile(dir, name, rec); err != nil {
		return publishBindErr(name, err)
	}
	return nil
}

func (r *Repo) publishFile(ctx context.Context, dir metadb.InodeID, name, srcPath string) (int64, error) {
	f, err := os.Open(srcPath)
	if err != nil {
		return 0, err
	}
	defer f.Close()
	info, err := f.Stat()
	if err != nil {
		return 0, err
	}
	size := info.Size()

	content, err := r.storeContent(ctx, f, size)
	if err != nil {
		return 0, err
	}
	// The source's own permission bits, not a constant: publishing a
	// tree of executables and mounting it has to give back executables,
	// and a hardcoded 0o644 silently strips every exec bit in the tree.
	rec := metadb.InodeRecord{Mode: permOf(info.Mode(), 0o644), MTime: info.ModTime(), NLink: 1}
	content.apply(&rec)
	if _, err := r.DB.PublishFile(dir, name, rec); err != nil {
		return 0, publishBindErr(name, err)
	}
	return size, nil
}

// permOf extracts the permission bits from a source file's mode,
// falling back to def for the degenerate all-zero case (a source with no
// permission bits at all would otherwise publish as unreadable).
func permOf(m os.FileMode, def uint32) uint32 {
	perm := uint32(m.Perm())
	if perm == 0 {
		return def
	}
	return perm
}

// contentRef is the chunk/manifest-shaped part of an InodeRecord, the
// piece both the bulk publish path (publishFile) and the single-file
// mutable write path (WriteHandle.Commit, DESIGN.md §16.1) produce
// identically — chunk, dedup-check, pack-or-single-object, inline a lone
// chunk or build a manifest. Only what happens to the *dentry* afterward
// differs between the two paths.
type contentRef struct {
	size        uint64
	hasInline   bool
	inlineChunk chunk.ID
	hasManifest bool
	manifestID  manifest.ID
}

func (c contentRef) apply(rec *metadb.InodeRecord) {
	rec.Size = c.size
	rec.HasInline = c.hasInline
	rec.InlineChunk = c.inlineChunk
	rec.HasManifest = c.hasManifest
	rec.ManifestID = c.manifestID
}

// storeContent chunks rd (size bytes), storing each chunk via the
// write-path dedup check (§16.1) and either inlining a lone chunk or
// building and storing a manifest (§5.3), exactly as publishFile always
// did — factored out so the mutable write path doesn't reimplement it.
func (r *Repo) storeContent(ctx context.Context, rd io.Reader, size int64) (contentRef, error) {
	if size == 0 {
		return contentRef{}, nil
	}
	useSingleObject := size >= r.SingleObjectThreshold
	chunks, err := chunk.SplitAll(rd, r.ChunkSize)
	if err != nil {
		return contentRef{}, err
	}
	for _, c := range chunks {
		if err := r.storeChunk(ctx, c, useSingleObject); err != nil {
			return contentRef{}, err
		}
	}
	if len(chunks) == 1 && !useSingleObject {
		return contentRef{size: uint64(size), hasInline: true, inlineChunk: chunks[0].ID}, nil
	}
	m := manifest.New(chunks, r.ChunkSize)
	mid, err := r.putManifest(ctx, m)
	if err != nil {
		return contentRef{}, err
	}
	return contentRef{size: uint64(size), hasManifest: true, manifestID: mid}, nil
}

// publishBindErr turns metadb's sentinel into the message the publish
// path has always given for a name collision, and passes everything else
// (notably ErrQuotaExceeded, which publish can now return since it
// charges the quota it consumes) through unwrapped so callers can still
// match on it.
func publishBindErr(name string, err error) error {
	if errors.Is(err, metadb.ErrExists) {
		return fmt.Errorf("repo: %q already published in this directory (immutable class: republish under a new path or version)", name)
	}
	return err
}

// storeChunk implements the write-path dedup check from DESIGN.md
// §16.1: a chunk is uploaded only if its content hash is not already
// known. Large-file chunks bypass the shared packer and become their
// own dedicated container objects (§5.5) so sequential reads are a
// direct range GET.
func (r *Repo) storeChunk(ctx context.Context, c chunk.Chunk, singleObject bool) error {
	if found, err := r.DB.HasLocator(r.Region, c.ID); err != nil {
		return err
	} else if found {
		return nil
	}
	if singleObject {
		loc, err := pack.PutSingleObject(ctx, r.Backend, r.Region, c)
		if err != nil {
			return err
		}
		return r.DB.PutLocator(r.Region, c.ID, loc)
	}
	r.packer.Add(c)
	if r.packer.Full() {
		return r.flushPacker(ctx)
	}
	return nil
}

func (r *Repo) flushPacker(ctx context.Context) error {
	locs, err := r.packer.Seal(ctx)
	if err != nil {
		return err
	}
	for id, loc := range locs {
		if err := r.DB.PutLocator(r.Region, id, loc); err != nil {
			return err
		}
	}
	return nil
}

func (r *Repo) putManifest(ctx context.Context, m *manifest.Manifest) (manifest.ID, error) {
	id := m.ID()
	data := m.EncodeBytes()
	_, err := r.Backend.Put(ctx, manifestKey(id), bytes.NewReader(data), int64(len(data)), store.PutOpts{IfAbsent: true})
	if err != nil && !errors.Is(err, os.ErrExist) {
		return manifest.ID{}, err
	}
	return id, nil
}

// --- read path ------------------------------------------------------------

// Resolve looks up a "/"-separated path and returns its inode record.
func (r *Repo) Resolve(p string) (metadb.InodeID, metadb.InodeRecord, error) {
	parts := splitPath(p)
	return r.DB.Resolve(parts)
}

func splitPath(p string) []string {
	p = strings.Trim(path.Clean("/"+p), "/")
	if p == "" {
		return nil
	}
	return strings.Split(p, "/")
}

func (r *Repo) getManifest(ctx context.Context, id manifest.ID) (*manifest.Manifest, error) {
	rc, err := r.Backend.Get(ctx, manifestKey(id), 0, -1)
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

// FileReader provides io.ReaderAt over a published file's content,
// resolving manifest entries (or the inline chunk) to locators to
// backend byte ranges on demand, with hash verification on every fetch
// (DESIGN.md §24.4).
type FileReader struct {
	repo *Repo
	ctx  context.Context

	size int64

	hasInline bool
	inline    chunk.ID

	m       *manifest.Manifest
	offsets []int64

	cache  map[chunk.ID][]byte
	cacheQ []chunk.ID
}

const fileReaderCacheEntries = 16

func (r *Repo) OpenFile(ctx context.Context, rec metadb.InodeRecord) (*FileReader, error) {
	fr := &FileReader{repo: r, ctx: ctx, size: int64(rec.Size), cache: map[chunk.ID][]byte{}}
	switch {
	case rec.HasInline:
		fr.hasInline = true
		fr.inline = rec.InlineChunk
	case rec.HasManifest:
		m, err := r.getManifest(ctx, rec.ManifestID)
		if err != nil {
			return nil, fmt.Errorf("repo: open: fetch manifest %s: %w", rec.ManifestID, err)
		}
		fr.m = m
		fr.offsets = make([]int64, len(m.Entries))
		var off int64
		for i, e := range m.Entries {
			fr.offsets[i] = off
			off += int64(e.Length)
		}
	}
	return fr, nil
}

// locate resolves a chunk ID to its locator in this repo's region.
func (fr *FileReader) locate(id chunk.ID) (pack.Locator, error) {
	loc, found, err := fr.repo.DB.GetLocator(fr.repo.Region, id)
	if err != nil {
		return pack.Locator{}, err
	}
	if !found {
		return pack.Locator{}, fmt.Errorf("repo: chunk %s: no locator in region %s", id, fr.repo.Region)
	}
	return loc, nil
}

// readChunkInto fetches a whole chunk directly into p, which must be
// exactly the chunk's length. This is the path that carries no
// allocation at all: the destination is the caller's, and verification
// happens in place (pack.FetchInto).
func (fr *FileReader) readChunkInto(id chunk.ID, p []byte) error {
	loc, err := fr.locate(id)
	if err != nil {
		return err
	}
	return pack.FetchInto(fr.ctx, fr.repo.Backend, id, loc, p)
}

// chunkBytes returns a chunk's bytes, caching them so repeated reads
// within the same file (the random-access pattern) do not refetch. It
// reuses a pooled buffer per fetch rather than letting io.ReadAll size
// one by doubling — the same bytes end up cached either way, but the
// transient garbage does not.
func (fr *FileReader) chunkBytes(id chunk.ID) ([]byte, error) {
	if b, ok := fr.cache[id]; ok {
		return b, nil
	}
	loc, err := fr.locate(id)
	if err != nil {
		return nil, err
	}
	buf := make([]byte, loc.Length)
	if err := pack.FetchInto(fr.ctx, fr.repo.Backend, id, loc, buf); err != nil {
		return nil, err
	}
	fr.cache[id] = buf
	fr.cacheQ = append(fr.cacheQ, id)
	if len(fr.cacheQ) > fileReaderCacheEntries {
		old := fr.cacheQ[0]
		fr.cacheQ = fr.cacheQ[1:]
		delete(fr.cache, old)
	}
	return buf, nil
}

// ReadAt implements io.ReaderAt.
func (fr *FileReader) ReadAt(p []byte, off int64) (int, error) {
	if off < 0 {
		return 0, fmt.Errorf("repo: negative offset")
	}
	if off >= fr.size {
		return 0, io.EOF
	}
	if fr.hasInline {
		data, err := fr.chunkBytes(fr.inline)
		if err != nil {
			return 0, err
		}
		if off >= int64(len(data)) {
			return 0, io.EOF
		}
		n := copy(p, data[off:])
		return n, nil
	}
	if fr.m == nil {
		return 0, io.EOF // zero-length file
	}

	total := 0
	for total < len(p) && off < fr.size {
		idx := sort.Search(len(fr.offsets), func(i int) bool {
			end := fr.offsets[i] + int64(fr.m.Entries[i].Length)
			return end > off
		})
		if idx >= len(fr.offsets) {
			break
		}
		entry := fr.m.Entries[idx]
		localOff := off - fr.offsets[idx]

		// Fast path: this read starts exactly on a chunk boundary and has
		// room for the whole chunk, and the chunk is not already cached.
		// The chunk can then land directly in the caller's buffer — no
		// intermediate allocation and no copy. This is the shape a
		// GPUDirect destination would take, which is why it is worth
		// having even though the sequential-read win alone would justify
		// it (a whole-file read is entirely made of such chunks).
		if _, cached := fr.cache[entry.ChunkID]; !cached && localOff == 0 && len(p)-total >= int(entry.Length) {
			dst := p[total : total+int(entry.Length)]
			if err := fr.readChunkInto(entry.ChunkID, dst); err != nil {
				return total, err
			}
			total += int(entry.Length)
			off += int64(entry.Length)
			continue
		}

		data, err := fr.chunkBytes(entry.ChunkID)
		if err != nil {
			return total, err
		}
		n := copy(p[total:], data[localOff:])
		total += n
		off += int64(n)
	}
	if total == 0 {
		return 0, io.EOF
	}
	return total, nil
}

// ReadAll reads the whole file. Convenience for small files and tests;
// FUSE reads use ReadAt directly.
func (fr *FileReader) ReadAll() ([]byte, error) {
	buf := make([]byte, fr.size)
	var off int64
	for off < fr.size {
		n, err := fr.ReadAt(buf[off:], off)
		off += int64(n)
		if err != nil && err != io.EOF {
			return nil, err
		}
		if n == 0 {
			break
		}
	}
	return buf[:off], nil
}

// Readdir lists a directory's children.
func (r *Repo) Readdir(inode metadb.InodeID) ([]metadb.DirEntry, error) {
	return r.DB.Readdir(inode)
}
