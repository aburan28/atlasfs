// Package repo ties chunk/manifest/pack/metadb together into the
// publish and read paths for one `immutable`-class subtree (DESIGN.md
// §8, §16). A Repo is a single-node, single-region AtlasFS repository:
// a metadb.DB for dentries/inodes/locators and a store.Backend for
// sealed container and manifest objects.
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

	"github.com/aburan28/atlasfs/pkg/chunk"
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

type Repo struct {
	Dir     string
	DB      *metadb.DB
	Backend store.Backend
	Region  string

	ChunkSize int
	packer    *pack.Packer
}

// Open opens (or initializes) a repo rooted at dir: dir/meta.db for
// metadata, dir/objects/ as the local backend's container/manifest
// object store.
func Open(dir string) (*Repo, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	db, err := metadb.Open(filepath.Join(dir, "meta.db"))
	if err != nil {
		return nil, err
	}
	backend, err := local.New(filepath.Join(dir, "objects"))
	if err != nil {
		db.Close()
		return nil, err
	}
	r := &Repo{
		Dir:       dir,
		DB:        db,
		Backend:   backend,
		Region:    DefaultRegion,
		ChunkSize: chunk.DefaultSize,
	}
	r.packer = pack.NewPacker(backend, r.Region, pack.DefaultSealSize)
	return r, nil
}

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
			id, err := r.DB.AllocInode()
			if err != nil {
				return err
			}
			info, err := d.Info()
			if err != nil {
				return err
			}
			if err := r.DB.PutInode(id, metadb.InodeRecord{IsDir: true, Mode: 0o755, MTime: info.ModTime(), NLink: 2}); err != nil {
				return err
			}
			if err := r.DB.CreateDentry(parentInode, d.Name(), id); err != nil && !errors.Is(err, metadb.ErrExists) {
				return err
			} else if errors.Is(err, metadb.ErrExists) {
				id, err = r.DB.Lookup(parentInode, d.Name())
				if err != nil {
					return err
				}
			}
			dirInodes[rel] = id
			dirs++
			return nil
		}

		if !d.Type().IsRegular() {
			// symlinks, devices, etc. are out of scope for this
			// build's read-only immutable slice.
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

	id, err := r.DB.AllocInode()
	if err != nil {
		return 0, err
	}

	if size == 0 {
		rec := metadb.InodeRecord{Mode: 0o644, Size: 0, MTime: info.ModTime(), NLink: 1}
		if err := r.DB.PutInode(id, rec); err != nil {
			return 0, err
		}
		return 0, r.createDentryAllowExists(dir, name, id)
	}

	useSingleObject := size >= pack.SingleObjectThreshold
	chunks, err := chunk.SplitAll(f, r.ChunkSize)
	if err != nil {
		return 0, err
	}

	for _, c := range chunks {
		if err := r.storeChunk(ctx, c, useSingleObject); err != nil {
			return 0, err
		}
	}

	rec := metadb.InodeRecord{Mode: 0o644, Size: uint64(size), MTime: info.ModTime(), NLink: 1}
	if len(chunks) == 1 && !useSingleObject {
		rec.HasInline = true
		rec.InlineChunk = chunks[0].ID
	} else {
		m := manifest.New(chunks, r.ChunkSize)
		mid, err := r.putManifest(ctx, m)
		if err != nil {
			return 0, err
		}
		rec.HasManifest = true
		rec.ManifestID = mid
	}
	if err := r.DB.PutInode(id, rec); err != nil {
		return 0, err
	}
	return size, r.createDentryAllowExists(dir, name, id)
}

func (r *Repo) createDentryAllowExists(dir metadb.InodeID, name string, id metadb.InodeID) error {
	err := r.DB.CreateDentry(dir, name, id)
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

func (fr *FileReader) chunkBytes(id chunk.ID) ([]byte, error) {
	if b, ok := fr.cache[id]; ok {
		return b, nil
	}
	loc, found, err := fr.repo.DB.GetLocator(fr.repo.Region, id)
	if err != nil {
		return nil, err
	}
	if !found {
		return nil, fmt.Errorf("repo: chunk %s: no locator in region %s", id, fr.repo.Region)
	}
	data, err := pack.Fetch(fr.ctx, fr.repo.Backend, id, loc)
	if err != nil {
		return nil, err
	}
	fr.cache[id] = data
	fr.cacheQ = append(fr.cacheQ, id)
	if len(fr.cacheQ) > fileReaderCacheEntries {
		old := fr.cacheQ[0]
		fr.cacheQ = fr.cacheQ[1:]
		delete(fr.cache, old)
	}
	return data, nil
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
		data, err := fr.chunkBytes(entry.ChunkID)
		if err != nil {
			return total, err
		}
		localOff := off - fr.offsets[idx]
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
