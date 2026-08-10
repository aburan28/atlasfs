// Package local implements the store.Backend interface (DESIGN.md §24)
// over a local POSIX directory. It stands in for S3/GCS/Azure in this
// build: same interface, so a later cloud backend is additive rather than
// a rewrite of anything that calls Backend. It is also the reference
// implementation of the "POSIX" row in DESIGN.md §24.2's capability
// matrix — conditional put via O_EXCL, no multipart, unlink-loop delete.
package local

import (
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"github.com/aburan28/atlasfs/pkg/store"
)

type Backend struct {
	root string
}

// New returns a local-disk backend rooted at dir. dir is created if it
// does not exist.
func New(dir string) (*Backend, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, fmt.Errorf("local backend: %w", err)
	}
	abs, err := filepath.Abs(dir)
	if err != nil {
		return nil, err
	}
	return &Backend{root: abs}, nil
}

func (b *Backend) path(key string) (string, error) {
	// keys are always slash-separated, relative, no traversal.
	clean := filepath.Clean("/" + key)[1:]
	if clean == "" || clean == "." {
		return "", fmt.Errorf("local backend: empty key")
	}
	p := filepath.Join(b.root, filepath.FromSlash(clean))
	if !strings.HasPrefix(p, b.root+string(filepath.Separator)) {
		return "", fmt.Errorf("local backend: key escapes root: %q", key)
	}
	return p, nil
}

// GetInto implements store.GetterInto: one pread(2) straight into the
// caller's buffer, with no intermediate allocation and no stream to
// drain. This is the local-disk analogue of what a GPUDirect backend
// would do into device memory, and it is what makes the seam worth
// having rather than merely tidy — see pkg/store/getinto.go.
func (b *Backend) GetInto(_ context.Context, key string, off int64, p []byte) error {
	path, err := b.path(key)
	if err != nil {
		return err
	}
	f, err := os.Open(path)
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return store.ErrNotFound
		}
		return err
	}
	defer f.Close()
	// ReadAt is documented to read len(p) bytes or return an error, which
	// is exactly GetterInto's contract — no short-read handling needed.
	if _, err := f.ReadAt(p, off); err != nil {
		return fmt.Errorf("local backend: read %s at %d: %w", key, off, err)
	}
	return nil
}

func (b *Backend) Get(_ context.Context, key string, off, length int64) (io.ReadCloser, error) {
	p, err := b.path(key)
	if err != nil {
		return nil, err
	}
	f, err := os.Open(p)
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return nil, store.ErrNotFound
		}
		return nil, err
	}
	if off > 0 {
		if _, err := f.Seek(off, io.SeekStart); err != nil {
			f.Close()
			return nil, err
		}
	}
	if length < 0 {
		return f, nil
	}
	return &limitedReadCloser{r: io.LimitReader(f, length), c: f}, nil
}

type limitedReadCloser struct {
	r io.Reader
	c io.Closer
}

func (l *limitedReadCloser) Read(p []byte) (int, error) { return l.r.Read(p) }
func (l *limitedReadCloser) Close() error               { return l.c.Close() }

func (b *Backend) Put(_ context.Context, key string, r io.Reader, size int64, o store.PutOpts) (store.ObjectInfo, error) {
	p, err := b.path(key)
	if err != nil {
		return store.ObjectInfo{}, err
	}
	if err := os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
		return store.ObjectInfo{}, err
	}

	if o.IfAbsent {
		// Conditional create: DESIGN.md §24.2's local-backend row
		// ("O_EXCL + rename"). Write to a temp file first so a
		// concurrent reader never observes a partial object, then
		// link it into place with O_EXCL as the create-only check.
		tmp, err := os.CreateTemp(filepath.Dir(p), ".atlas-tmp-*")
		if err != nil {
			return store.ObjectInfo{}, err
		}
		defer os.Remove(tmp.Name())
		n, err := io.Copy(tmp, r)
		if err != nil {
			tmp.Close()
			return store.ObjectInfo{}, err
		}
		if err := tmp.Sync(); err != nil {
			tmp.Close()
			return store.ObjectInfo{}, err
		}
		tmp.Close()
		if err := os.Link(tmp.Name(), p); err != nil {
			if errors.Is(err, fs.ErrExist) {
				return store.ObjectInfo{}, fmt.Errorf("local backend: %w: %s", os.ErrExist, key)
			}
			return store.ObjectInfo{}, err
		}
		info, err := os.Stat(p)
		if err != nil {
			return store.ObjectInfo{}, err
		}
		return store.ObjectInfo{Key: key, Size: n, ModTime: info.ModTime()}, nil
	}

	// Non-conditional: atomic replace via temp + rename.
	tmp, err := os.CreateTemp(filepath.Dir(p), ".atlas-tmp-*")
	if err != nil {
		return store.ObjectInfo{}, err
	}
	defer os.Remove(tmp.Name())
	n, err := io.Copy(tmp, r)
	if err != nil {
		tmp.Close()
		return store.ObjectInfo{}, err
	}
	if err := tmp.Sync(); err != nil {
		tmp.Close()
		return store.ObjectInfo{}, err
	}
	tmp.Close()
	if err := os.Rename(tmp.Name(), p); err != nil {
		return store.ObjectInfo{}, err
	}
	info, err := os.Stat(p)
	if err != nil {
		return store.ObjectInfo{}, err
	}
	return store.ObjectInfo{Key: key, Size: n, ModTime: info.ModTime()}, nil
}

func (b *Backend) Delete(_ context.Context, keys []string) ([]store.DeleteResult, error) {
	out := make([]store.DeleteResult, len(keys))
	for i, k := range keys {
		p, err := b.path(k)
		if err == nil {
			err = os.Remove(p)
			if errors.Is(err, fs.ErrNotExist) {
				err = nil
			}
		}
		out[i] = store.DeleteResult{Key: k, Err: err}
	}
	return out, nil
}

func (b *Backend) Stat(_ context.Context, key string) (store.ObjectInfo, error) {
	p, err := b.path(key)
	if err != nil {
		return store.ObjectInfo{}, err
	}
	info, err := os.Stat(p)
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return store.ObjectInfo{}, store.ErrNotFound
		}
		return store.ObjectInfo{}, err
	}
	return store.ObjectInfo{Key: key, Size: info.Size(), ModTime: info.ModTime()}, nil
}

func (b *Backend) List(_ context.Context, prefix, cursor string, limit int) (store.ListPage, error) {
	prefixPath, err := b.path(prefix)
	// A prefix need not itself be a valid object key (it's usually a
	// directory-shaped prefix like "atlas/c/us-west-2/a8/"); only
	// reject genuine traversal attempts.
	if err != nil && !strings.Contains(err.Error(), "empty key") {
		return store.ListPage{}, err
	}
	if err != nil {
		prefixPath = b.root
	}

	var all []store.ObjectInfo
	walkRoot := prefixPath
	if info, statErr := os.Stat(walkRoot); statErr != nil || !info.IsDir() {
		walkRoot = filepath.Dir(walkRoot)
	}
	_ = filepath.WalkDir(walkRoot, func(p string, d fs.DirEntry, err error) error {
		if err != nil || d.IsDir() {
			return nil
		}
		rel, err := filepath.Rel(b.root, p)
		if err != nil {
			return nil
		}
		key := filepath.ToSlash(rel)
		if !strings.HasPrefix(key, prefix) || strings.Contains(filepath.Base(p), ".atlas-tmp-") {
			return nil
		}
		info, err := d.Info()
		if err != nil {
			return nil
		}
		all = append(all, store.ObjectInfo{Key: key, Size: info.Size(), ModTime: info.ModTime()})
		return nil
	})
	sort.Slice(all, func(i, j int) bool { return all[i].Key < all[j].Key })

	start := 0
	if cursor != "" {
		start = sort.Search(len(all), func(i int) bool { return all[i].Key > cursor })
	}
	if limit <= 0 || limit > len(all)-start {
		limit = len(all) - start
	}
	page := all[start : start+limit]
	next := ""
	if start+limit < len(all) {
		next = page[len(page)-1].Key
	}
	return store.ListPage{Keys: page, NextCursor: next}, nil
}

func (b *Backend) Caps() store.Caps {
	return store.Caps{
		ConditionalPut:  true,
		MultipartUpload: false,
		BatchDelete:     0,
		MaxObjectSize:   1 << 40,
		StorageTiers:    nil,
		// Local disk: no egress or request cost. Kept as zeros so the
		// cost model (DESIGN.md §12) reports $0 for local-backend repos
		// rather than a hardcoded S3 estimate.
	}
}
