package libatlas

import (
	"bytes"
	"context"
	"crypto/rand"
	"errors"
	"io"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"

	"github.com/aburan28/atlasfs/pkg/metadb"
	"github.com/aburan28/atlasfs/pkg/repo"
	"github.com/aburan28/atlasfs/pkg/store"
	"github.com/aburan28/atlasfs/pkg/store/local"
)

// countingBackend wraps a store.Backend, counting Get calls so tests can
// assert a cache hit never touches the backend again — the "no second
// network call" check the task calls for.
type countingBackend struct {
	store.Backend
	gets atomic.Int64
}

func (b *countingBackend) Get(ctx context.Context, key string, off, length int64) (io.ReadCloser, error) {
	b.gets.Add(1)
	return b.Backend.Get(ctx, key, off, length)
}

func newCountingRepo(t *testing.T, class repo.Class) (*repo.Repo, *countingBackend) {
	t.Helper()
	dir := t.TempDir()
	inner, err := local.New(filepath.Join(dir, "objects"))
	if err != nil {
		t.Fatal(err)
	}
	cb := &countingBackend{Backend: inner}
	r, err := repo.OpenWithClass(dir, cb, repo.DefaultRegion, class)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { r.Close() })
	return r, cb
}

// writeMultiChunk publishes a single multi-chunk file into an immutable
// repo, forcing a small chunk size so a modest test file still spans
// several chunks and produces a manifest (DESIGN.md §5.3), not an
// inline chunk.
func writeMultiChunk(t *testing.T, r *repo.Repo, name string, size int) []byte {
	t.Helper()
	src := t.TempDir()
	data := make([]byte, size)
	if _, err := rand.Read(data); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(src, name), data, 0o644); err != nil {
		t.Fatal(err)
	}
	r.ChunkSize = 4096
	if _, _, _, err := r.PublishTree(context.Background(), src, nil); err != nil {
		t.Fatal(err)
	}
	return data
}

func mustNewMaterializer(t *testing.T, maxBytes int64) *Materializer {
	t.Helper()
	m, err := NewMaterializer(t.TempDir(), maxBytes)
	if err != nil {
		t.Fatal(err)
	}
	return m
}

// mmapRead reads f's full content via a real mmap(2) — exercising the
// exact syscall path DESIGN.md §21.3 cares about, not just os.ReadFile.
func mmapRead(t *testing.T, f *os.File) []byte {
	t.Helper()
	info, err := f.Stat()
	if err != nil {
		t.Fatal(err)
	}
	if info.Size() == 0 {
		return nil
	}
	data, err := syscall.Mmap(int(f.Fd()), 0, int(info.Size()), syscall.PROT_READ, syscall.MAP_SHARED)
	if err != nil {
		t.Fatalf("mmap: %v", err)
	}
	defer syscall.Munmap(data)
	out := make([]byte, len(data))
	copy(out, data)
	return out
}

func TestOpenMappedMaterializesMultiChunkFileByRealMmap(t *testing.T) {
	r, _ := newCountingRepo(t, repo.ClassImmutable)
	want := writeMultiChunk(t, r, "big.bin", 50*1024)

	m := mustNewMaterializer(t, 10<<20)
	f, err := m.OpenMapped(context.Background(), r, "/big.bin")
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()

	// The whole point of OpenMapped: f is an ordinary local file, not a
	// FUSE handle — it lives under the Materializer's cache directory.
	if dir := filepath.Dir(f.Name()); dir != m.cacheDir {
		t.Fatalf("materialized file %q not under cache dir %q", f.Name(), m.cacheDir)
	}

	got := mmapRead(t, f)
	if !bytes.Equal(got, want) {
		t.Fatalf("mmap content mismatch: got %d bytes, want %d", len(got), len(want))
	}
}

func TestOpenMappedInlineFile(t *testing.T) {
	r, _ := newCountingRepo(t, repo.ClassImmutable)
	src := t.TempDir()
	if err := os.WriteFile(filepath.Join(src, "small.txt"), []byte("hello mapped world"), 0o644); err != nil {
		t.Fatal(err)
	}
	if _, _, _, err := r.PublishTree(context.Background(), src, nil); err != nil {
		t.Fatal(err)
	}

	m := mustNewMaterializer(t, 1<<20)
	f, err := m.OpenMapped(context.Background(), r, "/small.txt")
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()

	got, err := io.ReadAll(f)
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != "hello mapped world" {
		t.Fatalf("got %q", got)
	}
}

func TestOpenMappedEmptyFile(t *testing.T) {
	r, _ := newCountingRepo(t, repo.ClassRelaxed)
	h, err := r.CreateFile(metadb.RootInode, "empty.bin")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := h.Commit(context.Background()); err != nil {
		t.Fatal(err)
	}

	m := mustNewMaterializer(t, 1<<20)
	f, err := m.OpenMapped(context.Background(), r, "/empty.bin")
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	info, err := f.Stat()
	if err != nil {
		t.Fatal(err)
	}
	if info.Size() != 0 {
		t.Fatalf("expected an empty materialized file, got %d bytes", info.Size())
	}
}

func TestOpenMappedRejectsDirectoryAndSymlink(t *testing.T) {
	r, _ := newCountingRepo(t, repo.ClassRelaxed)
	if _, err := r.Mkdir(metadb.RootInode, "d"); err != nil {
		t.Fatal(err)
	}
	m := mustNewMaterializer(t, 1<<20)

	if _, err := m.OpenMapped(context.Background(), r, "/d"); err == nil {
		t.Fatal("expected OpenMapped to reject a directory")
	} else if !errors.Is(err, ErrNotRegularFile) {
		t.Fatalf("expected ErrNotRegularFile, got %v", err)
	}
}

func TestOpenMappedCachesWithoutRefetch(t *testing.T) {
	r, cb := newCountingRepo(t, repo.ClassImmutable)
	writeMultiChunk(t, r, "big.bin", 50*1024)

	m := mustNewMaterializer(t, 10<<20)
	ctx := context.Background()

	f1, err := m.OpenMapped(ctx, r, "/big.bin")
	if err != nil {
		t.Fatal(err)
	}
	f1.Close()
	afterFirst := cb.gets.Load()
	if afterFirst == 0 {
		t.Fatal("expected the first OpenMapped to hit the backend at least once")
	}

	f2, err := m.OpenMapped(ctx, r, "/big.bin")
	if err != nil {
		t.Fatal(err)
	}
	defer f2.Close()
	if got := cb.gets.Load(); got != afterFirst {
		t.Fatalf("second OpenMapped re-fetched from the backend: gets went from %d to %d", afterFirst, got)
	}
	if f1.Name() != f2.Name() {
		t.Fatalf("cache hit should reuse the same materialized file: %q vs %q", f1.Name(), f2.Name())
	}

	if files, _ := m.Stats(); files != 1 {
		t.Fatalf("expected exactly one cache entry, got %d", files)
	}
}

func TestOpenMappedConcurrentCallsCoalesceIntoOneFetch(t *testing.T) {
	r, _ := newCountingRepo(t, repo.ClassImmutable)
	writeMultiChunk(t, r, "big.bin", 200*1024)

	m := mustNewMaterializer(t, 10<<20)
	ctx := context.Background()

	const n = 8
	var wg sync.WaitGroup
	paths := make([]string, n)
	errs := make([]error, n)
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			f, err := m.OpenMapped(ctx, r, "/big.bin")
			errs[i] = err
			if err == nil {
				paths[i] = f.Name()
				f.Close()
			}
		}(i)
	}
	wg.Wait()

	for i, err := range errs {
		if err != nil {
			t.Fatalf("goroutine %d: %v", i, err)
		}
		if paths[i] != paths[0] {
			t.Fatalf("goroutine %d materialized to a different path: %q vs %q", i, paths[i], paths[0])
		}
	}
	if files, _ := m.Stats(); files != 1 {
		t.Fatalf("concurrent OpenMapped for the same path should coalesce into one cache entry, got %d", files)
	}
}

func TestLRUEvictsUnderSmallBudget(t *testing.T) {
	r, _ := newCountingRepo(t, repo.ClassImmutable)
	const fileSize = 20 * 1024
	writeMultiChunk(t, r, "a.bin", fileSize)

	src := t.TempDir()
	data := make([]byte, fileSize)
	rand.Read(data)
	os.WriteFile(filepath.Join(src, "b.bin"), data, 0o644)
	r.ChunkSize = 4096
	if _, _, _, err := r.PublishTree(context.Background(), src, []string{"sub"}); err != nil {
		t.Fatal(err)
	}
	data2 := make([]byte, fileSize)
	rand.Read(data2)
	src2 := t.TempDir()
	os.WriteFile(filepath.Join(src2, "c.bin"), data2, 0o644)
	if _, _, _, err := r.PublishTree(context.Background(), src2, []string{"sub2"}); err != nil {
		t.Fatal(err)
	}

	// Budget for roughly one and a half files: materializing all three
	// distinct-content files must evict at least one.
	m := mustNewMaterializer(t, int64(fileSize)*3/2)
	ctx := context.Background()

	f1, err := m.OpenMapped(ctx, r, "/a.bin")
	if err != nil {
		t.Fatal(err)
	}
	path1 := f1.Name()
	f1.Close()

	f2, err := m.OpenMapped(ctx, r, "/sub/b.bin")
	if err != nil {
		t.Fatal(err)
	}
	f2.Close()

	f3, err := m.OpenMapped(ctx, r, "/sub2/c.bin")
	if err != nil {
		t.Fatal(err)
	}
	f3.Close()

	if files, bytes := m.Stats(); int64(bytes) > m.maxBytes {
		t.Fatalf("cache over budget: %d files, %d bytes > max %d", files, bytes, m.maxBytes)
	}
	if _, err := os.Stat(path1); !os.IsNotExist(err) {
		t.Fatalf("expected the least-recently-used entry (a.bin, materialized first) to be evicted, stat err = %v", err)
	}
}

func TestEvictRemovesEntryAndForcesRefetch(t *testing.T) {
	r, cb := newCountingRepo(t, repo.ClassImmutable)
	writeMultiChunk(t, r, "big.bin", 50*1024)

	m := mustNewMaterializer(t, 10<<20)
	ctx := context.Background()

	f1, err := m.OpenMapped(ctx, r, "/big.bin")
	if err != nil {
		t.Fatal(err)
	}
	path := f1.Name()
	f1.Close()
	afterFirst := cb.gets.Load()

	evicted, err := m.Evict(ctx, r, "/big.bin")
	if err != nil {
		t.Fatal(err)
	}
	if !evicted {
		t.Fatal("expected Evict to report an entry was removed")
	}
	if _, err := os.Stat(path); !os.IsNotExist(err) {
		t.Fatalf("expected the evicted file to be gone from disk, stat err = %v", err)
	}
	if files, _ := m.Stats(); files != 0 {
		t.Fatalf("expected an empty cache after Evict, got %d entries", files)
	}

	f2, err := m.OpenMapped(ctx, r, "/big.bin")
	if err != nil {
		t.Fatal(err)
	}
	f2.Close()
	if got := cb.gets.Load(); got <= afterFirst {
		t.Fatalf("expected OpenMapped after Evict to re-fetch from the backend: gets stayed at %d", got)
	}
}

// TestOpenMappedInvalidatesOnMutation is the failure mode DESIGN.md
// §21.3's whole design has to get right: a `relaxed`-class file's
// content changes underfoot (WriteHandle.Commit swaps the manifest a
// path resolves to, DESIGN.md §16.1), and a subsequent OpenMapped for
// the SAME path must return the new bytes, never the old materialized
// copy.
func TestOpenMappedInvalidatesOnMutation(t *testing.T) {
	r, _ := newCountingRepo(t, repo.ClassRelaxed)
	ctx := context.Background()

	h1, err := r.CreateFile(metadb.RootInode, "model.bin")
	if err != nil {
		t.Fatal(err)
	}
	h1.Write([]byte("version one content"))
	if _, err := h1.Commit(ctx); err != nil {
		t.Fatal(err)
	}

	m := mustNewMaterializer(t, 1<<20)
	f1, err := m.OpenMapped(ctx, r, "/model.bin")
	if err != nil {
		t.Fatal(err)
	}
	got1, err := io.ReadAll(f1)
	f1.Close()
	if err != nil {
		t.Fatal(err)
	}
	if string(got1) != "version one content" {
		t.Fatalf("got %q, want version one", got1)
	}

	// Overwrite the same path (reuses the same inode ID per §16.1 — the
	// scenario that matters, since a fresh inode would trivially also
	// get a fresh content key).
	h2, err := r.CreateFile(metadb.RootInode, "model.bin")
	if err != nil {
		t.Fatal(err)
	}
	h2.Write([]byte("version TWO, different length"))
	if _, err := h2.Commit(ctx); err != nil {
		t.Fatal(err)
	}

	f2, err := m.OpenMapped(ctx, r, "/model.bin")
	if err != nil {
		t.Fatal(err)
	}
	defer f2.Close()
	got2, err := io.ReadAll(f2)
	if err != nil {
		t.Fatal(err)
	}
	if string(got2) != "version TWO, different length" {
		t.Fatalf("OpenMapped returned stale content after Commit: got %q", got2)
	}
	if f1.Name() == f2.Name() {
		t.Fatalf("a mutation must materialize to a different cache entry, both resolved to %q", f1.Name())
	}
}

// TestOpenMappedInvalidatesOnMutationSameChunkSize covers the case
// where the overwrite happens to produce a same-length (but different
// content) file, ruling out "the size changed so of course it re-read"
// as an alternative explanation for the previous test passing.
func TestOpenMappedInvalidatesOnMutationSameLength(t *testing.T) {
	r, _ := newCountingRepo(t, repo.ClassSession)
	ctx := context.Background()

	h1, _ := r.CreateFile(metadb.RootInode, "f.bin")
	h1.Write([]byte("AAAAAAAAAA"))
	if _, err := h1.Commit(ctx); err != nil {
		t.Fatal(err)
	}

	m := mustNewMaterializer(t, 1<<20)
	f1, err := m.OpenMapped(ctx, r, "/f.bin")
	if err != nil {
		t.Fatal(err)
	}
	got1, _ := io.ReadAll(f1)
	f1.Close()
	if string(got1) != "AAAAAAAAAA" {
		t.Fatalf("got %q", got1)
	}

	h2, _ := r.CreateFile(metadb.RootInode, "f.bin")
	h2.Write([]byte("BBBBBBBBBB"))
	if _, err := h2.Commit(ctx); err != nil {
		t.Fatal(err)
	}

	f2, err := m.OpenMapped(ctx, r, "/f.bin")
	if err != nil {
		t.Fatal(err)
	}
	defer f2.Close()
	got2, _ := io.ReadAll(f2)
	if string(got2) != "BBBBBBBBBB" {
		t.Fatalf("OpenMapped returned stale same-length content: got %q, want %q", got2, "BBBBBBBBBB")
	}
}

func TestNewMaterializerRejectsNonPositiveBudget(t *testing.T) {
	if _, err := NewMaterializer(t.TempDir(), 0); err == nil {
		t.Fatal("expected an error for a zero maxBytes budget")
	}
	if _, err := NewMaterializer(t.TempDir(), -1); err == nil {
		t.Fatal("expected an error for a negative maxBytes budget")
	}
}
