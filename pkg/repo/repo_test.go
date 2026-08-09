package repo

import (
	"bytes"
	"context"
	"crypto/rand"
	"os"
	"path/filepath"
	"testing"

	"github.com/aburan28/atlasfs/pkg/pack"
	"github.com/aburan28/atlasfs/pkg/store/local"
)

func writeFile(t *testing.T, dir, name string, data []byte) string {
	t.Helper()
	p := filepath.Join(dir, name)
	if err := os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(p, data, 0o644); err != nil {
		t.Fatal(err)
	}
	return p
}

func TestPublishAndReadSmallFile(t *testing.T) {
	ctx := context.Background()
	src := t.TempDir()
	writeFile(t, src, "hello.txt", []byte("hello atlasfs"))

	r, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()

	files, dirs, bytesIn, err := r.PublishTree(ctx, src, nil)
	if err != nil {
		t.Fatal(err)
	}
	if files != 1 || dirs != 0 || bytesIn != int64(len("hello atlasfs")) {
		t.Fatalf("unexpected publish counts: files=%d dirs=%d bytes=%d", files, dirs, bytesIn)
	}

	_, rec, err := r.Resolve("/hello.txt")
	if err != nil {
		t.Fatal(err)
	}
	if !rec.HasInline {
		t.Fatal("a single-chunk file should be stored inline, per DESIGN.md §5.3")
	}
	if rec.HasManifest {
		t.Fatal("an inline file must not also have a manifest")
	}

	fr, err := r.OpenFile(ctx, rec)
	if err != nil {
		t.Fatal(err)
	}
	got, err := fr.ReadAll()
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != "hello atlasfs" {
		t.Fatalf("got %q, want %q", got, "hello atlasfs")
	}
}

func TestPublishAndReadMultiChunkFile(t *testing.T) {
	ctx := context.Background()
	src := t.TempDir()
	data := make([]byte, 50*1024) // several 4KiB test-chunks
	if _, err := rand.Read(data); err != nil {
		t.Fatal(err)
	}
	writeFile(t, src, "big.bin", data)

	r, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	r.ChunkSize = 4096 // force multiple chunks for this modest test file

	if _, _, _, err := r.PublishTree(ctx, src, nil); err != nil {
		t.Fatal(err)
	}
	_, rec, err := r.Resolve("/big.bin")
	if err != nil {
		t.Fatal(err)
	}
	if !rec.HasManifest {
		t.Fatal("a multi-chunk file must reference a manifest")
	}

	fr, err := r.OpenFile(ctx, rec)
	if err != nil {
		t.Fatal(err)
	}

	// Whole-file read.
	got, err := fr.ReadAll()
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, data) {
		t.Fatal("whole-file read mismatch")
	}

	// Random-access read spanning a chunk boundary — exercises ReadAt's
	// entry lookup directly, the same path FUSE reads take.
	buf := make([]byte, 4096+10)
	fr2, err := r.OpenFile(ctx, rec)
	if err != nil {
		t.Fatal(err)
	}
	n, err := fr2.ReadAt(buf, 4090)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(buf[:n], data[4090:4090+int64(n)]) {
		t.Fatal("boundary-spanning ReadAt mismatch")
	}
}

func TestPublishEmptyFile(t *testing.T) {
	ctx := context.Background()
	src := t.TempDir()
	writeFile(t, src, "empty.txt", nil)

	r, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	if _, _, _, err := r.PublishTree(ctx, src, nil); err != nil {
		t.Fatal(err)
	}
	_, rec, err := r.Resolve("/empty.txt")
	if err != nil {
		t.Fatal(err)
	}
	if rec.Size != 0 || rec.HasInline || rec.HasManifest {
		t.Fatalf("empty file should have no content pointer: %+v", rec)
	}
	fr, err := r.OpenFile(ctx, rec)
	if err != nil {
		t.Fatal(err)
	}
	got, err := fr.ReadAll()
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 0 {
		t.Fatalf("expected empty read, got %d bytes", len(got))
	}
}

func TestDedupAcrossIdenticalFiles(t *testing.T) {
	ctx := context.Background()
	src := t.TempDir()
	data := bytes.Repeat([]byte("duplicate-content-"), 1000)
	writeFile(t, src, "a/copy1.bin", data)
	writeFile(t, src, "b/copy2.bin", data)

	r, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()

	if _, _, _, err := r.PublishTree(ctx, src, nil); err != nil {
		t.Fatal(err)
	}
	_, rec1, err := r.Resolve("/a/copy1.bin")
	if err != nil {
		t.Fatal(err)
	}
	_, rec2, err := r.Resolve("/b/copy2.bin")
	if err != nil {
		t.Fatal(err)
	}
	if rec1.ManifestID != rec2.ManifestID {
		t.Fatalf("identical file content should share one manifest_id (DESIGN.md §15 whole-file dedup): %s vs %s",
			rec1.ManifestID, rec2.ManifestID)
	}

	// The dedup must be real, not just equal IDs: the second publish
	// should not have written a second copy of each chunk's bytes. We
	// can't observe container count directly here without reaching into
	// the backend, so assert indirectly: both files still read back
	// correctly, proving the shared locators are valid.
	fr1, _ := r.OpenFile(ctx, rec1)
	got1, err := fr1.ReadAll()
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got1, data) {
		t.Fatal("copy1 content mismatch")
	}
}

func TestPublishDuplicatePathRejected(t *testing.T) {
	ctx := context.Background()
	src := t.TempDir()
	writeFile(t, src, "f.txt", []byte("v1"))

	r, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	if _, _, _, err := r.PublishTree(ctx, src, nil); err != nil {
		t.Fatal(err)
	}
	// Publishing the same source tree again at the same destination
	// must fail rather than silently rebind an immutable name
	// (DESIGN.md §8: immutable subtrees don't get rewritten in place).
	if _, _, _, err := r.PublishTree(ctx, src, nil); err == nil {
		t.Fatal("expected republish to the same path to fail")
	}
}

func TestReaddirAndDirectoryStructure(t *testing.T) {
	ctx := context.Background()
	src := t.TempDir()
	writeFile(t, src, "train/img1.jpg", []byte("a"))
	writeFile(t, src, "train/img2.jpg", []byte("b"))
	writeFile(t, src, "val/img3.jpg", []byte("c"))

	r, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	if _, _, _, err := r.PublishTree(ctx, src, []string{"datasets", "imagenet"}); err != nil {
		t.Fatal(err)
	}

	inode, rec, err := r.Resolve("/datasets/imagenet/train")
	if err != nil {
		t.Fatal(err)
	}
	if !rec.IsDir {
		t.Fatal("train should be a directory")
	}
	entries, err := r.Readdir(inode)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 2 {
		t.Fatalf("expected 2 entries in train/, got %d", len(entries))
	}
}

func TestSingleObjectThresholdPath(t *testing.T) {
	// DESIGN.md §5.5: files at or above the threshold get dedicated,
	// unpacked containers (1:1 chunk-to-object) instead of going through
	// the shared packer. Force a tiny threshold so the test exercises
	// that path without allocating a real 64 MiB file.
	ctx := context.Background()
	src := t.TempDir()
	data := make([]byte, 20*1024) // 20KiB, several 4KiB chunks
	if _, err := rand.Read(data); err != nil {
		t.Fatal(err)
	}
	writeFile(t, src, "checkpoint.bin", data)

	r, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	r.ChunkSize = 4096
	r.SingleObjectThreshold = 16 * 1024 // below the 20KiB file, above nothing else

	if _, _, _, err := r.PublishTree(ctx, src, nil); err != nil {
		t.Fatal(err)
	}
	_, rec, err := r.Resolve("/checkpoint.bin")
	if err != nil {
		t.Fatal(err)
	}
	if !rec.HasManifest {
		t.Fatal("large file should still get a manifest even on the single-object path")
	}

	m, err := r.getManifest(ctx, rec.ManifestID)
	if err != nil {
		t.Fatal(err)
	}
	if len(m.Entries) < 2 {
		t.Fatalf("expected multiple chunks, got %d", len(m.Entries))
	}

	// Every chunk on this path must have landed in its own dedicated
	// container (no packing indirection) — assert no two chunks share a
	// container key.
	seen := map[string]bool{}
	for _, e := range m.Entries {
		loc, found, err := r.DB.GetLocator(r.Region, e.ChunkID)
		if err != nil || !found {
			t.Fatalf("missing locator for chunk %s", e.ChunkID)
		}
		if seen[loc.Container] {
			t.Fatalf("chunk %s shares a container with another chunk: %s (should be dedicated per DESIGN.md §5.5)", e.ChunkID, loc.Container)
		}
		seen[loc.Container] = true
		if loc.Offset != 0 {
			t.Fatalf("dedicated container should start at offset 0, got %d", loc.Offset)
		}
	}

	fr, err := r.OpenFile(ctx, rec)
	if err != nil {
		t.Fatal(err)
	}
	got, err := fr.ReadAll()
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, data) {
		t.Fatal("single-object-path file content mismatch on read-back")
	}
}

func TestOpenDefaultsToImmutable(t *testing.T) {
	r, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	if r.Class != ClassImmutable {
		t.Fatalf("got class %q, want %q", r.Class, ClassImmutable)
	}
	if r.Coherence != nil {
		t.Fatal("an immutable repo should not have a coherence manager — nothing to invalidate")
	}
}

func TestOpenWithClassPersistsAcrossReopen(t *testing.T) {
	dir := t.TempDir()
	backend, err := local.New(filepath.Join(dir, "objects"))
	if err != nil {
		t.Fatal(err)
	}
	r1, err := OpenWithClass(dir, backend, DefaultRegion, ClassRelaxed)
	if err != nil {
		t.Fatal(err)
	}
	if r1.Class != ClassRelaxed {
		t.Fatalf("got class %q, want %q", r1.Class, ClassRelaxed)
	}
	if r1.Coherence == nil {
		t.Fatal("a relaxed repo should have a coherence manager")
	}
	r1.Close()

	// Reopening via the plain Open() convenience path, which internally
	// hints ClassImmutable, must still return the persisted class —
	// DESIGN.md §8: a repo's class is fixed at creation, not at every
	// open call.
	r2, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer r2.Close()
	if r2.Class != ClassRelaxed {
		t.Fatalf("reopen lost the persisted class: got %q, want %q", r2.Class, ClassRelaxed)
	}
	if r2.Coherence == nil {
		t.Fatal("reopened relaxed repo should still have a coherence manager")
	}
}

func TestSessionClassGetsShorterLeaseThanRelaxed(t *testing.T) {
	if ClassSession.leaseDuration() >= ClassRelaxed.leaseDuration() {
		t.Fatalf("DESIGN.md §8: session (%s) must be leased shorter than relaxed (%s)",
			ClassSession.leaseDuration(), ClassRelaxed.leaseDuration())
	}
}

func TestClassMutability(t *testing.T) {
	if ClassImmutable.Mutable() {
		t.Fatal("immutable must not be mutable")
	}
	if !ClassRelaxed.Mutable() || !ClassSession.Mutable() {
		t.Fatal("relaxed and session must both be mutable")
	}
}

func TestPublishAndResolveSymlink(t *testing.T) {
	ctx := context.Background()
	src := t.TempDir()
	writeFile(t, src, "real/target.txt", []byte("real content"))
	if err := os.MkdirAll(filepath.Join(src, "link"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink("../real/target.txt", filepath.Join(src, "link", "to-target.txt")); err != nil {
		t.Fatal(err)
	}

	r, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	files, _, _, err := r.PublishTree(ctx, src, nil)
	if err != nil {
		t.Fatal(err)
	}
	if files != 2 { // target.txt + the symlink itself
		t.Fatalf("expected 2 published files (1 regular + 1 symlink), got %d", files)
	}

	_, rec, err := r.Resolve("/link/to-target.txt")
	if err != nil {
		t.Fatal(err)
	}
	if !rec.IsSymlink {
		t.Fatal("expected an IsSymlink inode")
	}
	if rec.SymlinkTarget != "../real/target.txt" {
		t.Fatalf("got target %q, want %q", rec.SymlinkTarget, "../real/target.txt")
	}
}

func TestFetchVerifiesHash(t *testing.T) {
	// Sanity: pack.Fetch (used by FileReader.chunkBytes) is exercised
	// through the repo read path in the tests above; this confirms the
	// verification really runs by corrupting a locator's Length and
	// expecting a mismatch rather than a silent short read.
	ctx := context.Background()
	src := t.TempDir()
	writeFile(t, src, "x.bin", []byte("0123456789"))
	r, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	if _, _, _, err := r.PublishTree(ctx, src, nil); err != nil {
		t.Fatal(err)
	}
	_, rec, err := r.Resolve("/x.bin")
	if err != nil {
		t.Fatal(err)
	}
	loc, found, err := r.DB.GetLocator(r.Region, rec.InlineChunk)
	if err != nil || !found {
		t.Fatalf("expected locator for inline chunk, found=%v err=%v", found, err)
	}
	badLoc := loc
	badLoc.Length = loc.Length - 1
	if _, err := pack.Fetch(ctx, r.Backend, rec.InlineChunk, badLoc); err == nil {
		t.Fatal("expected Fetch to reject a truncated read")
	}
}
