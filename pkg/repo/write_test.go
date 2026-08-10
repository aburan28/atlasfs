package repo

import (
	"bytes"
	"context"
	"errors"
	"testing"

	"github.com/aburan28/atlasfs/pkg/chunk"
	"github.com/aburan28/atlasfs/pkg/metadb"
	"github.com/aburan28/atlasfs/pkg/store/local"
)

func openRelaxed(t *testing.T) *Repo {
	t.Helper()
	dir := t.TempDir()
	backend, err := local.New(dir + "/objects")
	if err != nil {
		t.Fatal(err)
	}
	r, err := OpenWithClass(dir, backend, DefaultRegion, ClassRelaxed)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { r.Close() })
	return r
}

func TestImmutableRepoRejectsWrites(t *testing.T) {
	r, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()

	if _, err := r.CreateFile(metadb.RootInode, "f.txt"); !errors.Is(err, ErrReadOnly) {
		t.Fatalf("expected ErrReadOnly, got %v", err)
	}
	if err := r.Unlink(metadb.RootInode, "f.txt"); !errors.Is(err, ErrReadOnly) {
		t.Fatalf("expected ErrReadOnly, got %v", err)
	}
	if _, err := r.Mkdir(metadb.RootInode, "d"); !errors.Is(err, ErrReadOnly) {
		t.Fatalf("expected ErrReadOnly, got %v", err)
	}
	if err := r.Rmdir(metadb.RootInode, "d"); !errors.Is(err, ErrReadOnly) {
		t.Fatalf("expected ErrReadOnly, got %v", err)
	}
}

func TestCreateFileWriteCommitRead(t *testing.T) {
	r := openRelaxed(t)
	ctx := context.Background()

	h, err := r.CreateFile(metadb.RootInode, "f.txt")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := h.Write([]byte("hello ")); err != nil {
		t.Fatal(err)
	}
	if _, err := h.Write([]byte("relaxed world")); err != nil {
		t.Fatal(err)
	}
	id, err := h.Commit(ctx)
	if err != nil {
		t.Fatal(err)
	}

	_, rec, err := r.Resolve("/f.txt")
	if err != nil {
		t.Fatal(err)
	}
	if !rec.HasInline {
		t.Fatal("a small write should still be inlined")
	}
	fr, err := r.OpenFile(ctx, rec)
	if err != nil {
		t.Fatal(err)
	}
	got, err := fr.ReadAll()
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != "hello relaxed world" {
		t.Fatalf("got %q, want %q", got, "hello relaxed world")
	}

	inode2, _, err := r.Resolve("/f.txt")
	if err != nil {
		t.Fatal(err)
	}
	if inode2 != id {
		t.Fatalf("Resolve's inode (%d) should match Commit's returned inode (%d)", inode2, id)
	}
}

func TestOverwriteReusesInodeAndInvalidatesLease(t *testing.T) {
	r := openRelaxed(t)
	ctx := context.Background()

	h1, _ := r.CreateFile(metadb.RootInode, "f.txt")
	h1.Write([]byte("version one"))
	id1, err := h1.Commit(ctx)
	if err != nil {
		t.Fatal(err)
	}

	// Simulate two readers of this inode, the way fuseserver will (task
	// 20) — DESIGN.md §16.1: overwrite must update the SAME inode in
	// place, which is what makes a cached lease on it meaningful to
	// invalidate rather than simply orphaned. "subscribed" mirrors a
	// holder that registered for push invalidation (best-effort,
	// §10.2); "unsubscribed" mirrors one relying on lease expiry alone.
	v0 := r.Coherence.Grant("subscribed", InodeCoherenceKey(id1))
	r.Coherence.Grant("unsubscribed", InodeCoherenceKey(id1))
	invalidated := false
	if !r.Coherence.Subscribe("subscribed", InodeCoherenceKey(id1), func() { invalidated = true }) {
		t.Fatal("subscribe should succeed under the registry cap")
	}

	h2, _ := r.CreateFile(metadb.RootInode, "f.txt")
	h2.Write([]byte("version two, longer"))
	id2, err := h2.Commit(ctx)
	if err != nil {
		t.Fatal(err)
	}

	if id2 != id1 {
		t.Fatalf("overwrite should reuse the existing inode: got %d, want %d", id2, id1)
	}

	if !invalidated {
		t.Fatal("overwrite must push-invalidate a subscribed lease on the file's inode key (§10.2)")
	}
	// §10.2's actual guarantee: correctness must not depend on push.
	// An unsubscribed holder's lease survives the Bump — it is still
	// serving a stale-but-bounded read, not a bug (mirrors
	// pkg/coherence's own TestUnsubscribedHolderStaysBoundedStaleUntilExpiry).
	if _, ok := r.Coherence.TrustedVersion("unsubscribed", InodeCoherenceKey(id1)); !ok {
		t.Fatal("an unsubscribed holder's lease must remain valid until its own expiry, even across an overwrite")
	}

	v1 := r.Coherence.Grant("subscribed", InodeCoherenceKey(id1))
	if v1 <= v0 {
		t.Fatalf("version must advance across an overwrite: v0=%d v1=%d", v0, v1)
	}

	_, rec, err := r.Resolve("/f.txt")
	if err != nil {
		t.Fatal(err)
	}
	fr, err := r.OpenFile(ctx, rec)
	if err != nil {
		t.Fatal(err)
	}
	got, err := fr.ReadAll()
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != "version two, longer" {
		t.Fatalf("got %q, want the overwritten content", got)
	}
}

func TestUnlinkRemovesNameAndBumpsDirver(t *testing.T) {
	r := openRelaxed(t)
	ctx := context.Background()

	h, _ := r.CreateFile(metadb.RootInode, "f.txt")
	h.Write([]byte("x"))
	if _, err := h.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	before := r.Coherence.DirVersion(DirCoherenceKey(metadb.RootInode))

	if err := r.Unlink(metadb.RootInode, "f.txt"); err != nil {
		t.Fatal(err)
	}
	after := r.Coherence.DirVersion(DirCoherenceKey(metadb.RootInode))
	if after == before {
		t.Fatal("Unlink should bump the parent directory's version")
	}

	if _, _, err := r.Resolve("/f.txt"); err == nil {
		t.Fatal("expected the file to be gone after Unlink")
	}
}

func TestUnlinkNonexistentFails(t *testing.T) {
	r := openRelaxed(t)
	if err := r.Unlink(metadb.RootInode, "nope.txt"); err == nil {
		t.Fatal("expected an error unlinking a name that was never bound")
	}
}

func TestMkdirRmdir(t *testing.T) {
	r := openRelaxed(t)

	id, err := r.Mkdir(metadb.RootInode, "sub")
	if err != nil {
		t.Fatal(err)
	}
	inode, rec, err := r.Resolve("/sub")
	if err != nil {
		t.Fatal(err)
	}
	if inode != id || !rec.IsDir {
		t.Fatalf("mkdir result mismatch: inode=%d id=%d isDir=%v", inode, id, rec.IsDir)
	}

	if err := r.Rmdir(metadb.RootInode, "sub"); err != nil {
		t.Fatal(err)
	}
	if _, _, err := r.Resolve("/sub"); err == nil {
		t.Fatal("expected /sub to be gone after Rmdir")
	}
}

func TestRmdirRefusesNonEmpty(t *testing.T) {
	r := openRelaxed(t)
	ctx := context.Background()

	if _, err := r.Mkdir(metadb.RootInode, "sub"); err != nil {
		t.Fatal(err)
	}
	subInode, _, err := r.Resolve("/sub")
	if err != nil {
		t.Fatal(err)
	}
	h, _ := r.CreateFile(subInode, "f.txt")
	h.Write([]byte("x"))
	if _, err := h.Commit(ctx); err != nil {
		t.Fatal(err)
	}

	if err := r.Rmdir(metadb.RootInode, "sub"); err == nil {
		t.Fatal("expected Rmdir to refuse a non-empty directory")
	}
}

func TestCommitAfterDiscardOrCommitFails(t *testing.T) {
	r := openRelaxed(t)
	ctx := context.Background()

	h, _ := r.CreateFile(metadb.RootInode, "a.txt")
	h.Write([]byte("x"))
	if _, err := h.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := h.Commit(ctx); err == nil {
		t.Fatal("expected a second Commit on the same handle to fail")
	}

	h2, _ := r.CreateFile(metadb.RootInode, "b.txt")
	h2.Discard()
	if _, err := h2.Write([]byte("x")); err == nil {
		t.Fatal("expected Write after Discard to fail")
	}
	if _, _, err := r.Resolve("/b.txt"); err == nil {
		t.Fatal("a discarded handle must not create anything")
	}
}

func TestCommitRefusesToOverwriteADirectory(t *testing.T) {
	r := openRelaxed(t)
	ctx := context.Background()

	if _, err := r.Mkdir(metadb.RootInode, "d"); err != nil {
		t.Fatal(err)
	}
	h, _ := r.CreateFile(metadb.RootInode, "d")
	h.Write([]byte("x"))
	if _, err := h.Commit(ctx); err == nil {
		t.Fatal("expected Commit to refuse overwriting a directory with a file")
	}
}

func TestMkdirRefusesExistingName(t *testing.T) {
	r := openRelaxed(t)
	ctx := context.Background()

	h, _ := r.CreateFile(metadb.RootInode, "f.txt")
	h.Write([]byte("x"))
	if _, err := h.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := r.Mkdir(metadb.RootInode, "f.txt"); err == nil {
		t.Fatal("expected Mkdir to refuse a name that already exists as a file")
	}

	if _, err := r.Mkdir(metadb.RootInode, "d"); err != nil {
		t.Fatal(err)
	}
	if _, err := r.Mkdir(metadb.RootInode, "d"); err == nil {
		t.Fatal("expected Mkdir to refuse a name that already exists as a directory")
	}
}

func TestEmptyWriteProducesZeroLengthFile(t *testing.T) {
	r := openRelaxed(t)
	ctx := context.Background()

	h, _ := r.CreateFile(metadb.RootInode, "empty.txt")
	if _, err := h.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	_, rec, err := r.Resolve("/empty.txt")
	if err != nil {
		t.Fatal(err)
	}
	if rec.Size != 0 || rec.HasInline || rec.HasManifest {
		t.Fatalf("expected an empty content record, got %+v", rec)
	}
}

func TestWriteHandleBuffersMultiChunkContent(t *testing.T) {
	r := openRelaxed(t)
	r.ChunkSize = 4096
	ctx := context.Background()

	data := bytes.Repeat([]byte("relaxed-write-"), 1000) // several chunks
	h, _ := r.CreateFile(metadb.RootInode, "big.bin")
	if _, err := h.Write(data); err != nil {
		t.Fatal(err)
	}
	if _, err := h.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	_, rec, err := r.Resolve("/big.bin")
	if err != nil {
		t.Fatal(err)
	}
	if !rec.HasManifest {
		t.Fatal("a multi-chunk write should produce a manifest")
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
		t.Fatal("multi-chunk write content mismatch on read-back")
	}
}

// TestQuotaRejectsOverByteLimitLeavesNoTrace exercises the byte-limit
// half of DESIGN.md §18.3 through the public Repo API: a write that
// would exceed the limit fails with ErrQuotaExceeded, and — because
// Commit checks quota before it ever chunks/stores content (see
// Commit's doc comment) — leaves no dangling chunk locator and no
// partial inode/dentry behind.
func TestQuotaRejectsOverByteLimitLeavesNoTrace(t *testing.T) {
	r := openRelaxed(t)
	ctx := context.Background()

	if err := r.SetQuota(10, 0); err != nil {
		t.Fatal(err)
	}

	payload := []byte("this payload is well over ten bytes")
	h, err := r.CreateFile(metadb.RootInode, "big.bin")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := h.Write(payload); err != nil {
		t.Fatal(err)
	}
	if _, err := h.Commit(ctx); !errors.Is(err, ErrQuotaExceeded) {
		t.Fatalf("expected ErrQuotaExceeded, got %v", err)
	}

	if _, _, err := r.Resolve("/big.bin"); !errors.Is(err, metadb.ErrNotFound) {
		t.Fatalf("expected the rejected write to leave no dentry, got %v", err)
	}
	// payload is small enough that storeContent would inline it as a
	// single chunk whose ID is Sum(payload) — if Commit had chunked and
	// stored it before the quota check, a locator for that chunk would
	// now exist despite nothing referencing it.
	if found, err := r.DB.HasLocator(r.Region, chunk.Sum(payload)); err != nil {
		t.Fatal(err)
	} else if found {
		t.Fatal("a rejected write must not leave a dangling chunk locator behind")
	}
	bytesUsed, inodesUsed, err := r.QuotaUsage()
	if err != nil {
		t.Fatal(err)
	}
	if bytesUsed != 0 || inodesUsed != 0 {
		t.Fatalf("usage after a rejected write = bytes=%d inodes=%d, want 0,0", bytesUsed, inodesUsed)
	}
}

// TestQuotaRejectsMkdirOverInodeLimit exercises the inode-limit half of
// DESIGN.md §18.3: Mkdir past the inode limit fails with
// ErrQuotaExceeded and creates nothing.
func TestQuotaRejectsMkdirOverInodeLimit(t *testing.T) {
	r := openRelaxed(t)

	if err := r.SetQuota(0, 1); err != nil {
		t.Fatal(err)
	}
	if _, err := r.Mkdir(metadb.RootInode, "first"); err != nil {
		t.Fatalf("first mkdir within the inode limit should succeed, got %v", err)
	}
	if _, err := r.Mkdir(metadb.RootInode, "second"); !errors.Is(err, ErrQuotaExceeded) {
		t.Fatalf("expected ErrQuotaExceeded, got %v", err)
	}
	if _, _, err := r.Resolve("/second"); !errors.Is(err, metadb.ErrNotFound) {
		t.Fatalf("expected the rejected mkdir to leave nothing behind, got %v", err)
	}
}

// TestQuotaOverwriteChargesOnlyDelta exercises the overwrite-accounting
// requirement through the Repo API: overwriting a file must charge only
// the size delta, never the old content's bytes a second time.
func TestQuotaOverwriteChargesOnlyDelta(t *testing.T) {
	r := openRelaxed(t)
	ctx := context.Background()

	if err := r.SetQuota(15, 0); err != nil {
		t.Fatal(err)
	}

	h1, _ := r.CreateFile(metadb.RootInode, "f.txt")
	h1.Write(bytes.Repeat([]byte("a"), 10))
	if _, err := h1.Commit(ctx); err != nil {
		t.Fatalf("initial 10-byte write within limit should succeed, got %v", err)
	}
	if bytesUsed, _, _ := r.QuotaUsage(); bytesUsed != 10 {
		t.Fatalf("bytesUsed after create = %d, want 10", bytesUsed)
	}

	// 10 -> 12 bytes: a delta of 2, landing at 12 total (well within 15).
	// Double-counting the old 10 bytes would put this at 22 and wrongly
	// reject it.
	h2, _ := r.CreateFile(metadb.RootInode, "f.txt")
	h2.Write(bytes.Repeat([]byte("b"), 12))
	if _, err := h2.Commit(ctx); err != nil {
		t.Fatalf("overwrite within limit should succeed, got %v", err)
	}
	bytesUsed, _, err := r.QuotaUsage()
	if err != nil {
		t.Fatal(err)
	}
	if bytesUsed != 12 {
		t.Fatalf("bytesUsed after overwrite = %d, want 12 (delta-only accounting)", bytesUsed)
	}
}

// TestQuotaUsageMatchesContentAcrossWritesAndUnlinks is the accounting
// end-to-end check DESIGN.md §18.3 calls for: after a sequence of
// creates, an overwrite, a directory, and an unlink, QuotaUsage must
// match what is actually still reachable, not just monotonically grow.
func TestQuotaUsageMatchesContentAcrossWritesAndUnlinks(t *testing.T) {
	r := openRelaxed(t)
	ctx := context.Background()

	write := func(name string, n int) {
		t.Helper()
		h, err := r.CreateFile(metadb.RootInode, name)
		if err != nil {
			t.Fatal(err)
		}
		h.Write(bytes.Repeat([]byte("x"), n))
		if _, err := h.Commit(ctx); err != nil {
			t.Fatal(err)
		}
	}

	write("a.txt", 5)
	write("b.txt", 7)
	if _, err := r.Mkdir(metadb.RootInode, "d"); err != nil {
		t.Fatal(err)
	}
	bytesUsed, inodesUsed, err := r.QuotaUsage()
	if err != nil {
		t.Fatal(err)
	}
	if bytesUsed != 12 || inodesUsed != 3 {
		t.Fatalf("usage after 2 writes + mkdir = bytes=%d inodes=%d, want 12,3", bytesUsed, inodesUsed)
	}

	// Overwrite a.txt from 5 to 3 bytes: usage should shrink by 2, not
	// grow.
	write("a.txt", 3)
	if bytesUsed, _, _ := r.QuotaUsage(); bytesUsed != 10 {
		t.Fatalf("bytesUsed after shrinking overwrite = %d, want 10", bytesUsed)
	}

	if err := r.Unlink(metadb.RootInode, "b.txt"); err != nil {
		t.Fatal(err)
	}
	bytesUsed, inodesUsed, err = r.QuotaUsage()
	if err != nil {
		t.Fatal(err)
	}
	if bytesUsed != 3 || inodesUsed != 2 {
		t.Fatalf("usage after unlinking b.txt = bytes=%d inodes=%d, want 3,2", bytesUsed, inodesUsed)
	}

	if err := r.Rmdir(metadb.RootInode, "d"); err != nil {
		t.Fatal(err)
	}
	bytesUsed, inodesUsed, err = r.QuotaUsage()
	if err != nil {
		t.Fatal(err)
	}
	if bytesUsed != 3 || inodesUsed != 1 {
		t.Fatalf("usage after rmdir d = bytes=%d inodes=%d, want 3,1", bytesUsed, inodesUsed)
	}
}
