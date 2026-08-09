package repo

import (
	"bytes"
	"context"
	"errors"
	"testing"

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
