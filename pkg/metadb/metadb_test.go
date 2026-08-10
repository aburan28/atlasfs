package metadb

import (
	"errors"
	"path/filepath"
	"testing"

	"github.com/aburan28/atlasfs/pkg/chunk"
	"github.com/aburan28/atlasfs/pkg/pack"
)

func openTestDB(t *testing.T) *DB {
	t.Helper()
	db, err := Open(filepath.Join(t.TempDir(), "meta.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	return db
}

func TestRootExists(t *testing.T) {
	db := openTestDB(t)
	rec, err := db.GetInode(RootInode)
	if err != nil {
		t.Fatal(err)
	}
	if !rec.IsDir {
		t.Fatal("root inode should be a directory")
	}
}

func TestEnsureDirAndResolve(t *testing.T) {
	db := openTestDB(t)
	leaf, err := db.EnsureDir([]string{"datasets", "imagenet", "train"})
	if err != nil {
		t.Fatal(err)
	}
	inode, rec, err := db.Resolve([]string{"datasets", "imagenet", "train"})
	if err != nil {
		t.Fatal(err)
	}
	if inode != leaf || !rec.IsDir {
		t.Fatalf("resolve mismatch: inode=%d leaf=%d isDir=%v", inode, leaf, rec.IsDir)
	}

	// idempotent
	leaf2, err := db.EnsureDir([]string{"datasets", "imagenet", "train"})
	if err != nil {
		t.Fatal(err)
	}
	if leaf2 != leaf {
		t.Fatalf("EnsureDir not idempotent: %d vs %d", leaf, leaf2)
	}
}

func TestCreateDentryRejectsDuplicateName(t *testing.T) {
	db := openTestDB(t)
	id1, _ := db.AllocInode()
	id2, _ := db.AllocInode()
	if err := db.PutInode(id1, InodeRecord{Mode: 0o644, Size: 1}); err != nil {
		t.Fatal(err)
	}
	if err := db.PutInode(id2, InodeRecord{Mode: 0o644, Size: 2}); err != nil {
		t.Fatal(err)
	}
	if err := db.CreateDentry(RootInode, "f.txt", id1); err != nil {
		t.Fatal(err)
	}
	err := db.CreateDentry(RootInode, "f.txt", id2)
	if !errors.Is(err, ErrExists) {
		t.Fatalf("expected ErrExists, got %v", err)
	}
}

func TestReaddirOrderedByName(t *testing.T) {
	db := openTestDB(t)
	names := []string{"charlie", "alpha", "bravo"}
	for _, n := range names {
		id, _ := db.AllocInode()
		if err := db.PutInode(id, InodeRecord{Mode: 0o644}); err != nil {
			t.Fatal(err)
		}
		if err := db.CreateDentry(RootInode, n, id); err != nil {
			t.Fatal(err)
		}
	}
	entries, err := db.Readdir(RootInode)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 3 {
		t.Fatalf("got %d entries, want 3", len(entries))
	}
	want := []string{"alpha", "bravo", "charlie"}
	for i, w := range want {
		if entries[i].Name != w {
			t.Fatalf("entry %d = %q, want %q (lexicographic order per DESIGN.md §18.1)", i, entries[i].Name, w)
		}
	}
}

func TestQuotaUnsetIsUnlimited(t *testing.T) {
	db := openTestDB(t)
	id, _ := db.AllocInode()
	if err := db.PutInode(id, InodeRecord{Mode: 0o644, Size: 1 << 30}); err != nil {
		t.Fatal(err)
	}
	// CheckQuota against a huge delta with no limit set must pass — the
	// zero value of quotaRecord means "unlimited", not "zero".
	if err := db.CheckQuota(1<<30, 1); err != nil {
		t.Fatalf("expected no quota set to mean unlimited, got %v", err)
	}
}

func TestCommitFileRejectsOverByteLimit(t *testing.T) {
	db := openTestDB(t)
	if err := db.SetQuotaLimits(10, 0); err != nil {
		t.Fatal(err)
	}

	_, err := db.CommitFile(RootInode, "big.bin", InodeRecord{Mode: 0o644, Size: 11})
	if !errors.Is(err, ErrQuotaExceeded) {
		t.Fatalf("expected ErrQuotaExceeded, got %v", err)
	}

	// Rejected: no dentry, no usage charged.
	if _, err := db.Lookup(RootInode, "big.bin"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("expected the rejected commit to leave no dentry, got %v", err)
	}
	bytesUsed, inodesUsed, err := db.GetQuotaUsage()
	if err != nil {
		t.Fatal(err)
	}
	if bytesUsed != 0 || inodesUsed != 0 {
		t.Fatalf("expected zero usage after a rejected commit, got bytes=%d inodes=%d", bytesUsed, inodesUsed)
	}
}

func TestCommitFileAllowsExactlyAtLimit(t *testing.T) {
	db := openTestDB(t)
	if err := db.SetQuotaLimits(10, 0); err != nil {
		t.Fatal(err)
	}
	if _, err := db.CommitFile(RootInode, "f.txt", InodeRecord{Mode: 0o644, Size: 10}); err != nil {
		t.Fatalf("expected a write landing exactly at the limit to succeed, got %v", err)
	}
	bytesUsed, _, err := db.GetQuotaUsage()
	if err != nil {
		t.Fatal(err)
	}
	if bytesUsed != 10 {
		t.Fatalf("bytesUsed = %d, want 10", bytesUsed)
	}
}

func TestCommitFileOverwriteChargesOnlyDelta(t *testing.T) {
	db := openTestDB(t)
	if err := db.SetQuotaLimits(15, 0); err != nil {
		t.Fatal(err)
	}

	id1, err := db.CommitFile(RootInode, "f.txt", InodeRecord{Mode: 0o644, Size: 10})
	if err != nil {
		t.Fatal(err)
	}
	if bytesUsed, _, _ := db.GetQuotaUsage(); bytesUsed != 10 {
		t.Fatalf("bytesUsed after create = %d, want 10", bytesUsed)
	}

	// Growing overwrite: 10 -> 12 should charge a delta of 2, landing at
	// 12 total, not 10+12=22 — an overwrite must not double-count the
	// bytes it replaces.
	id2, err := db.CommitFile(RootInode, "f.txt", InodeRecord{Mode: 0o644, Size: 12})
	if err != nil {
		t.Fatalf("overwrite within limit should succeed, got %v", err)
	}
	if id2 != id1 {
		t.Fatalf("overwrite should reuse the existing inode: got %d, want %d", id2, id1)
	}
	bytesUsed, inodesUsed, err := db.GetQuotaUsage()
	if err != nil {
		t.Fatal(err)
	}
	if bytesUsed != 12 {
		t.Fatalf("bytesUsed after overwrite = %d, want 12 (delta-only accounting)", bytesUsed)
	}
	if inodesUsed != 1 {
		t.Fatalf("inodesUsed after overwrite = %d, want 1 (overwrite reuses the inode)", inodesUsed)
	}

	// A further overwrite to 20 bytes needs a delta of 8, which would
	// push total usage to 20 > the limit of 15: must be rejected, and
	// the previous 12-byte content must survive untouched.
	if _, err := db.CommitFile(RootInode, "f.txt", InodeRecord{Mode: 0o644, Size: 20}); !errors.Is(err, ErrQuotaExceeded) {
		t.Fatalf("expected ErrQuotaExceeded, got %v", err)
	}
	rec, err := db.GetInode(id1)
	if err != nil {
		t.Fatal(err)
	}
	if rec.Size != 12 {
		t.Fatalf("a rejected overwrite must leave the prior content in place: size = %d, want 12", rec.Size)
	}
	bytesUsed, _, err = db.GetQuotaUsage()
	if err != nil {
		t.Fatal(err)
	}
	if bytesUsed != 12 {
		t.Fatalf("bytesUsed after a rejected overwrite = %d, want unchanged at 12", bytesUsed)
	}
}

func TestCommitMkdirRejectsOverInodeLimit(t *testing.T) {
	db := openTestDB(t)
	if err := db.SetQuotaLimits(0, 1); err != nil {
		t.Fatal(err)
	}

	if _, err := db.CommitMkdir(RootInode, "a", InodeRecord{IsDir: true, Mode: 0o755}); err != nil {
		t.Fatalf("first mkdir within the inode limit should succeed, got %v", err)
	}
	if _, err := db.CommitMkdir(RootInode, "b", InodeRecord{IsDir: true, Mode: 0o755}); !errors.Is(err, ErrQuotaExceeded) {
		t.Fatalf("expected ErrQuotaExceeded, got %v", err)
	}
	if _, err := db.Lookup(RootInode, "b"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("expected the rejected mkdir to leave no dentry, got %v", err)
	}
	_, inodesUsed, err := db.GetQuotaUsage()
	if err != nil {
		t.Fatal(err)
	}
	if inodesUsed != 1 {
		t.Fatalf("inodesUsed = %d, want 1 (rejected mkdir must not be charged)", inodesUsed)
	}
}

func TestRemoveEntryReleasesQuotaUsage(t *testing.T) {
	db := openTestDB(t)
	if _, err := db.CommitFile(RootInode, "f.txt", InodeRecord{Mode: 0o644, Size: 7}); err != nil {
		t.Fatal(err)
	}
	if _, err := db.CommitMkdir(RootInode, "d", InodeRecord{IsDir: true, Mode: 0o755}); err != nil {
		t.Fatal(err)
	}
	bytesUsed, inodesUsed, err := db.GetQuotaUsage()
	if err != nil {
		t.Fatal(err)
	}
	if bytesUsed != 7 || inodesUsed != 2 {
		t.Fatalf("usage before removal = bytes=%d inodes=%d, want 7,2", bytesUsed, inodesUsed)
	}

	if err := db.RemoveEntry(RootInode, "f.txt"); err != nil {
		t.Fatal(err)
	}
	if err := db.RemoveEntry(RootInode, "d"); err != nil {
		t.Fatal(err)
	}
	bytesUsed, inodesUsed, err = db.GetQuotaUsage()
	if err != nil {
		t.Fatal(err)
	}
	if bytesUsed != 0 || inodesUsed != 0 {
		t.Fatalf("usage after removing everything = bytes=%d inodes=%d, want 0,0", bytesUsed, inodesUsed)
	}
}

func TestLocatorPutGetHas(t *testing.T) {
	db := openTestDB(t)
	id := chunk.Sum([]byte("payload"))

	if found, err := db.HasLocator("local", id); err != nil || found {
		t.Fatalf("expected no locator yet, found=%v err=%v", found, err)
	}
	loc := pack.Locator{Container: "atlas/c/local/aa/c1", Offset: 10, Length: 20}
	if err := db.PutLocator("local", id, loc); err != nil {
		t.Fatal(err)
	}
	got, found, err := db.GetLocator("local", id)
	if err != nil || !found {
		t.Fatalf("expected locator found, got=%v err=%v", found, err)
	}
	if got != loc {
		t.Fatalf("locator mismatch: got %+v want %+v", got, loc)
	}
}
