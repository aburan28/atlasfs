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
