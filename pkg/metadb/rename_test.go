package metadb

import (
	"errors"
	"testing"
	"time"
)

func mustFile(t *testing.T, db *DB, dir InodeID, name string, size uint64) InodeID {
	t.Helper()
	id, err := db.CommitFile(dir, name, InodeRecord{Mode: 0o644, Size: size, MTime: time.Now(), NLink: 1})
	if err != nil {
		t.Fatal(err)
	}
	return id
}

func mustDir(t *testing.T, db *DB, dir InodeID, name string) InodeID {
	t.Helper()
	id, err := db.CommitMkdir(dir, name, InodeRecord{IsDir: true, Mode: 0o755, MTime: time.Now(), NLink: 2})
	if err != nil {
		t.Fatal(err)
	}
	return id
}

// TestRenameKeepsInodeIdentity is the property that makes rename a
// namespace operation rather than a copy: the file keeps its inode, so a
// cached lookup-by-inode-ID stays valid and no content is re-uploaded.
func TestRenameKeepsInodeIdentity(t *testing.T) {
	db := openTestDB(t)
	sub := mustDir(t, db, RootInode, "sub")
	id := mustFile(t, db, RootInode, "a.txt", 10)

	if err := db.Rename(RootInode, "a.txt", sub, "b.txt", time.Now()); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Lookup(RootInode, "a.txt"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("old name still resolves: %v", err)
	}
	moved, err := db.Lookup(sub, "b.txt")
	if err != nil {
		t.Fatal(err)
	}
	if moved != id {
		t.Fatalf("rename allocated a new inode: got %d, want %d", moved, id)
	}
}

// TestRenameOverFileReleasesQuota checks the displaced target is graved
// and its bytes returned: without this, renaming over a file would leak
// its quota charge forever, since nothing else ever unbinds that inode.
func TestRenameOverFileReleasesQuota(t *testing.T) {
	db := openTestDB(t)
	mustFile(t, db, RootInode, "src", 100)
	dstID := mustFile(t, db, RootInode, "dst", 400)

	bytesBefore, inodesBefore, err := db.GetQuotaUsage()
	if err != nil {
		t.Fatal(err)
	}
	if bytesBefore != 500 || inodesBefore != 2 {
		t.Fatalf("setup usage = %d bytes / %d inodes, want 500/2", bytesBefore, inodesBefore)
	}

	if err := db.Rename(RootInode, "src", RootInode, "dst", time.Now()); err != nil {
		t.Fatal(err)
	}
	bytesAfter, inodesAfter, err := db.GetQuotaUsage()
	if err != nil {
		t.Fatal(err)
	}
	if bytesAfter != 100 || inodesAfter != 1 {
		t.Fatalf("usage after rename = %d bytes / %d inodes, want 100/1", bytesAfter, inodesAfter)
	}

	graves, err := db.ListGraveyard()
	if err != nil {
		t.Fatal(err)
	}
	found := false
	for _, g := range graves {
		if g.InodeID == dstID {
			found = true
		}
	}
	if !found {
		t.Fatalf("displaced inode %d never reached the graveyard; its chunks would leak", dstID)
	}
}

// TestRenameRefusals covers the cases POSIX rename(2) must reject rather
// than silently do something surprising.
func TestRenameRefusals(t *testing.T) {
	db := openTestDB(t)
	parent := mustDir(t, db, RootInode, "parent")
	child := mustDir(t, db, parent, "child")
	mustFile(t, db, RootInode, "file", 1)
	occupied := mustDir(t, db, RootInode, "occupied")
	mustFile(t, db, occupied, "inside", 1)

	t.Run("dir into its own subtree", func(t *testing.T) {
		err := db.Rename(RootInode, "parent", child, "moved", time.Now())
		if !errors.Is(err, ErrInvalidRename) {
			t.Fatalf("got %v, want ErrInvalidRename", err)
		}
	})
	t.Run("file over a directory", func(t *testing.T) {
		err := db.Rename(RootInode, "file", RootInode, "parent", time.Now())
		if !errors.Is(err, ErrIsDirectory) {
			t.Fatalf("got %v, want ErrIsDirectory", err)
		}
	})
	t.Run("directory over a file", func(t *testing.T) {
		err := db.Rename(RootInode, "parent", RootInode, "file", time.Now())
		if !errors.Is(err, ErrNotDir) {
			t.Fatalf("got %v, want ErrNotDir", err)
		}
	})
	t.Run("directory over a non-empty directory", func(t *testing.T) {
		err := db.Rename(RootInode, "parent", RootInode, "occupied", time.Now())
		if !errors.Is(err, ErrNotEmpty) {
			t.Fatalf("got %v, want ErrNotEmpty", err)
		}
	})
	t.Run("source does not exist", func(t *testing.T) {
		err := db.Rename(RootInode, "nope", RootInode, "x", time.Now())
		if !errors.Is(err, ErrNotFound) {
			t.Fatalf("got %v, want ErrNotFound", err)
		}
	})

	// Every refusal above must have left the tree untouched — a
	// half-applied rename is worse than a rejected one.
	if _, err := db.Lookup(RootInode, "parent"); err != nil {
		t.Fatalf("parent lost after refused renames: %v", err)
	}
	if _, err := db.Lookup(parent, "child"); err != nil {
		t.Fatalf("child lost after refused renames: %v", err)
	}
	if _, err := db.Lookup(RootInode, "file"); err != nil {
		t.Fatalf("file lost after refused renames: %v", err)
	}
}

// TestRenameDirectoryOverEmptyDirectory is the case POSIX does allow:
// the empty target is replaced.
func TestRenameDirectoryOverEmptyDirectory(t *testing.T) {
	db := openTestDB(t)
	src := mustDir(t, db, RootInode, "src")
	mustFile(t, db, src, "keep", 7)
	mustDir(t, db, RootInode, "dst")

	if err := db.Rename(RootInode, "src", RootInode, "dst", time.Now()); err != nil {
		t.Fatal(err)
	}
	moved, err := db.Lookup(RootInode, "dst")
	if err != nil {
		t.Fatal(err)
	}
	if moved != src {
		t.Fatalf("dst binds inode %d, want the moved directory %d", moved, src)
	}
	// The subtree moves with the directory: dentries are keyed by parent
	// inode, so nothing under src needed rewriting.
	if _, err := db.Lookup(src, "keep"); err != nil {
		t.Fatalf("subtree lost in the move: %v", err)
	}
}

func TestCreateSymlinkRecord(t *testing.T) {
	db := openTestDB(t)
	id, err := db.CreateSymlink(RootInode, "link", "../target")
	if err != nil {
		t.Fatal(err)
	}
	rec, err := db.GetInode(id)
	if err != nil {
		t.Fatal(err)
	}
	if !rec.IsSymlink || rec.SymlinkTarget != "../target" {
		t.Fatalf("got IsSymlink=%v target=%q", rec.IsSymlink, rec.SymlinkTarget)
	}
	if rec.Size != uint64(len("../target")) {
		t.Fatalf("symlink size = %d, want %d", rec.Size, len("../target"))
	}
	// symlink(2) never clobbers.
	if _, err := db.CreateSymlink(RootInode, "link", "elsewhere"); !errors.Is(err, ErrExists) {
		t.Fatalf("got %v, want ErrExists", err)
	}
}
