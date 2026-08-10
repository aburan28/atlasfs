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
	id, err := db.CreateSymlink(RootInode, "link", "../target", 0, 0)
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
	if _, err := db.CreateSymlink(RootInode, "link", "elsewhere", 0, 0); !errors.Is(err, ErrExists) {
		t.Fatalf("got %v, want ErrExists", err)
	}
}

// TestLinkTracksNLink is DESIGN.md §19.3's core property: an inode with
// more than one name must survive losing one of them. Graving it early
// would let GC reclaim chunks the surviving name still reads.
func TestLinkTracksNLink(t *testing.T) {
	db := openTestDB(t)
	id := mustFile(t, db, RootInode, "one", 100)

	rec, err := db.Link(RootInode, "two", id)
	if err != nil {
		t.Fatal(err)
	}
	if rec.NLink != 2 {
		t.Fatalf("nlink after link = %d, want 2", rec.NLink)
	}
	if got, err := db.Lookup(RootInode, "two"); err != nil || got != id {
		t.Fatalf("second name resolves to %d err=%v, want %d", got, err, id)
	}

	// A link creates a dentry, not an inode, and duplicates no bytes.
	bytesUsed, inodesUsed, err := db.GetQuotaUsage()
	if err != nil {
		t.Fatal(err)
	}
	if bytesUsed != 100 || inodesUsed != 1 {
		t.Fatalf("usage after link = %d bytes / %d inodes, want 100/1", bytesUsed, inodesUsed)
	}

	if err := db.RemoveEntry(RootInode, "one", time.Now()); err != nil {
		t.Fatal(err)
	}
	after, err := db.GetInode(id)
	if err != nil {
		t.Fatalf("inode gone while a name still points at it: %v", err)
	}
	if after.NLink != 1 {
		t.Fatalf("nlink after removing one of two names = %d, want 1", after.NLink)
	}
	graves, err := db.ListGraveyard()
	if err != nil {
		t.Fatal(err)
	}
	if len(graves) != 0 {
		t.Fatalf("inode graved with a name still bound: %+v", graves)
	}

	// The last name going is what graves it.
	if err := db.RemoveEntry(RootInode, "two", time.Now()); err != nil {
		t.Fatal(err)
	}
	graves, err = db.ListGraveyard()
	if err != nil {
		t.Fatal(err)
	}
	if len(graves) != 1 || graves[0].InodeID != id {
		t.Fatalf("last unlink did not grave the inode: %+v", graves)
	}
	if bytesUsed, inodesUsed, _ := db.GetQuotaUsage(); bytesUsed != 0 || inodesUsed != 0 {
		t.Fatalf("quota not released on the last unlink: %d bytes / %d inodes", bytesUsed, inodesUsed)
	}
}

// TestOverwriteKeepsNLink: CommitFile takes a caller-built record whose
// NLink is 1, and writing that verbatim would reset a hard-linked file's
// count — after which removing one name would grave an inode the other
// name still resolves to.
func TestOverwriteKeepsNLink(t *testing.T) {
	db := openTestDB(t)
	id := mustFile(t, db, RootInode, "one", 10)
	if _, err := db.Link(RootInode, "two", id); err != nil {
		t.Fatal(err)
	}
	if _, err := db.CommitFile(RootInode, "one", InodeRecord{Mode: 0o644, Size: 20, NLink: 1, MTime: time.Now()}); err != nil {
		t.Fatal(err)
	}
	rec, err := db.GetInode(id)
	if err != nil {
		t.Fatal(err)
	}
	if rec.NLink != 2 {
		t.Fatalf("nlink after overwriting one of two names = %d, want 2", rec.NLink)
	}
	if rec.Size != 20 {
		t.Fatalf("overwrite did not take effect: size = %d", rec.Size)
	}
}

func TestLinkRefusals(t *testing.T) {
	db := openTestDB(t)
	dir := mustDir(t, db, RootInode, "d")
	id := mustFile(t, db, RootInode, "f", 1)
	mustFile(t, db, RootInode, "taken", 1)

	// POSIX reserves directory hard links to the kernel's own "."/"..".
	if _, err := db.Link(RootInode, "dlink", dir); !errors.Is(err, ErrIsDirectory) {
		t.Fatalf("linking a directory: got %v, want ErrIsDirectory", err)
	}
	if _, err := db.Link(RootInode, "taken", id); !errors.Is(err, ErrExists) {
		t.Fatalf("linking over an existing name: got %v, want ErrExists", err)
	}
	if _, err := db.Link(RootInode, "x", 99999); !errors.Is(err, ErrNotFound) {
		t.Fatalf("linking a missing inode: got %v, want ErrNotFound", err)
	}
}

// TestRmdirStillGravesTheDirectory guards the interaction between nlink
// and a directory's conventional NLink of 2: decrementing that instead of
// graving would leave every removed directory unreclaimable.
func TestRmdirStillGravesTheDirectory(t *testing.T) {
	db := openTestDB(t)
	id := mustDir(t, db, RootInode, "gone")
	if err := db.RemoveEntry(RootInode, "gone", time.Now()); err != nil {
		t.Fatal(err)
	}
	graves, err := db.ListGraveyard()
	if err != nil {
		t.Fatal(err)
	}
	if len(graves) != 1 || graves[0].InodeID != id {
		t.Fatalf("rmdir did not grave the directory inode: %+v", graves)
	}
}
