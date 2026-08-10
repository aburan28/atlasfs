package fuseserver

import (
	"context"
	"os"
	"path/filepath"
	"syscall"
	"testing"

	"github.com/aburan28/atlasfs/pkg/repo"
)

func statOf(t *testing.T, path string) *syscall.Stat_t {
	t.Helper()
	fi, err := os.Lstat(path)
	if err != nil {
		t.Fatal(err)
	}
	st, ok := fi.Sys().(*syscall.Stat_t)
	if !ok {
		t.Fatalf("no syscall.Stat_t for %s", path)
	}
	return st
}

// TestCreateRecordsTheCallersOwnership (DESIGN.md §20): a new inode
// belongs to whoever made the syscall. Reporting a constant 0/0 instead
// would make every file look root-owned, and with default_permissions
// the kernel would then refuse an ordinary user access to files they
// just created.
func TestCreateRecordsTheCallersOwnership(t *testing.T) {
	_, mnt := mountWritable(t)
	wantUID, wantGID := uint32(os.Getuid()), uint32(os.Getgid())

	if err := os.WriteFile(filepath.Join(mnt, "f"), []byte("x"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(filepath.Join(mnt, "d"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink("f", filepath.Join(mnt, "l")); err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"f", "d", "l"} {
		st := statOf(t, filepath.Join(mnt, name))
		if st.Uid != wantUID || st.Gid != wantGID {
			t.Errorf("%s owned by %d:%d, want %d:%d", name, st.Uid, st.Gid, wantUID, wantGID)
		}
	}
}

// TestChownPersists: ownership has to stick, and survive an overwrite —
// writing to a file you do not own must not transfer it to you.
func TestChownPersists(t *testing.T) {
	_, mnt := mountWritable(t)
	if os.Getuid() != 0 {
		t.Skip("changing a file's owner to an arbitrary uid needs root")
	}
	path := filepath.Join(mnt, "owned")
	if err := os.WriteFile(path, []byte("x"), 0o644); err != nil {
		t.Fatal(err)
	}

	const uid, gid = 4242, 4343
	if err := os.Chown(path, uid, gid); err != nil {
		t.Fatalf("chown: %v", err)
	}
	if st := statOf(t, path); st.Uid != uid || st.Gid != gid {
		t.Fatalf("after chown: %d:%d, want %d:%d", st.Uid, st.Gid, uid, gid)
	}

	if err := os.WriteFile(path, []byte("rewritten"), 0o644); err != nil {
		t.Fatal(err)
	}
	if st := statOf(t, path); st.Uid != uid || st.Gid != gid {
		t.Fatalf("an overwrite transferred ownership: %d:%d, want %d:%d", st.Uid, st.Gid, uid, gid)
	}
}

// TestChownClearsSetuid mirrors what chown(2) does on a real filesystem:
// a setuid bit surviving an ownership change is a privilege-escalation
// primitive, so it is dropped.
func TestChownClearsSetuid(t *testing.T) {
	_, mnt := mountWritable(t)
	if os.Getuid() != 0 {
		t.Skip("needs root")
	}
	path := filepath.Join(mnt, "suid")
	if err := os.WriteFile(path, []byte("x"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(path, os.FileMode(0o755)|os.ModeSetuid); err != nil {
		t.Fatal(err)
	}
	if st := statOf(t, path); st.Mode&syscall.S_ISUID == 0 {
		t.Fatal("setuid bit did not take")
	}
	if err := os.Chown(path, 4242, 4343); err != nil {
		t.Fatal(err)
	}
	if st := statOf(t, path); st.Mode&syscall.S_ISUID != 0 {
		t.Fatal("setuid bit survived a chown")
	}
}

// TestPublishPreservesOwnership: a published tree keeps the ownership it
// had on disk, so a read-only dataset presents the same identities it was
// built with.
func TestPublishPreservesOwnership(t *testing.T) {
	if os.Getuid() != 0 {
		t.Skip("needs root to build a source tree with a foreign owner")
	}
	src := t.TempDir()
	f := filepath.Join(src, "owned.txt")
	mustWrite(t, f, []byte("x"))
	if err := os.Chown(f, 4242, 4343); err != nil {
		t.Fatal(err)
	}

	r, err := repo.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	if _, _, _, err := r.PublishTree(context.Background(), src, nil); err != nil {
		t.Fatal(err)
	}
	mnt := mountForTest(t, r)

	if st := statOf(t, filepath.Join(mnt, "owned.txt")); st.Uid != 4242 || st.Gid != 4343 {
		t.Fatalf("published file owned by %d:%d, want 4242:4343", st.Uid, st.Gid)
	}
}
