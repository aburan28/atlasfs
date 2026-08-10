package mdsfuse

import (
	"context"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"syscall"
	"testing"
	"time"

	"github.com/aburan28/atlasfs/pkg/mds"
	"github.com/aburan28/atlasfs/pkg/repo"
)

// TestRenameAndSymlinkThroughAuthority drives both operations with the
// real coreutils against a mount whose entire namespace lives behind an
// RPC boundary. mv and ln -s issue a messier syscall sequence than Go's
// wrappers do, and that sequence is where this project's FUSE bugs have
// surfaced.
func TestRenameAndSymlinkThroughAuthority(t *testing.T) {
	c := startCluster(t, mds.Config{LeaseDuration: 30 * time.Second})
	mnt := c.mountAt(t, "writer", 0)

	if err := os.WriteFile(filepath.Join(mnt, "a.txt"), []byte("through the authority"), 0o644); err != nil {
		t.Fatal(err)
	}
	if out, err := exec.Command("mv", filepath.Join(mnt, "a.txt"), filepath.Join(mnt, "b.txt")).CombinedOutput(); err != nil {
		t.Fatalf("mv: %v: %s", err, out)
	}
	got, err := os.ReadFile(filepath.Join(mnt, "b.txt"))
	if err != nil || string(got) != "through the authority" {
		t.Fatalf("after mv got %q err=%v", got, err)
	}
	if _, err := os.Stat(filepath.Join(mnt, "a.txt")); !os.IsNotExist(err) {
		t.Fatalf("old name survived mv: %v", err)
	}

	if err := os.Mkdir(filepath.Join(mnt, "d"), 0o755); err != nil {
		t.Fatal(err)
	}
	if out, err := exec.Command("mv", filepath.Join(mnt, "b.txt"), filepath.Join(mnt, "d")).CombinedOutput(); err != nil {
		t.Fatalf("cross-directory mv: %v: %s", err, out)
	}
	if got, err := os.ReadFile(filepath.Join(mnt, "d", "b.txt")); err != nil || string(got) != "through the authority" {
		t.Fatalf("after cross-directory mv got %q err=%v", got, err)
	}

	if out, err := exec.Command("ln", "-s", "d/b.txt", filepath.Join(mnt, "link")).CombinedOutput(); err != nil {
		t.Fatalf("ln -s: %v: %s", err, out)
	}
	target, err := os.Readlink(filepath.Join(mnt, "link"))
	if err != nil || target != "d/b.txt" {
		t.Fatalf("readlink = %q err=%v, want %q", target, err, "d/b.txt")
	}
	if got, err := os.ReadFile(filepath.Join(mnt, "link")); err != nil || string(got) != "through the authority" {
		t.Fatalf("read through symlink got %q err=%v", got, err)
	}
}

// TestRenameOnOneMountBecomesVisibleOnAnother is the coherence half: a
// rename touches two names in (possibly) two directories, and every one
// of those listings must be invalidated on the other holder. A rename
// that only bumped the source directory would leave the second mount
// unable to see the new name until its lease expired.
func TestRenameOnOneMountBecomesVisibleOnAnother(t *testing.T) {
	c := startCluster(t, mds.Config{LeaseDuration: 30 * time.Second})
	writer := c.mountAt(t, "writer", 0)
	reader := c.mountAt(t, "reader", 0)

	if err := os.Mkdir(filepath.Join(writer, "dst"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(writer, "moving.txt"), []byte("payload"), 0o644); err != nil {
		t.Fatal(err)
	}

	// Populate the reader's caches for both ends of the move — both the
	// positive entry it is about to lose and the negative entry it is
	// about to gain (§10.4's directory-version gate is what clears the
	// latter).
	if _, err := os.Stat(filepath.Join(reader, "moving.txt")); err != nil {
		t.Fatalf("reader cannot see the file before the move: %v", err)
	}
	if _, err := os.Stat(filepath.Join(reader, "dst", "moved.txt")); !os.IsNotExist(err) {
		t.Fatalf("expected ENOENT before the move, got %v", err)
	}

	if err := os.Rename(filepath.Join(writer, "moving.txt"), filepath.Join(writer, "dst", "moved.txt")); err != nil {
		t.Fatal(err)
	}

	if got, err := os.ReadFile(filepath.Join(reader, "dst", "moved.txt")); err != nil || string(got) != "payload" {
		t.Fatalf("reader does not see the moved file: got %q err=%v", got, err)
	}
	if _, err := os.Stat(filepath.Join(reader, "moving.txt")); !os.IsNotExist(err) {
		t.Fatalf("reader still sees the pre-rename name: %v", err)
	}
}

// TestRenameRefusalsThroughAuthority pins the errno a caller actually
// observes through the mount.
//
// Which layer produces it, measured rather than assumed: Linux's VFS
// resolves both ends from its dentry cache and rejects the type
// mismatches itself, so EISDIR and ENOTDIR here never reach the
// authority — stripping pkg/mds's errno tagging leaves this test
// passing. That is fine; the test's job is the observable behaviour.
// The tagging's own coverage is TestErrnoSurvivesTheWire in pkg/mds,
// where a direct API caller has no VFS in front of it.
func TestRenameRefusalsThroughAuthority(t *testing.T) {
	c := startCluster(t, mds.Config{LeaseDuration: 30 * time.Second})
	mnt := c.mountAt(t, "writer", 0)

	if err := os.MkdirAll(filepath.Join(mnt, "parent", "child"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(filepath.Join(mnt, "occupied", "inside"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(mnt, "file"), []byte("f"), 0o644); err != nil {
		t.Fatal(err)
	}

	cases := []struct {
		name     string
		from, to string
		want     syscall.Errno
	}{
		{"file over a directory", "file", "parent", syscall.EISDIR},
		{"directory over a file", "parent", "file", syscall.ENOTDIR},
		{"directory over a non-empty directory", "parent", "occupied", syscall.ENOTEMPTY},
		{"missing source", "nope", "anything", syscall.ENOENT},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			// syscall.Rename rather than os.Rename: os.Rename Lstats the
			// target and short-circuits with its own EEXIST when it is a
			// directory, hiding the errno the filesystem returned.
			err := syscall.Rename(filepath.Join(mnt, tc.from), filepath.Join(mnt, tc.to))
			if err == nil {
				t.Fatalf("rename %s -> %s unexpectedly succeeded", tc.from, tc.to)
			}
			var errno syscall.Errno
			if !errors.As(err, &errno) || errno != tc.want {
				t.Fatalf("got %v (errno %d), want %v", err, errno, tc.want)
			}
		})
	}
}

// TestRenameWhileOpenForWriteThroughAuthority: Commit binds a (dir,
// name) dentry, so a handle opened before a rename and flushed after it
// must follow the file to its new name rather than recreating the old
// one.
func TestRenameWhileOpenForWriteThroughAuthority(t *testing.T) {
	c := startCluster(t, mds.Config{LeaseDuration: 30 * time.Second})
	mnt := c.mountAt(t, "writer", 0)

	before := filepath.Join(mnt, "before")
	after := filepath.Join(mnt, "after")
	if err := os.WriteFile(before, []byte("initial"), 0o644); err != nil {
		t.Fatal(err)
	}
	f, err := os.OpenFile(before, os.O_WRONLY, 0o644)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Rename(before, after); err != nil {
		f.Close()
		t.Fatal(err)
	}
	if _, err := f.Write([]byte("written!")); err != nil {
		f.Close()
		t.Fatal(err)
	}
	if err := f.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	if got, err := os.ReadFile(after); err != nil || string(got) != "written!" {
		t.Fatalf("renamed file = %q err=%v, want %q", got, err, "written!")
	}
	if _, err := os.Stat(before); !os.IsNotExist(err) {
		t.Fatal("the deferred commit resurrected the pre-rename name")
	}
}

// TestRenameAndSymlinkRefusedOnReadOnlyMount: both are mutations, and a
// read-only mount refuses them before the RPC is ever made.
func TestRenameAndSymlinkRefusedOnReadOnlyMount(t *testing.T) {
	e := setupCfg(t, true, func(t *testing.T, r *repo.Repo) {
		src := t.TempDir()
		writeSrc(t, src, "hello.txt", []byte("hi"))
		if _, _, _, err := r.PublishTree(context.Background(), src, nil); err != nil {
			t.Fatal(err)
		}
	})
	if err := os.Symlink("target", filepath.Join(e.mountpoint, "link")); err == nil {
		t.Fatal("expected symlink on a read-only mount to fail")
	}
	if err := syscall.Rename(filepath.Join(e.mountpoint, "hello.txt"), filepath.Join(e.mountpoint, "moved.txt")); err == nil {
		t.Fatal("expected rename on a read-only mount to fail")
	}
}
