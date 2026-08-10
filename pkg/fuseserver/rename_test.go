package fuseserver

import (
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"syscall"
	"testing"

	"github.com/aburan28/atlasfs/pkg/repo"
)

// mountWritable opens a fresh relaxed-class repo and mounts it.
func mountWritable(t *testing.T) (*repo.Repo, string) {
	t.Helper()
	repoDir := t.TempDir()
	r, err := repo.OpenWithClass(repoDir, mustLocalBackend(t, repoDir), repo.DefaultRegion, repo.ClassRelaxed)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { r.Close() })
	return r, mountForTest(t, r)
}

// TestMountRenameAndSymlinkViaShell drives rename and symlink creation
// with the actual coreutils, not Go's syscall wrappers. Go's file APIs
// produce a tidier syscall sequence than a shell does — mv stats both
// ends, tries renameat2 first, and falls back — and every FUSE bug this
// project has hit so far showed up only under the messier one.
func TestMountRenameAndSymlinkViaShell(t *testing.T) {
	_, mnt := mountWritable(t)

	if err := os.WriteFile(filepath.Join(mnt, "a.txt"), []byte("payload"), 0o644); err != nil {
		t.Fatal(err)
	}
	if out, err := exec.Command("mv", filepath.Join(mnt, "a.txt"), filepath.Join(mnt, "b.txt")).CombinedOutput(); err != nil {
		t.Fatalf("mv through the mount: %v: %s", err, out)
	}
	got, err := os.ReadFile(filepath.Join(mnt, "b.txt"))
	if err != nil {
		t.Fatalf("read the renamed file: %v", err)
	}
	if string(got) != "payload" {
		t.Fatalf("content after mv = %q, want %q", got, "payload")
	}
	if _, err := os.Stat(filepath.Join(mnt, "a.txt")); !os.IsNotExist(err) {
		t.Fatalf("old name still present after mv: %v", err)
	}

	if err := os.Mkdir(filepath.Join(mnt, "sub"), 0o755); err != nil {
		t.Fatal(err)
	}
	if out, err := exec.Command("mv", filepath.Join(mnt, "b.txt"), filepath.Join(mnt, "sub")).CombinedOutput(); err != nil {
		t.Fatalf("mv into a subdirectory: %v: %s", err, out)
	}
	got, err = os.ReadFile(filepath.Join(mnt, "sub", "b.txt"))
	if err != nil || string(got) != "payload" {
		t.Fatalf("after cross-directory mv got %q err=%v", got, err)
	}

	if out, err := exec.Command("ln", "-s", "sub/b.txt", filepath.Join(mnt, "link")).CombinedOutput(); err != nil {
		t.Fatalf("ln -s through the mount: %v: %s", err, out)
	}
	target, err := os.Readlink(filepath.Join(mnt, "link"))
	if err != nil {
		t.Fatalf("readlink: %v", err)
	}
	if target != "sub/b.txt" {
		t.Fatalf("symlink target = %q, want %q", target, "sub/b.txt")
	}
	// Following the link is what proves the record is a real symlink to
	// the kernel and not just a file whose contents happen to be a path.
	viaLink, err := os.ReadFile(filepath.Join(mnt, "link"))
	if err != nil {
		t.Fatalf("read through the symlink: %v", err)
	}
	if string(viaLink) != "payload" {
		t.Fatalf("read through symlink = %q, want %q", viaLink, "payload")
	}
}

// TestMountRenameOverExistingFile checks the displacing rename: the
// target's name survives, bound to the source's content.
func TestMountRenameOverExistingFile(t *testing.T) {
	_, mnt := mountWritable(t)

	src := filepath.Join(mnt, "src")
	dst := filepath.Join(mnt, "dst")
	if err := os.WriteFile(src, []byte("new content"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(dst, []byte("old content"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.Rename(src, dst); err != nil {
		t.Fatalf("rename over an existing file: %v", err)
	}
	got, err := os.ReadFile(dst)
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != "new content" {
		t.Fatalf("dst = %q after displacing rename, want %q", got, "new content")
	}
	if _, err := os.Stat(src); !os.IsNotExist(err) {
		t.Fatalf("src still present: %v", err)
	}
}

// TestMountRenameDirectory moves a whole subtree by rebinding one
// dentry — the entries underneath are keyed by their parent inode, so
// none of them are touched.
func TestMountRenameDirectory(t *testing.T) {
	_, mnt := mountWritable(t)

	if err := os.MkdirAll(filepath.Join(mnt, "old", "deep"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(mnt, "old", "deep", "f"), []byte("deep"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.Rename(filepath.Join(mnt, "old"), filepath.Join(mnt, "new")); err != nil {
		t.Fatalf("rename a directory: %v", err)
	}
	got, err := os.ReadFile(filepath.Join(mnt, "new", "deep", "f"))
	if err != nil || string(got) != "deep" {
		t.Fatalf("subtree lost in the move: got %q err=%v", got, err)
	}
}

// TestMountRenameRefusals is the errno surface: a caller must be able to
// tell "not empty" from "wrong type" from "would detach the tree", since
// mv prints whatever we return.
func TestMountRenameRefusals(t *testing.T) {
	_, mnt := mountWritable(t)

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
		{"dir into its own subtree", "parent", "parent/child/moved", syscall.EINVAL},
		{"file over a directory", "file", "parent", syscall.EISDIR},
		{"directory over a file", "parent", "file", syscall.ENOTDIR},
		{"directory over a non-empty directory", "parent", "occupied", syscall.ENOTEMPTY},
		{"missing source", "nope", "anything", syscall.ENOENT},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			// syscall.Rename, not os.Rename: os.Rename Lstats the target
			// first and short-circuits with its own EEXIST whenever the
			// target is a directory, so it would report Go's opinion rather
			// than the errno this filesystem actually returns.
			err := syscall.Rename(filepath.Join(mnt, tc.from), filepath.Join(mnt, tc.to))
			if err == nil {
				t.Fatalf("rename %s -> %s unexpectedly succeeded", tc.from, tc.to)
			}
			var errno syscall.Errno
			if !errors.As(err, &errno) || errno != tc.want {
				t.Fatalf("got %v (errno %d), want errno %v", err, errno, tc.want)
			}
		})
	}
}

// TestMountRenameWhileOpenForWrite is the case the (parent, name)
// rebinding exists for. A commit binds a dentry, not an inode ID, so a
// handle opened before the rename and flushed after it would otherwise
// recreate the name the rename just removed — and drop the write on the
// floor as far as the new name is concerned.
func TestMountRenameWhileOpenForWrite(t *testing.T) {
	_, mnt := mountWritable(t)

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
		t.Fatalf("close (which is where the commit happens): %v", err)
	}

	got, err := os.ReadFile(after)
	if err != nil {
		t.Fatalf("read the renamed file after the deferred commit: %v", err)
	}
	if string(got) != "written!" {
		t.Fatalf("renamed file = %q, want %q", got, "written!")
	}
	if _, err := os.Stat(before); !os.IsNotExist(err) {
		t.Fatal("the commit resurrected the pre-rename name")
	}
}

// TestMountSymlinkOnImmutableIsReadOnly: symlink creation is a mutation
// like any other, and DESIGN.md §8's immutable class refuses it.
func TestMountSymlinkOnImmutableIsReadOnly(t *testing.T) {
	repoDir := t.TempDir()
	r, err := repo.Open(repoDir)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	mnt := mountForTest(t, r)

	err = os.Symlink("target", filepath.Join(mnt, "link"))
	if err == nil {
		t.Fatal("expected symlink on an immutable mount to fail")
	}
	err = os.Rename(filepath.Join(mnt, "a"), filepath.Join(mnt, "b"))
	if err == nil {
		t.Fatal("expected rename on an immutable mount to fail")
	}
}
