package fuseserver

import (
	"context"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/aburan28/atlasfs/pkg/repo"
)

// TestPublishPreservesTheExecutableBit is the end of a real workflow:
// publish a tree containing a script, mount it, and run the script. A
// hardcoded 0o644 on publish makes that fail with EACCES no matter what
// the source tree said.
func TestPublishPreservesTheExecutableBit(t *testing.T) {
	src := t.TempDir()
	script := filepath.Join(src, "run.sh")
	if err := os.WriteFile(script, []byte("#!/bin/sh\necho ran-from-atlasfs\n"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(src, "data.txt"), []byte("plain"), 0o600); err != nil {
		t.Fatal(err)
	}

	repoDir := t.TempDir()
	r, err := repo.Open(repoDir)
	if err != nil {
		t.Fatal(err)
	}
	if _, _, _, err := r.PublishTree(context.Background(), src, nil); err != nil {
		t.Fatal(err)
	}
	defer r.Close()

	mnt := mountForTest(t, r)

	fi, err := os.Stat(filepath.Join(mnt, "run.sh"))
	if err != nil {
		t.Fatal(err)
	}
	if fi.Mode().Perm()&0o111 == 0 {
		t.Fatalf("published script is not executable through the mount: mode %v", fi.Mode())
	}
	// An immutable mount clears the write bits; the exec bit is the whole
	// point and must survive that.
	if fi.Mode().Perm()&0o222 != 0 {
		t.Fatalf("immutable mount advertises write bits: mode %v", fi.Mode())
	}

	out, err := exec.Command(filepath.Join(mnt, "run.sh")).CombinedOutput()
	if err != nil {
		t.Fatalf("running the published script: %v: %s", err, out)
	}
	if !strings.Contains(string(out), "ran-from-atlasfs") {
		t.Fatalf("script output = %q", out)
	}

	// A non-default mode on an ordinary file survives too, minus the
	// write bits the read-only mount strips.
	data, err := os.Stat(filepath.Join(mnt, "data.txt"))
	if err != nil {
		t.Fatal(err)
	}
	if data.Mode().Perm() != 0o400 {
		t.Fatalf("0o600 source published as %v through a read-only mount, want 0400", data.Mode().Perm())
	}
}

// TestChmodPersists: chmod through the mount has to stick, and survive
// an overwrite — a commit carries content, not permissions.
func TestChmodPersists(t *testing.T) {
	_, mnt := mountWritable(t)
	path := filepath.Join(mnt, "script")
	if err := os.WriteFile(path, []byte("#!/bin/sh\necho hi\n"), 0o644); err != nil {
		t.Fatal(err)
	}

	if err := os.Chmod(path, 0o755); err != nil {
		t.Fatalf("chmod: %v", err)
	}
	fi, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if fi.Mode().Perm() != 0o755 {
		t.Fatalf("mode after chmod = %v, want 0755", fi.Mode().Perm())
	}

	// Rewriting the file must not strip the bit back off.
	if err := os.WriteFile(path, []byte("#!/bin/sh\necho hi again\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	fi, err = os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if fi.Mode().Perm() != 0o755 {
		t.Fatalf("overwrite reset the mode to %v; chmod +x then edit would silently break", fi.Mode().Perm())
	}
}

// TestChmodOnImmutableIsReadOnly: a mode change is a mutation like any
// other.
func TestChmodOnImmutableIsReadOnly(t *testing.T) {
	repoDir := t.TempDir()
	r, err := repo.Open(repoDir)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	src := t.TempDir()
	mustWrite(t, filepath.Join(src, "f"), []byte("x"))
	if _, _, _, err := r.PublishTree(context.Background(), src, nil); err != nil {
		t.Fatal(err)
	}
	mnt := mountForTest(t, r)

	if err := os.Chmod(filepath.Join(mnt, "f"), 0o777); err == nil {
		t.Fatal("expected chmod on an immutable mount to fail")
	}
}

// TestUtimesPersists: touch -d has to stick. tar -p and cp -p both
// restore mtimes, and a filesystem that accepts and drops them makes
// every incremental build tool think nothing ever changed.
func TestUtimesPersists(t *testing.T) {
	_, mnt := mountWritable(t)
	path := filepath.Join(mnt, "dated")
	if err := os.WriteFile(path, []byte("x"), 0o644); err != nil {
		t.Fatal(err)
	}

	want := time.Date(2001, 2, 3, 4, 5, 6, 0, time.UTC)
	if err := os.Chtimes(path, want, want); err != nil {
		t.Fatalf("utimes: %v", err)
	}
	fi, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if !fi.ModTime().Equal(want) {
		t.Fatalf("mtime after utimes = %v, want %v", fi.ModTime().UTC(), want)
	}
}

// TestCreateHonoursTheRequestedMode: open(2) with O_CREAT carries the
// mode the file should get, already umask-applied by the kernel.
// Ignoring it makes every program that creates an executable directly —
// install, tar restoring an archive, a build emitting a script — produce
// a 0644 file, and whether that is noticed depends on whether the
// program bothers to chmod afterwards. (CI caught this: as root, GNU tar
// issued a follow-up chmod and the bug was invisible; as the runner's
// non-root user it relied on the create mode and the test failed.)
func TestCreateHonoursTheRequestedMode(t *testing.T) {
	_, mnt := mountWritable(t)

	for _, mode := range []os.FileMode{0o755, 0o600, 0o444} {
		name := filepath.Join(mnt, "created-"+mode.String())
		f, err := os.OpenFile(name, os.O_CREATE|os.O_WRONLY, mode)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := f.Write([]byte("content")); err != nil {
			t.Fatal(err)
		}
		if err := f.Close(); err != nil {
			t.Fatal(err)
		}
		fi, err := os.Stat(name)
		if err != nil {
			t.Fatal(err)
		}
		if fi.Mode().Perm() != mode {
			t.Errorf("O_CREAT with mode %v produced %v", mode, fi.Mode().Perm())
		}
	}
}

// TestMkdirHonoursTheRequestedMode: the same for mkdir(2).
func TestMkdirHonoursTheRequestedMode(t *testing.T) {
	_, mnt := mountWritable(t)
	path := filepath.Join(mnt, "d")
	if err := os.Mkdir(path, 0o700); err != nil {
		t.Fatal(err)
	}
	fi, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if fi.Mode().Perm() != 0o700 {
		t.Fatalf("mkdir 0700 produced %v", fi.Mode().Perm())
	}
}

// TestCreateWithModeZero: `open(path, O_CREAT|O_WRONLY, 0)` asks for a
// file with no permission bits at all. That is a legitimate request, and
// spelling "no mode given" as zero internally turned it into 0644 — a
// file the caller explicitly wanted unreadable coming out world-readable.
func TestCreateWithModeZero(t *testing.T) {
	_, mnt := mountWritable(t)
	path := filepath.Join(mnt, "locked")
	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY, 0)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.Write([]byte("x")); err != nil {
		t.Fatal(err)
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}
	fi, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if fi.Mode().Perm() != 0 {
		t.Fatalf("O_CREAT with mode 0 produced %v", fi.Mode().Perm())
	}
}

// TestUtimesSetsAtimeSeparately: atime and mtime are distinct
// attributes, and reporting mtime for both makes utimensat look like it
// silently ignored half its argument.
func TestUtimesSetsAtimeSeparately(t *testing.T) {
	_, mnt := mountWritable(t)
	path := filepath.Join(mnt, "times")
	if err := os.WriteFile(path, []byte("x"), 0o644); err != nil {
		t.Fatal(err)
	}
	atime := time.Date(2001, 2, 3, 4, 5, 6, 0, time.UTC)
	mtime := time.Date(2011, 12, 13, 14, 15, 16, 0, time.UTC)
	if err := os.Chtimes(path, atime, mtime); err != nil {
		t.Fatal(err)
	}
	st := statOf(t, path)
	if got := time.Unix(st.Atim.Sec, 0).UTC(); !got.Equal(atime) {
		t.Errorf("atime = %v, want %v", got, atime)
	}
	if got := time.Unix(st.Mtim.Sec, 0).UTC(); !got.Equal(mtime) {
		t.Errorf("mtime = %v, want %v", got, mtime)
	}
}

// TestCtimeMovesOnMetadataChange: ctime is the inode-change time and is
// not settable by the caller — that is the whole point of it. A chmod
// must move it even though it leaves mtime alone.
func TestCtimeMovesOnMetadataChange(t *testing.T) {
	_, mnt := mountWritable(t)
	path := filepath.Join(mnt, "ct")
	if err := os.WriteFile(path, []byte("x"), 0o644); err != nil {
		t.Fatal(err)
	}
	old := time.Date(2001, 2, 3, 4, 5, 6, 0, time.UTC)
	if err := os.Chtimes(path, old, old); err != nil {
		t.Fatal(err)
	}
	before := statOf(t, path)
	if err := os.Chmod(path, 0o600); err != nil {
		t.Fatal(err)
	}
	after := statOf(t, path)
	// Compared against mtime rather than against the previous ctime: both
	// chmod and the utimes before it stamp ctime with "now", and at
	// one-second stat granularity those are usually the same value.
	if after.Ctim.Sec <= after.Mtim.Sec {
		t.Fatalf("ctime %d did not move past the explicitly-set mtime %d on chmod", after.Ctim.Sec, after.Mtim.Sec)
	}
	_ = before
	if after.Mtim.Sec != before.Mtim.Sec {
		t.Fatalf("chmod changed mtime: %d -> %d", before.Mtim.Sec, after.Mtim.Sec)
	}
}

// TestNameTooLong: NAME_MAX is 255 and POSIX requires ENAMETOOLONG past
// it. The kernel does not enforce it for a FUSE filesystem — an
// over-long name arrives intact — so the filesystem has to.
func TestNameTooLong(t *testing.T) {
	_, mnt := mountWritable(t)
	long := filepath.Join(mnt, strings.Repeat("x", 256))

	for _, tc := range []struct {
		name string
		op   func() error
	}{
		{"create", func() error { return os.WriteFile(long, []byte("x"), 0o644) }},
		{"mkdir", func() error { return os.Mkdir(long, 0o755) }},
		{"symlink", func() error { return os.Symlink("t", long) }},
		{"stat", func() error { _, err := os.Stat(long); return err }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			err := tc.op()
			var errno syscall.Errno
			if !errors.As(err, &errno) || errno != syscall.ENAMETOOLONG {
				t.Fatalf("got %v, want ENAMETOOLONG", err)
			}
		})
	}
}

// TestDirectoryTimestampsMoveOnEntryChange: POSIX requires a directory's
// mtime and ctime to move when an entry is added or removed. Without it
// every tool that watches a directory's mtime — make, rsync, any file
// watcher — never notices a file appearing in it.
func TestDirectoryTimestampsMoveOnEntryChange(t *testing.T) {
	_, mnt := mountWritable(t)
	dir := filepath.Join(mnt, "d")
	if err := os.Mkdir(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	old := time.Date(2001, 2, 3, 4, 5, 6, 0, time.UTC)
	if err := os.Chtimes(dir, old, old); err != nil {
		t.Fatal(err)
	}
	if got := statOf(t, dir).Mtim.Sec; got != old.Unix() {
		t.Fatalf("setup: dir mtime %d, want %d", got, old.Unix())
	}

	if err := os.WriteFile(filepath.Join(dir, "f"), []byte("x"), 0o644); err != nil {
		t.Fatal(err)
	}
	if got := statOf(t, dir).Mtim.Sec; got <= old.Unix() {
		t.Fatalf("dir mtime did not move when a file was created in it: %d", got)
	}

	if err := os.Chtimes(dir, old, old); err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(filepath.Join(dir, "f")); err != nil {
		t.Fatal(err)
	}
	if got := statOf(t, dir).Mtim.Sec; got <= old.Unix() {
		t.Fatalf("dir mtime did not move when a file was removed from it: %d", got)
	}
}
