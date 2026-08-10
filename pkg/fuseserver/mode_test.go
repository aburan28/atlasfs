package fuseserver

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
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
