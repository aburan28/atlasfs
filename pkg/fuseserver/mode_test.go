package fuseserver

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

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
