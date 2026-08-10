package fuseserver

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// TestTarRoundTripThroughTheMount is the closest thing this build has to
// a conformance run: tar exercises nearly the whole namespace surface in
// one go — mkdir, create, write, chmod, utimes, symlink, hard link — and
// then reads it all back and compares. Each of those has its own unit
// test above; what this adds is that they compose under a real program
// that does them in tar's order, not the tests' order.
//
// diff -r is the assertion for content; the stat comparison afterwards is
// for the metadata diff -r does not look at.
func TestTarRoundTripThroughTheMount(t *testing.T) {
	for _, bin := range []string{"tar", "diff"} {
		if _, err := exec.LookPath(bin); err != nil {
			t.Skipf("%s not available", bin)
		}
	}
	_, mnt := mountWritable(t)

	src := t.TempDir()
	mustWrite(t, filepath.Join(src, "top.txt"), []byte("top level"))
	mustWrite(t, filepath.Join(src, "sub", "nested.txt"), []byte("nested content"))
	mustWrite(t, filepath.Join(src, "sub", "deep", "leaf.bin"), make([]byte, 9000))
	script := filepath.Join(src, "bin", "run.sh")
	mustWrite(t, script, []byte("#!/bin/sh\necho hello\n"))
	if err := os.Chmod(script, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink("../top.txt", filepath.Join(src, "sub", "link-to-top")); err != nil {
		t.Fatal(err)
	}
	if err := os.Link(filepath.Join(src, "top.txt"), filepath.Join(src, "hard-to-top")); err != nil {
		t.Fatal(err)
	}

	archive := filepath.Join(t.TempDir(), "tree.tar")
	if out, err := execCommand("tar", "-cf", archive, "-C", src, ".").CombinedOutput(); err != nil {
		t.Fatalf("tar -c from a plain directory: %v: %s", err, out)
	}
	// -p so tar restores modes rather than applying the umask; the exec
	// bit surviving extraction is half of what this test is checking.
	if out, err := execCommand("tar", "-xpf", archive, "-C", mnt).CombinedOutput(); err != nil {
		t.Fatalf("tar -x into the mount: %v: %s", err, out)
	}

	if out, err := execCommand("diff", "-r", src, mnt).CombinedOutput(); err != nil {
		t.Fatalf("extracted tree differs from the source:\n%s", out)
	}

	// The exec bit made it through tar and the filesystem both.
	fi, err := os.Stat(filepath.Join(mnt, "bin", "run.sh"))
	if err != nil {
		t.Fatal(err)
	}
	if fi.Mode().Perm()&0o111 == 0 {
		t.Fatalf("extracted script is not executable: %v", fi.Mode())
	}

	// The symlink is a symlink, not a copy of its target.
	li, err := os.Lstat(filepath.Join(mnt, "sub", "link-to-top"))
	if err != nil {
		t.Fatal(err)
	}
	if li.Mode()&os.ModeSymlink == 0 {
		t.Fatalf("symlink extracted as %v", li.Mode())
	}

	// And the hard link is one inode with two names. GNU tar records the
	// second entry as a link, so this checks the mount's Link path.
	a, err := os.Stat(filepath.Join(mnt, "top.txt"))
	if err != nil {
		t.Fatal(err)
	}
	b, err := os.Stat(filepath.Join(mnt, "hard-to-top"))
	if err != nil {
		t.Fatal(err)
	}
	if !os.SameFile(a, b) {
		t.Fatal("tar's hard link extracted as a separate inode")
	}

	// Re-archiving from the mount and comparing listings closes the loop:
	// everything tar wrote in, it can read back out.
	back := filepath.Join(t.TempDir(), "back.tar")
	if out, err := execCommand("tar", "-cf", back, "-C", mnt, ".").CombinedOutput(); err != nil {
		t.Fatalf("tar -c from the mount: %v: %s", err, out)
	}
	listing, err := execCommand("tar", "-tf", back).Output()
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{"./top.txt", "./sub/deep/leaf.bin", "./bin/run.sh", "./sub/link-to-top"} {
		if !strings.Contains(string(listing), want) {
			t.Fatalf("%q missing from an archive made from the mount:\n%s", want, listing)
		}
	}
}

// execCommand runs an external tool against the mount under a timeout.
//
// Every one of these drives the mount from a separate process, which is
// the realistic shape — but they run against a FUSE server living in
// this test binary, and a wedged request there blocks the tool forever.
// CI has lost a whole run to that. A bounded command fails by name in
// seconds instead.
func execCommand(name string, args ...string) *exec.Cmd {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	cmd := exec.CommandContext(ctx, name, args...)
	// The cancel leaks deliberately until the process exits: Cmd holds
	// the context for its own lifetime, and a test process is short.
	_ = cancel
	return cmd
}
