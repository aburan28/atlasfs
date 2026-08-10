package mdsfuse

import (
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	"github.com/aburan28/atlasfs/pkg/mds"
)

// TestTarRoundTripThroughTheAuthority is the same composition check as
// pkg/fuseserver's, but with every namespace operation crossing an RPC
// boundary and every byte going to object storage. tar drives mkdir,
// create, write, chmod, symlink and hard link in its own order, which is
// not the order any single unit test uses.
func TestTarRoundTripThroughTheAuthority(t *testing.T) {
	for _, bin := range []string{"tar", "diff"} {
		if _, err := exec.LookPath(bin); err != nil {
			t.Skipf("%s not available", bin)
		}
	}
	c := startCluster(t, mds.Config{LeaseDuration: 30 * time.Second})
	mnt := c.mountAt(t, "writer", 0)

	src := t.TempDir()
	writeSrc(t, src, "top.txt", []byte("top level"))
	writeSrc(t, src, "sub/nested.txt", []byte("nested"))
	writeSrc(t, src, "sub/deep/leaf.bin", make([]byte, 9000))
	writeSrc(t, src, "bin/run.sh", []byte("#!/bin/sh\necho hello\n"))
	if err := os.Chmod(filepath.Join(src, "bin/run.sh"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink("../top.txt", filepath.Join(src, "sub", "link-to-top")); err != nil {
		t.Fatal(err)
	}
	if err := os.Link(filepath.Join(src, "top.txt"), filepath.Join(src, "hard-to-top")); err != nil {
		t.Fatal(err)
	}

	archive := filepath.Join(t.TempDir(), "tree.tar")
	if out, err := exec.Command("tar", "-cf", archive, "-C", src, ".").CombinedOutput(); err != nil {
		t.Fatalf("tar -c: %v: %s", err, out)
	}
	if out, err := exec.Command("tar", "-xpf", archive, "-C", mnt).CombinedOutput(); err != nil {
		t.Fatalf("tar -x into the authority-backed mount: %v: %s", err, out)
	}
	if out, err := exec.Command("diff", "-r", src, mnt).CombinedOutput(); err != nil {
		t.Fatalf("extracted tree differs from the source:\n%s", out)
	}

	fi, err := os.Stat(filepath.Join(mnt, "bin", "run.sh"))
	if err != nil {
		t.Fatal(err)
	}
	if fi.Mode().Perm()&0o111 == 0 {
		t.Fatalf("extracted script is not executable: %v", fi.Mode())
	}
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
}
