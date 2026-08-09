package fuseserver

import (
	"bytes"
	"context"
	"crypto/rand"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/hanwen/go-fuse/v2/fuse"

	"github.com/aburan28/atlasfs/pkg/repo"
)

// TestMountReadBack is the full vertical slice, end to end: publish a
// small tree into a repo, mount it read-only over a real FUSE
// connection, and read the files back through the kernel — not through
// the repo API directly. This is the test that actually exercises
// /dev/fuse; if the sandbox forbids FUSE mounts it skips rather than
// failing the suite.
func TestMountReadBack(t *testing.T) {
	if _, err := os.Stat("/dev/fuse"); err != nil {
		t.Skip("no /dev/fuse in this environment")
	}

	src := t.TempDir()
	mustWrite(t, filepath.Join(src, "hello.txt"), []byte("hello atlasfs"))
	big := make([]byte, 40*1024)
	if _, err := rand.Read(big); err != nil {
		t.Fatal(err)
	}
	mustWrite(t, filepath.Join(src, "data", "big.bin"), big)

	repoDir := t.TempDir()
	r, err := repo.Open(repoDir)
	if err != nil {
		t.Fatal(err)
	}
	r.ChunkSize = 4096
	if _, _, _, err := r.PublishTree(context.Background(), src, nil); err != nil {
		t.Fatal(err)
	}
	r.Close() // mount path reopens the repo like the CLI does

	r2, err := repo.Open(repoDir)
	if err != nil {
		t.Fatal(err)
	}
	defer r2.Close()

	mountpoint := t.TempDir()
	mounted := make(chan *fuse.Server, 1)
	errCh := make(chan error, 1)

	go func() {
		err := Mount(context.Background(), r2, mountpoint, func(s *fuse.Server) {
			mounted <- s
		})
		errCh <- err
	}()

	var server *fuse.Server
	select {
	case server = <-mounted:
	case err := <-errCh:
		t.Fatalf("mount failed before reaching onMounted: %v", err)
	case <-time.After(10 * time.Second):
		t.Fatal("timed out waiting for FUSE mount")
	}
	defer func() {
		_ = server.Unmount()
		<-errCh
	}()

	got, err := os.ReadFile(filepath.Join(mountpoint, "hello.txt"))
	if err != nil {
		t.Fatalf("read hello.txt through FUSE mount: %v", err)
	}
	if string(got) != "hello atlasfs" {
		t.Fatalf("got %q, want %q", got, "hello atlasfs")
	}

	gotBig, err := os.ReadFile(filepath.Join(mountpoint, "data", "big.bin"))
	if err != nil {
		t.Fatalf("read data/big.bin through FUSE mount: %v", err)
	}
	if !bytes.Equal(gotBig, big) {
		t.Fatal("multi-chunk file content mismatch through FUSE mount")
	}

	entries, err := os.ReadDir(mountpoint)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 2 {
		t.Fatalf("expected 2 top-level entries, got %d", len(entries))
	}

	// The mount is read-only end to end: DESIGN.md §8's `immutable`
	// class means write attempts must fail, not merely be undocumented.
	err = os.WriteFile(filepath.Join(mountpoint, "new.txt"), []byte("x"), 0o644)
	if err == nil {
		t.Fatal("expected write to a read-only atlasfs mount to fail")
	}
}

func mustWrite(t *testing.T, path string, data []byte) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, data, 0o644); err != nil {
		t.Fatal(err)
	}
}
