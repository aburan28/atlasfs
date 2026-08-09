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
	"github.com/aburan28/atlasfs/pkg/store/local"
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
	if err := os.Symlink("hello.txt", filepath.Join(src, "hello-link.txt")); err != nil {
		t.Fatal(err)
	}

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
	if len(entries) != 3 {
		t.Fatalf("expected 3 top-level entries, got %d", len(entries))
	}

	linkTarget, err := os.Readlink(filepath.Join(mountpoint, "hello-link.txt"))
	if err != nil {
		t.Fatalf("readlink through FUSE mount: %v", err)
	}
	if linkTarget != "hello.txt" {
		t.Fatalf("got symlink target %q, want %q", linkTarget, "hello.txt")
	}
	gotViaLink, err := os.ReadFile(filepath.Join(mountpoint, "hello-link.txt"))
	if err != nil {
		t.Fatalf("read through symlink: %v", err)
	}
	if string(gotViaLink) != "hello atlasfs" {
		t.Fatalf("read through symlink got %q", gotViaLink)
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

// mountForTest mounts r at a fresh temp mountpoint and returns it,
// registering cleanup to unmount. Skips the test if /dev/fuse isn't
// available in this sandbox.
func mountForTest(t *testing.T, r *repo.Repo) string {
	t.Helper()
	if _, err := os.Stat("/dev/fuse"); err != nil {
		t.Skip("no /dev/fuse in this environment")
	}
	mountpoint := t.TempDir()
	mounted := make(chan *fuse.Server, 1)
	errCh := make(chan error, 1)
	go func() {
		errCh <- Mount(context.Background(), r, mountpoint, func(s *fuse.Server) { mounted <- s })
	}()
	var server *fuse.Server
	select {
	case server = <-mounted:
	case err := <-errCh:
		t.Fatalf("mount failed before reaching onMounted: %v", err)
	case <-time.After(10 * time.Second):
		t.Fatal("timed out waiting for FUSE mount")
	}
	t.Cleanup(func() {
		_ = server.Unmount()
		<-errCh
	})
	return mountpoint
}

// TestMountReadWrite is the writable-class vertical slice, end to end,
// over a real FUSE connection: create a file, write it, read it back
// (through a *different* open — not the same fd — so this actually
// exercises the commit-on-Release path rather than just the in-handle
// write buffer), overwrite it, delete it, and make a directory.
func TestMountReadWrite(t *testing.T) {
	repoDir := t.TempDir()
	r, err := repo.OpenWithClass(repoDir, mustLocalBackend(t, repoDir), repo.DefaultRegion, repo.ClassRelaxed)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()

	mountpoint := mountForTest(t, r)

	path := filepath.Join(mountpoint, "new.txt")
	if err := os.WriteFile(path, []byte("hello relaxed fuse"), 0o644); err != nil {
		t.Fatalf("create+write through FUSE mount: %v", err)
	}
	got, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read back through a fresh open: %v", err)
	}
	if string(got) != "hello relaxed fuse" {
		t.Fatalf("got %q, want %q", got, "hello relaxed fuse")
	}

	// Overwrite: DESIGN.md §16.1's atomic swap, exercised through the
	// kernel this time, not just pkg/repo directly.
	if err := os.WriteFile(path, []byte("overwritten"), 0o644); err != nil {
		t.Fatalf("overwrite through FUSE mount: %v", err)
	}
	got2, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if string(got2) != "overwritten" {
		t.Fatalf("got %q after overwrite, want %q", got2, "overwritten")
	}

	if err := os.Mkdir(filepath.Join(mountpoint, "sub"), 0o755); err != nil {
		t.Fatalf("mkdir through FUSE mount: %v", err)
	}
	subPath := filepath.Join(mountpoint, "sub", "f.txt")
	if err := os.WriteFile(subPath, []byte("in a subdir"), 0o644); err != nil {
		t.Fatalf("write in a newly-created subdir: %v", err)
	}
	gotSub, err := os.ReadFile(subPath)
	if err != nil || string(gotSub) != "in a subdir" {
		t.Fatalf("got %q err=%v, want %q", gotSub, err, "in a subdir")
	}

	if err := os.Remove(path); err != nil {
		t.Fatalf("unlink through FUSE mount: %v", err)
	}
	if _, err := os.Stat(path); err == nil {
		t.Fatal("expected the file to be gone after Remove")
	}

	// A negative lookup of the just-removed name must not be served
	// from a stale negative cache entry created before the removal —
	// there was never one to begin with here, but this exercises the
	// path where a fresh ENOENT after a real mutation is correct.
	if _, err := os.Stat(path); !os.IsNotExist(err) {
		t.Fatalf("expected ENOENT-shaped error, got %v", err)
	}

	if err := os.Remove(subPath); err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(filepath.Join(mountpoint, "sub")); err != nil {
		t.Fatalf("rmdir through FUSE mount: %v", err)
	}
}

// TestMountNegativeCacheInvalidatedByCreate exercises DESIGN.md §10.4
// through the real mount: repeatedly stat a name that doesn't exist yet
// (populating the negative cache), create it, and confirm the very next
// stat sees it — proving the dirver bump on Create actually invalidates
// the negative entry rather than leaving a stale ENOENT cached for up to
// the class's full lease duration.
func TestMountNegativeCacheInvalidatedByCreate(t *testing.T) {
	repoDir := t.TempDir()
	r, err := repo.OpenWithClass(repoDir, mustLocalBackend(t, repoDir), repo.DefaultRegion, repo.ClassRelaxed)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()

	mountpoint := mountForTest(t, r)
	path := filepath.Join(mountpoint, "appears-later.txt")

	for i := 0; i < 3; i++ {
		if _, err := os.Stat(path); !os.IsNotExist(err) {
			t.Fatalf("expected ENOENT before creation, got %v", err)
		}
	}

	if err := os.WriteFile(path, []byte("now it exists"), 0o644); err != nil {
		t.Fatal(err)
	}

	// ClassRelaxed's lease is 30s (DESIGN.md §8) — if the negative cache
	// weren't invalidated by Create's dirver bump, this would still
	// report ENOENT for up to 30 seconds. It must not.
	if _, err := os.Stat(path); err != nil {
		t.Fatalf("stat after create should succeed immediately, got %v", err)
	}
	got, err := os.ReadFile(path)
	if err != nil || string(got) != "now it exists" {
		t.Fatalf("got %q err=%v", got, err)
	}
}

// TestWriteFileHandleMultipleFlushesCommitLatestContent is a regression
// test for a real bug found via manual shell testing (bash's `>`
// redirection, not Go's os.WriteFile, which never happened to trigger
// it): FLUSH can fire more than once within a single open-for-write
// session — observed empirically, once right after a truncate arrives
// via Setattr (before all of a session's Write calls have necessarily
// happened) and again at the eventual close(2). The original
// commitIfDirty used a one-shot "committed" latch, so that first,
// premature flush — often over an empty or partial buffer — set the
// latch and silently blocked every later flush from ever persisting
// anything again, even though more data was written afterward.
//
// This exercises the exact shape at the unit level (bypassing kernel
// timing, which isn't reproducible on demand) so the fix doesn't need a
// live shell to keep it honest.
func TestWriteFileHandleMultipleFlushesCommitLatestContent(t *testing.T) {
	repoDir := t.TempDir()
	r, err := repo.OpenWithClass(repoDir, mustLocalBackend(t, repoDir), repo.DefaultRegion, repo.ClassRelaxed)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	ctx := context.Background()

	// A file must already exist for the "truncate then rewrite" shape
	// this bug depends on (create's own immediate empty-commit doesn't
	// go through writeFileHandle at all).
	wh, err := r.CreateFile(1, "f.txt") // metadb.RootInode == 1
	if err != nil {
		t.Fatal(err)
	}
	wh.Write([]byte("original content"))
	if _, err := wh.Commit(ctx); err != nil {
		t.Fatal(err)
	}

	h := &writeFileHandle{repo: r, parent: 1, name: "f.txt"}

	// Simulates Open() preloading existing content, then Setattr(size=0)
	// resizing that same buffer — the truncate path.
	h.buf = []byte("original content")
	h.resize(0)

	// The premature flush: fires before the real Write below, over an
	// empty buffer.
	if errno := h.commitIfDirty(ctx); errno != 0 {
		t.Fatalf("first commitIfDirty: errno %v", errno)
	}

	if _, errno := h.Write(ctx, []byte("the actual new content"), 0); errno != 0 {
		t.Fatalf("Write: errno %v", errno)
	}

	// The real flush, containing the data that must survive.
	if errno := h.commitIfDirty(ctx); errno != 0 {
		t.Fatalf("second commitIfDirty: errno %v", errno)
	}

	_, rec, err := r.Resolve("/f.txt")
	if err != nil {
		t.Fatal(err)
	}
	fr, err := r.OpenFile(ctx, rec)
	if err != nil {
		t.Fatal(err)
	}
	got, err := fr.ReadAll()
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != "the actual new content" {
		t.Fatalf("got %q, want %q — a later Flush must still be able to commit after an earlier one already fired", got, "the actual new content")
	}
}

func mustLocalBackend(t *testing.T, repoDir string) *local.Backend {
	t.Helper()
	b, err := local.New(filepath.Join(repoDir, "objects"))
	if err != nil {
		t.Fatal(err)
	}
	return b
}
