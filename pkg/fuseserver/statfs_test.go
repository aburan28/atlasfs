package fuseserver

import (
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"testing"

	"github.com/aburan28/atlasfs/pkg/repo"
)

// TestStatfsReportsTheQuota: df on the mount must show the subtree's
// quota, since the backend has no capacity of its own to report.
func TestStatfsReportsTheQuota(t *testing.T) {
	r, mnt := mountWritable(t)
	const limit = 16 << 20
	if err := r.SetQuota(limit, 100); err != nil {
		t.Fatal(err)
	}

	var st syscall.Statfs_t
	if err := syscall.Statfs(mnt, &st); err != nil {
		t.Fatalf("statfs: %v", err)
	}
	if total := st.Blocks * uint64(st.Bsize); total != limit {
		t.Fatalf("df total = %d bytes, want %d", total, limit)
	}
	if st.Files != 100 {
		t.Fatalf("df inode total = %d, want 100", st.Files)
	}
	freeBefore := st.Bavail

	if err := os.WriteFile(filepath.Join(mnt, "f"), make([]byte, 1<<20), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := syscall.Statfs(mnt, &st); err != nil {
		t.Fatal(err)
	}
	if st.Bavail >= freeBefore {
		t.Fatalf("free blocks did not fall after a 1 MiB write: %d -> %d", freeBefore, st.Bavail)
	}

	// And df itself parses it, which is the actual user-facing check.
	out, err := execCommand("df", "-k", mnt).CombinedOutput()
	if err != nil {
		t.Fatalf("df: %v: %s", err, out)
	}
	if !strings.Contains(string(out), mnt) {
		t.Fatalf("df did not report the mountpoint:\n%s", out)
	}
}

// TestStatfsWithoutAQuotaIsNotFull is the reason for the placeholder
// capacity: an unquotaed repo reporting zero free would make every tool
// that pre-checks space refuse to write to it.
func TestStatfsWithoutAQuotaIsNotFull(t *testing.T) {
	_, mnt := mountWritable(t)

	var st syscall.Statfs_t
	if err := syscall.Statfs(mnt, &st); err != nil {
		t.Fatal(err)
	}
	if st.Bavail == 0 {
		t.Fatal("an unquotaed repo reports zero free space; df would call it full")
	}
	if total := st.Blocks * uint64(st.Bsize); total != repo.UnboundedCapacity {
		t.Fatalf("df total = %d, want the placeholder %d", total, repo.UnboundedCapacity)
	}
}

// TestFsyncMakesTheWriteVisible is DESIGN.md §16.2's fsync(fd) contract:
// durable in the home region when it returns. Without a real Fsync the
// content would sit in the handle's buffer until close(2), so a process
// that wrote, fsynced and then died would lose it — with fsync having
// returned success.
func TestFsyncMakesTheWriteVisible(t *testing.T) {
	r, mnt := mountWritable(t)

	f, err := os.Create(filepath.Join(mnt, "synced"))
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	if _, err := f.Write([]byte("committed by fsync")); err != nil {
		t.Fatal(err)
	}
	if err := f.Sync(); err != nil {
		t.Fatalf("fsync: %v", err)
	}

	// Read through the repo API rather than through the mount: a read via
	// the same mount could be served from the still-open handle's buffer,
	// which would pass whether or not anything was committed.
	_, rec, err := r.Resolve("/synced")
	if err != nil {
		t.Fatalf("fsync returned success but nothing was committed: %v", err)
	}
	fr, err := r.OpenFile(t.Context(), rec)
	if err != nil {
		t.Fatal(err)
	}
	got, err := fr.ReadAll()
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != "committed by fsync" {
		t.Fatalf("committed content = %q, want %q", got, "committed by fsync")
	}
}
