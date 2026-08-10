package fuseserver

import (
	"os"
	"os/exec"
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
	out, err := exec.Command("df", "-k", mnt).CombinedOutput()
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
