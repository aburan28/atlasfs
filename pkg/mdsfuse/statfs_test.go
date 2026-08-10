package mdsfuse

import (
	"os"
	"path/filepath"
	"syscall"
	"testing"
	"time"

	"github.com/aburan28/atlasfs/pkg/mds"
)

// TestStatfsThroughAuthority: df on an authority-backed mount reports the
// authority's quota, not the node's local disk. Getting this from the
// backend instead would report whatever happens to be mounted at the
// object cache path, which is a different filesystem entirely.
func TestStatfsThroughAuthority(t *testing.T) {
	c := startCluster(t, mds.Config{LeaseDuration: 30 * time.Second})
	const limit = 8 << 20
	if err := c.db.SetQuotaLimits(limit, 50); err != nil {
		t.Fatal(err)
	}
	mnt := c.mountAt(t, "writer", 0)

	var st syscall.Statfs_t
	if err := syscall.Statfs(mnt, &st); err != nil {
		t.Fatalf("statfs: %v", err)
	}
	if total := st.Blocks * uint64(st.Bsize); total != limit {
		t.Fatalf("df total = %d bytes, want the authority's quota %d", total, limit)
	}
	if st.Files != 50 {
		t.Fatalf("df inode total = %d, want 50", st.Files)
	}

	freeBefore := st.Bavail
	if err := os.WriteFile(filepath.Join(mnt, "f"), make([]byte, 1<<20), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := syscall.Statfs(mnt, &st); err != nil {
		t.Fatal(err)
	}
	if st.Bavail >= freeBefore {
		t.Fatalf("free blocks did not fall after a 1 MiB write through the authority: %d -> %d", freeBefore, st.Bavail)
	}
}
