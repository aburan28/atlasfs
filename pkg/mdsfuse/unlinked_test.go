package mdsfuse

import (
	"os"
	"path/filepath"
	"syscall"
	"testing"
	"time"

	"github.com/aburan28/atlasfs/pkg/mds"
)

// The authority-backed twin of pkg/fuseserver's open-but-unlinked tests.
// DESIGN.md §19.3's guarantee is a property of the filesystem, not of one
// mount implementation, and this mount reaches the inode over RPC rather
// than through a local metadb — a completely separate lookup path, with
// its own caches (pkg/mds/client.go's negative cache and lease epochs)
// that an unlink invalidates.
func TestReadThroughADescriptorSurvivesUnlinkThroughAuthority(t *testing.T) {
	c := startCluster(t, mds.Config{LeaseDuration: 30 * time.Second})
	mnt := c.mountAt(t, "holder", 0)

	p := filepath.Join(mnt, "doomed.txt")
	const payload = "still readable once the name is gone"
	if err := os.WriteFile(p, []byte(payload), 0o644); err != nil {
		t.Fatal(err)
	}

	f, err := os.Open(p)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()

	if err := os.Remove(p); err != nil {
		t.Fatalf("unlink: %v", err)
	}
	if _, err := os.Stat(p); !os.IsNotExist(err) {
		t.Fatalf("the name should be gone after unlink, got %v", err)
	}

	got := make([]byte, len(payload))
	if _, err := f.ReadAt(got, 0); err != nil {
		t.Fatalf("read through a descriptor open across unlink: %v", err)
	}
	if string(got) != payload {
		t.Fatalf("content through the surviving descriptor = %q, want %q", got, payload)
	}

	var st syscall.Stat_t
	if err := syscall.Fstat(int(f.Fd()), &st); err != nil {
		t.Fatalf("fstat through the surviving descriptor: %v", err)
	}
	if st.Nlink != 0 {
		t.Errorf("fstat nlink = %d, want 0 for an unlinked-but-open file", st.Nlink)
	}
	if st.Size != int64(len(payload)) {
		t.Errorf("fstat size = %d, want %d", st.Size, len(payload))
	}
}

// nlink must stay accurate while a file still has names — the unlink that
// drops one of several links reports the remaining count, and only the
// last one reports 0. Getting this wrong in the "0 means unset" direction
// is what made the unlinked case report 1; getting it wrong the other way
// would make a hard-linked file look deletable.
func TestNlinkCountsDownToZeroThroughAuthority(t *testing.T) {
	c := startCluster(t, mds.Config{LeaseDuration: 30 * time.Second})
	mnt := c.mountAt(t, "holder", 0)

	first := filepath.Join(mnt, "one.txt")
	second := filepath.Join(mnt, "two.txt")
	if err := os.WriteFile(first, []byte("two names"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.Link(first, second); err != nil {
		t.Fatal(err)
	}

	f, err := os.Open(first)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()

	if got := nlinkOfPath(t, second); got != 2 {
		t.Fatalf("nlink with two names = %d, want 2", got)
	}
	if err := os.Remove(first); err != nil {
		t.Fatal(err)
	}
	if got := nlinkOfPath(t, second); got != 1 {
		t.Fatalf("nlink after dropping one of two names = %d, want 1", got)
	}
	if err := os.Remove(second); err != nil {
		t.Fatal(err)
	}

	var st syscall.Stat_t
	if err := syscall.Fstat(int(f.Fd()), &st); err != nil {
		t.Fatalf("fstat after the last name went: %v", err)
	}
	if st.Nlink != 0 {
		t.Fatalf("nlink after the last name went = %d, want 0", st.Nlink)
	}
}

func nlinkOfPath(t *testing.T, path string) uint64 {
	t.Helper()
	var st syscall.Stat_t
	if err := syscall.Stat(path, &st); err != nil {
		t.Fatalf("stat %s: %v", path, err)
	}
	return uint64(st.Nlink)
}
