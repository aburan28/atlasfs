package mdsfuse

import (
	"bytes"
	"os"
	"path/filepath"
	"syscall"
	"testing"
	"time"

	"github.com/aburan28/atlasfs/pkg/mds"
)

// fallocate(2) through the authority-backed mount. The semantics are
// shared with the in-process mount (repo.ApplyFallocate), so what this
// adds is that the mode reaches this mount's handle at all and that its
// effect survives a commit across the RPC boundary — the buffer here is
// a bytes.Buffer rebuilt on every allocate, not a slice mutated in
// place.
func TestFallocateThroughAuthority(t *testing.T) {
	const (
		keepSize  = 0x01
		punchHole = 0x02
		zeroRange = 0x10
		collapse  = 0x08
	)
	c := startCluster(t, mds.Config{LeaseDuration: 30 * time.Second})
	mnt := c.mountAt(t, "writer", 0)

	p := filepath.Join(mnt, "holey")
	if err := os.WriteFile(p, bytes.Repeat([]byte("A"), 100), 0o644); err != nil {
		t.Fatal(err)
	}
	f, err := os.OpenFile(p, os.O_RDWR, 0o644)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()

	if err := syscall.Fallocate(int(f.Fd()), punchHole|keepSize, 20, 30); err != nil {
		t.Fatalf("fallocate PUNCH_HOLE: %v", err)
	}
	if err := syscall.Fallocate(int(f.Fd()), zeroRange, 100, 20); err != nil {
		t.Fatalf("fallocate ZERO_RANGE past EOF: %v", err)
	}
	if err := syscall.Fallocate(int(f.Fd()), collapse, 0, 8); err != syscall.EOPNOTSUPP {
		t.Fatalf("COLLAPSE_RANGE = %v, want EOPNOTSUPP", err)
	}
	if err := f.Close(); err != nil { // the flush that commits
		t.Fatal(err)
	}

	got, err := os.ReadFile(p)
	if err != nil {
		t.Fatal(err)
	}
	want := bytes.Repeat([]byte("A"), 100)
	clear(want[20:50])
	want = append(want, make([]byte, 20)...)
	if !bytes.Equal(got, want) {
		t.Fatalf("fallocate result did not survive the commit:\n got %q\nwant %q", got, want)
	}
}
