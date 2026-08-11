package fuseserver

import (
	"bytes"
	"os"
	"path/filepath"
	"syscall"
	"testing"
)

// fallocate(2) through a real mount. xfstests skips eleven of its quick
// tests when a filesystem lacks PUNCH_HOLE and ZERO_RANGE, so this is
// coverage as well as correctness.
//
// The observable contract of every mode here is about which bytes read
// as zeros and whether the file's length moves — that is what these
// assert, rather than anything about allocation, which a buffered write
// path has no way to express.
func TestFallocateThroughTheMount(t *testing.T) {
	_, mnt := mountWritable(t)

	const (
		keepSize  = 0x01
		punchHole = 0x02
		zeroRange = 0x10
		collapse  = 0x08
	)

	write := func(t *testing.T, name string, data []byte) *os.File {
		t.Helper()
		p := filepath.Join(mnt, name)
		if err := os.WriteFile(p, data, 0o644); err != nil {
			t.Fatal(err)
		}
		f, err := os.OpenFile(p, os.O_RDWR, 0o644)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { f.Close() })
		return f
	}
	readBack := func(t *testing.T, f *os.File) []byte {
		t.Helper()
		if err := f.Sync(); err != nil {
			t.Fatalf("fsync: %v", err)
		}
		st, err := f.Stat()
		if err != nil {
			t.Fatal(err)
		}
		buf := make([]byte, st.Size())
		if len(buf) > 0 {
			if _, err := f.ReadAt(buf, 0); err != nil {
				t.Fatalf("read back: %v", err)
			}
		}
		return buf
	}

	t.Run("punch hole zeroes the range and keeps the size", func(t *testing.T) {
		f := write(t, "punch", bytes.Repeat([]byte("A"), 100))
		if err := syscall.Fallocate(int(f.Fd()), punchHole|keepSize, 20, 30); err != nil {
			t.Fatalf("fallocate PUNCH_HOLE: %v", err)
		}
		got := readBack(t, f)
		if len(got) != 100 {
			t.Fatalf("size after PUNCH_HOLE = %d, want 100 (it must not change)", len(got))
		}
		want := append(append(bytes.Repeat([]byte("A"), 20), make([]byte, 30)...), bytes.Repeat([]byte("A"), 50)...)
		if !bytes.Equal(got, want) {
			t.Fatalf("PUNCH_HOLE did not zero exactly [20,50): got %q", got)
		}
	})

	t.Run("zero range may grow the file", func(t *testing.T) {
		f := write(t, "zero", bytes.Repeat([]byte("B"), 10))
		if err := syscall.Fallocate(int(f.Fd()), zeroRange, 5, 20); err != nil {
			t.Fatalf("fallocate ZERO_RANGE: %v", err)
		}
		got := readBack(t, f)
		if len(got) != 25 {
			t.Fatalf("size after ZERO_RANGE past EOF = %d, want 25", len(got))
		}
		want := append(bytes.Repeat([]byte("B"), 5), make([]byte, 20)...)
		if !bytes.Equal(got, want) {
			t.Fatalf("ZERO_RANGE result = %q", got)
		}
	})

	t.Run("plain allocate grows without disturbing data", func(t *testing.T) {
		f := write(t, "alloc", []byte("keepme"))
		if err := syscall.Fallocate(int(f.Fd()), 0, 0, 4096); err != nil {
			t.Fatalf("fallocate: %v", err)
		}
		got := readBack(t, f)
		if len(got) != 4096 {
			t.Fatalf("size after plain fallocate = %d, want 4096", len(got))
		}
		if !bytes.Equal(got[:6], []byte("keepme")) {
			t.Fatalf("plain fallocate overwrote existing data: %q", got[:6])
		}
		if !bytes.Equal(got[6:], make([]byte, 4096-6)) {
			t.Fatal("plain fallocate left non-zero bytes in the new range")
		}
	})

	t.Run("keep size does not grow the file", func(t *testing.T) {
		f := write(t, "keep", []byte("short"))
		if err := syscall.Fallocate(int(f.Fd()), keepSize, 0, 8192); err != nil {
			t.Fatalf("fallocate KEEP_SIZE: %v", err)
		}
		got := readBack(t, f)
		if len(got) != 5 {
			t.Fatalf("size after KEEP_SIZE = %d, want 5 (unchanged)", len(got))
		}
	})

	// Refused rather than silently ignored: a caller that gets
	// EOPNOTSUPP can fall back, one that gets success has lost the
	// operation without knowing.
	t.Run("collapse range is refused", func(t *testing.T) {
		f := write(t, "collapse", bytes.Repeat([]byte("C"), 100))
		err := syscall.Fallocate(int(f.Fd()), collapse, 0, 10)
		if err == nil {
			t.Fatal("COLLAPSE_RANGE reported success but the file is not reshaped")
		}
		if err != syscall.EOPNOTSUPP {
			t.Fatalf("COLLAPSE_RANGE = %v, want EOPNOTSUPP", err)
		}
	})

	t.Run("zero length is EINVAL", func(t *testing.T) {
		f := write(t, "zerolen", []byte("data"))
		if err := syscall.Fallocate(int(f.Fd()), 0, 0, 0); err != syscall.EINVAL {
			t.Fatalf("fallocate with len 0 = %v, want EINVAL", err)
		}
	})
}

// The commit path has to see fallocate's effect: a hole punched and
// never followed by a write must still be there after close and reopen,
// which means Allocate has to mark the handle dirty.
func TestFallocateSurvivesCommit(t *testing.T) {
	_, mnt := mountWritable(t)
	p := filepath.Join(mnt, "persisted")
	if err := os.WriteFile(p, bytes.Repeat([]byte("Z"), 64), 0o644); err != nil {
		t.Fatal(err)
	}

	f, err := os.OpenFile(p, os.O_RDWR, 0o644)
	if err != nil {
		t.Fatal(err)
	}
	if err := syscall.Fallocate(int(f.Fd()), 0x02|0x01, 16, 16); err != nil {
		t.Fatalf("fallocate PUNCH_HOLE: %v", err)
	}
	if err := f.Close(); err != nil { // the flush that commits it
		t.Fatal(err)
	}

	got, err := os.ReadFile(p)
	if err != nil {
		t.Fatal(err)
	}
	want := append(append(bytes.Repeat([]byte("Z"), 16), make([]byte, 16)...), bytes.Repeat([]byte("Z"), 32)...)
	if !bytes.Equal(got, want) {
		t.Fatalf("punched hole did not survive the commit: got %q", got)
	}
}
