package fuseserver

import (
	"os"
	"path/filepath"
	"syscall"
	"testing"
)

// DESIGN.md §19.3: an unlinked file stays alive while any client holds an
// open handle. POSIX requires the descriptor to keep working — read,
// write and fstat — until the last one closes, which is what makes the
// "open a temp file and immediately unlink it" idiom safe.
//
// This is pjdfstest's unlink/14, the one case that suite still fails
// here, reproduced directly so the failure is diagnosable without
// standing up the whole suite.
func TestReadThroughADescriptorSurvivesUnlink(t *testing.T) {
	_, mnt := mountWritable(t)
	p := filepath.Join(mnt, "doomed.txt")
	const payload = "still here after the name is gone"

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

	// The descriptor was opened before the unlink, so it must still read.
	got, err := readAllAt(f)
	if err != nil {
		t.Fatalf("read through a descriptor open across unlink: %v", err)
	}
	if string(got) != payload {
		t.Fatalf("content through the surviving descriptor = %q, want %q", got, payload)
	}

	// fstat(2) on the descriptor must work too, and report nlink 0 — the
	// signal applications use to tell "unlinked but open" from "linked".
	var st syscall.Stat_t
	if err := fstatFile(f, &st); err != nil {
		t.Fatalf("fstat through the surviving descriptor: %v", err)
	}
	if st.Size != int64(len(payload)) {
		t.Fatalf("fstat size = %d, want %d", st.Size, len(payload))
	}
	if st.Nlink != 0 {
		t.Errorf("fstat nlink = %d, want 0 for an unlinked-but-open file", st.Nlink)
	}
}

// The write half of the same guarantee: the unlinked file is still a
// file, so writing through the descriptor and reading it back must work.
// This is the idiom every mkstemp-then-unlink scratch file depends on.
func TestWriteThroughADescriptorSurvivesUnlink(t *testing.T) {
	_, mnt := mountWritable(t)
	p := filepath.Join(mnt, "scratch.tmp")

	f, err := os.OpenFile(p, os.O_RDWR|os.O_CREATE, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	if err := os.Remove(p); err != nil {
		t.Fatalf("unlink: %v", err)
	}

	const payload = "written after the name was removed"
	if _, err := f.WriteAt([]byte(payload), 0); err != nil {
		t.Fatalf("write through a descriptor open across unlink: %v", err)
	}
	if err := f.Sync(); err != nil {
		t.Fatalf("fsync through a descriptor open across unlink: %v", err)
	}

	got := make([]byte, len(payload))
	if _, err := f.ReadAt(got, 0); err != nil {
		t.Fatalf("read back through the surviving descriptor: %v", err)
	}
	if string(got) != payload {
		t.Fatalf("read back = %q, want %q", got, payload)
	}
}

func readAllAt(f *os.File) ([]byte, error) {
	st, err := f.Stat()
	if err != nil {
		return nil, err
	}
	buf := make([]byte, st.Size())
	if len(buf) == 0 {
		return buf, nil
	}
	_, err = f.ReadAt(buf, 0)
	return buf, err
}

func fstatFile(f *os.File, st *syscall.Stat_t) error {
	return syscall.Fstat(int(f.Fd()), st)
}
