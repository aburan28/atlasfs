package fuseserver

import (
	"os"
	"path/filepath"
	"syscall"
	"testing"
	"time"
)

// The three timestamps move independently, and xfstests generic/003
// checks exactly which of them each operation is allowed to touch. Two
// of its expectations failed here for unrelated reasons, both fixed:
//
//   - Writing a file moved atime, because atime fell back to mtime
//     whenever it was unset (InodeRecord.Atime) — so the two aliased and
//     any write appeared to be an access.
//   - Renaming a file did not move ctime, which POSIX requires: the
//     inode's metadata changed even though its content did not.
//
// The rename case needed two fixes, and the second is the interesting
// one: storing the new ctime is not enough, because ctime is in the
// *inode's* lease domain (§10.5) and Rename bumped only the two
// directories, so this mount kept serving the pre-rename record from
// cache.
func TestTimestampsFollowPOSIXRules(t *testing.T) {
	_, mnt := mountWritable(t)
	p := filepath.Join(mnt, "f")

	times := func(t *testing.T, path string) (atime, mtime, ctime time.Time) {
		t.Helper()
		var st syscall.Stat_t
		if err := syscall.Lstat(path, &st); err != nil {
			t.Fatal(err)
		}
		return time.Unix(st.Atim.Sec, st.Atim.Nsec),
			time.Unix(st.Mtim.Sec, st.Mtim.Nsec),
			time.Unix(st.Ctim.Sec, st.Ctim.Nsec)
	}

	if err := os.WriteFile(p, []byte("one"), 0o644); err != nil {
		t.Fatal(err)
	}
	a1, m1, c1 := times(t, p)

	// A write changes content and metadata, but is not an access.
	time.Sleep(20 * time.Millisecond)
	if err := os.WriteFile(p, []byte("two"), 0o644); err != nil {
		t.Fatal(err)
	}
	a2, m2, c2 := times(t, p)
	if !a2.Equal(a1) {
		t.Errorf("atime moved on write (%v -> %v); writing a file is not accessing it, and this is what atime aliasing to mtime looks like", a1, a2)
	}
	if !m2.After(m1) {
		t.Errorf("mtime did not move on write: %v -> %v", m1, m2)
	}
	if !c2.After(c1) {
		t.Errorf("ctime did not move on write: %v -> %v", c1, c2)
	}

	// A rename changes the inode's metadata and nothing else.
	renamed := filepath.Join(mnt, "g")
	time.Sleep(20 * time.Millisecond)
	if err := syscall.Rename(p, renamed); err != nil {
		t.Fatal(err)
	}
	a3, m3, c3 := times(t, renamed)
	if !a3.Equal(a2) {
		t.Errorf("atime moved on rename: %v -> %v", a2, a3)
	}
	if !m3.Equal(m2) {
		t.Errorf("mtime moved on rename (%v -> %v); a rename does not change content", m2, m3)
	}
	if !c3.After(c2) {
		t.Errorf("ctime did not move on rename (%v -> %v); POSIX requires it, and the inode's own lease has to be bumped for this mount to see it", c2, c3)
	}
}
