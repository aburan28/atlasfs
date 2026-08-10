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

// TestFsyncThroughAuthority: §16.2's fsync(fd) commits at the authority
// when it returns, rather than leaving the content buffered until
// close(2). A second, independent mount is the observer — reading back
// through the same mount could be served from the still-open handle's
// own buffer and would pass either way.
func TestFsyncThroughAuthority(t *testing.T) {
	c := startCluster(t, mds.Config{LeaseDuration: 30 * time.Second})
	writer := c.mountAt(t, "writer", 0)
	reader := c.mountAt(t, "reader", 0)

	f, err := os.Create(filepath.Join(writer, "synced"))
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

	awaitOrFail(t, "fsync returned success but the other mount never saw the content", func() bool {
		got, err := os.ReadFile(filepath.Join(reader, "synced"))
		return err == nil && string(got) == "committed by fsync"
	})
}

// TestChmodThroughAuthority: a mode change is metadata, so it goes to the
// authority and shows up on every other holder — the same invalidation
// path a write takes.
func TestChmodThroughAuthority(t *testing.T) {
	c := startCluster(t, mds.Config{LeaseDuration: 30 * time.Second})
	writer := c.mountAt(t, "writer", 0)
	reader := c.mountAt(t, "reader", 0)

	path := filepath.Join(writer, "script")
	if err := os.WriteFile(path, []byte("#!/bin/sh\necho hi\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	// The reader caches the pre-chmod mode under its lease.
	if fi, err := os.Stat(filepath.Join(reader, "script")); err != nil {
		t.Fatal(err)
	} else if fi.Mode().Perm() != 0o644 {
		t.Fatalf("initial mode = %v, want 0644", fi.Mode().Perm())
	}

	if err := os.Chmod(path, 0o755); err != nil {
		t.Fatalf("chmod: %v", err)
	}
	if fi, err := os.Stat(path); err != nil || fi.Mode().Perm() != 0o755 {
		t.Fatalf("writer sees mode %v err=%v, want 0755", fi.Mode().Perm(), err)
	}

	awaitOrFail(t, "the other mount never saw the chmod", func() bool {
		fi, err := os.Stat(filepath.Join(reader, "script"))
		return err == nil && fi.Mode().Perm() == 0o755
	})

	// And it survives a rewrite: a commit carries content, not permissions.
	if err := os.WriteFile(path, []byte("#!/bin/sh\necho hi again\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	if fi, err := os.Stat(path); err != nil || fi.Mode().Perm() != 0o755 {
		t.Fatalf("overwrite reset the mode to %v err=%v", fi.Mode().Perm(), err)
	}
}

// TestCreateHonoursTheRequestedModeThroughAuthority: the create mode has
// to survive the RPC to the authority too, or a program that creates an
// executable directly gets a 0644 file.
func TestCreateHonoursTheRequestedModeThroughAuthority(t *testing.T) {
	c := startCluster(t, mds.Config{LeaseDuration: 30 * time.Second})
	mnt := c.mountAt(t, "writer", 0)

	for _, mode := range []os.FileMode{0o755, 0o600} {
		name := filepath.Join(mnt, "created-"+mode.String())
		f, err := os.OpenFile(name, os.O_CREATE|os.O_WRONLY, mode)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := f.Write([]byte("content")); err != nil {
			t.Fatal(err)
		}
		if err := f.Close(); err != nil {
			t.Fatal(err)
		}
		fi, err := os.Stat(name)
		if err != nil {
			t.Fatal(err)
		}
		if fi.Mode().Perm() != mode {
			t.Errorf("O_CREAT with mode %v produced %v", mode, fi.Mode().Perm())
		}
	}
}

// TestOwnershipThroughAuthority: the authority cannot derive who made a
// syscall, so the mount forwards the caller's uid/gid on every creating
// operation and on chown (DESIGN.md §20).
func TestOwnershipThroughAuthority(t *testing.T) {
	c := startCluster(t, mds.Config{LeaseDuration: 30 * time.Second})
	mnt := c.mountAt(t, "writer", 0)

	if err := os.WriteFile(filepath.Join(mnt, "f"), []byte("x"), 0o644); err != nil {
		t.Fatal(err)
	}
	fi, err := os.Lstat(filepath.Join(mnt, "f"))
	if err != nil {
		t.Fatal(err)
	}
	st := fi.Sys().(*syscall.Stat_t)
	if st.Uid != uint32(os.Getuid()) || st.Gid != uint32(os.Getgid()) {
		t.Fatalf("new file owned by %d:%d, want %d:%d", st.Uid, st.Gid, os.Getuid(), os.Getgid())
	}

	if os.Getuid() != 0 {
		t.Skip("changing a file's owner to an arbitrary uid needs root")
	}
	if err := os.Chown(filepath.Join(mnt, "f"), 4242, 4343); err != nil {
		t.Fatalf("chown: %v", err)
	}
	fi, err = os.Lstat(filepath.Join(mnt, "f"))
	if err != nil {
		t.Fatal(err)
	}
	st = fi.Sys().(*syscall.Stat_t)
	if st.Uid != 4242 || st.Gid != 4343 {
		t.Fatalf("after chown through the authority: %d:%d, want 4242:4343", st.Uid, st.Gid)
	}
}
