package mds

import (
	"context"
	"errors"
	"syscall"
	"testing"
	"time"

	"github.com/aburan28/atlasfs/pkg/metadb"
)

func TestToStatusCarriesErrno(t *testing.T) {
	cases := []struct {
		err  error
		want syscall.Errno
	}{
		{metadb.ErrNotFound, syscall.ENOENT},
		{metadb.ErrExists, syscall.EEXIST},
		{metadb.ErrNotDir, syscall.ENOTDIR},
		{metadb.ErrIsDirectory, syscall.EISDIR},
		{metadb.ErrNotEmpty, syscall.ENOTEMPTY},
		{metadb.ErrInvalidRename, syscall.EINVAL},
		{metadb.ErrQuotaExceeded, syscall.EDQUOT},
	}
	for _, tc := range cases {
		got, ok := ErrnoOf(toStatus(tc.err))
		if !ok || got != tc.want {
			t.Errorf("ErrnoOf(toStatus(%v)) = %v ok=%v, want %v", tc.err, got, ok, tc.want)
		}
	}
	// An untagged error must report false rather than a bogus errno, so
	// the caller falls back to mapping the gRPC code.
	if _, ok := ErrnoOf(errors.New("plain")); ok {
		t.Error("ErrnoOf reported an errno for an untagged error")
	}
	if _, ok := ErrnoOf(nil); ok {
		t.Error("ErrnoOf reported an errno for nil")
	}
}

// TestErrnoSurvivesTheWire is the case the tagging exists for: a direct
// API caller has no VFS in front of it to pre-empt type mismatches, so
// the distinction has to survive the RPC itself.
func TestErrnoSurvivesTheWire(t *testing.T) {
	env := startServer(t, Config{LeaseDuration: 30 * time.Second})
	c := env.client("caller", nil)
	ctx := context.Background()

	dir, err := c.Mkdir(ctx, metadb.RootInode, "d")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := c.Mkdir(ctx, dir, "inside"); err != nil {
		t.Fatal(err)
	}
	if _, err := c.Commit(ctx, metadb.RootInode, "f", metadb.InodeRecord{Mode: 0o644, NLink: 1}); err != nil {
		t.Fatal(err)
	}

	cases := []struct {
		name string
		call func() error
		want syscall.Errno
	}{
		{"rmdir a non-empty directory", func() error { return c.Rmdir(ctx, metadb.RootInode, "d") }, syscall.ENOTEMPTY},
		{"rename a file over a directory", func() error { return c.Rename(ctx, metadb.RootInode, "f", metadb.RootInode, "d") }, syscall.EISDIR},
		{"rename a directory over a file", func() error { return c.Rename(ctx, metadb.RootInode, "d", metadb.RootInode, "f") }, syscall.ENOTDIR},
		{"rename a missing name", func() error { return c.Rename(ctx, metadb.RootInode, "gone", metadb.RootInode, "x") }, syscall.ENOENT},
		{"symlink over an existing name", func() error {
			_, err := c.Symlink(ctx, metadb.RootInode, "f", "target")
			return err
		}, syscall.EEXIST},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := tc.call()
			if err == nil {
				t.Fatal("expected a failure")
			}
			got, ok := ErrnoOf(err)
			if !ok || got != tc.want {
				t.Fatalf("got %v (errno %v ok=%v), want %v", err, got, ok, tc.want)
			}
		})
	}
}

// TestNegativeEntryIsClearedByADirectoryMutation exercises §10.4 on the
// client side: a cached miss is validated against the directory, so any
// mutation under it clears every miss at once.
func TestNegativeEntryIsClearedByADirectoryMutation(t *testing.T) {
	env := startServer(t, Config{LeaseDuration: 30 * time.Second})
	c := env.client("caller", nil)
	ctx := context.Background()

	if resp, err := c.Lookup(ctx, metadb.RootInode, "later"); err != nil || resp.Found {
		t.Fatalf("expected a miss, got found=%v err=%v", resp.Found, err)
	}
	if !c.cachedNegative(metadb.RootInode, "later") {
		t.Fatal("the miss was not cached; §10.4's negative entry is not being held")
	}

	// This holder's own mutation must clear its own negative entry — it
	// is deliberately excluded from its own recall, so nothing else will.
	if _, err := c.Commit(ctx, metadb.RootInode, "later", metadb.InodeRecord{Mode: 0o644, NLink: 1}); err != nil {
		t.Fatal(err)
	}
	if c.cachedNegative(metadb.RootInode, "later") {
		t.Fatal("the negative entry survived a create of the same name")
	}
	if resp, err := c.Lookup(ctx, metadb.RootInode, "later"); err != nil || !resp.Found {
		t.Fatalf("created name not visible: found=%v err=%v", resp.Found, err)
	}
}

// TestStoreDropsAResponseInvalidatedInFlight is the epoch guard. A
// response that was already on the wire when an invalidation landed
// carries a pre-invalidation view; storing it would resurrect precisely
// what the invalidation removed, and the entry would then be trusted for
// a full lease with nothing left to clear it.
func TestStoreDropsAResponseInvalidatedInFlight(t *testing.T) {
	env := startServer(t, Config{LeaseDuration: 30 * time.Second})
	c := env.client("caller", nil)

	obj := inodeObj(metadb.RootInode)
	since := c.epochOf(obj)
	c.invalidate(obj) // the push lands while the response is in flight
	c.store(obj, metadb.InodeRecord{Size: 42}, Lease{TTL: time.Minute, Version: 1}, since)

	if _, ok := c.CachedInode(metadb.RootInode); ok {
		t.Fatal("a response invalidated in flight was cached anyway")
	}
	// The same store with a current epoch must still land, or the guard
	// would just be disabling the cache.
	c.store(obj, metadb.InodeRecord{Size: 42}, Lease{TTL: time.Minute, Version: 1}, c.epochOf(obj))
	if _, ok := c.CachedInode(metadb.RootInode); !ok {
		t.Fatal("the guard rejected a store it should have accepted")
	}
}
