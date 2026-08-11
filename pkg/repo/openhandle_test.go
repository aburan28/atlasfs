package repo

import (
	"bytes"
	"context"
	"testing"
	"time"

	"github.com/aburan28/atlasfs/pkg/metadb"
)

// DESIGN.md §19.3: "the inode stays reachable while any client holds an
// open handle." That is the half of open-but-unlinked that GC owns —
// unlinking a file someone still has open and then sweeping it past
// grace must not pull the content out from under that reader.
//
// Grace does not cover this on its own: Invariant GC-1 bounds an
// uncommitted *write* session, not how long a descriptor may stay open,
// and an open descriptor may outlive any grace period.
func TestSweepSparesInodeWithAnOpenHandle(t *testing.T) {
	r, clk := openRelaxedWithClock(t)
	ctx := context.Background()

	data := bytes.Repeat([]byte("open-across-unlink-"), 40)
	writeCommit(t, r, metadb.RootInode, "doomed.txt", data)

	id, rec, err := r.Resolve("/doomed.txt")
	if err != nil {
		t.Fatal(err)
	}
	if !rec.HasInline {
		t.Fatalf("expected the payload to inline as a single chunk, got %+v", rec)
	}

	// A reader opens the file, then the name goes away. This is the
	// mkstemp-then-unlink idiom: the descriptor must keep working.
	release := r.OpenHandles.Acquire(id)
	defer release()

	if err := r.Unlink(metadb.RootInode, "doomed.txt"); err != nil {
		t.Fatal(err)
	}
	clk.advance(DefaultGraceDuration + time.Hour) // well past grace

	collected, err := r.Sweep(ctx, DefaultGraceDuration)
	if err != nil {
		t.Fatal(err)
	}
	if collected != 0 {
		t.Fatalf("Sweep collected %d chunks from an inode that still has an open handle; §19.3 requires it to stay reachable", collected)
	}

	// The strongest form: the content must still read back through the
	// record the holder already has, not merely have a surviving locator.
	fr, err := r.OpenFile(ctx, rec)
	if err != nil {
		t.Fatalf("opening the unlinked-but-held inode after Sweep: %v", err)
	}
	got, err := fr.ReadAll()
	if err != nil {
		t.Fatalf("reading the unlinked-but-held inode after Sweep: %v", err)
	}
	if !bytes.Equal(got, data) {
		t.Fatal("content of an unlinked-but-open file was corrupted by Sweep")
	}

	// The inode record itself must survive too — an fstat through the
	// descriptor has to keep answering.
	if _, err := r.DB.GetInode(id); err != nil {
		t.Fatalf("inode record of an unlinked-but-held file was deleted by Sweep: %v", err)
	}
}

// The other side of the same guarantee: once the last handle closes, the
// inode must become collectable again. A registry that pins forever is a
// storage leak wearing a correctness costume.
func TestSweepCollectsOnceTheLastOpenHandleCloses(t *testing.T) {
	r, clk := openRelaxedWithClock(t)
	ctx := context.Background()

	data := bytes.Repeat([]byte("collect-me-after-close-"), 40)
	writeCommit(t, r, metadb.RootInode, "doomed.txt", data)
	id, rec, err := r.Resolve("/doomed.txt")
	if err != nil {
		t.Fatal(err)
	}
	chunkID := rec.InlineChunk

	release := r.OpenHandles.Acquire(id)
	if err := r.Unlink(metadb.RootInode, "doomed.txt"); err != nil {
		t.Fatal(err)
	}
	clk.advance(DefaultGraceDuration + time.Hour)

	if collected, err := r.Sweep(ctx, DefaultGraceDuration); err != nil || collected != 0 {
		t.Fatalf("Sweep with the handle still open: collected=%d err=%v, want 0/nil", collected, err)
	}

	release() // last close

	collected, err := r.Sweep(ctx, DefaultGraceDuration)
	if err != nil {
		t.Fatal(err)
	}
	if collected != 1 {
		t.Fatalf("Sweep after the last handle closed collected %d, want 1", collected)
	}
	if found, err := r.DB.HasLocator(r.Region, chunkID); err != nil || found {
		t.Fatalf("expected the chunk's locator to be gone after the final sweep, found=%v err=%v", found, err)
	}
}

// Acquire/release is refcounted, not a boolean: two readers may hold the
// same inode, and the first one closing must not un-pin it for the
// second. A boolean flag here would be a use-after-free.
func TestOpenHandlesAreRefcounted(t *testing.T) {
	r, clk := openRelaxedWithClock(t)
	ctx := context.Background()

	writeCommit(t, r, metadb.RootInode, "doomed.txt", []byte("two readers hold this"))
	id, _, err := r.Resolve("/doomed.txt")
	if err != nil {
		t.Fatal(err)
	}

	first := r.OpenHandles.Acquire(id)
	second := r.OpenHandles.Acquire(id)
	if err := r.Unlink(metadb.RootInode, "doomed.txt"); err != nil {
		t.Fatal(err)
	}
	clk.advance(DefaultGraceDuration + time.Hour)

	first() // one of two readers closes
	if collected, err := r.Sweep(ctx, DefaultGraceDuration); err != nil || collected != 0 {
		t.Fatalf("Sweep with one of two handles still open: collected=%d err=%v, want 0/nil", collected, err)
	}
	if _, err := r.DB.GetInode(id); err != nil {
		t.Fatalf("inode deleted while a second handle was still open: %v", err)
	}

	second() // last close
	if collected, err := r.Sweep(ctx, DefaultGraceDuration); err != nil || collected != 1 {
		t.Fatalf("Sweep after both handles closed: collected=%d err=%v, want 1/nil", collected, err)
	}
}

// Releasing twice must not drive the count negative and un-pin an inode
// another holder still has open. FUSE can deliver RELEASE more than once
// for the same handle under interrupt-and-resend, which is exactly the
// path that produced a double-applied mutation in this codebase before.
func TestOpenHandleReleaseIsIdempotent(t *testing.T) {
	r, clk := openRelaxedWithClock(t)
	ctx := context.Background()

	writeCommit(t, r, metadb.RootInode, "doomed.txt", []byte("released twice"))
	id, _, err := r.Resolve("/doomed.txt")
	if err != nil {
		t.Fatal(err)
	}

	doubled := r.OpenHandles.Acquire(id)
	held := r.OpenHandles.Acquire(id)
	defer held()

	doubled()
	doubled() // the resend: must be a no-op, not a second decrement

	if err := r.Unlink(metadb.RootInode, "doomed.txt"); err != nil {
		t.Fatal(err)
	}
	clk.advance(DefaultGraceDuration + time.Hour)

	if collected, err := r.Sweep(ctx, DefaultGraceDuration); err != nil || collected != 0 {
		t.Fatalf("a doubled release un-pinned an inode another holder still has open: collected=%d err=%v", collected, err)
	}
}
