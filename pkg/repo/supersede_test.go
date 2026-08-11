package repo

import (
	"context"
	"testing"
	"time"

	"github.com/aburan28/atlasfs/pkg/chunk"
	"github.com/aburan28/atlasfs/pkg/metadb"
	"github.com/aburan28/atlasfs/pkg/pack"
)

// Overwriting a file leaves its previous chunks referenced by nothing:
// the inode now points at the new content, and no other name holds the
// old. DESIGN.md §19.1's mark phase is defined over reachability, so
// those chunks are garbage by definition and a sweep past grace must
// reclaim them.
//
// This is the failure mode behind §27's fsx soak filling the volume —
// 21 GB of backing store for a 256 KB file — so the assertion here is
// the one that matters for whether a long-running writer is viable at
// all, not just whether GC frees something eventually.
func TestSweepReclaimsChunksSupersededByAnOverwrite(t *testing.T) {
	r, clk := openRelaxedWithClock(t)
	ctx := context.Background()

	first := []byte("the original content of this file, superseded below")
	writeCommit(t, r, metadb.RootInode, "f.txt", first)
	_, rec1, err := r.Resolve("/f.txt")
	if err != nil {
		t.Fatal(err)
	}
	if !rec1.HasInline {
		t.Fatalf("expected a single inline chunk, got %+v", rec1)
	}
	superseded := rec1.InlineChunk

	second := []byte("completely different content that replaces the first")
	writeCommit(t, r, metadb.RootInode, "f.txt", second)
	_, rec2, err := r.Resolve("/f.txt")
	if err != nil {
		t.Fatal(err)
	}
	if rec2.InlineChunk == superseded {
		t.Fatal("the overwrite did not change the content pointer; this test proves nothing")
	}

	clk.advance(DefaultGraceDuration + time.Hour)
	if _, err := r.Sweep(ctx, DefaultGraceDuration); err != nil {
		t.Fatal(err)
	}

	if found, err := r.DB.HasLocator(r.Region, superseded); err != nil {
		t.Fatal(err)
	} else if found {
		t.Fatal("the superseded chunk's locator survived a sweep past grace: an overwrite leaks its old content forever")
	}

	// The surviving content must be untouched — a reclaim that also eats
	// the live chunk is not a fix.
	fr, err := r.OpenFile(ctx, rec2)
	if err != nil {
		t.Fatal(err)
	}
	got, err := fr.ReadAll()
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != string(second) {
		t.Fatalf("current content = %q, want %q", got, second)
	}
}

// The same leak reached through the metric that made it visible: repeated
// overwrites of one file must not grow the number of live locators
// without bound. Each round supersedes the previous round's chunk, so
// after a sweep the count should return to one-per-live-file rather than
// one-per-write.
func TestRepeatedOverwritesDoNotGrowStorageWithoutBound(t *testing.T) {
	r, clk := openRelaxedWithClock(t)
	ctx := context.Background()

	const rounds = 25
	for i := range rounds {
		writeCommit(t, r, metadb.RootInode, "hot.txt", []byte("revision number "+string(rune('a'+i))+" of a hot file"))
	}

	before, err := countLocators(r)
	if err != nil {
		t.Fatal(err)
	}
	if before < rounds {
		t.Fatalf("expected %d rounds to leave %d locators before GC, got %d; the test is not exercising what it claims", rounds, rounds, before)
	}

	clk.advance(DefaultGraceDuration + time.Hour)
	if _, err := r.Sweep(ctx, DefaultGraceDuration); err != nil {
		t.Fatal(err)
	}

	after, err := countLocators(r)
	if err != nil {
		t.Fatal(err)
	}
	if after != 1 {
		t.Fatalf("after sweeping %d overwrites of one file: %d locators live, want 1 (was %d before GC)", rounds, after, before)
	}
}

// The safety direction, and the one that matters more: a chunk whose
// container is still within grace must survive, even though nothing
// references it yet.
//
// This is not hypothetical. DESIGN.md §16.1's write path seals chunks and
// registers their locators *before* committing the inode that points at
// them, so between those two steps a perfectly healthy in-flight write
// is indistinguishable from garbage by reachability alone. Invariant
// GC-1 exists to size T_grace past that window. A sweep that collected
// on reachability without the grace gate would corrupt concurrent
// writers — a far worse bug than the leak it fixes.
func TestSweepLeavesUnreferencedButRecentChunksAlone(t *testing.T) {
	r, clk := openRelaxedWithClock(t)
	ctx := context.Background()

	// Stand in for a write session that has sealed and registered its
	// chunks but has not yet committed the inode.
	data := []byte("sealed and registered, inode commit still pending")
	c := chunk.SplitBytes(data, chunk.DefaultSize)[0]
	loc, err := sealOneChunk(ctx, r, c)
	if err != nil {
		t.Fatal(err)
	}
	if err := r.DB.PutLocator(r.Region, c.ID, loc, r.Clock.Now()); err != nil {
		t.Fatal(err)
	}

	// Advance far enough that any *graveyard* entry would be collectable,
	// but keep the container itself inside grace.
	clk.advance(DefaultGraceDuration / 2)

	if _, err := r.Sweep(ctx, DefaultGraceDuration); err != nil {
		t.Fatal(err)
	}
	if found, err := r.DB.HasLocator(r.Region, c.ID); err != nil {
		t.Fatal(err)
	} else if !found {
		t.Fatal("a chunk sealed within grace was collected: an in-flight write session would lose its content")
	}

	// Past grace with still nothing referencing it, it is genuinely
	// garbage and must go — otherwise the gate is just a leak with extra
	// steps.
	clk.advance(DefaultGraceDuration)
	if _, err := r.Sweep(ctx, DefaultGraceDuration); err != nil {
		t.Fatal(err)
	}
	if found, err := r.DB.HasLocator(r.Region, c.ID); err != nil {
		t.Fatal(err)
	} else if found {
		t.Fatal("a chunk unreferenced and past grace survived the sweep")
	}
}

// sealOneChunk packs a single chunk into its own container and returns
// its locator, without touching any inode.
func sealOneChunk(ctx context.Context, r *Repo, c chunk.Chunk) (pack.Locator, error) {
	packer := pack.NewPacker(r.Backend, r.Region, 0)
	packer.Add(c)
	locs, err := packer.Seal(ctx)
	if err != nil {
		return pack.Locator{}, err
	}
	return locs[c.ID], nil
}

func countLocators(r *Repo) (int, error) {
	entries, err := r.DB.ListLocators(r.Region)
	if err != nil {
		return 0, err
	}
	return len(entries), nil
}
