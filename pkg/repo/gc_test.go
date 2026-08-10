package repo

import (
	"bytes"
	"context"
	"errors"
	"testing"
	"time"

	"github.com/aburan28/atlasfs/pkg/metadb"
	"github.com/aburan28/atlasfs/pkg/store/local"
)

// fakeClock gives GC tests exact control over "now" — the same role it
// plays in pkg/coherence's own tests — so a grace period well over an
// hour can be exercised without sleeping for real.
type fakeClock struct{ now time.Time }

func (c *fakeClock) Now() time.Time          { return c.now }
func (c *fakeClock) advance(d time.Duration) { c.now = c.now.Add(d) }

func openRelaxedWithClock(t *testing.T) (*Repo, *fakeClock) {
	t.Helper()
	r := openRelaxed(t)
	clk := &fakeClock{now: time.Unix(1_700_000_000, 0)}
	r.Clock = clk
	return r, clk
}

func writeCommit(t *testing.T, r *Repo, dir metadb.InodeID, name string, data []byte) metadb.InodeID {
	t.Helper()
	h, err := r.CreateFile(dir, name)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := h.Write(data); err != nil {
		t.Fatal(err)
	}
	id, err := h.Commit(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	return id
}

// TestSweepRefusesGraceDurationViolatingGC1 is the DESIGN.md §19.2
// Invariant GC-1 check: Sweep must refuse to run — not merely warn —
// when graceDuration does not satisfy T_grace > T_write_max + D_max +
// epsilon.
func TestSweepRefusesGraceDurationViolatingGC1(t *testing.T) {
	r, _ := openRelaxedWithClock(t)
	ctx := context.Background()

	for _, d := range []time.Duration{0, time.Second, TWriteMax, MinGraceDuration} {
		if _, err := r.Sweep(ctx, d); !errors.Is(err, ErrGraceTooShort) {
			t.Fatalf("Sweep(%s) = %v, want ErrGraceTooShort", d, err)
		}
	}

	// A graceDuration that does clear the invariant must be accepted
	// (an empty graveyard, so nothing to collect, but no error either).
	if _, err := r.Sweep(ctx, DefaultGraceDuration); err != nil {
		t.Fatalf("Sweep(DefaultGraceDuration) on an empty graveyard: %v", err)
	}
}

// TestDMaxMatchesRelaxedLeaseDuration pins the DMax constant to
// ClassRelaxed's actual lease duration (see gc.go's doc comment on
// DMax) so the two cannot silently drift apart.
func TestDMaxMatchesRelaxedLeaseDuration(t *testing.T) {
	if got := ClassRelaxed.leaseDuration(); got != DMax {
		t.Fatalf("ClassRelaxed.leaseDuration() = %s, want DMax = %s", got, DMax)
	}
}

// TestSweepPreservesChunkSharedByALiveInode is the single most
// important GC test: two files with byte-identical content dedup down
// to one shared chunk (DESIGN.md §15). Unlinking one of them and
// sweeping past grace must NOT collect that chunk, because the other
// file still references it — a naive per-inode "this inode's chunks are
// now free" sweep would corrupt the surviving file.
func TestSweepPreservesChunkSharedByALiveInode(t *testing.T) {
	r, clk := openRelaxedWithClock(t)
	ctx := context.Background()

	data := bytes.Repeat([]byte("shared-content-"), 50) // one inline chunk
	writeCommit(t, r, metadb.RootInode, "a.txt", data)
	writeCommit(t, r, metadb.RootInode, "b.txt", data)

	_, recB, err := r.Resolve("/b.txt")
	if err != nil {
		t.Fatal(err)
	}
	if !recB.HasInline {
		t.Fatalf("expected the test payload to inline as a single chunk, got %+v", recB)
	}
	sharedChunk := recB.InlineChunk
	if found, err := r.DB.HasLocator(r.Region, sharedChunk); err != nil || !found {
		t.Fatalf("expected the shared chunk to have a locator before unlink, found=%v err=%v", found, err)
	}

	if err := r.Unlink(metadb.RootInode, "a.txt"); err != nil {
		t.Fatal(err)
	}
	clk.advance(DefaultGraceDuration + time.Hour) // well past grace

	collected, err := r.Sweep(ctx, DefaultGraceDuration)
	if err != nil {
		t.Fatal(err)
	}
	if collected != 0 {
		t.Fatalf("expected Sweep to collect nothing (the chunk is still live via b.txt), got collected=%d", collected)
	}
	if found, err := r.DB.HasLocator(r.Region, sharedChunk); err != nil || !found {
		t.Fatalf("expected the shared chunk's locator to survive, found=%v err=%v", found, err)
	}

	// The strongest form of this assertion: b.txt must still actually
	// read back correctly, proving the surviving locator is not just
	// present but valid.
	_, recB2, err := r.Resolve("/b.txt")
	if err != nil {
		t.Fatal(err)
	}
	fr, err := r.OpenFile(ctx, recB2)
	if err != nil {
		t.Fatal(err)
	}
	got, err := fr.ReadAll()
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, data) {
		t.Fatal("b.txt content corrupted by Sweep — the shared chunk was wrongly collected")
	}
}

// TestSweepCollectsPastGraceLeavesNotYetPastGraceAlone exercises grace-
// period gating directly: an entry deleted long enough ago must be
// swept; one deleted more recently, within the same Sweep call, must be
// left completely alone (chunk and graveyard entry both untouched).
func TestSweepCollectsPastGraceLeavesNotYetPastGraceAlone(t *testing.T) {
	r, clk := openRelaxedWithClock(t)
	ctx := context.Background()

	dataA := []byte("unique-content-for-a")
	dataB := []byte("totally-different-content-for-b")
	writeCommit(t, r, metadb.RootInode, "a.txt", dataA)
	writeCommit(t, r, metadb.RootInode, "b.txt", dataB)

	if err := r.Unlink(metadb.RootInode, "a.txt"); err != nil {
		t.Fatal(err)
	}
	chunkA := metadb.InodeRecord{}
	// a.txt's dentry is gone; read its chunk id via the graveyard entry
	// metadb still holds, the same way Sweep itself will.
	entries, err := r.DB.ListGraveyard()
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 1 {
		t.Fatalf("expected exactly one graveyard entry after unlinking a.txt, got %d", len(entries))
	}
	chunkA, err = r.DB.GetInode(entries[0].InodeID)
	if err != nil {
		t.Fatal(err)
	}

	clk.advance(DefaultGraceDuration + time.Hour) // a.txt now well past grace

	if err := r.Unlink(metadb.RootInode, "b.txt"); err != nil {
		t.Fatal(err)
	}
	// b.txt's graveyard entry is timestamped at the advanced "now", so
	// relative to it, b.txt is freshly deleted — not past grace yet.

	collected, err := r.Sweep(ctx, DefaultGraceDuration)
	if err != nil {
		t.Fatal(err)
	}
	if collected != 1 {
		t.Fatalf("expected Sweep to collect exactly a.txt's 1 chunk, got collected=%d", collected)
	}

	if found, err := r.DB.HasLocator(r.Region, chunkA.InlineChunk); err != nil || found {
		t.Fatalf("expected a.txt's chunk locator to be gone, found=%v err=%v", found, err)
	}

	entries, err = r.DB.ListGraveyard()
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 1 {
		t.Fatalf("expected b.txt's graveyard entry to remain untouched (not yet past grace), got %d entries", len(entries))
	}
	bRec, err := r.DB.GetInode(entries[0].InodeID)
	if err != nil {
		t.Fatal(err)
	}
	if found, err := r.DB.HasLocator(r.Region, bRec.InlineChunk); err != nil || !found {
		t.Fatalf("expected b.txt's chunk locator to still be present (not yet past grace), found=%v err=%v", found, err)
	}
}

// TestPublishUnlinkSweepEndToEnd is the full DESIGN.md §19 flow through
// the public Repo API: publish two files, unlink one, sweep past grace,
// and confirm the unlinked file's content is really gone while the
// other file is still fully readable.
func TestPublishUnlinkSweepEndToEnd(t *testing.T) {
	ctx := context.Background()
	src := t.TempDir()
	writeFile(t, src, "keep.txt", []byte("keep me around"))
	writeFile(t, src, "gone.txt", []byte("delete me please"))

	dir := t.TempDir()
	backend, err := local.New(dir + "/objects")
	if err != nil {
		t.Fatal(err)
	}
	r, err := OpenWithClass(dir, backend, DefaultRegion, ClassRelaxed)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	clk := &fakeClock{now: time.Unix(1_700_000_000, 0)}
	r.Clock = clk

	if _, _, _, err := r.PublishTree(ctx, src, nil); err != nil {
		t.Fatal(err)
	}

	_, goneRec, err := r.Resolve("/gone.txt")
	if err != nil {
		t.Fatal(err)
	}
	goneChunk := goneRec.InlineChunk

	if err := r.Unlink(metadb.RootInode, "gone.txt"); err != nil {
		t.Fatal(err)
	}
	if _, _, err := r.Resolve("/gone.txt"); err == nil {
		t.Fatal("expected gone.txt to already be unresolvable right after Unlink")
	}

	clk.advance(DefaultGraceDuration + time.Hour)
	collected, err := r.Sweep(ctx, DefaultGraceDuration)
	if err != nil {
		t.Fatal(err)
	}
	if collected != 1 {
		t.Fatalf("expected Sweep to collect gone.txt's 1 chunk, got collected=%d", collected)
	}

	// Content gone: no locator left for the chunk that was gone.txt's
	// sole content.
	if found, err := r.DB.HasLocator(r.Region, goneChunk); err != nil || found {
		t.Fatalf("expected gone.txt's chunk to be collected, found=%v err=%v", found, err)
	}

	// Other file still readable end to end.
	_, keepRec, err := r.Resolve("/keep.txt")
	if err != nil {
		t.Fatal(err)
	}
	fr, err := r.OpenFile(ctx, keepRec)
	if err != nil {
		t.Fatal(err)
	}
	got, err := fr.ReadAll()
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != "keep me around" {
		t.Fatalf("keep.txt content = %q, want %q", got, "keep me around")
	}
}
