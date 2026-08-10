package pack

import (
	"bytes"
	"context"
	"testing"

	"github.com/aburan28/atlasfs/pkg/chunk"
	"github.com/aburan28/atlasfs/pkg/store"
	"github.com/aburan28/atlasfs/pkg/store/local"
)

func newTestBackend(t *testing.T) store.Backend {
	t.Helper()
	b, err := local.New(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	return b
}

func TestPackerSealAndFetch(t *testing.T) {
	backend, err := local.New(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	p := NewPacker(backend, "local", 1<<20) // small seal size for the test

	data := bytes.Repeat([]byte("small-file-content-"), 100)
	chunks := chunk.SplitBytes(data, 4096)
	for _, c := range chunks {
		p.Add(c)
	}
	if p.Empty() {
		t.Fatal("packer should not be empty after Add")
	}
	locs, err := p.Seal(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(locs) != len(chunks) {
		t.Fatalf("got %d locators, want %d", len(locs), len(chunks))
	}

	// All packed chunks must land in the same container object — that's
	// the entire point of packing (DESIGN.md §14).
	container := ""
	for _, loc := range locs {
		if container == "" {
			container = loc.Container
		} else if loc.Container != container {
			t.Fatalf("chunks scattered across containers: %s vs %s", container, loc.Container)
		}
	}

	for _, c := range chunks {
		loc := locs[c.ID]
		got, err := Fetch(ctx, backend, c.ID, loc)
		if err != nil {
			t.Fatalf("fetch chunk %s: %v", c.ID, err)
		}
		if !bytes.Equal(got, c.Data) {
			t.Fatalf("fetched bytes mismatch for chunk %s", c.ID)
		}
	}
}

func TestSealEmptyIsNoop(t *testing.T) {
	backend, err := local.New(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	p := NewPacker(backend, "local", DefaultSealSize)
	locs, err := p.Seal(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if locs != nil {
		t.Fatalf("expected nil locators from sealing an empty packer, got %d", len(locs))
	}
}

func TestFetchDetectsCorruption(t *testing.T) {
	backend, err := local.New(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	p := NewPacker(backend, "local", 1<<20)
	c := chunk.Sum([]byte("original content"))
	p.Add(chunk.Chunk{ID: c, Data: []byte("original content")})
	locs, err := p.Seal(ctx)
	if err != nil {
		t.Fatal(err)
	}
	loc := locs[c]

	// Corrupt the underlying container object directly, bypassing the
	// packer, to simulate a backend serving wrong bytes (DESIGN.md
	// §24.4: reads must be self-verifying).
	if _, err := backend.Put(ctx, loc.Container, bytes.NewReader([]byte("corrupted!!!!!!!")), 16, store.PutOpts{}); err != nil {
		t.Fatal(err)
	}
	if _, err := Fetch(ctx, backend, c, loc); err == nil {
		t.Fatal("expected Fetch to detect corrupted content via hash verification")
	}
}

// TestFetchIntoVerifiesHash: the caller-supplied-destination path must
// uphold DESIGN.md §24.4 exactly as Fetch does. If FetchInto skipped
// verification, the zero-copy read path added for GPUDirect would be a
// hole straight through the filesystem's self-verification guarantee —
// and it would be an invisible one, since the bytes would look fine.
func TestFetchIntoVerifiesHash(t *testing.T) {
	ctx := context.Background()
	backend := newTestBackend(t)

	data := []byte("content that will be corrupted underneath us")
	c := chunk.Chunk{ID: chunk.Sum(data), Data: data}
	loc, err := PutSingleObject(ctx, backend, "local", c)
	if err != nil {
		t.Fatal(err)
	}

	// Reading it honestly succeeds.
	p := make([]byte, loc.Length)
	if err := FetchInto(ctx, backend, c.ID, loc, p); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(p, data) {
		t.Fatal("FetchInto returned the wrong bytes")
	}

	// Asking for the same range under a different chunk ID is exactly what
	// a corrupted or substituted object looks like, and must be rejected.
	other := chunk.Sum([]byte("something else entirely"))
	if err := FetchInto(ctx, backend, other, loc, p); err == nil {
		t.Fatal("FetchInto accepted bytes that do not hash to the requested chunk ID")
	}
}

// TestFetchIntoRejectsWrongSizedBuffer: the contract is that p is exactly
// the chunk. A caller passing a larger buffer would otherwise get a
// partially-filled slice whose tail is stale or zero, and the hash check
// would fail confusingly rather than the size check failing clearly.
func TestFetchIntoRejectsWrongSizedBuffer(t *testing.T) {
	ctx := context.Background()
	backend := newTestBackend(t)
	data := []byte("exact size matters")
	c := chunk.Chunk{ID: chunk.Sum(data), Data: data}
	loc, err := PutSingleObject(ctx, backend, "local", c)
	if err != nil {
		t.Fatal(err)
	}
	for _, size := range []int{0, len(data) - 1, len(data) + 1} {
		if err := FetchInto(ctx, backend, c.ID, loc, make([]byte, size)); err == nil {
			t.Fatalf("FetchInto accepted a %d-byte buffer for a %d-byte chunk", size, len(data))
		}
	}
}

// TestAlignedPackingProducesAlignedLocators is the GPUDirect
// precondition (see GDSAlignment): with alignment on, every chunk's
// offset within its container must be 4 KiB aligned, or every GDS read
// falls into the bounce-buffer path the feature exists to avoid.
func TestAlignedPackingProducesAlignedLocators(t *testing.T) {
	ctx := context.Background()
	backend := newTestBackend(t)
	p := NewPacker(backend, "local", 1<<20)
	p.SetAlignment(GDSAlignment)

	// Deliberately awkward sizes: none is a multiple of 4 KiB, so an
	// unaligned packer would put almost every chunk off a boundary.
	var ids []chunk.ID
	for _, size := range []int{1, 100, 4095, 4097, 9000, 13} {
		data := bytes.Repeat([]byte{byte(size)}, size)
		c := chunk.Chunk{ID: chunk.Sum(data), Data: data}
		ids = append(ids, c.ID)
		p.Add(c)
	}
	locs, err := p.Seal(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(locs) != len(ids) {
		t.Fatalf("got %d locators, want %d", len(locs), len(ids))
	}
	for _, id := range ids {
		loc, ok := locs[id]
		if !ok {
			t.Fatalf("no locator for chunk %s", id)
		}
		if loc.Offset%GDSAlignment != 0 {
			t.Fatalf("chunk %s at offset %d is not %d-aligned", id, loc.Offset, GDSAlignment)
		}
	}

	// Alignment must not change what the bytes are: padding is storage
	// only, never visible through a locator.
	for i, id := range ids {
		data, err := Fetch(ctx, backend, id, locs[id])
		if err != nil {
			t.Fatalf("chunk %d: %v", i, err)
		}
		if chunk.Sum(data) != id {
			t.Fatalf("chunk %d read back wrong bytes through an aligned locator", i)
		}
	}
}

// TestUnalignedPackingIsStillTheDefault pins the default, because
// turning alignment on globally would silently inflate every small-file
// dataset (DESIGN.md §14 packs small files precisely to avoid waste).
func TestUnalignedPackingIsStillTheDefault(t *testing.T) {
	p := NewPacker(newTestBackend(t), "local", 1<<20)
	if p.Alignment() != 0 {
		t.Fatalf("default alignment = %d, want 0 (tight packing)", p.Alignment())
	}

	// And with the default, small chunks really do sit back to back.
	ctx := context.Background()
	a := chunk.Chunk{ID: chunk.Sum([]byte("aaa")), Data: []byte("aaa")}
	b := chunk.Chunk{ID: chunk.Sum([]byte("bbbb")), Data: []byte("bbbb")}
	p.Add(a)
	p.Add(b)
	locs, err := p.Seal(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if got := locs[b.ID].Offset; got != 3 {
		t.Fatalf("second chunk starts at %d, want 3 (tightly packed)", got)
	}
}

// TestAlignmentPaddingCost measures what alignment actually costs on the
// workload it hurts most — many small files — so the tradeoff is a
// number in the repo rather than a hand-wave.
func TestAlignmentPaddingCost(t *testing.T) {
	ctx := context.Background()
	const nFiles = 200
	const fileSize = 1024 // 1 KiB: the small-file case §14 targets

	measure := func(align int) int64 {
		backend := newTestBackend(t)
		p := NewPacker(backend, "local", 1<<30) // never auto-seal
		p.SetAlignment(align)
		for i := 0; i < nFiles; i++ {
			data := bytes.Repeat([]byte{byte(i)}, fileSize)
			p.Add(chunk.Chunk{ID: chunk.Sum(data), Data: data})
		}
		locs, err := p.Seal(ctx)
		if err != nil {
			t.Fatal(err)
		}
		var maxEnd int64
		for _, l := range locs {
			if end := l.Offset + l.Length; end > maxEnd {
				maxEnd = end
			}
		}
		return maxEnd
	}

	tight := measure(0)
	aligned := measure(GDSAlignment)
	if aligned <= tight {
		t.Fatalf("aligned container (%d) should be larger than tight (%d)", aligned, tight)
	}
	ratio := float64(aligned) / float64(tight)
	t.Logf("%d x %d B files: tight=%d aligned=%d (%.2fx)", nFiles, fileSize, tight, aligned, ratio)
	// 1 KiB chunks padded to 4 KiB is a 4x blowup; assert the shape so a
	// future change that silently made alignment cheaper or costlier
	// shows up here.
	if ratio < 3.5 || ratio > 4.5 {
		t.Fatalf("expected roughly 4x inflation for 1 KiB chunks at 4 KiB alignment, got %.2fx", ratio)
	}
}
