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
