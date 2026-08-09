package pack

import (
	"bytes"
	"context"
	"testing"

	"github.com/aburan28/atlasfs/pkg/chunk"
	"github.com/aburan28/atlasfs/pkg/store"
	"github.com/aburan28/atlasfs/pkg/store/local"
)

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
