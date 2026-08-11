package repo

import (
	"context"
	"testing"

	"github.com/aburan28/atlasfs/pkg/chunk"
	"github.com/aburan28/atlasfs/pkg/metadb"
	"github.com/aburan28/atlasfs/pkg/store/local"
)

// DESIGN.md §14.3 makes chunk_size a per-subtree policy. It has to
// survive a reopen for the same reason the consistency class does, only
// more sharply: chunk IDs are a function of the chunk boundaries, so a
// repo that came back with a different size would dedup against nothing
// and store a second complete copy of every file it touched next.
func TestChunkSizePolicyIsFixedAtCreation(t *testing.T) {
	dir := t.TempDir()
	const want = 16 << 10

	r, err := OpenWithPolicy(dir, mustLocalBackend(t, dir), DefaultRegion, ClassRelaxed, want)
	if err != nil {
		t.Fatal(err)
	}
	if r.ChunkSize != want {
		t.Fatalf("ChunkSize at creation = %d, want %d", r.ChunkSize, want)
	}
	if err := r.Close(); err != nil {
		t.Fatal(err)
	}

	// Reopening with a different request must not change it.
	// Closed between opens on purpose: metadb's bbolt file takes an
	// exclusive lock per open file description, so holding two open at
	// once deadlocks even inside one process.
	again, err := OpenWithPolicy(dir, mustLocalBackend(t, dir), DefaultRegion, ClassRelaxed, 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	gotAgain := again.ChunkSize
	if err := again.Close(); err != nil {
		t.Fatal(err)
	}
	if gotAgain != want {
		t.Fatalf("ChunkSize after reopening with a different request = %d, want the persisted %d", gotAgain, want)
	}

	// And the path that does not ask at all — every pre-existing caller —
	// must also get the persisted value, not the default.
	plain, err := OpenWithClass(dir, mustLocalBackend(t, dir), DefaultRegion, ClassRelaxed)
	if err != nil {
		t.Fatal(err)
	}
	defer plain.Close()
	if plain.ChunkSize != want {
		t.Fatalf("OpenWithClass on a repo created with chunk size %d gave %d", want, plain.ChunkSize)
	}
}

// A fresh repo that asks for nothing keeps the documented default, so
// this policy is opt-in and changes no existing behaviour.
func TestChunkSizeDefaultsToTheDesignDefault(t *testing.T) {
	dir := t.TempDir()
	r, err := OpenWithClass(dir, mustLocalBackend(t, dir), DefaultRegion, ClassRelaxed)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	if r.ChunkSize != chunk.DefaultSize {
		t.Fatalf("default ChunkSize = %d, want %d", r.ChunkSize, chunk.DefaultSize)
	}
}

// The point of the policy, measured rather than asserted: rewriting one
// file repeatedly stores far less at a chunk size below the file size,
// because only the chunks a write actually touched are new and the rest
// dedup. This is the mechanism behind §27's fsx soak reaching 21 GB for
// a 256 KB file.
func TestSmallerChunksCutRewriteAmplification(t *testing.T) {
	const fileSize = 64 << 10
	const rewrites = 20

	stored := func(chunkSize int) int64 {
		dir := t.TempDir()
		r, err := OpenWithPolicy(dir, mustLocalBackend(t, dir), DefaultRegion, ClassRelaxed, chunkSize)
		if err != nil {
			t.Fatal(err)
		}
		defer r.Close()
		ctx := context.Background()

		content := make([]byte, fileSize)
		for i := range content {
			content[i] = byte(i)
		}
		for w := range rewrites {
			// Touch one small region per rewrite, leaving the rest of the
			// file byte-identical — the shape of an in-place update.
			content[w] = byte(w + 1)
			h, err := r.CreateFile(metadb.RootInode, "hot.bin")
			if err != nil {
				t.Fatal(err)
			}
			if _, err := h.Write(content); err != nil {
				t.Fatal(err)
			}
			if _, err := h.Commit(ctx); err != nil {
				t.Fatal(err)
			}
		}
		return backendBytes(t, r)
	}

	whole := stored(fileSize)     // one chunk per rewrite: no dedup possible
	split := stored(fileSize / 8) // eight chunks, only the touched one is new

	// Bytes, not chunk count: eight small chunks per commit is *more*
	// chunks and far fewer bytes, and bytes are what filled the volume.
	if whole < int64(fileSize)*rewrites/2 {
		t.Fatalf("with a whole-file chunk, %d rewrites stored only %d bytes; the test is not reproducing rewrite amplification", rewrites, whole)
	}
	if split*2 > whole {
		t.Fatalf("smaller chunks stored %d bytes vs %d for whole-file chunks: dedup is not cutting amplification", split, whole)
	}
	t.Logf("%d rewrites of a %d KiB file, one byte changed each time: %d KiB stored at whole-file chunks, %d KiB at %d KiB chunks (%.1fx less)",
		rewrites, fileSize>>10, whole>>10, split>>10, (fileSize/8)>>10, float64(whole)/float64(split))
}

func mustLocalBackend(t *testing.T, dir string) *local.Backend {
	t.Helper()
	b, err := local.New(dir + "/objects")
	if err != nil {
		t.Fatal(err)
	}
	return b
}
