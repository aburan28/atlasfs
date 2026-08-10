package azure

// Proves the Backend isn't just correct in isolation (azure_test.go) but
// actually carries a full publish + read cycle through pkg/repo over the
// real Azure SDK wire protocol — DESIGN.md §24's pluggability claim,
// verified rather than asserted.

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob"

	"github.com/aburan28/atlasfs/pkg/repo"
)

func TestPublishAndReadThroughAzureBackend(t *testing.T) {
	srv := newFakeAzure("atlas-container")
	defer srv.Close()

	ctx := context.Background()
	backend, err := New(ctx, srv.URL, Config{Container: "atlas-container"}, func(o *azblob.ClientOptions) {
		o.Retry.MaxRetries = 0
	})
	if err != nil {
		t.Fatal(err)
	}

	r, err := repo.OpenRemote(t.TempDir(), backend, "test-region")
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	r.ChunkSize = 4096

	src := t.TempDir()
	data := bytes.Repeat([]byte("azure-backed-content-"), 2000) // multi-chunk
	if err := os.MkdirAll(filepath.Join(src, "dir"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(src, "dir", "f.bin"), data, 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(src, "small.txt"), []byte("small"), 0o644); err != nil {
		t.Fatal(err)
	}

	files, _, _, err := r.PublishTree(ctx, src, []string{"datasets", "demo"})
	if err != nil {
		t.Fatal(err)
	}
	if files != 2 {
		t.Fatalf("expected 2 published files, got %d", files)
	}

	_, rec, err := r.Resolve("/datasets/demo/dir/f.bin")
	if err != nil {
		t.Fatal(err)
	}
	fr, err := r.OpenFile(ctx, rec)
	if err != nil {
		t.Fatal(err)
	}
	got, err := fr.ReadAll()
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, data) {
		t.Fatal("content mismatch reading a multi-chunk file back over the Azure backend")
	}

	_, smallRec, err := r.Resolve("/datasets/demo/small.txt")
	if err != nil {
		t.Fatal(err)
	}
	if !smallRec.HasInline {
		t.Fatal("small file should still be inlined even with a remote backend")
	}
	fr2, err := r.OpenFile(ctx, smallRec)
	if err != nil {
		t.Fatal(err)
	}
	gotSmall, err := fr2.ReadAll()
	if err != nil {
		t.Fatal(err)
	}
	if string(gotSmall) != "small" {
		t.Fatalf("got %q, want %q", gotSmall, "small")
	}
}
