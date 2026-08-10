package gcs

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net/http"
	"os"
	"testing"

	"google.golang.org/api/option"

	"github.com/aburan28/atlasfs/pkg/store"
)

func newTestBackend(t *testing.T, srvURL, bucket string) *Backend {
	t.Helper()
	ctx := context.Background()
	b, err := New(ctx, Config{Bucket: bucket},
		option.WithEndpoint(srvURL),
		option.WithoutAuthentication(),
		option.WithHTTPClient(&http.Client{}),
	)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	return b
}

func TestGCSPutGetStat(t *testing.T) {
	srv := newFakeGCS("bkt")
	defer srv.Close()
	b := newTestBackend(t, srv.URL, "bkt")
	ctx := context.Background()

	data := []byte("hello over real GCS wire protocol")
	if _, err := b.Put(ctx, "atlas/c/local/aa/c1", bytes.NewReader(data), int64(len(data)), store.PutOpts{}); err != nil {
		t.Fatal(err)
	}

	info, err := b.Stat(ctx, "atlas/c/local/aa/c1")
	if err != nil {
		t.Fatal(err)
	}
	if info.Size != int64(len(data)) {
		t.Fatalf("size mismatch: got %d want %d", info.Size, len(data))
	}

	rc, err := b.Get(ctx, "atlas/c/local/aa/c1", 0, -1)
	if err != nil {
		t.Fatal(err)
	}
	defer rc.Close()
	got, err := io.ReadAll(rc)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, data) {
		t.Fatalf("got %q, want %q", got, data)
	}
}

func TestGCSRangedGet(t *testing.T) {
	srv := newFakeGCS("bkt")
	defer srv.Close()
	b := newTestBackend(t, srv.URL, "bkt")
	ctx := context.Background()

	data := []byte("0123456789abcdefghij")
	if _, err := b.Put(ctx, "obj", bytes.NewReader(data), int64(len(data)), store.PutOpts{}); err != nil {
		t.Fatal(err)
	}

	rc, err := b.Get(ctx, "obj", 5, 4)
	if err != nil {
		t.Fatal(err)
	}
	defer rc.Close()
	got, err := io.ReadAll(rc)
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != "5678" {
		t.Fatalf("ranged read: got %q, want %q", got, "5678")
	}
}

func TestGCSGetNotFound(t *testing.T) {
	srv := newFakeGCS("bkt")
	defer srv.Close()
	b := newTestBackend(t, srv.URL, "bkt")
	ctx := context.Background()

	_, err := b.Get(ctx, "does/not/exist", 0, -1)
	if !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("expected ErrNotFound, got %v", err)
	}
}

func TestGCSStatNotFound(t *testing.T) {
	srv := newFakeGCS("bkt")
	defer srv.Close()
	b := newTestBackend(t, srv.URL, "bkt")
	ctx := context.Background()

	_, err := b.Stat(ctx, "does/not/exist")
	if !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("expected ErrNotFound from Stat, got %v", err)
	}
}

func TestGCSConditionalPutProbeDetectsSupport(t *testing.T) {
	srv := newFakeGCS("bkt")
	defer srv.Close()
	b := newTestBackend(t, srv.URL, "bkt")

	if !b.Caps().ConditionalPut {
		t.Fatal("probe should have detected the fake server's ifGenerationMatch support")
	}
}

func TestGCSConditionalPutRejectsDuplicate(t *testing.T) {
	srv := newFakeGCS("bkt")
	defer srv.Close()
	b := newTestBackend(t, srv.URL, "bkt")
	ctx := context.Background()

	if _, err := b.Put(ctx, "atlas/m/aa/m1", bytes.NewReader([]byte("v1")), 2, store.PutOpts{IfAbsent: true}); err != nil {
		t.Fatal(err)
	}
	_, err := b.Put(ctx, "atlas/m/aa/m1", bytes.NewReader([]byte("v2")), 2, store.PutOpts{IfAbsent: true})
	if !errors.Is(err, os.ErrExist) {
		t.Fatalf("expected ErrExist on conditional put race, got %v", err)
	}

	rc, err := b.Get(ctx, "atlas/m/aa/m1", 0, -1)
	if err != nil {
		t.Fatal(err)
	}
	defer rc.Close()
	got, _ := io.ReadAll(rc)
	if string(got) != "v1" {
		t.Fatalf("conditional put race overwrote content: got %q", got)
	}
}

func TestGCSListPrefixAndCursor(t *testing.T) {
	srv := newFakeGCS("bkt")
	defer srv.Close()
	b := newTestBackend(t, srv.URL, "bkt")
	ctx := context.Background()

	keys := []string{"atlas/c/local/aa/c1", "atlas/c/local/aa/c2", "atlas/c/local/aa/c3", "atlas/c/local/bb/c4"}
	for _, k := range keys {
		if _, err := b.Put(ctx, k, bytes.NewReader([]byte("x")), 1, store.PutOpts{}); err != nil {
			t.Fatal(err)
		}
	}

	page, err := b.List(ctx, "atlas/c/local/aa/", "", 100)
	if err != nil {
		t.Fatal(err)
	}
	if len(page.Keys) != 3 {
		t.Fatalf("expected 3 keys under aa/, got %d: %+v", len(page.Keys), page.Keys)
	}

	// Paginate with a small page size and follow the cursor to prove
	// GCS's pageToken round-trips correctly end to end.
	first, err := b.List(ctx, "atlas/c/local/aa/", "", 2)
	if err != nil {
		t.Fatal(err)
	}
	if len(first.Keys) != 2 || first.NextCursor == "" {
		t.Fatalf("expected a truncated first page with a cursor, got %+v", first)
	}
	second, err := b.List(ctx, "atlas/c/local/aa/", first.NextCursor, 100)
	if err != nil {
		t.Fatal(err)
	}
	if len(second.Keys) != 1 {
		t.Fatalf("expected 1 remaining key after cursor, got %d", len(second.Keys))
	}
}

func TestGCSDelete(t *testing.T) {
	srv := newFakeGCS("bkt")
	defer srv.Close()
	b := newTestBackend(t, srv.URL, "bkt")
	ctx := context.Background()

	if _, err := b.Put(ctx, "obj", bytes.NewReader([]byte("x")), 1, store.PutOpts{}); err != nil {
		t.Fatal(err)
	}
	results, err := b.Delete(ctx, []string{"obj"})
	if err != nil {
		t.Fatal(err)
	}
	if len(results) != 1 || results[0].Err != nil {
		t.Fatalf("unexpected delete result: %+v", results)
	}
	if _, err := b.Stat(ctx, "obj"); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("expected object gone after delete, got %v", err)
	}
}

func TestGCSKeyPrefixIsolation(t *testing.T) {
	// A repo-level "prefix" (e.g. sharing one bucket across subtrees)
	// must not leak into keys returned to the caller.
	srv := newFakeGCS("bkt")
	defer srv.Close()
	ctx := context.Background()
	b, err := New(ctx, Config{Bucket: "bkt", Prefix: "tenant-a/"},
		option.WithEndpoint(srv.URL),
		option.WithoutAuthentication(),
		option.WithHTTPClient(&http.Client{}),
	)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := b.Put(ctx, "obj", bytes.NewReader([]byte("x")), 1, store.PutOpts{}); err != nil {
		t.Fatal(err)
	}
	page, err := b.List(ctx, "", "", 100)
	if err != nil {
		t.Fatal(err)
	}
	if len(page.Keys) != 1 || page.Keys[0].Key != "obj" {
		t.Fatalf("expected key %q with prefix stripped, got %+v", "obj", page.Keys)
	}
}
