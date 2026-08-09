package local

import (
	"bytes"
	"context"
	"errors"
	"io"
	"os"
	"testing"

	"github.com/aburan28/atlasfs/pkg/store"
)

func TestPutGetStat(t *testing.T) {
	b, err := New(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	data := []byte("hello atlasfs container")

	if _, err := b.Put(ctx, "atlas/c/local/aa/container1", bytes.NewReader(data), int64(len(data)), store.PutOpts{}); err != nil {
		t.Fatal(err)
	}

	info, err := b.Stat(ctx, "atlas/c/local/aa/container1")
	if err != nil {
		t.Fatal(err)
	}
	if info.Size != int64(len(data)) {
		t.Fatalf("size mismatch: got %d want %d", info.Size, len(data))
	}

	rc, err := b.Get(ctx, "atlas/c/local/aa/container1", 6, 5)
	if err != nil {
		t.Fatal(err)
	}
	defer rc.Close()
	got, err := io.ReadAll(rc)
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != "atlas" {
		t.Fatalf("ranged read mismatch: got %q want %q", got, "atlas")
	}
}

func TestGetNotFound(t *testing.T) {
	b, err := New(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	_, err = b.Get(context.Background(), "does/not/exist", 0, -1)
	if !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("expected ErrNotFound, got %v", err)
	}
}

func TestConditionalPutRejectsDuplicate(t *testing.T) {
	b, err := New(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	if _, err := b.Put(ctx, "atlas/m/aa/manifest1", bytes.NewReader([]byte("v1")), 2, store.PutOpts{IfAbsent: true}); err != nil {
		t.Fatal(err)
	}
	_, err = b.Put(ctx, "atlas/m/aa/manifest1", bytes.NewReader([]byte("v2")), 2, store.PutOpts{IfAbsent: true})
	if !errors.Is(err, os.ErrExist) {
		t.Fatalf("expected ErrExist on conditional put race, got %v", err)
	}

	// Content must be unchanged (the race was rejected, not silently overwritten).
	rc, err := b.Get(ctx, "atlas/m/aa/manifest1", 0, -1)
	if err != nil {
		t.Fatal(err)
	}
	defer rc.Close()
	got, _ := io.ReadAll(rc)
	if string(got) != "v1" {
		t.Fatalf("conditional put race overwrote content: got %q", got)
	}
}

func TestListPrefix(t *testing.T) {
	b, err := New(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	keys := []string{
		"atlas/c/local/aa/c1",
		"atlas/c/local/aa/c2",
		"atlas/c/local/bb/c3",
	}
	for _, k := range keys {
		if _, err := b.Put(ctx, k, bytes.NewReader([]byte("x")), 1, store.PutOpts{}); err != nil {
			t.Fatal(err)
		}
	}
	page, err := b.List(ctx, "atlas/c/local/aa/", "", 100)
	if err != nil {
		t.Fatal(err)
	}
	if len(page.Keys) != 2 {
		t.Fatalf("expected 2 keys under aa/, got %d: %+v", len(page.Keys), page.Keys)
	}
}

func TestCapsReportsConditionalPut(t *testing.T) {
	b, err := New(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	if !b.Caps().ConditionalPut {
		t.Fatal("local backend should report ConditionalPut support")
	}
}
