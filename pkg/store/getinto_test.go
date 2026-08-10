package store_test

import (
	"bytes"
	"context"
	"errors"
	"io"
	"testing"

	"github.com/aburan28/atlasfs/pkg/store"
	"github.com/aburan28/atlasfs/pkg/store/local"
)

// streamOnlyBackend wraps a Backend and hides any GetterInto it has, so
// store.GetInto is forced down its streaming fallback. That fallback is
// what every non-local backend uses today, so it has to be exercised
// directly rather than assumed correct.
type streamOnlyBackend struct{ store.Backend }

func newLocal(t *testing.T) *local.Backend {
	t.Helper()
	b, err := local.New(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	return b
}

func put(t *testing.T, b store.Backend, key string, data []byte) {
	t.Helper()
	if _, err := b.Put(context.Background(), key, bytes.NewReader(data), int64(len(data)), store.PutOpts{}); err != nil {
		t.Fatal(err)
	}
}

func TestLocalBackendImplementsGetterInto(t *testing.T) {
	if !store.SupportsGetInto(newLocal(t)) {
		t.Fatal("local backend should implement store.GetterInto natively")
	}
	if store.SupportsGetInto(streamOnlyBackend{newLocal(t)}) {
		t.Fatal("streamOnlyBackend should not expose GetterInto")
	}
}

// TestGetIntoAgreesAcrossBothPaths is the property that matters: the
// native path and the streaming fallback must return byte-identical
// results for every range, or a backend's presence or absence of
// GetterInto would silently change what a read returns.
func TestGetIntoAgreesAcrossBothPaths(t *testing.T) {
	ctx := context.Background()
	data := make([]byte, 8192)
	for i := range data {
		data[i] = byte(i * 7)
	}

	native := newLocal(t)
	put(t, native, "obj", data)
	fallbackBackend := newLocal(t)
	put(t, fallbackBackend, "obj", data)
	fallback := streamOnlyBackend{fallbackBackend}

	for _, tc := range []struct{ off, length int }{
		{0, 1}, {0, 4096}, {0, 8192}, {1, 100}, {4095, 2}, {7000, 1192}, {8191, 1},
	} {
		gotNative := make([]byte, tc.length)
		if err := store.GetInto(ctx, native, "obj", int64(tc.off), gotNative); err != nil {
			t.Fatalf("native off=%d len=%d: %v", tc.off, tc.length, err)
		}
		gotFallback := make([]byte, tc.length)
		if err := store.GetInto(ctx, fallback, "obj", int64(tc.off), gotFallback); err != nil {
			t.Fatalf("fallback off=%d len=%d: %v", tc.off, tc.length, err)
		}
		want := data[tc.off : tc.off+tc.length]
		if !bytes.Equal(gotNative, want) {
			t.Fatalf("native path wrong bytes at off=%d len=%d", tc.off, tc.length)
		}
		if !bytes.Equal(gotFallback, want) {
			t.Fatalf("fallback path wrong bytes at off=%d len=%d", tc.off, tc.length)
		}
	}
}

// TestGetIntoShortReadIsAnError: GetterInto's contract is all-or-nothing.
// A read that runs off the end of the object must fail rather than
// silently leaving the tail of p zeroed, which would hand a caller
// plausible-looking wrong data.
func TestGetIntoShortReadIsAnError(t *testing.T) {
	ctx := context.Background()
	b := newLocal(t)
	put(t, b, "obj", []byte("0123456789"))

	p := make([]byte, 20) // twice the object's length
	if err := store.GetInto(ctx, b, "obj", 0, p); err == nil {
		t.Fatal("expected an error reading past the end of the object")
	}
	if err := store.GetInto(ctx, streamOnlyBackend{b}, "obj", 0, p); err == nil {
		t.Fatal("expected the fallback to error reading past the end too")
	}
}

func TestGetIntoMissingObject(t *testing.T) {
	ctx := context.Background()
	b := newLocal(t)
	p := make([]byte, 4)
	if err := store.GetInto(ctx, b, "nope", 0, p); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("got %v, want store.ErrNotFound", err)
	}
}

func TestGetIntoEmptyBufferIsANoOp(t *testing.T) {
	// Nothing requested means nothing to do — and in particular it must
	// not turn into a Get for a key that may not exist.
	if err := store.GetInto(context.Background(), newLocal(t), "absent", 0, nil); err != nil {
		t.Fatalf("zero-length GetInto should be a no-op, got %v", err)
	}
}

var _ io.Reader = (*bytes.Reader)(nil)
