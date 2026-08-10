// Package store defines the pluggable backend interface from DESIGN.md
// §24.1. The interface is deliberately small: AtlasFS never asks a
// backend for rename, directory semantics, partial overwrite, append, or
// listing on the read path. Container objects are written once, sealed,
// read by range, and eventually deleted — that is the entire contract.
package store

import (
	"context"
	"io"
	"time"
)

// ObjectInfo describes a stored container object.
type ObjectInfo struct {
	Key     string
	Size    int64
	ETag    string // backend-specific; used for conditional-put probing
	ModTime time.Time
}

// PutOpts controls a Put call.
type PutOpts struct {
	// IfAbsent requests a conditional put (create-only). Backends that
	// cannot honor it (Caps().ConditionalPut == false) must ignore it —
	// DESIGN.md §24.3: a seal race becomes a duplicate object reclaimed
	// by GC compaction, never a correctness problem.
	IfAbsent bool
}

// DeleteResult reports the outcome of one key in a batch delete.
type DeleteResult struct {
	Key string
	Err error
}

// ListPage is one page of a prefix listing.
type ListPage struct {
	Keys       []ObjectInfo
	NextCursor string // empty when there are no more pages
}

// Caps describes what a backend can do, probed at startup per DESIGN.md
// §24.2 rather than assumed from the endpoint. The cost fields feed the
// cost model (§12) directly.
type Caps struct {
	ConditionalPut  bool
	MultipartUpload bool
	BatchDelete     int // max keys per batch request; 0 = one at a time
	MaxObjectSize   int64
	StorageTiers    []string

	EgressUSDPerGB float64
	GetUSDPer1k    float64
	PutUSDPer1k    float64
}

// Backend is the pluggable object-storage interface (DESIGN.md §24.1).
// Implementations: local POSIX (this package's local subpackage) now;
// S3/GCS/Azure/S3-compatible are additive per §24.2 and do not require
// changing this interface or any caller of it.
type Backend interface {
	Get(ctx context.Context, key string, off, length int64) (io.ReadCloser, error)
	Put(ctx context.Context, key string, r io.Reader, size int64, o PutOpts) (ObjectInfo, error)
	Delete(ctx context.Context, keys []string) ([]DeleteResult, error)
	List(ctx context.Context, prefix, cursor string, limit int) (ListPage, error)
	Stat(ctx context.Context, key string) (ObjectInfo, error)
	Caps() Caps
}

// ErrNotFound is returned by Get/Stat when the key does not exist.
var ErrNotFound = errNotFound{}

type errNotFound struct{}

func (errNotFound) Error() string { return "store: object not found" }
