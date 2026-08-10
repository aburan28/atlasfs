// Package gcs implements the store.Backend interface (DESIGN.md §24) over
// Google Cloud Storage, using the real cloud.google.com/go/storage client.
// It is the "GCS" row of §24.2's capability matrix: conditional put via
// ifGenerationMatch=0, no batch delete (GCS's batch endpoint is a bundled
// set of independent sub-requests, not a cost or latency win worth the
// complexity here — see the Caps doc comment), no multipart (containers
// are sealed in memory and Put in one call, same reasoning as pkg/store/s3;
// GCS's "resumable" upload session is the closer analogue and is skipped
// for the same reason MPU is skipped in S3).
//
// Per DESIGN.md §24.2: capabilities are probed at startup, not assumed
// from the endpoint. Probe does exactly that: two real conditional
// creates against a throwaway key, decided from what the server actually
// does — GCS supports ifGenerationMatch=0, so this reports true against a
// correct server, but the probe is what makes that a verified fact rather
// than an assumption baked into the code.
package gcs

import (
	"context"
	"errors"
	"fmt"
	"io"
	"math"
	"math/rand"
	"os"
	"strconv"
	"strings"
	"time"

	"cloud.google.com/go/storage"
	"google.golang.org/api/googleapi"
	"google.golang.org/api/iterator"
	"google.golang.org/api/option"

	"github.com/aburan28/atlasfs/pkg/store"
)

type Backend struct {
	client *storage.Client
	bucket string
	prefix string
	caps   store.Caps
}

// Config is the subset of connection details a caller supplies; GCP
// credential resolution otherwise follows the client library's normal
// chain (ADC, GOOGLE_APPLICATION_CREDENTIALS, metadata server, etc.) via
// the option.ClientOption values passed to New.
type Config struct {
	Bucket string
	Prefix string // optional key prefix, e.g. "atlas/" for a shared bucket

	// EgressUSDPerGB/GetUSDPer1k/PutUSDPer1k feed the cost model
	// (DESIGN.md §12); left zero-value here means "unknown", not "free" —
	// callers wire real numbers per DESIGN.md §12.1's pricing table, same
	// reasoning as pkg/store/s3.Config: a backend doesn't know its own
	// price without being told.
	EgressUSDPerGB float64
	GetUSDPer1k    float64
	PutUSDPer1k    float64
}

// New constructs a Backend and probes its conditional-create capability
// against the real bucket (DESIGN.md §24.2). opts are forwarded to
// storage.NewClient — production callers pass none and get the SDK's
// normal ADC chain; tests pass option.WithEndpoint,
// option.WithoutAuthentication, and option.WithHTTPClient to point the
// client at a fake server instead. storage.WithJSONReads is always added
// so reads go through the JSON API's ?alt=media (DESIGN.md §24's wire
// shape) rather than the SDK's default XML-style bucket-in-path GET.
func New(ctx context.Context, cfg Config, opts ...option.ClientOption) (*Backend, error) {
	allOpts := append([]option.ClientOption{storage.WithJSONReads()}, opts...)
	client, err := storage.NewClient(ctx, allOpts...)
	if err != nil {
		return nil, fmt.Errorf("gcs backend: %w", err)
	}
	b := &Backend{client: client, bucket: cfg.Bucket, prefix: cfg.Prefix}
	condPut, err := probeConditionalPut(ctx, client, cfg.Bucket, cfg.Prefix)
	if err != nil {
		return nil, fmt.Errorf("gcs backend: capability probe: %w", err)
	}
	b.caps = store.Caps{
		ConditionalPut:  condPut,
		MultipartUpload: false,
		BatchDelete:     0,
		MaxObjectSize:   5 << 40, // GCS's stated per-object ceiling; this build never composes past it
		StorageTiers:    []string{"STANDARD", "NEARLINE", "COLDLINE", "ARCHIVE"},
		EgressUSDPerGB:  cfg.EgressUSDPerGB,
		GetUSDPer1k:     cfg.GetUSDPer1k,
		PutUSDPer1k:     cfg.PutUSDPer1k,
	}
	return b, nil
}

// probeConditionalPut writes a throwaway key twice with
// ifGenerationMatch=0 and checks whether the second write is rejected.
// A real probe, not a version-string guess, for the same reason
// pkg/store/s3's probe is: guessing wrong about conditional create turns
// a correctness property into a silent race (DESIGN.md §24.2).
func probeConditionalPut(ctx context.Context, client *storage.Client, bucket, prefix string) (bool, error) {
	key := prefix + fmt.Sprintf("atlas/.probe-%d-%d", time.Now().UnixNano(), rand.Int63())
	obj := client.Bucket(bucket).Object(key)
	defer func() { _ = obj.Delete(ctx) }()

	put := func() error {
		w := obj.If(storage.Conditions{DoesNotExist: true}).NewWriter(ctx)
		w.ChunkSize = 0
		w.DisableAutoChecksum = true
		if _, err := w.Write([]byte("probe")); err != nil {
			return err
		}
		return w.Close()
	}
	if err := put(); err != nil {
		return false, fmt.Errorf("initial probe put: %w", err)
	}
	err := put()
	if err == nil {
		// Second conditional create succeeded when it should have been
		// rejected: this endpoint does not enforce ifGenerationMatch.
		return false, nil
	}
	if isPreconditionFailed(err) {
		return true, nil
	}
	// Some other error (network, permissions) — cannot conclude support
	// either way; report false rather than guess true, since a false
	// negative only costs an optimization (DESIGN.md §24.3) while a false
	// positive on this specific property is a correctness bug.
	return false, nil
}

func isPreconditionFailed(err error) bool {
	var gerr *googleapi.Error
	if errors.As(err, &gerr) {
		return gerr.Code == 412
	}
	return strings.Contains(err.Error(), "412") || strings.Contains(err.Error(), "Precondition Failed")
}

func isNotFound(err error) bool {
	return errors.Is(err, storage.ErrObjectNotExist)
}

func (b *Backend) fullKey(key string) string { return b.prefix + key }

func (b *Backend) object(key string) *storage.ObjectHandle {
	return b.client.Bucket(b.bucket).Object(b.fullKey(key))
}

func (b *Backend) Get(ctx context.Context, key string, off, length int64) (io.ReadCloser, error) {
	rc, err := b.object(key).NewRangeReader(ctx, off, length)
	if err != nil {
		if isNotFound(err) {
			return nil, store.ErrNotFound
		}
		return nil, err
	}
	return rc, nil
}

func (b *Backend) Put(ctx context.Context, key string, r io.Reader, size int64, o store.PutOpts) (store.ObjectInfo, error) {
	obj := b.object(key)
	if o.IfAbsent {
		if !b.caps.ConditionalPut {
			return store.ObjectInfo{}, fmt.Errorf("gcs backend: IfAbsent requested but this endpoint does not support conditional create (DESIGN.md §24.3: caller must tolerate this — a seal race becomes a duplicate object)")
		}
		obj = obj.If(storage.Conditions{DoesNotExist: true})
	}

	w := obj.NewWriter(ctx)
	// ChunkSize 0 disables the resumable-session protocol and sends the
	// whole object in one multipart/related request — the JSON-API
	// analogue of pkg/store/s3's single PutObject call. DisableAutoChecksum
	// skips the SDK's own CRC32C round-trip verification: content is
	// already hash-verified at the chunk/manifest layer (DESIGN.md §24.4),
	// so a second checksum here is redundant cost, not correctness.
	w.ChunkSize = 0
	w.DisableAutoChecksum = true
	if _, err := io.Copy(w, r); err != nil {
		_ = w.Close()
		return store.ObjectInfo{}, err
	}
	if err := w.Close(); err != nil {
		if o.IfAbsent && isPreconditionFailed(err) {
			// Wrap the same sentinel every other backend wraps (os.ErrExist)
			// so callers like repo.putManifest work identically across
			// backends without a type switch.
			return store.ObjectInfo{}, fmt.Errorf("gcs backend: %w: %s", os.ErrExist, key)
		}
		return store.ObjectInfo{}, err
	}
	attrs := w.Attrs()
	return store.ObjectInfo{
		Key:     key,
		Size:    attrs.Size,
		ETag:    strconv.FormatInt(attrs.Generation, 10),
		ModTime: attrs.Updated,
	}, nil
}

func (b *Backend) Delete(ctx context.Context, keys []string) ([]store.DeleteResult, error) {
	out := make([]store.DeleteResult, len(keys))
	for i, k := range keys {
		err := b.object(k).Delete(ctx)
		out[i] = store.DeleteResult{Key: k, Err: err}
	}
	return out, nil
}

func (b *Backend) Stat(ctx context.Context, key string) (store.ObjectInfo, error) {
	attrs, err := b.object(key).Attrs(ctx)
	if err != nil {
		if isNotFound(err) {
			return store.ObjectInfo{}, store.ErrNotFound
		}
		return store.ObjectInfo{}, err
	}
	return store.ObjectInfo{
		Key:     key,
		Size:    attrs.Size,
		ETag:    strconv.FormatInt(attrs.Generation, 10),
		ModTime: attrs.Updated,
	}, nil
}

func (b *Backend) List(ctx context.Context, prefix, cursor string, limit int) (store.ListPage, error) {
	fullPrefix := b.fullKey(prefix)
	it := b.client.Bucket(b.bucket).Objects(ctx, &storage.Query{Prefix: fullPrefix})

	pageSize := limit
	if pageSize <= 0 {
		// Unbounded: iterator.Pager requires a positive size, so ask for
		// everything in one logical page; the Pager loops HTTP requests
		// internally until the prefix is exhausted.
		pageSize = math.MaxInt32
	}
	pager := iterator.NewPager(it, pageSize, cursor)
	var page []*storage.ObjectAttrs
	next, err := pager.NextPage(&page)
	if err != nil {
		return store.ListPage{}, err
	}

	keys := make([]store.ObjectInfo, 0, len(page))
	for _, attrs := range page {
		keys = append(keys, store.ObjectInfo{
			Key:     strings.TrimPrefix(attrs.Name, b.prefix),
			Size:    attrs.Size,
			ETag:    strconv.FormatInt(attrs.Generation, 10),
			ModTime: attrs.Updated,
		})
	}
	// next is GCS's own opaque continuation token, not a key — it is
	// passed back verbatim to the next List call's cursor, never parsed
	// or reconstructed from a key (unlike S3's StartAfter, which is a
	// real key and safe to derive).
	return store.ListPage{Keys: keys, NextCursor: next}, nil
}

func (b *Backend) Caps() store.Caps { return b.caps }
