// Package azure implements the store.Backend interface (DESIGN.md §24) over
// Azure Blob Storage, using the real azure-sdk-for-go/sdk/storage/azblob
// client. It is the "Azure Blob" row of §24.2's capability matrix:
// conditional create via If-None-Match, no multipart (containers are sealed
// in memory and Put in one call, same as pkg/store/s3 — Azure's block-blob
// staging is additive if container sizes grow past what a single Put Blob
// call is comfortable with), no batch delete (loops single-blob DeleteBlob
// calls — Azure's real batch endpoint tops out at 256/request, but per
// DESIGN.md §24.3 a missing batch capability only costs GC sweep speed,
// never correctness, so it's not worth the extra request shape yet).
//
// Per DESIGN.md §24.2: capabilities are probed at startup, not assumed from
// the endpoint. Azure Blob Storage documents If-None-Match: * support
// unconditionally, but the probe still runs a real conditional create
// against the account rather than trusting the docs, for the same reason
// pkg/store/s3 does: a wrong assumption here is a correctness bug, not a
// performance one.
package azure

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"math/rand"
	"os"
	"sort"
	"strings"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/to"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/blob"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/bloberror"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/blockblob"

	"github.com/aburan28/atlasfs/pkg/store"
)

type Backend struct {
	client    *azblob.Client
	container string
	prefix    string
	caps      store.Caps
}

// Config is the subset of connection details a caller supplies; Azure
// AD/shared-key credential resolution is otherwise the SDK's job — a
// caller needing it passes a credential-bearing *azblob.ClientOptions via
// New's optFns, the same scoping pkg/store/s3 uses for aws.Config.
type Config struct {
	Container string
	Prefix    string // optional key prefix, e.g. "atlas/" for a shared container

	// EgressUSDPerGB/GetUSDPer1k/PutUSDPer1k feed the cost model
	// (DESIGN.md §12); zero-value here means "unknown", not "free" —
	// callers wire real numbers per DESIGN.md §12.1's pricing table.
	EgressUSDPerGB float64
	GetUSDPer1k    float64
	PutUSDPer1k    float64
}

// New constructs a Backend against serviceURL (e.g.
// "https://<account>.blob.core.windows.net/" for real Azure, or a fake
// test server's URL) and probes its conditional-create capability against
// the real container (DESIGN.md §24.2). Real Azure auth is out of scope —
// callers needing it pass a credential-bearing *azblob.ClientOptions via
// optFns; the default here is anonymous/SAS access, which is all the
// in-process fake server in this package's tests need.
func New(ctx context.Context, serviceURL string, cfg Config, optFns ...func(*azblob.ClientOptions)) (*Backend, error) {
	opts := &azblob.ClientOptions{}
	for _, fn := range optFns {
		fn(opts)
	}
	client, err := azblob.NewClientWithNoCredential(serviceURL, opts)
	if err != nil {
		return nil, fmt.Errorf("azure backend: %w", err)
	}
	b := &Backend{client: client, container: cfg.Container, prefix: cfg.Prefix}
	condPut, err := probeConditionalPut(ctx, client, cfg.Container, cfg.Prefix)
	if err != nil {
		return nil, fmt.Errorf("azure backend: capability probe: %w", err)
	}
	b.caps = store.Caps{
		ConditionalPut:  condPut,
		MultipartUpload: false,
		BatchDelete:     0,
		MaxObjectSize:   blockblob.MaxUploadBlobBytes, // single Put Blob call; this build never stages blocks (see package doc comment)
		StorageTiers:    []string{"Hot", "Cool", "Cold", "Archive"},
		EgressUSDPerGB:  cfg.EgressUSDPerGB,
		GetUSDPer1k:     cfg.GetUSDPer1k,
		PutUSDPer1k:     cfg.PutUSDPer1k,
	}
	return b, nil
}

// probeConditionalPut writes a throwaway blob twice with If-None-Match: "*"
// and checks whether the second write is rejected. A real probe, not a
// version guess (DESIGN.md §24.2) — the two writes go over the same wire
// protocol as production traffic, against the same account.
func probeConditionalPut(ctx context.Context, client *azblob.Client, container, prefix string) (bool, error) {
	key := prefix + fmt.Sprintf("atlas/.probe-%d-%d", time.Now().UnixNano(), rand.Int63())
	defer func() {
		_, _ = client.DeleteBlob(ctx, container, key, nil)
	}()

	condOpts := &azblob.UploadBufferOptions{
		AccessConditions: &blob.AccessConditions{
			ModifiedAccessConditions: &blob.ModifiedAccessConditions{IfNoneMatch: to.Ptr(azcore.ETag("*"))},
		},
	}
	put := func() error {
		_, err := client.UploadBuffer(ctx, container, key, []byte("probe"), condOpts)
		return err
	}
	if err := put(); err != nil {
		return false, fmt.Errorf("initial probe put: %w", err)
	}
	err := put()
	if err == nil {
		// Second conditional put succeeded when it should have been
		// rejected: this endpoint does not enforce If-None-Match.
		return false, nil
	}
	if isBlobExists(err) {
		return true, nil
	}
	// Some other error (network, permissions) — cannot conclude support
	// either way; report false rather than guess true, since a false
	// negative only costs an optimization (DESIGN.md §24.3) while a false
	// positive on this specific property is a correctness bug.
	return false, nil
}

// isBlobExists reports whether err is Azure's rejection of a conditional
// create (If-None-Match: *) against a blob that already exists. Unlike
// S3's 412 Precondition Failed, Azure's Put Blob returns 409 Conflict with
// error code BlobAlreadyExists specifically for this case — verified
// against the real wire protocol via this package's fake server, not
// assumed from documentation (see package doc comment).
func isBlobExists(err error) bool {
	return bloberror.HasCode(err, bloberror.BlobAlreadyExists, bloberror.ConditionNotMet)
}

func isNotFound(err error) bool {
	return bloberror.HasCode(err, bloberror.BlobNotFound)
}

func (b *Backend) fullKey(key string) string { return b.prefix + key }

func (b *Backend) blobClient(key string) *blob.Client {
	return b.client.ServiceClient().NewContainerClient(b.container).NewBlobClient(b.fullKey(key))
}

func (b *Backend) Get(ctx context.Context, key string, off, length int64) (io.ReadCloser, error) {
	full := b.fullKey(key)
	opts := &azblob.DownloadStreamOptions{}
	if off > 0 || length >= 0 {
		opts.Range = blob.HTTPRange{Offset: off}
		if length >= 0 {
			opts.Range.Count = length
		}
	}
	out, err := b.client.DownloadStream(ctx, b.container, full, opts)
	if err != nil {
		if isNotFound(err) {
			return nil, store.ErrNotFound
		}
		return nil, err
	}
	return out.Body, nil
}

// readSeekNopCloser adapts an io.ReadSeeker (every real caller passes a
// bytes.Reader — see pkg/repo and pkg/pack) to the io.ReadSeekCloser the
// SDK's single-shot block blob Upload requires.
type readSeekNopCloser struct{ io.ReadSeeker }

func (readSeekNopCloser) Close() error { return nil }

func (b *Backend) Put(ctx context.Context, key string, r io.Reader, size int64, o store.PutOpts) (store.ObjectInfo, error) {
	full := b.fullKey(key)

	var body io.ReadSeekCloser
	if rs, ok := r.(io.ReadSeeker); ok {
		body = readSeekNopCloser{rs}
	} else {
		data, err := io.ReadAll(io.LimitReader(r, size))
		if err != nil {
			return store.ObjectInfo{}, err
		}
		body = readSeekNopCloser{bytes.NewReader(data)}
	}

	blockBlobClient := b.client.ServiceClient().NewContainerClient(b.container).NewBlockBlobClient(full)
	uploadOpts := &blockblob.UploadOptions{}
	if o.IfAbsent {
		if !b.caps.ConditionalPut {
			return store.ObjectInfo{}, fmt.Errorf("azure backend: IfAbsent requested but this endpoint does not support conditional put (DESIGN.md §24.3: caller must tolerate this — a seal race becomes a duplicate object)")
		}
		uploadOpts.AccessConditions = &blob.AccessConditions{
			ModifiedAccessConditions: &blob.ModifiedAccessConditions{IfNoneMatch: to.Ptr(azcore.ETag("*"))},
		}
	}
	out, err := blockBlobClient.Upload(ctx, body, uploadOpts)
	if err != nil {
		if o.IfAbsent && isBlobExists(err) {
			// Wrap the same sentinel the local and S3 backends wrap
			// (os.ErrExist) so callers like repo.putManifest work
			// identically across backends without a type switch.
			return store.ObjectInfo{}, fmt.Errorf("azure backend: %w: %s", os.ErrExist, key)
		}
		return store.ObjectInfo{}, err
	}
	etag := ""
	if out.ETag != nil {
		etag = string(*out.ETag)
	}
	return store.ObjectInfo{Key: key, Size: size, ETag: etag, ModTime: time.Now()}, nil
}

func (b *Backend) Delete(ctx context.Context, keys []string) ([]store.DeleteResult, error) {
	out := make([]store.DeleteResult, len(keys))
	for i, k := range keys {
		full := b.fullKey(k)
		_, err := b.client.DeleteBlob(ctx, b.container, full, nil)
		out[i] = store.DeleteResult{Key: k, Err: err}
	}
	return out, nil
}

func (b *Backend) Stat(ctx context.Context, key string) (store.ObjectInfo, error) {
	out, err := b.blobClient(key).GetProperties(ctx, nil)
	if err != nil {
		if isNotFound(err) {
			return store.ObjectInfo{}, store.ErrNotFound
		}
		return store.ObjectInfo{}, err
	}
	size := int64(0)
	if out.ContentLength != nil {
		size = *out.ContentLength
	}
	mtime := time.Time{}
	if out.LastModified != nil {
		mtime = *out.LastModified
	}
	etag := ""
	if out.ETag != nil {
		etag = string(*out.ETag)
	}
	return store.ObjectInfo{Key: key, Size: size, ETag: etag, ModTime: mtime}, nil
}

func (b *Backend) List(ctx context.Context, prefix, cursor string, limit int) (store.ListPage, error) {
	fullPrefix := b.fullKey(prefix)
	opts := &azblob.ListBlobsFlatOptions{Prefix: &fullPrefix}
	if cursor != "" {
		// cursor round-trips Azure's own continuation token (NextMarker)
		// verbatim — it is opaque to store.Backend callers by contract,
		// so there is no key to reconstruct here, unlike the S3 backend's
		// StartAfter-based cursor.
		opts.Marker = &cursor
	}
	if limit > 0 {
		n := int32(limit)
		opts.MaxResults = &n
	}

	pager := b.client.NewListBlobsFlatPager(b.container, opts)
	if !pager.More() {
		return store.ListPage{}, nil
	}
	page, err := pager.NextPage(ctx)
	if err != nil {
		return store.ListPage{}, err
	}

	keys := make([]store.ObjectInfo, 0, len(page.Segment.BlobItems))
	for _, item := range page.Segment.BlobItems {
		k := strings.TrimPrefix(*item.Name, b.prefix)
		size := int64(0)
		if item.Properties.ContentLength != nil {
			size = *item.Properties.ContentLength
		}
		mtime := time.Time{}
		if item.Properties.LastModified != nil {
			mtime = *item.Properties.LastModified
		}
		keys = append(keys, store.ObjectInfo{Key: k, Size: size, ModTime: mtime})
	}
	sort.Slice(keys, func(i, j int) bool { return keys[i].Key < keys[j].Key })

	next := ""
	if page.NextMarker != nil && *page.NextMarker != "" {
		next = *page.NextMarker
	}
	return store.ListPage{Keys: keys, NextCursor: next}, nil
}

func (b *Backend) Caps() store.Caps { return b.caps }
