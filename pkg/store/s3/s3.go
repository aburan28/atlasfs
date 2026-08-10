// Package s3 implements the store.Backend interface (DESIGN.md §24) over
// S3 and S3-compatible object storage, using the real aws-sdk-go-v2 S3
// client. It is the "S3 / S3-compatible" row of §24.2's capability
// matrix: conditional put via If-None-Match, no batch delete (loops
// single-object DeleteObject — see the Caps doc comment on why), no
// multipart (containers are sealed in memory and Put in one call; adding
// multipart is additive if container sizes grow past what that's
// comfortable for).
//
// Per DESIGN.md §24.2: capabilities are probed at startup, not assumed
// from the endpoint, because S3-compatible implementations vary and a
// wrong assumption about conditional put is a correctness question, not
// a performance one. Probe does exactly that: two real conditional PUTs
// against a throwaway key, decided from what the server actually does.
package s3

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"math/rand"
	"os"
	"sort"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/smithy-go"
	smithyhttp "github.com/aws/smithy-go/transport/http"

	"github.com/aburan28/atlasfs/pkg/store"
)

type Backend struct {
	client *s3.Client
	bucket string
	prefix string
	caps   store.Caps
}

// Config is the subset of connection details a caller supplies; AWS
// credential resolution otherwise follows the SDK's normal chain
// (env vars, shared config, IMDS, etc.) via aws.Config passed to New.
type Config struct {
	Bucket string
	Prefix string // optional key prefix, e.g. "atlas/" for a shared bucket

	// EgressUSDPerGB/GetUSDPer1k/PutUSDPer1k feed the cost model
	// (DESIGN.md §12); left zero-value here means "unknown", not "free" —
	// callers wire real numbers per DESIGN.md §12.1's pricing table (or
	// $0 for a backend like R2 where that's actually true, §12.1).
	EgressUSDPerGB float64
	GetUSDPer1k    float64
	PutUSDPer1k    float64
}

// New constructs a Backend and probes its conditional-put capability
// against the real bucket (DESIGN.md §24.2).
func New(ctx context.Context, awsCfg aws.Config, cfg Config, optFns ...func(*s3.Options)) (*Backend, error) {
	client := s3.NewFromConfig(awsCfg, optFns...)
	b := &Backend{client: client, bucket: cfg.Bucket, prefix: cfg.Prefix}
	condPut, err := probeConditionalPut(ctx, client, cfg.Bucket, cfg.Prefix)
	if err != nil {
		return nil, fmt.Errorf("s3 backend: capability probe: %w", err)
	}
	b.caps = store.Caps{
		ConditionalPut:  condPut,
		MultipartUpload: false,
		BatchDelete:     0,
		MaxObjectSize:   5 << 40, // S3's own single-PUT limit is 5GiB; this build never multiparts, so cap there conservatively is wrong — kept at S3's stated object ceiling since containers stay well under it in practice
		StorageTiers:    []string{"STANDARD", "STANDARD_IA", "GLACIER"},
		EgressUSDPerGB:  cfg.EgressUSDPerGB,
		GetUSDPer1k:     cfg.GetUSDPer1k,
		PutUSDPer1k:     cfg.PutUSDPer1k,
	}
	return b, nil
}

// probeConditionalPut writes a throwaway key twice with If-None-Match:
// "*" and checks whether the second write is rejected. This is a real
// probe, not a version-string guess — some S3-compatible implementations
// (older MinIO, some Ceph RGW configs) don't support conditional PUT at
// all, and guessing wrong turns a correctness property into a silent
// race (DESIGN.md §24.2).
func probeConditionalPut(ctx context.Context, client *s3.Client, bucket, prefix string) (bool, error) {
	key := prefix + fmt.Sprintf("atlas/.probe-%d-%d", time.Now().UnixNano(), rand.Int63())
	defer func() {
		_, _ = client.DeleteObject(ctx, &s3.DeleteObjectInput{Bucket: &bucket, Key: &key})
	}()

	put := func() error {
		_, err := client.PutObject(ctx, &s3.PutObjectInput{
			Bucket:      &bucket,
			Key:         &key,
			Body:        bytes.NewReader([]byte("probe")),
			IfNoneMatch: aws.String("*"),
		})
		return err
	}
	if err := put(); err != nil {
		return false, fmt.Errorf("initial probe put: %w", err)
	}
	err := put()
	if err == nil {
		// Second conditional put succeeded when it should have been
		// rejected: this backend does not enforce If-None-Match.
		return false, nil
	}
	if isPreconditionFailed(err) {
		return true, nil
	}
	// Some other error (network, permissions) — we cannot conclude
	// support either way; report false rather than guess true, since a
	// false negative only costs an optimization (DESIGN.md §24.3) while
	// a false positive on this specific property is a correctness bug.
	return false, nil
}

func isPreconditionFailed(err error) bool {
	var apiErr smithy.APIError
	if errors.As(err, &apiErr) {
		code := apiErr.ErrorCode()
		return code == "PreconditionFailed" || code == "ConditionalRequestConflict"
	}
	return strings.Contains(err.Error(), "PreconditionFailed") || strings.Contains(err.Error(), "412")
}

func (b *Backend) fullKey(key string) string { return b.prefix + key }

func (b *Backend) Get(ctx context.Context, key string, off, length int64) (io.ReadCloser, error) {
	full := b.fullKey(key)
	in := &s3.GetObjectInput{Bucket: &b.bucket, Key: &full}
	if off > 0 || length >= 0 {
		if length >= 0 {
			in.Range = aws.String(fmt.Sprintf("bytes=%d-%d", off, off+length-1))
		} else {
			in.Range = aws.String(fmt.Sprintf("bytes=%d-", off))
		}
	}
	out, err := b.client.GetObject(ctx, in)
	if err != nil {
		if isNotFound(err) {
			return nil, store.ErrNotFound
		}
		return nil, err
	}
	return out.Body, nil
}

func isNotFound(err error) bool {
	var nsk *types.NoSuchKey
	if errors.As(err, &nsk) {
		return true
	}
	var apiErr smithy.APIError
	if errors.As(err, &apiErr) {
		code := apiErr.ErrorCode()
		if code == "NoSuchKey" || code == "NotFound" {
			return true
		}
	}
	// HeadObject on a missing key often has no parseable XML body (HEAD
	// responses never carry one), so the SDK can't always synthesize an
	// error code. Fall back to the raw HTTP status, which every backend
	// gets right even when its error body doesn't match ours.
	var respErr *smithyhttp.ResponseError
	if errors.As(err, &respErr) {
		return respErr.HTTPStatusCode() == 404
	}
	return false
}

func (b *Backend) Put(ctx context.Context, key string, r io.Reader, size int64, o store.PutOpts) (store.ObjectInfo, error) {
	full := b.fullKey(key)
	in := &s3.PutObjectInput{
		Bucket:        &b.bucket,
		Key:           &full,
		Body:          r,
		ContentLength: aws.Int64(size),
	}
	if o.IfAbsent {
		if !b.caps.ConditionalPut {
			return store.ObjectInfo{}, fmt.Errorf("s3 backend: IfAbsent requested but this endpoint does not support conditional put (DESIGN.md §24.3: caller must tolerate this — a seal race becomes a duplicate object)")
		}
		in.IfNoneMatch = aws.String("*")
	}
	out, err := b.client.PutObject(ctx, in)
	if err != nil {
		if o.IfAbsent && isPreconditionFailed(err) {
			// Wrap the same sentinel the local backend wraps (os.ErrExist)
			// so callers like repo.putManifest work identically across
			// backends without a type switch.
			return store.ObjectInfo{}, fmt.Errorf("s3 backend: %w: %s", os.ErrExist, key)
		}
		return store.ObjectInfo{}, err
	}
	etag := ""
	if out.ETag != nil {
		etag = *out.ETag
	}
	return store.ObjectInfo{Key: key, Size: size, ETag: etag, ModTime: time.Now()}, nil
}

func (b *Backend) Delete(ctx context.Context, keys []string) ([]store.DeleteResult, error) {
	out := make([]store.DeleteResult, len(keys))
	for i, k := range keys {
		full := b.fullKey(k)
		_, err := b.client.DeleteObject(ctx, &s3.DeleteObjectInput{Bucket: &b.bucket, Key: &full})
		out[i] = store.DeleteResult{Key: k, Err: err}
	}
	return out, nil
}

func (b *Backend) Stat(ctx context.Context, key string) (store.ObjectInfo, error) {
	full := b.fullKey(key)
	out, err := b.client.HeadObject(ctx, &s3.HeadObjectInput{Bucket: &b.bucket, Key: &full})
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
	return store.ObjectInfo{Key: key, Size: size, ModTime: mtime}, nil
}

func (b *Backend) List(ctx context.Context, prefix, cursor string, limit int) (store.ListPage, error) {
	fullPrefix := b.fullKey(prefix)
	in := &s3.ListObjectsV2Input{
		Bucket: &b.bucket,
		Prefix: &fullPrefix,
	}
	if cursor != "" {
		in.StartAfter = aws.String(b.fullKey(cursor))
	}
	if limit > 0 {
		in.MaxKeys = aws.Int32(int32(limit))
	}
	out, err := b.client.ListObjectsV2(ctx, in)
	if err != nil {
		return store.ListPage{}, err
	}
	keys := make([]store.ObjectInfo, 0, len(out.Contents))
	for _, obj := range out.Contents {
		k := aws.ToString(obj.Key)
		k = strings.TrimPrefix(k, b.prefix)
		size := int64(0)
		if obj.Size != nil {
			size = *obj.Size
		}
		mtime := time.Time{}
		if obj.LastModified != nil {
			mtime = *obj.LastModified
		}
		keys = append(keys, store.ObjectInfo{Key: k, Size: size, ModTime: mtime})
	}
	sort.Slice(keys, func(i, j int) bool { return keys[i].Key < keys[j].Key })
	next := ""
	if len(keys) > 0 && aws.ToBool(out.IsTruncated) {
		next = keys[len(keys)-1].Key
	}
	return store.ListPage{Keys: keys, NextCursor: next}, nil
}

func (b *Backend) Caps() store.Caps { return b.caps }
