// Package pack implements container packing and chunk locators
// (DESIGN.md §5.4, §5.5, §14). Chunks are content-addressed and
// immutable, but a chunk is never its own backend object — thousands of
// small chunks are packed into one sealed container object, and a
// separate locator map resolves a chunk_id to (container, offset,
// length) within some region. This indirection is what lets small-file
// datasets avoid one-object-per-file (§14) without ever touching a
// manifest, and what would let a later cloud backend replicate by
// rewriting locators instead of data (§13).
package pack

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"io"

	"github.com/aburan28/atlasfs/pkg/chunk"
	"github.com/aburan28/atlasfs/pkg/store"
)

// DefaultSealSize is the container seal threshold (DESIGN.md §5.5).
const DefaultSealSize = 128 << 20 // 128 MiB

// SingleObjectThreshold: files at or above this size get a dedicated,
// unpacked container so large sequential reads are a direct range GET
// with no packing indirection cost (DESIGN.md §5.5).
const SingleObjectThreshold = 64 << 20 // 64 MiB

// Locator resolves a chunk to its location within one region's storage.
// DESIGN.md §5.4: locator: chunk_id -> {region -> {container, offset,
// length, codec, uncompressed_length}}. This build is single-region, so
// the region key is carried by the caller (metadb key), not here.
type Locator struct {
	Container string
	Offset    int64
	Length    int64
}

// containerKey builds the sharded object key from DESIGN.md §5.5:
// atlas/c/{region}/{shard}/{container_id}.
func containerKey(region, shard, id string) string {
	return fmt.Sprintf("atlas/c/%s/%s/%s", region, shard, id)
}

func newContainerID() string {
	var b [16]byte
	_, _ = rand.Read(b[:])
	return hex.EncodeToString(b[:])
}

// Packer accumulates chunks into a container buffer and seals it to a
// Backend once it reaches sealSize (or on an explicit Seal call). It is
// not safe for concurrent use — DESIGN.md's per-node packer (§5.5) is
// one writer per container.
type Packer struct {
	backend  store.Backend
	region   string
	sealSize int

	buf     bytes.Buffer
	pending []pendingEntry
}

type pendingEntry struct {
	id     chunk.ID
	offset int64
	length int64
}

func NewPacker(backend store.Backend, region string, sealSize int) *Packer {
	if sealSize <= 0 {
		sealSize = DefaultSealSize
	}
	return &Packer{backend: backend, region: region, sealSize: sealSize}
}

// Add appends c to the current container buffer, recording its offset.
// Locators for it are only valid once the container containing it is
// sealed — call Full to know when to Seal.
func (p *Packer) Add(c chunk.Chunk) {
	off := int64(p.buf.Len())
	p.buf.Write(c.Data)
	p.pending = append(p.pending, pendingEntry{id: c.ID, offset: off, length: int64(len(c.Data))})
}

// Full reports whether the buffer has reached the seal threshold.
func (p *Packer) Full() bool { return p.buf.Len() >= p.sealSize }

// Empty reports whether there is nothing pending.
func (p *Packer) Empty() bool { return p.buf.Len() == 0 }

// PendingIDs returns the chunk IDs currently buffered but not yet sealed
// into a container. A pending chunk has no locator yet (storeChunk only
// calls PutLocator after Seal), so GC's mark phase (DESIGN.md §19.1)
// has nothing to remove for one regardless — this exists so that fact
// is an explicit, checked part of the invariant rather than an
// incidental one.
func (p *Packer) PendingIDs() []chunk.ID {
	ids := make([]chunk.ID, len(p.pending))
	for i, e := range p.pending {
		ids[i] = e.id
	}
	return ids
}

// Seal writes the current buffer as one sealed container object and
// returns the locators for every chunk packed into it. It is a no-op
// (returns nil, nil) if nothing is pending.
func (p *Packer) Seal(ctx context.Context) (map[chunk.ID]Locator, error) {
	if p.buf.Len() == 0 {
		return nil, nil
	}
	id := newContainerID()
	key := containerKey(p.region, id[:2], id)
	data := p.buf.Bytes()
	if _, err := p.backend.Put(ctx, key, bytes.NewReader(data), int64(len(data)), store.PutOpts{}); err != nil {
		return nil, fmt.Errorf("pack: seal container: %w", err)
	}
	out := make(map[chunk.ID]Locator, len(p.pending))
	for _, e := range p.pending {
		out[e.id] = Locator{Container: key, Offset: e.offset, Length: e.length}
	}
	p.buf.Reset()
	p.pending = nil
	return out, nil
}

// PutSingleObject writes data as its own dedicated container object,
// bypassing packing entirely — the >= SingleObjectThreshold path from
// DESIGN.md §5.5 that keeps large sequential reads a single direct range
// GET. Used for whole large files and for checkpoint-class chunks.
func PutSingleObject(ctx context.Context, backend store.Backend, region string, c chunk.Chunk) (Locator, error) {
	id := newContainerID()
	key := containerKey(region, id[:2], id)
	if _, err := backend.Put(ctx, key, bytes.NewReader(c.Data), int64(len(c.Data)), store.PutOpts{}); err != nil {
		return Locator{}, fmt.Errorf("pack: put single object: %w", err)
	}
	return Locator{Container: key, Offset: 0, Length: int64(len(c.Data))}, nil
}

// FetchInto reads a chunk into p, which must be exactly the chunk's
// length, and verifies the content hash in place — no intermediate
// buffer, no allocation. It is Fetch's caller-supplied-destination form
// (see pkg/store/getinto.go for why that shape matters).
//
// The verification tension a GPUDirect backend will have to resolve,
// stated now rather than discovered later: DESIGN.md §24.4 makes reads
// self-verifying, and this function upholds that by hashing p after the
// read. That works because p is host memory the CPU can read back. A
// real GDS path DMAs storage straight into device memory, where hashing
// on the CPU would mean copying it back and forfeiting the entire
// benefit. Such a backend has exactly two honest options — verify on the
// GPU, or declare verification waived for that path — and the choice
// belongs to whoever builds it. What this seam guarantees is that the
// choice is visible: every host-memory read through here is verified,
// so an unverified path has to say so explicitly.
func FetchInto(ctx context.Context, backend store.Backend, id chunk.ID, loc Locator, p []byte) error {
	if int64(len(p)) != loc.Length {
		return fmt.Errorf("pack: FetchInto buffer is %d bytes, chunk %s is %d", len(p), id, loc.Length)
	}
	if err := store.GetInto(ctx, backend, loc.Container, loc.Offset, p); err != nil {
		return err
	}
	if got := chunk.Sum(p); got != id {
		return fmt.Errorf("pack: chunk %s failed verification (got %s)", id, got)
	}
	return nil
}

// Fetch reads a chunk's bytes from the backend at its locator and
// verifies the content hash, per DESIGN.md §24.4: reads are
// self-verifying, so a backend can never serve wrong bytes undetected.
func Fetch(ctx context.Context, backend store.Backend, id chunk.ID, loc Locator) ([]byte, error) {
	rc, err := backend.Get(ctx, loc.Container, loc.Offset, loc.Length)
	if err != nil {
		return nil, err
	}
	defer rc.Close()
	data, err := io.ReadAll(rc)
	if err != nil {
		return nil, err
	}
	if int64(len(data)) != loc.Length {
		return nil, fmt.Errorf("pack: short read for chunk %s: got %d want %d", id, len(data), loc.Length)
	}
	if got := chunk.Sum(data); got != id {
		return nil, fmt.Errorf("pack: chunk %s failed verification (got %s)", id, got)
	}
	return data, nil
}
