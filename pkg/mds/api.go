// Package mds is the AtlasFS metadata service: the process boundary that
// turns DESIGN.md §10's coherence protocol from an in-process function
// call into a real distributed protocol.
//
// Why this package exists. Every earlier build of this repo ran the
// metadata store, the coherence manager, and the FUSE mount inside one
// process, which meant "holder" was always a string constant and there
// was only ever one of them. Three things in the design are not
// implementable in that shape, no matter how carefully the logic is
// written:
//
//   - §10.6's blocking recall has nobody to recall from.
//   - §10.2's push invalidation cannot be lost, so nothing ever tests the
//     claim that correctness does not depend on it.
//   - §17's cross-node locking has no second node.
//
// The split is metadata-only, and that is deliberate rather than a
// shortcut: DESIGN.md §11 has clients read chunk data from object storage
// (or a peer) directly, never through the metadata authority. So a client
// talks to this service for inodes, dentries, and leases, and talks to
// store.Backend for bytes. Funnelling data through the authority would be
// a different — and worse — architecture, not a more complete one.
//
// What is NOT here, stated plainly: this service owns one region's
// metadata. Home-region routing, the namespace map, and rehoming (§7)
// are still unbuilt and still need a real FoundationDB deployment; what
// this adds is the client/authority boundary those would sit on top of,
// not the multi-region topology itself.
package mds

import (
	"time"

	"github.com/aburan28/atlasfs/pkg/chunk"
	"github.com/aburan28/atlasfs/pkg/metadb"
	"github.com/aburan28/atlasfs/pkg/pack"
)

// ServiceName is the gRPC service this package implements.
const ServiceName = "atlas.mds.v1.Metadata"

// Lease is what an authority hands back with any cached-able answer
// (DESIGN.md §10.2). TTL is a *duration*, never an absolute server
// timestamp, because §10.7 requires the holder to measure expiry on its
// own clock: shipping an absolute deadline would silently make
// correctness depend on clock agreement between authority and client,
// which is the one thing §10.7 is written to avoid.
type Lease struct {
	Object  string        `json:"object"`
	Version uint64        `json:"version"`
	TTL     time.Duration `json:"ttl"`
}

type GetInodeRequest struct {
	Holder string         `json:"holder"`
	Inode  metadb.InodeID `json:"inode"`
}

type GetInodeResponse struct {
	Record metadb.InodeRecord `json:"record"`
	Lease  Lease              `json:"lease"`
}

type LookupRequest struct {
	Holder string         `json:"holder"`
	Dir    metadb.InodeID `json:"dir"`
	Name   string         `json:"name"`
}

type LookupResponse struct {
	Found  bool               `json:"found"`
	Inode  metadb.InodeID     `json:"inode"`
	Record metadb.InodeRecord `json:"record"`
	Lease  Lease              `json:"lease"`
	// DirVersion accompanies a negative answer so the client can cache it
	// and revalidate against a single directory version rather than per
	// name (DESIGN.md §10.4).
	DirVersion uint64 `json:"dirVersion"`

	// DirLease is the lease on the *directory*, which is what makes a
	// positive dentry cacheable. §10.5 keeps directory and inode leases
	// in separate domains precisely so that "this name maps to this
	// inode" and "this inode has these attrs" can be invalidated
	// independently — a create in a directory invalidates the former for
	// every holder without touching the latter.
	DirLease Lease `json:"dirLease"`
}

type ReaddirRequest struct {
	Holder string         `json:"holder"`
	Dir    metadb.InodeID `json:"dir"`
}

type ReaddirResponse struct {
	Entries []metadb.DirEntry `json:"entries"`
	Lease   Lease             `json:"lease"`
}

// CommitRequest binds name in dir to Record. The client has already
// written any chunk data to object storage and resolved its content
// pointer (DESIGN.md §16.1 step 2) — the authority's job is the metadata
// transaction and the coherence side effects, not the bytes.
type CommitRequest struct {
	Holder string             `json:"holder"`
	Dir    metadb.InodeID     `json:"dir"`
	Name   string             `json:"name"`
	Record metadb.InodeRecord `json:"record"`
}

type CommitResponse struct {
	Inode   metadb.InodeID `json:"inode"`
	Version uint64         `json:"version"`
	// RecalledHolders / TimedOutHolders are populated for the `posix`
	// class only, reporting what the blocking recall actually achieved
	// (§10.6). A non-empty TimedOutHolders means the commit proceeded on
	// the DRECALL deadline without confirmation from those holders —
	// permitted by the design, and exactly the fact an operator needs
	// when a client is partitioned, so it is reported rather than hidden.
	RecalledHolders []string `json:"recalledHolders,omitempty"`
	TimedOutHolders []string `json:"timedOutHolders,omitempty"`
}

type UnlinkRequest struct {
	Holder string         `json:"holder"`
	Dir    metadb.InodeID `json:"dir"`
	Name   string         `json:"name"`
}

type UnlinkResponse struct {
	Version uint64 `json:"version"`
}

// SubscribeRequest opens a holder's invalidation stream. One stream per
// holder carries every object's events; the registry cap in
// pkg/coherence still applies per object.
type SubscribeRequest struct {
	Holder string `json:"holder"`
}

// EventKind distinguishes the two things an authority pushes.
type EventKind string

const (
	// EventInvalidate is DESIGN.md §10.2's best-effort push. Losing it
	// costs responsiveness and nothing else — the holder's lease still
	// bounds staleness on its own.
	EventInvalidate EventKind = "invalidate"

	// EventRecall is §10.6's blocking recall. A commit is waiting on the
	// holder to drop its copy and call AckRecall with this RecallID. Not
	// answering does not stall the writer forever (DRECALL bounds it) but
	// it does mean the writer proceeds without confirmation.
	EventRecall EventKind = "recall"
)

type Event struct {
	Kind     EventKind `json:"kind"`
	Object   string    `json:"object"`
	RecallID uint64    `json:"recallId,omitempty"`
}

type AckRecallRequest struct {
	Holder   string `json:"holder"`
	RecallID uint64 `json:"recallId"`
}

type AckRecallResponse struct{}

// GetLocatorRequest resolves a chunk ID to where its bytes live in this
// region.
//
// A locator is metadata, so the authority owns it — but note what is
// *not* here: the chunk's bytes. The client takes the (container,
// offset, length) this returns and fetches straight from store.Backend,
// which is DESIGN.md §11's split holding even at the RPC boundary. The
// authority never sees file data.
type GetLocatorRequest struct {
	Holder  string   `json:"holder"`
	ChunkID chunk.ID `json:"chunkId"`
}

type GetLocatorResponse struct {
	Found   bool         `json:"found"`
	Locator pack.Locator `json:"locator"`
}

// PutLocatorRequest registers where a chunk's bytes live. The client has
// already sealed a container to object storage; this is the metadata
// half of that (DESIGN.md §16.1 step 2).
//
// §7.5 calls this write commutative and safe without home-region
// arbitration: two clients racing to register the same content-addressed
// chunk are both correct, because they are asserting the same fact.
type PutLocatorRequest struct {
	Holder  string       `json:"holder"`
	ChunkID chunk.ID     `json:"chunkId"`
	Locator pack.Locator `json:"locator"`
}

type PutLocatorResponse struct{}

// HasLocatorRequest is the write-path dedup check (§16.1 step 2:
// "uploaded only if absent"). Asking before uploading is what makes
// republishing an unchanged dataset nearly free.
type HasLocatorRequest struct {
	Holder  string   `json:"holder"`
	ChunkID chunk.ID `json:"chunkId"`
}

type HasLocatorResponse struct {
	Found bool `json:"found"`
}

type MkdirRequest struct {
	Holder string         `json:"holder"`
	Dir    metadb.InodeID `json:"dir"`
	Name   string         `json:"name"`
}

type MkdirResponse struct {
	Inode metadb.InodeID `json:"inode"`
}

// RmdirRequest removes an empty directory. Emptiness is checked by the
// authority, not the client: a client-side check would be a TOCTOU race
// against another holder creating an entry.
type RmdirRequest struct {
	Holder string         `json:"holder"`
	Dir    metadb.InodeID `json:"dir"`
	Name   string         `json:"name"`
}

type RmdirResponse struct{}

// Full method names, as they appear on the wire.
const (
	MethodGetInode   = "/" + ServiceName + "/GetInode"
	MethodLookup     = "/" + ServiceName + "/Lookup"
	MethodReaddir    = "/" + ServiceName + "/Readdir"
	MethodCommit     = "/" + ServiceName + "/Commit"
	MethodUnlink     = "/" + ServiceName + "/Unlink"
	MethodSubscribe  = "/" + ServiceName + "/Subscribe"
	MethodAckRecall  = "/" + ServiceName + "/AckRecall"
	MethodGetLocator = "/" + ServiceName + "/GetLocator"
	MethodPutLocator = "/" + ServiceName + "/PutLocator"
	MethodHasLocator = "/" + ServiceName + "/HasLocator"
	MethodMkdir      = "/" + ServiceName + "/Mkdir"
	MethodRmdir      = "/" + ServiceName + "/Rmdir"
)
