// Garbage collection (DESIGN.md §19): mark-and-sweep against the
// metadata store as ground truth. Unlink/Rmdir (write.go) already move
// an unlinked inode into metadb's graveyard instead of deleting it;
// Sweep here is the second half — it walks the graveyard and reclaims
// an entry's chunks once they are provably unreferenced and the entry
// is past DESIGN.md §19.2's grace period.
//
// Two simplifications from the full design, stated plainly:
//
//   - §19.1 gates sweeping on a *container's* seal time ("its container
//     was sealed before V − T_grace"), which lets one timestamp cover
//     every chunk in that container. This build gates on the
//     *graveyard entry's* delete time instead — simpler, and correct
//     for the same reason: a chunk that survives the mark phase (still
//     reachable from the live tree or a not-yet-expired graveyard
//     entry) is never swept regardless of which timestamp gated the
//     scan. What DESIGN.md §19.1 step 3 ("compact") would additionally
//     buy — reclaiming a partially-live container's actual backend
//     bytes — is not implemented here: Sweep removes locator entries
//     (metadb bucketLocator), not container objects (store.Backend),
//     so backend storage is not actually shrunk by a Sweep run. That is
//     real remaining scope, not a hidden gap.
//   - §19.3's "open-but-unlinked" guarantee — an inode staying reachable
//     while any client holds an open handle, via a leased open-handle
//     registry — is not implemented. This build has no open-file-handle
//     tracking across process boundaries (single process, so pkg/
//     fuseserver's Node.activeWrite is the closest thing, and it is not
//     wired into Sweep). An inode is gravable immediately on unlink;
//     unlinking a file a FUSE client still has open and then sweeping
//     it past grace would pull the content out from under that client.
//     Real remaining scope, stated rather than silently built partial.
//
// GC-1's other half, the client-side "fail any write session older than
// T_write_max with ESTALE," is also not implemented: WriteHandle does
// not record when its write session began (a session can span multiple
// Write calls before Commit), so there is nothing to check it against
// yet. Only the piece that is mechanically enforceable without a real
// distributed writer-liveness check is enforced here: Sweep refuses to
// run with a graceDuration that would violate Invariant GC-1.
package repo

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/aburan28/atlasfs/pkg/chunk"
	"github.com/aburan28/atlasfs/pkg/metadb"
)

// DESIGN.md §19.2's Invariant GC-1 inputs, defaults as stated there.
const (
	// TWriteMax is the maximum permitted age of an uncommitted write
	// session before a client must fail it with ESTALE — the
	// client-side half of GC-1 this build does not enforce (see this
	// file's package doc).
	TWriteMax = 1 * time.Hour

	// DMax is the longest lease granted on a *mutable* subtree —
	// ClassRelaxed's lease (Class.leaseDuration), the longest this
	// build has since `posix` is not implemented and `immutable`'s
	// unbounded lease is irrelevant to GC-1 per DESIGN.md §19.2.
	// TestDMaxMatchesRelaxedLeaseDuration pins this to
	// Class.leaseDuration so the two cannot silently drift apart.
	DMax = 30 * time.Second

	// Epsilon is DESIGN.md §19.2's clock-skew/scheduling margin.
	Epsilon = 500 * time.Millisecond
)

// MinGraceDuration is Invariant GC-1's lower bound: T_grace must be
// strictly greater than T_write_max + D_max + epsilon. Sweep refuses to
// run with a graceDuration that does not clear it.
const MinGraceDuration = TWriteMax + DMax + Epsilon

// DefaultGraceDuration is DESIGN.md §19.2's stated default T_grace.
const DefaultGraceDuration = 24 * time.Hour

// ErrGraceTooShort is returned by Sweep when graceDuration would violate
// Invariant GC-1. DESIGN.md §19.2: "the policy validator enforces GC-1
// and rejects configurations that violate it, so the invariant cannot
// be broken by configuration" — Sweep is that validator for this build.
var ErrGraceTooShort = errors.New("repo: graceDuration violates DESIGN.md §19.2 invariant GC-1")

// Sweep implements DESIGN.md §19.1's mark-and-sweep: it marks every
// chunk reachable from the live namespace or from a graveyard entry not
// yet past grace, then sweeps every graveyard entry that is past
// grace — removing the locator entries of any of its chunks that mark
// did not reach, then the entry itself (and its now fully-unreferenced
// inode record). collected is the number of distinct chunk locator
// entries removed.
//
// Sweep refuses to run (returning ErrGraceTooShort, nothing touched) if
// graceDuration does not satisfy Invariant GC-1 — see MinGraceDuration.
func (r *Repo) Sweep(ctx context.Context, graceDuration time.Duration) (collected int, err error) {
	if graceDuration <= MinGraceDuration {
		return 0, fmt.Errorf("%w: got %s, need > %s (T_write_max=%s + D_max=%s + epsilon=%s)",
			ErrGraceTooShort, graceDuration, MinGraceDuration, TWriteMax, DMax, Epsilon)
	}

	now := r.Clock.Now()
	cutoff := now.Add(-graceDuration)

	live, err := r.markLive(ctx, cutoff)
	if err != nil {
		return 0, fmt.Errorf("repo: sweep: mark phase: %w", err)
	}

	entries, err := r.DB.ListGraveyard()
	if err != nil {
		return 0, err
	}

	removed := map[chunk.ID]struct{}{}
	for _, e := range entries {
		if e.DeletedAt.After(cutoff) {
			continue // not yet past grace: still reachable per §19.1
		}

		rec, err := r.DB.GetInode(e.InodeID)
		if err != nil {
			if errors.Is(err, metadb.ErrNotFound) {
				// A prior, interrupted Sweep already deleted the inode
				// record but not this graveyard entry; finish the job.
				if err := r.DB.RemoveGraveyardEntry(e); err != nil {
					return len(removed), err
				}
				continue
			}
			return len(removed), err
		}

		ids, err := r.chunkIDsOf(ctx, rec)
		if err != nil {
			return len(removed), err
		}
		for _, id := range ids {
			if _, ok := live[id]; ok {
				continue // referenced by another live or not-yet-grave inode
			}
			if _, done := removed[id]; done {
				continue // already swept earlier this same run
			}
			if err := r.DB.DeleteLocator(r.Region, id); err != nil {
				return len(removed), err
			}
			removed[id] = struct{}{}
		}

		if err := r.DB.DeleteInode(e.InodeID); err != nil {
			return len(removed), err
		}
		if err := r.DB.RemoveGraveyardEntry(e); err != nil {
			return len(removed), err
		}
	}
	return len(removed), nil
}

// markLive is DESIGN.md §19.1 step 1: the set of chunk IDs reachable at
// "now" — from every inode still bound in the live namespace, from
// every graveyard entry not yet past grace (cutoff), and from the
// packer's not-yet-sealed buffer.
func (r *Repo) markLive(ctx context.Context, cutoff time.Time) (map[chunk.ID]struct{}, error) {
	live := map[chunk.ID]struct{}{}

	if err := r.markLiveTree(ctx, metadb.RootInode, live); err != nil {
		return nil, err
	}

	entries, err := r.DB.ListGraveyard()
	if err != nil {
		return nil, err
	}
	for _, e := range entries {
		if !e.DeletedAt.After(cutoff) {
			continue // past grace: this is exactly what Sweep may collect
		}
		rec, err := r.DB.GetInode(e.InodeID)
		if err != nil {
			return nil, err
		}
		ids, err := r.chunkIDsOf(ctx, rec)
		if err != nil {
			return nil, err
		}
		for _, id := range ids {
			live[id] = struct{}{}
		}
	}

	for _, id := range r.packer.PendingIDs() {
		live[id] = struct{}{}
	}
	return live, nil
}

// markLiveTree walks the live namespace from dir, adding every
// reachable file's chunk IDs to live. Dedup (DESIGN.md §15) means the
// same chunk ID can legitimately turn up under more than one inode;
// live is a set, so that is exactly what makes marking it twice safe.
func (r *Repo) markLiveTree(ctx context.Context, dir metadb.InodeID, live map[chunk.ID]struct{}) error {
	entries, err := r.DB.Readdir(dir)
	if err != nil {
		return err
	}
	for _, e := range entries {
		rec, err := r.DB.GetInode(e.Inode)
		if err != nil {
			return err
		}
		if rec.IsDir {
			if err := r.markLiveTree(ctx, e.Inode, live); err != nil {
				return err
			}
			continue
		}
		ids, err := r.chunkIDsOf(ctx, rec)
		if err != nil {
			return err
		}
		for _, id := range ids {
			live[id] = struct{}{}
		}
	}
	return nil
}

// chunkIDsOf returns the chunk IDs an inode record references: its
// inline chunk, every entry in its manifest (fetched the same way
// OpenFile does), or none for a directory, symlink, or empty file.
func (r *Repo) chunkIDsOf(ctx context.Context, rec metadb.InodeRecord) ([]chunk.ID, error) {
	switch {
	case rec.HasInline:
		return []chunk.ID{rec.InlineChunk}, nil
	case rec.HasManifest:
		m, err := r.getManifest(ctx, rec.ManifestID)
		if err != nil {
			return nil, fmt.Errorf("repo: sweep: fetch manifest %s: %w", rec.ManifestID, err)
		}
		ids := make([]chunk.ID, len(m.Entries))
		for i, entry := range m.Entries {
			ids[i] = entry.ChunkID
		}
		return ids, nil
	default:
		return nil, nil
	}
}
