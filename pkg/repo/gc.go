// Garbage collection (DESIGN.md §19): mark-and-sweep against the
// metadata store as ground truth. Unlink/Rmdir (write.go) already move
// an unlinked inode into metadb's graveyard instead of deleting it;
// Sweep here is the second half — it walks the graveyard and reclaims
// an entry's chunks once they are provably unreferenced and the entry
// is past DESIGN.md §19.2's grace period, then compacts the containers
// those chunks left half-empty (§19.1 step 3) so the reclaim actually
// reaches backend storage rather than stopping at the locator index.
//
// One simplification from the full design, stated plainly:
//
//   - §19.1 gates sweeping on a *container's* seal time ("its container
//     was sealed before V − T_grace"), which lets one timestamp cover
//     every chunk in that container. This build gates on the
//     *graveyard entry's* delete time instead — simpler, and correct
//     for the same reason: a chunk that survives the mark phase (still
//     reachable from the live tree or a not-yet-expired graveyard
//     entry) is never swept regardless of which timestamp gated the
//     scan.
//
//   - §19.3's nlink half *is* implemented: an inode with more than one
//     name only enters the graveyard when its last name goes (see
//     metadb's dropLinkTx), so unlinking one of two hard links leaves
//     the surviving name's chunks reachable by the mark phase.
//
//   - §19.3's "open-but-unlinked" guarantee is implemented for the
//     in-process mount: Repo.OpenHandles pins an inode for as long as a
//     descriptor is open on it, the mark phase treats a pinned inode as
//     reachable, and the sweep phase skips it — so unlinking a file a
//     client still has open and then sweeping past grace no longer pulls
//     the content out from under that client. Grace alone cannot cover
//     this: GC-1 sizes T_grace against T_write_max, and a descriptor may
//     stay open arbitrarily longer than that.
//
//     That per-process registry is sufficient here rather than a
//     simplification, because metadb's bbolt file takes an exclusive
//     inter-process lock: a second process cannot open the repo while a
//     mount holds it, so any Sweep necessarily runs inside the mount's
//     own process, where the registry lives. (Verified, not assumed —
//     a second opener blocks and times out.)
//
//     What remains is the authority-backed mount, where handles live in
//     a different process from the metadata: §19.3 specifies *leased*
//     open handles precisely so a holder that dies cannot pin an inode
//     forever. That is not built, and there is nothing for it to protect
//     yet either — pkg/mds exposes no sweep, so GC is not reachable
//     against an authority-served repo at all.
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
	"github.com/aburan28/atlasfs/pkg/pack"
	"github.com/aburan28/atlasfs/pkg/store"
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

	// One snapshot for the whole run, taken before the mark phase: the
	// mark and sweep halves must agree about which inodes are pinned, or
	// an inode could be marked live and then have its record deleted.
	held := r.OpenHandles.Snapshot()

	live, err := r.markLive(ctx, cutoff, held)
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
		if _, pinned := held[e.InodeID]; pinned {
			// §19.3: someone still holds this open. Leave the entry in
			// the graveyard so a later sweep collects it once the last
			// descriptor closes — grace has already elapsed, so the very
			// next sweep after the close will take it.
			continue
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

	// Deleting locators frees nothing on its own: the bytes are still
	// sitting inside sealed container objects. Compact is what turns a
	// sweep into an actual reclaim.
	if _, err := r.Compact(ctx, DefaultLivenessThreshold); err != nil {
		return len(removed), fmt.Errorf("repo: sweep: compact phase: %w", err)
	}
	if err := r.reapDeadContainers(ctx, cutoff); err != nil {
		return len(removed), fmt.Errorf("repo: sweep: reap phase: %w", err)
	}
	return len(removed), nil
}

// DefaultLivenessThreshold is DESIGN.md §19.1 step 3's stated default:
// containers below 50% live bytes are repacked.
const DefaultLivenessThreshold = 0.5

// Compact implements DESIGN.md §19.1 step 3. A container whose live
// fraction has fallen below threshold is rewritten to hold only the
// chunks still referenced; the chunks' locators are repointed at the new
// container and the old one is retired for later deletion. Only locators
// change (§5.4) — no manifest, inode, or chunk ID is touched, which is
// the entire reason identity and location are separate in the first
// place.
//
// Returns the number of containers rewritten. A container with no live
// chunks left is retired without writing a replacement.
//
// Ordering here is what makes it crash-safe, and it is deliberate:
// write the replacement first, then repoint locators, then retire the
// original. A crash between steps 1 and 2 leaks an unreferenced object
// (harmless; the next run rewrites it again). A crash between 2 and 3
// leaves the original retired-but-present, which is exactly the state
// the reap phase already handles. The one ordering that would lose data
// — retiring the original before its locators are repointed — never
// happens.
func (r *Repo) Compact(ctx context.Context, threshold float64) (rewritten int, err error) {
	locs, err := r.DB.ListLocators(r.Region)
	if err != nil {
		return 0, err
	}

	// Group surviving chunks by the container they live in, and total up
	// how many bytes of each container are still referenced.
	type containerState struct {
		entries   []metadb.LocatorEntry
		liveBytes int64
		extent    int64 // highest offset+length seen: the container's size floor
	}
	byContainer := map[string]*containerState{}
	for _, e := range locs {
		cs := byContainer[e.Locator.Container]
		if cs == nil {
			cs = &containerState{}
			byContainer[e.Locator.Container] = cs
		}
		cs.entries = append(cs.entries, e)
		cs.liveBytes += e.Locator.Length
		if end := e.Locator.Offset + e.Locator.Length; end > cs.extent {
			cs.extent = end
		}
	}

	// Enumerate containers from the *backend*, not from byContainer.
	// Iterating the locator index alone would never see a container whose
	// chunks were all collected — it has no surviving locators, so it
	// appears nowhere in the index — and those fully-dead containers are
	// exactly the ones with the most bytes to reclaim.
	containers, err := r.listContainers(ctx)
	if err != nil {
		return 0, err
	}

	now := r.Clock.Now()
	pendingDead, err := r.deadContainerSet()
	if err != nil {
		return 0, err
	}
	for _, c := range containers {
		if c.Size <= 0 {
			continue
		}
		if _, alreadyDead := pendingDead[c.Key]; alreadyDead {
			continue // retired by an earlier run, awaiting reap
		}
		var live int64
		var entries []metadb.LocatorEntry
		if cs := byContainer[c.Key]; cs != nil {
			live, entries = cs.liveBytes, cs.entries
		}
		if float64(live)/float64(c.Size) >= threshold {
			continue // still dense enough to leave alone
		}

		// entries is empty for a fully-dead container, and rewriteContainer
		// is a no-op on it — nothing is written, the original is simply
		// retired.
		if err := r.rewriteContainer(ctx, entries); err != nil {
			return rewritten, err
		}
		if err := r.DB.MarkContainerDead(c.Key, now); err != nil {
			return rewritten, err
		}
		rewritten++
	}
	return rewritten, nil
}

// listContainers returns every container object in this region. The
// prefix keeps it to containers: manifests live under atlas/m/ (see
// manifestKey) and must never be considered for compaction.
func (r *Repo) listContainers(ctx context.Context) ([]store.ObjectInfo, error) {
	prefix := fmt.Sprintf("atlas/c/%s/", r.Region)
	var out []store.ObjectInfo
	cursor := ""
	for {
		page, err := r.Backend.List(ctx, prefix, cursor, 0)
		if err != nil {
			return nil, fmt.Errorf("repo: compact: list containers: %w", err)
		}
		out = append(out, page.Keys...)
		if page.NextCursor == "" {
			return out, nil
		}
		cursor = page.NextCursor
	}
}

func (r *Repo) deadContainerSet() (map[string]struct{}, error) {
	dead, err := r.DB.ListDeadContainers()
	if err != nil {
		return nil, err
	}
	set := make(map[string]struct{}, len(dead))
	for _, d := range dead {
		set[d.Key] = struct{}{}
	}
	return set, nil
}

// rewriteContainer packs entries into a fresh container and repoints
// each chunk's locator at it. Chunks are refetched through pack.Fetch,
// so every byte moved is hash-verified on the way out (DESIGN.md §24.4)
// — compaction is exactly the moment a silent corruption would otherwise
// be laundered into a new container and forgotten.
func (r *Repo) rewriteContainer(ctx context.Context, entries []metadb.LocatorEntry) error {
	if len(entries) == 0 {
		return nil
	}
	packer := pack.NewPacker(r.Backend, r.Region, 0)
	for _, e := range entries {
		data, err := pack.Fetch(ctx, r.Backend, e.ChunkID, e.Locator)
		if err != nil {
			return fmt.Errorf("repo: compact: refetch chunk %s: %w", e.ChunkID, err)
		}
		packer.Add(chunk.Chunk{ID: e.ChunkID, Data: data})
	}
	newLocs, err := packer.Seal(ctx)
	if err != nil {
		return fmt.Errorf("repo: compact: seal replacement container: %w", err)
	}
	for id, loc := range newLocs {
		if err := r.DB.PutLocator(r.Region, id, loc); err != nil {
			return err
		}
	}
	return nil
}

// reapDeadContainers deletes the backend objects that compaction retired,
// once they are past the same grace period the graveyard uses. This is
// the step that actually shrinks storage.
//
// Deletes are issued in batches of the backend's advertised BatchDelete
// size (DESIGN.md §24.3: a backend without batch delete degrades to
// slower GC, never to incorrect GC), and the bookkeeping entry for a
// container is only dropped once that container's own delete reported
// success — a failed delete stays on the list and is retried next run
// rather than being forgotten with the object still in the bucket.
func (r *Repo) reapDeadContainers(ctx context.Context, cutoff time.Time) error {
	dead, err := r.DB.ListDeadContainers()
	if err != nil {
		return err
	}

	var keys []string
	for _, d := range dead {
		if d.RetiredAt.After(cutoff) {
			continue // still within grace: a reader may hold the old locator
		}
		keys = append(keys, d.Key)
	}
	if len(keys) == 0 {
		return nil
	}

	batch := r.Backend.Caps().BatchDelete
	if batch <= 0 {
		batch = 1
	}
	for start := 0; start < len(keys); start += batch {
		end := min(start+batch, len(keys))
		results, err := r.Backend.Delete(ctx, keys[start:end])
		if err != nil {
			return fmt.Errorf("repo: reap containers: %w", err)
		}
		for _, res := range results {
			// An object already absent is the success case, not a
			// failure: a previous run may have deleted it and crashed
			// before clearing the bookkeeping entry.
			if res.Err != nil && !errors.Is(res.Err, store.ErrNotFound) {
				return fmt.Errorf("repo: reap container %s: %w", res.Key, res.Err)
			}
			if err := r.DB.RemoveDeadContainer(res.Key); err != nil {
				return err
			}
		}
	}
	return nil
}

// markLive is DESIGN.md §19.1 step 1: the set of chunk IDs reachable at
// "now" — from every inode still bound in the live namespace, from
// every graveyard entry not yet past grace (cutoff) or still pinned by
// an open handle (§19.3), and from the packer's not-yet-sealed buffer.
func (r *Repo) markLive(ctx context.Context, cutoff time.Time, held map[metadb.InodeID]struct{}) (map[chunk.ID]struct{}, error) {
	live := map[chunk.ID]struct{}{}

	if err := r.markLiveTree(ctx, metadb.RootInode, live); err != nil {
		return nil, err
	}

	entries, err := r.DB.ListGraveyard()
	if err != nil {
		return nil, err
	}
	for _, e := range entries {
		_, pinned := held[e.InodeID]
		if !e.DeletedAt.After(cutoff) && !pinned {
			continue // past grace and unpinned: exactly what Sweep may collect
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
