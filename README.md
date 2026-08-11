# atlasfs

A POSIX filesystem whose namespace spans regions and cloud providers, built on three claims:

1. **Content-addressed chunk identity is the unit of placement and replication.** Replicating a dataset across clouds is a set difference over hashes; verifying a replica is a hash check.
2. **Metadata authority is regional and explicit.** Every mutable subtree has a home region that owns its metadata. Operations spanning home regions return `EXDEV` rather than silently paying a cross-continent round trip.
3. **Consistency is a per-subtree property.** A published dataset and a shared scratch directory should not pay the same coordination cost.

Consumed through a **Kubernetes CSI driver** (PersistentVolumes and Claims, snapshots, quota-as-capacity) or an **NFSv4.1 export** for clients that cannot run FUSE. Storage backends are **pluggable** — S3, GCS, Azure Blob, S3-compatible, and POSIX/NFS.

- [`DESIGN.md`](DESIGN.md) — the design.
- [`docs/review-response.md`](docs/review-response.md) — how the current draft answers the Draft 1 review.

## Status

Draft 3. Estimated at 4–6 engineer-years to reach the verification bar in DESIGN.md §27.

Phase 1 — immutable, content-addressed dataset and checkpoint storage with atomic publish, an S3 backend, and CSI read-only PVCs — is ~0.75 engineer-years and is where the value-to-effort ratio is best. See §28.

**Phases 1–3 are substantially built, and the metadata authority is now its own process.** Real reads and writes through real protocols end to end for `immutable`, `relaxed`, and `session`; pluggable backends; quotas; GC with real storage reclamation; a cost model; a formally-checked rehoming design; and — via `cmd/atlas-mds` — remote lease holders with `posix`-class blocking recall between separate processes. Phase 4 (multi-region, needs a live FoundationDB cluster) and Phase 6 (NFS, needs `nfs-ganesha`) remain genuinely out of reach here, and of §27's three conformance suites all three have now been run at least in part (pjdfstest in full, `fsx` and xfstests partially) — see "Not yet built" below. What's built and verified, not stubbed:

- Content-addressed chunking, the manifest format with single-chunk inlining, chunk/container/locator separation, small-file packing with whole-file dedup, and symlinks (§5, §14, §15).
- A `Backend` interface (§24.1) with **two working implementations**: local disk, and **S3** over the real `aws-sdk-go-v2` client — verified against an in-process fake S3 server speaking real HTTP/XML, including a genuine capability probe for conditional-PUT support (§24.2) rather than an assumption.
- A real **FUSE mount** (§21) — read-only for `immutable` (kernel-level reads verified byte-for-byte, writes rejected as `EROFS`); read-write for `relaxed`/`session` (create/write/overwrite/unlink/mkdir/rmdir/truncate/rename/symlink/hard-link, all verified through a real mount, including via an actual shell — `echo text > file`, `mv`, `ln -s`, and `ln` on the mount all work).
- A **coherence.Manager** (§10) implementing the protocol modeled in `spec/coherence.qnt` for real: per-holder leases on a monotonic clock, directory-version-gated negative caching, and a best-effort/bounded push-invalidation registry.
- A **client/server split** (`pkg/mds`, `cmd/atlas-mds`) — the metadata authority now runs as its own process, serving leases to named remote holders over real gRPC, with server-streamed push invalidation. This is what made the rest of §10 testable rather than merely written: a push that can actually be *lost*, and a holder that can actually be *recalled*. Metadata-only by design, not omission — §11 has clients read chunk bytes from object storage directly, so routing data through the authority would be a different and worse architecture.
- **`posix`-class blocking recall** (§10.6) — a mutation does not commit until every other lease holder has dropped its copy and acknowledged, or blown the `DRECALL` deadline. Verified between separate OS processes over a real socket, both directions: a holder acking in 500 ms released the commit in 500 ms (`acked=[reader2]`); a holder that never acked let the commit proceed at the 2 s deadline and be reported as `timedOut=[reader]`. Two properties carry the safety: the lease is revoked when the recall is *issued*, not when the ack arrives (so a silent holder can't keep serving from a grant the authority still honors), and a timeout is not an error (§10.6 bounds the wait so one partitioned client can't stall every writer). Confirmed load-bearing by disabling recall and watching all three tests fail.
- A **read-write FUSE mount backed by the remote authority** (`pkg/mdsfuse`, `atlas mount -mds=addr`) — the mountpoint is itself a lease holder: the kernel's lookups become RPCs, the authority's pushes invalidate this client's cache, and a recall against the mount is a recall against a genuinely separate process. Writes follow §16.1's upload-then-commit — the client chunks, dedups against the authority, uploads sealed containers **straight to object storage**, registers the locators, then commits the inode. Data never crosses the authority (§11), which is what keeps its cost independent of file size.
- **The full §10 loop, end to end through real mountpoints.** Two independent mounts against one authority, each its own holder with its own cache: a write on A becomes visible on B via push invalidation, an unlink on A disappears from B, and under `posix` a write on A **blocks until B's copy is recalled** (committed in 7 ms once B acked). Verified in-process and again across separate OS processes at the shell — A writes, B reads it, B writes back, A sees it. All three coherence tests were confirmed load-bearing by disabling push and watching them fail.
- **Kubernetes volumes backed by the authority** — a PV whose VolumeContext carries `mdsAddr` mounts through `pkg/mdsfuse` instead of opening metadata on the node, which is what makes a PVC a *shared, coherent* namespace rather than one node's private view: each pod mounting it is its own lease holder. Verified through a real gRPC CSI client, with the load-bearing check being that stopping the authority stops the volume resolving.
- **Rename and symlink creation on both mounts** (§16.1) — `mv` and `ln -s` work at the shell against the in-process mount and against the authority-backed one. Rename is one metadb transaction: it keeps the file's inode (so a cached lease on it stays valid and no content is re-uploaded), moves a whole subtree by rebinding a single dentry, displaces an existing target into the graveyard and releases its quota, and refuses the cases POSIX refuses — a directory into its own subtree, a type mismatch, a non-empty target. Two things the tests confirmed are load-bearing by breaking them: bumping *both* directories' versions (without the destination bump, a second mount keeps serving its cached miss for the new name), and re-reading the dentry at commit time (a file renamed while open for write otherwise resurrects its pre-rename name on flush).
- **xfstests runs against this filesystem** — it has first-class FUSE support, so `FSTYP=fuse` plus a `mount.fuse.atlasfs` helper is a supported configuration rather than a hack. It needed `atlas mount -fsname` (mount(8) identifies a mount by the device string it was given) and one line in xfstests' own `fuse.<fs>` → `<fs>` type mapping, both disclosed in [docs/conformance.md](docs/conformance.md). `generic/002` immediately found a coherence bug DESIGN.md had already specified: §10.5 puts `nlink` in the *inode's* lease domain, and `Unlink` bumped only the directory. Only a handful of tests plus a partial `quick` group have been run — that is **not** §27's "generic groups applicable to a network filesystem", and the document says so.
- **`fsx`: 170,000 operations with mmap, all clean** — §27 names it alongside pjdfstest and predicted it would be the suite to find real problems, because it hammers overlapping partial writes and truncates against a shadow copy and compares byte for byte. It failed on its *third* operation the first time: a write past EOF left `stat` reporting the pre-write size, since content commits on flush and nothing consulted the open write handle for the in-flight length. Fixed, it now completes 50,000 operations against the authority-backed mount, three concurrent 30,000-operation runs against the same mount, and 30,000 against the in-process mount — all with mmap reads and writes enabled. This is **not** §27's bar, which asks for a 24-hour soak; these runs take minutes, and [docs/conformance.md](docs/conformance.md) says so.
- **pjdfstest: 8798 / 8798 — a clean sweep**, run against a `posix`-class mount backed by a separate authority process, which is §27's actual gating configuration. `prove -r` reports `Files=238, Tests=8798, Result: PASS` with no failures and no enumerated exceptions left. It is the first time any of §27's conformance suites has been run here, and it found eleven real defects that this project's own tests did not, including two that are worse than conformance failures: `truncate` had no upper bound, so an unprivileged `truncate(f, 1<<62)` sized an in-memory buffer from it and killed the mount, and there was no ownership model at all, so a world-unreadable file was readable by anyone who could see the mount.  The last two failures were open-but-unlinked files (§19.3), and closing them turned up three defects rather than the one the exception described — including a flush that resurrected a file the user had deleted, and a GC that could reclaim a file still open. Setup and per-step scores are in [docs/conformance.md](docs/conformance.md).
- **A `tar` round trip through both mounts** — extract a tree containing nested directories, a multi-chunk file, an executable, a symlink and a hard link into the mountpoint, `diff -r` it against the source, then re-archive it from the mount and check the listing. Every one of those operations has its own unit test; what this adds is that they compose under a real program doing them in tar's order rather than the tests'. It is the closest thing here to a conformance run — §27's actual suites have still not been run.
- **Permissions that survive a round trip** — `atlas publish` stored a hardcoded `0644` for every file, so publishing a tree of scripts or binaries and mounting it gave back files you could not run. The source's own bits are now preserved, `open(2)`'s and `mkdir(2)`'s mode arguments are honoured instead of discarded, `chmod` through either mount persists (and reaches other holders through the same invalidation path a write takes), and an overwrite no longer strips the bits back off — a commit carries content, not permissions, and taking its mode verbatim is how "chmod +x then edit" silently breaks. A read-only mount clears the write bits but deliberately keeps the exec bit, since running a read-only published tree is the point. **Ownership (§20) is implemented too**: an inode belongs to whoever created it, `chown` persists on both mounts, publish preserves the source tree's ownership, and enforcement is the kernel's via `default_permissions` — so a world-unreadable file is genuinely unreadable rather than merely labelled that way. `chown` also clears set-user-ID, since a setuid bit surviving an ownership change is a privilege-escalation primitive.
- **`fsync(2)` that actually commits** (§16.2) — both mounts buffer a file and commit on close, so before this an fsync returned success while the content still sat in a handle waiting for a `close(2)` that a dying process would never make. `fsync(fd)` now means what §16.2 says it means: durable in the home region — chunks in the object store, manifest committed in metadata — and it never waits on cross-region replication, which §16.2 reserves for an explicit publish. Checked from a *second* mount, since reading back through the same one could be served from the open handle's own buffer and would pass either way.
- **`statfs(2)`, so `df` works** — an object backend has no capacity to report (S3 has none; a local backend's free space is the host's, not the subtree's), so the mount reports the §18.3 quota instead, on both the in-process and the authority-backed mount. With no quota set it advertises a placeholder capacity rather than zero: reporting zero free makes every tool that pre-checks space — tar, rsync, package managers — refuse to write. That placeholder is labelled as one in the code rather than dressed up as a measurement.
- **Hard links with transactional `nlink`** (§19.3) — `ln` works on both mounts, and `stat` reports the real link count rather than a constant. Removing one of two names decrements `nlink` instead of graving the inode; only the last name going graves it and returns its quota. That ordering is the whole point: an early grave would let GC reclaim chunks the surviving name still reads, which `TestSweepPreservesAHardLinkedFile` checks directly by sweeping past the grace period and then reading the survivor back. Overwriting one name of a hard-linked file also preserves the count — a commit carries content, not link structure, and taking its `NLink` verbatim would silently reset it to 1.
- **Client-side negative caching under the directory's lease** (§10.4) — the authority was already granting negative entries and no client consumed them, so every miss round-tripped. `mds.Client` now holds them under the directory's own lease, which is what makes the rename test above able to fail. The cache also carries an **in-flight invalidation guard**: a response that was already on the wire when a push landed is dropped rather than stored, since its lease was granted against a view that no longer exists.
- **Client-side dentry caching under the directory's lease** (§10.1/§10.5) — found because the coherence test above *passed with push disabled*: `Lookup` always round-tripped, so the kernel's LOOKUP-before-everything meant the lease cache was nearly unreachable through a filesystem. Dentries now live under the directory's own lease domain, and a directory bump drops every binding cached under it in one step.
- **Kernel-level attr/dentry caching bounded by the class's own `D`** — the kernel is a cache holder like any other and now runs under the same bound. `posix` gets a TTL of zero, because the kernel's cache cannot participate in a recall.
- A **mutable write path** in `pkg/repo` (§16.1): buffer-then-commit-on-close, overwrite reuses the existing inode in place (so a cached lease on it is something real to invalidate), every mutation bumps the affected directory's coherence version.
- **Quotas** (§18.3): a bbolt-backed byte/inode counter charged in the same transaction as the inode/dentry write it guards, so a rejection aborts cleanly — no orphan inode, no dangling chunk locator. `Repo.SetQuota`/`QuotaUsage`, `ErrQuotaExceeded` mapped to `EDQUOT` in the FUSE layer, verified through a real mount (`echo` past the byte limit fails with `EDQUOT` at the shell).
- **Garbage collection** (§19): a real two-pass mark-and-sweep against metadb as ground truth. `Unlink`/`Rmdir` move an inode into a graveyard bucket instead of deleting it; `Repo.Sweep` marks every chunk reachable from the live namespace or a not-yet-expired graveyard entry, then reclaims a past-grace entry's chunks that mark didn't reach — verified to correctly *not* collect a chunk two files still share via dedup when only one is unlinked. Invariant GC-1 (`T_grace > T_write_max + D_max + ε`, §19.2) is a checked precondition, not a comment: `Sweep` refuses to run on a `graceDuration` that violates it.
- **Container compaction** (§19.1 step 3) — the step that makes GC actually free storage. A container below 50% liveness is rewritten with only its live chunks and its locators repointed (§5.4: only locators change); the original is retired and deleted after the same grace period, so a reader mid-`Get` on an old locator isn't cut off. Measured: 2 MiB → 256 KiB, **88% of backend bytes reclaimed**, with the survivor still reading back byte-for-byte.
- **Orphan collection** — the phase that was missing, and the reason §27's fsx soak filled a volume with 21 GB for a 256 KB file. `Sweep` only ever walked the graveyard, and an overwrite creates no graveyard entry: the inode is repointed at new content and the old chunks are left referenced by nothing, invisible to both halves of mark-and-sweep. **25 overwrites of one file left 25 live locators and a full GC pass freed none of them.** `sweepOrphans` now collects every chunk the mark phase cannot reach, gated on its container's seal time — §19.1's own gate, and the thing that keeps an in-flight write from looking like garbage, since §16.1 registers a locator before committing the inode that references it. Confirmed load-bearing in both directions: the leak tests failed before the phase existed, and removing the grace gate makes the concurrent-writer test fail.
- **Open-but-unlinked files** (§19.3) — an inode stays reachable while any descriptor is open on it. Grace does not cover this: GC-1 sizes `T_grace` against `T_write_max`, the age of an uncommitted *write session*, while a descriptor may stay open as long as its process lives, so a long-lived reader plus a routine sweep used to delete the locators and the inode record out from under it. `Repo.OpenHandles` pins refcounted (several descriptors may share an inode) and releases idempotently (FUSE resends interrupted requests). Per-process is sufficient rather than a simplification, because bbolt's exclusive inter-process lock means any `Sweep` necessarily runs inside the mount's own process — verified, not assumed. §19.3's *leased* handles remain for the authority-backed mount, which has no sweep to protect against yet.
- **`chunk_size` as a real per-subtree policy** (§14.3) — persisted at creation like the consistency class, and fixed for the repo's lifetime for a sharper reason: re-chunking the same bytes at a different size changes every chunk ID, so a repo that could change it would silently dedup against nothing. It matters because a commit re-chunks the whole file, making the chunk the unit of write amplification: at the 4 MiB default, every rewrite of a smaller file stores a complete new copy. Measured over 200 rewrites of a 256 KiB file changing 4 KiB each — **50.0 MiB stored at the default, 13.3 MiB at 64 KiB chunks, 4.2 MiB at 16 KiB**. Reachable as `atlas -chunk-size` and the CSI `chunkSize` key.
- **Background GC from inside the mount** (`atlas mount -gc-interval`) — without it a long-running mount reclaims *nothing*, and not for want of trying: metadb's bbolt file takes an exclusive inter-process lock, so `atlas gc` in another shell blocks for as long as the mount is up (verified — it hangs until killed). Sweeping in-process is also what makes it correct rather than merely possible, because `Repo.OpenHandles` lives there and an out-of-process sweeper could not see which files are still open. Verified under load: 15,000 fsx operations all A-OK with a sweep firing every 20 s against the same live repo, and a `-gc-grace` under GC-1's floor is reported on every tick instead of silently doing nothing.
- Still missing from GC: client-side write-session-age `ESTALE` enforcement (the other half of GC-1).
- **Benchmarks and CI** — the first measurements of the paths §21.1 sets targets for, plus a GitHub Actions workflow running build/vet/gofmt/test/`-race` that fails hard if `/dev/fuse` is missing rather than letting the mount tests silently skip. See "Measured performance" below.
- A **caller-supplied-destination read path** (`store.GetterInto`, `store.GetInto`, `pack.FetchInto`) — the read path no longer allocates its own buffers on the hot path, and a chunk-aligned read lands directly in the caller's memory. Local disk implements it with `pread(2)`; every other backend gets the streaming fallback, which still beats `io.ReadAll`'s doubling. Measured on a 64 MiB sequential read: transient allocation **405 MB → 67.9 MB** (which is the caller's own destination buffer and essentially nothing else) and throughput **~555 → ~1198 MB/s**. This is also the prerequisite for GPUDirect Storage — see "GPUDirect / cuFile status" below for exactly how far it goes and what it does not yet cover.
- A **CSI node plugin** (§22.1's static case) — real gRPC `Identity`/`Node` services, driven end to end by a real gRPC client through `NodePublishVolume`/`NodeUnpublishVolume`, mounting and reading a repo exactly as kubelet would for a statically-provisioned PV. Read-only by default; a **writable RWX path exists for the §22.2 `disjoint-writers` carve-out** — a `relaxed`-class subtree opted in via the `rwxContract: disjoint-writers` VolumeContext key is mounted read-write instead of rejected, everything else non-read-only still rejected exactly as before. Verified against a real gRPC client, including a genuine `flock(2)` investigation (`TestRWXDisjointWritersFlockIsLocalOnly`) that honestly documents the gap: this build gives real local mutual exclusion but does not forward `flock`/`fcntl` across nodes the way `lock=error` in §17/§22.2 ultimately specifies. There is still no Controller service and no dynamic provisioning (§22, later phase).
- Two more **`Backend`** implementations alongside S3: **GCS** over the real `cloud.google.com/go/storage` client, and **Azure Blob** over `azure-sdk-for-go/sdk/storage/azblob` — each verified against a hand-written fake server speaking that provider's real wire protocol (JSON multipart insert + `alt=media` ranged GET for GCS; single-shot upload + `If-None-Match` + `x-ms-range` for Azure), plus a full publish-then-read integration test.
- A **cost model** (§12) — `pkg/costmodel` computes egress/request cost estimates and budget admission; its tests reproduce §12's own worked numbers exactly ($4,500 egress at $0.09/GB for 50TB; $1,717/hr for 1.19M cold GET/s; $86/hr for 60k GET/s at 95% hit rate).
- The **libatlas escape hatch** (§21.3) — `pkg/libatlas` materializes a file to a local, LRU-evicted cache directory for mmap-heavy workloads that can't go through FUSE directly, with coherence-aware invalidation when the underlying content changes.
- A **TLA+ model of rehoming** (§7.4/§27) — `spec/rehoming.tla`, checked exhaustively (not simulated) with TLC against `NoDualAcceptance` and `TypeOK`: 249 states generated, 144 distinct, no violation, to a search depth of 15. See [`spec/rehoming-README.md`](spec/rehoming-README.md).
- A **Quint model** of the §10 metadata cache coherence protocol (Phase 0 per §10.8) — see [`spec/README.md`](spec/README.md). It's been run against the real `quint` checker, and the first draft's invariant was too strong and caught a real (non-)issue, fixed by narrowing it to what §10 actually promises.

Manual testing on this pass found and fixed two real bugs no unit test happened to catch on its own: a standalone `Commit` left its chunk unsealed in the packer's buffer (fixed by flushing per-commit), and `Flush` firing more than once in a single open-for-write session (observed: once right after a truncate, again at close) let a one-shot "committed" latch silently drop everything written after the first flush. Both now have regression tests.

```
go build -o atlas ./cmd/atlas -o atlas-csi ./cmd/atlas-csi

./atlas publish  [flags] <repo-dir> <src-dir> [dest-path]   # ingest a directory tree
./atlas ls       [flags] <repo-dir> [path]                  # list a directory
./atlas cat      [flags] <repo-dir> <path>                  # print a file's content
./atlas stat     [flags] <repo-dir> <path>                  # print an inode's metadata
./atlas mount    [flags] <repo-dir> <mountpoint>             # FUSE mount (Ctrl-C to unmount)

# every subcommand takes -backend=local|s3|gcs|azure (§24); cloud creds via each SDK's normal chain
./atlas publish -backend=s3 -s3-bucket=my-bucket ./repo ./my-dataset /datasets/demo

# -class=immutable|relaxed|session on a brand-new repo (§8); ignored on reopen — the
# persisted class always wins, so this only matters the first time a repo-dir is used
./atlas mount -class=relaxed ./repo ./mnt   # now a real read-write mount

./atlas gc      [flags] <repo-dir>    # mark-and-sweep + compact (§19); -grace, -dry-run
./atlas quota   [flags] <repo-dir>    # show or set the quota (§18.3); -bytes, -inodes

./atlas-csi -csi-endpoint=unix:///var/lib/kubelet/plugins/csi.atlas.io/csi.sock

# the metadata authority (§7/§10) — clients hold leases against it remotely,
# and -class=posix turns on §10.6's blocking recall
./atlas-mds -repo=./repo -class=posix -listen=127.0.0.1:9713 -recall-deadline=2s

# mount against that authority instead of opening metadata locally: metadata
# crosses gRPC, bytes go straight to/from the object backend. Read-write by
# default; -mds-read-only for an immutable subtree. Two of these against one
# authority are two independent lease holders.
./atlas mount -mds=127.0.0.1:9713 ./repo ./mnt
```

`go test ./...` covers chunk/manifest round-trips, packing and locator resolution with corruption detection, dedup, metadb dentry/inode semantics, the coherence manager against all four properties the Quint model checks, the mutable write path (create/overwrite/unlink/mkdir/rmdir/rename/symlink/hard-link, including the sentinel-errno mapping), the S3 backend against a real fake-S3 server, the CSI driver against a real gRPC client, and full end-to-end FUSE tests — read-only and read-write — that mount over a real kernel connection and exercise it exactly as a real client would. All of `pkg/fuseserver`, `pkg/coherence`, and `pkg/repo` are additionally clean under `-race`.

## Measured performance

DESIGN.md §21.1 states throughput targets. Nothing had ever measured them, so here is what this build actually does — on one shared cloud vCPU set (Xeon @ 2.10GHz, 4 cores), local-disk backend, `go test -bench`.

**Read these as medians of 5–6 runs with the full range shown, not as single figures.** Run-to-run spread on a shared vCPU is up to 2×, so any one number is nearly meaningless; quoting the best run would be flattering and wrong.

| Path | Median (range) | §21.1 target |
|---|---|---|
| FUSE sequential read, 64 MiB | 897 MB/s (597–1147) | ≥ 5 GB/s (cache-hit) |
| FUSE read, 64-way concurrent | 426 MB/s (374–717) | ≥ 2 GB/s (cold, 64-way) |
| FUSE `stat`, kernel-cached | 800 ns (712–826), 2 allocs | — |
| Library read, warm chunk cache | 4.05 GB/s (3.41–4.20) | — |
| Library read, cold (local disk) | 1198 MB/s (978–1382) | — |
| Publish (chunk + hash + pack) | 251 MB/s (153–312) | — |

The cold library read row moved after the caller-supplied-destination work: it was 555 MB/s (482–614) allocating 405 MB per 64 MiB read, and is now ~1198 MB/s allocating 67.9 MB — the latter being the caller's own destination buffer and little else. The remaining ~11,900 allocations per run are in the metadata lookup (one bbolt transaction and gob decode per chunk locator), not the data path.

**The FUSE rows did not move, and that is expected rather than disappointing.** The zero-copy path fires only when a read is chunk-aligned and the caller's buffer can take a whole chunk; the kernel issues FUSE reads in ~128 KiB units against 4 MiB chunks, so it never qualifies and those reads still go through the cached-chunk path. FUSE keeps the smaller win (one exact-size allocation per chunk instead of `io.ReadAll` growing one by doubling). Spot checks before and after are within noise of each other in both directions, so this is "no change", not "an improvement too small to see". Making the mount benefit would mean raising the read size the kernel issues — `MaxReadAhead` and friends, which is squarely the §21.2 tuning work still outstanding.

The FUSE numbers are **~5× short on sequential and ~5× short on concurrent**, and that gap is the honest size of the remaining §21.2 work: FUSE_PASSTHROUGH, writeback caching, large read sizes, and multi-queue are all listed there as the mechanisms the targets assume, and none of them is implemented. The targets are for a tuned client; this is the naive one.

Adding the kernel attr/entry timeouts (above) was worth **~170×** on the metadata path — `stat` went from ~130 µs and 269 allocations to ~800 ns and 2, because before it there were no timeouts set at all and every `stat` round-tripped to userspace. That one is a ratio between two configurations measured the same way, so the shared-vCPU noise largely cancels.

## GPUDirect / cuFile status

Full plan, including the Dragonfly P2P half: [`docs/gds-p2p-integration.md`](docs/gds-p2p-integration.md).

Two things landed since the seam below was written, both grounded in NVIDIA's published API:

- **4 KiB container alignment** (`pack.GDSAlignment`, `-gds-align`). NVIDIA defines an I/O as unaligned if the file offset, size, device pointer, *or* device-buffer offset is not 4 KiB aligned, and serves unaligned I/O through GPU bounce buffers. §5.5 packs chunks back to back, so **every GDS read of a normally-packed AtlasFS chunk would be unaligned by construction** — the feature would degrade to a slower POSIX read. Alignment is a precondition, not a tuning knob. It is opt-in because it costs: measured **3.98× container inflation** for 1 KiB files, which is exactly what §14's small-file packing exists to avoid.
- **`store.Dest` and `pkg/gds`** — a destination spanning host and device memory (mirroring `cuFileRead`'s base+offset shape), plus a cgo binding behind `-tags cufile` with a stub that **fails closed** rather than silently falling back to a host read. The cgo file has never been compiled — there is no CUDA here — and says so in its package doc.

## Dragonfly P2P (DESIGN.md §11.1)

Container reads can route through a [Dragonfly](https://d7y.io) peer, making §11.1's P2P chunk exchange a deployment option without AtlasFS implementing a peer protocol. This is a **cost** feature: §12.2 calls request charges the binding constraint ($1,717/hour for its worked cold-read fleet), and P2P collapses origin GETs from once-per-node to once-per-cluster. Safe with no invalidation protocol because containers are immutable and content-addressed.

The whole integration is an `*http.Client` handed to the S3 backend — `pkg/store/s3` is untouched and unaware. Enable with `-dragonfly-proxy=auto`, or `dragonflyProxy` in a CSI VolumeContext.

Verified against a real forwarding proxy, checking the *path* rather than the payload (if the wiring were wrong the bytes would still arrive, straight from origin): requests must land as absolute URIs, `Range` must survive intact, and a full S3 publish/read cycle put 5 requests through the proxy with the capability probe agreeing with the direct path.

## Go vs. Rust, measured

A filesystem in Go invites the question, so here are the three usual arguments with numbers from this codebase rather than from general principle:

| Claim | Measured here |
|---|---|
| "GC costs throughput" | **No.** `GOGC=off` was *slower* (1316–1568 MB/s) than default GC (1639–1791 MB/s) on the 64 MiB read — an unbounded heap hurts locality more than collection costs. |
| "GC pauses wreck tail latency" | STW pauses are **20–200 µs typical**, one 1.7 ms outlier. Against a 2–4 ms local chunk read, or 10–100 ms from S3, that is noise. Against a **718 ns cached `stat`** it is 140×, so it *does* matter for metadata-heavy tree walks (§18.1). |
| "cgo will strangle the cuFile path" | cgo call overhead is **56 ns** (vs 0.16 ns for a Go call). A 4 MiB GDS read at 10 GB/s is ~400 µs, so cgo is **0.014%** of it. Quantitatively irrelevant. |

**Recommendation: no rewrite.** The decisive argument isn't the table above, it's this: the FUSE path is ~5× off its §21.1 target, and that gap is caused by unimplemented §21.2 mechanisms — FUSE_PASSTHROUGH, writeback caching, large reads, multi-queue. Rewriting in Rust without implementing those reproduces the same 5× gap in a different language. **The measured bottleneck is architectural, not linguistic.** Against that, a rewrite costs 6–12 months to return to parity — working FUSE, gRPC, CSI, a metadata service, formal specs wired to the implementation — and buys no capability.

Where Rust would genuinely earn its place is narrower and worth naming: a metadata path under heavy tree-walk load, where 100 µs pauses are large next to sub-microsecond operations. That is a component-sized argument, not a rewrite-sized one, and it can be taken later without disturbing the control plane — which is an argument for *not* pre-committing now.

## GPUDirect / cuFile: the seam

AtlasFS targets checkpoint and dataset I/O (§2), so NVIDIA GPUDirect Storage — DMA from storage straight into GPU memory, no CPU bounce buffer — is a natural fit. **The seam is built; the cuFile binding is not.** Being precise about the line:

**Done.** The read path from `repo.FileReader.ReadAt` through `pack.FetchInto` down to `store.GetInto` no longer allocates buffers of its own, and the destination belongs to the caller. That was the blocking structural problem: an API whose only shape is "here is a stream, you find somewhere to put it" cannot express a DMA target at all.

**Not done, and why.** `GetterInto`'s destination is a `[]byte` — host memory. Real cuFile reads into a `CUdeviceptr` registered via `cuFileBufRegister`, which no `[]byte` can name. Closing that gap needs a destination type spanning host and device memory, and it should be designed against a real cuFile binding rather than guessed at, because the registration and alignment rules (`cuFileHandleRegister` on the fd, 4 KiB alignment on offsets and sizes, device-buffer lifetime) are what decide whether the API shape is right.

**Two things a GDS implementer must resolve**, both documented at the call sites rather than left to be discovered:

1. **Verification vs. DMA.** §24.4 makes reads self-verifying, and `pack.FetchInto` upholds that by hashing the destination after the read — which works only because host memory can be read back. DMA'ing into device memory and then hashing on the CPU would forfeit the entire benefit. The two honest options are verifying on the GPU or declaring verification waived for that path; the seam guarantees the choice is *visible* rather than accidental.
2. **Where it plugs in.** GDS needs a driver-supported filesystem, which for AtlasFS means the container objects — so a GDS backend belongs in `pkg/store`, not `pkg/fuseserver`.

This work was prompted by a request to integrate a specific cuFile library, which could not be reached from this environment (and there is no GPU, CUDA, or `libcufile` here to build or test against either). The seam is the part that was buildable and verifiable without them.

Code layout:

| Package | DESIGN.md § | What it does |
|---|---|---|
| `pkg/chunk` | §5.1 | Fixed-size content-addressed chunking (BLAKE3) |
| `pkg/manifest` | §5.2/§5.3 | Canonical manifest encoding, content-addressed manifest IDs |
| `pkg/pack` | §5.4/§5.5/§14 | Container packing, chunk locators, hash-verified fetch |
| `pkg/store` + `local`/`s3`/`gcs`/`azure` | §24 | Pluggable backend interface; local-disk, S3, GCS, and Azure Blob implementations |
| `pkg/metadb` | §6/§7/§8 | Embedded (bbolt) stand-in for a per-region metadata cluster; persists each repo's class |
| `pkg/coherence` | §10, §10.6 | Lease/negative-cache manager and blocking recall — the real implementation of `spec/coherence.qnt` |
| `pkg/mds` | §10, §7 | Metadata authority as a gRPC service: remote holders, streamed invalidation, posix recall |
| `pkg/repo` | §16, §8 | Publish/read paths, plus the mutable write path (Create/Write/Unlink/Mkdir/Rmdir/Rename/Symlink/Link) for non-immutable classes |
| `pkg/mdsfuse` | §21, §10, §11 | Read-write FUSE mount backed by a remote metadata authority; the mountpoint is itself a lease holder |
| `pkg/fuseserver` | §21, §10 | FUSE mount — read-only for `immutable`, read-write for `relaxed`/`session`, coherence-aware attr/negative caching |
| `pkg/repoopen` | §24, §8 | Shared backend + class selection logic (CLI flags and CSI `VolumeContext` both use it) |
| `pkg/csidriver` | §22.1/§22.2 | CSI Identity + Node gRPC services — local or authority-backed PVs, plus writable RWX for the `disjoint-writers` carve-out |
| `pkg/costmodel` | §12 | Egress/request cost estimation and budget admission |
| `pkg/libatlas` | §21.3 | Materialize-then-passthrough escape hatch for mmap-heavy workloads |
| `pkg/gds` | — | cuFile/GPUDirect binding (`-tags cufile`) and its fail-closed stub |
| `pkg/store/dragonfly` | §11.1 | Route container GETs through a Dragonfly peer for P2P chunk exchange |
| `cmd/atlas` | — | CLI (publish/ls/cat/stat/mount/gc/quota) |
| `cmd/atlas-csi` | — | CSI node plugin binary |
| `cmd/atlas-mds` | §7, §10 | Metadata authority server |
| `spec/coherence.qnt` | §10, §10.8 | Quint model of the metadata cache coherence protocol |
| `spec/rehoming.tla` | §7.4, §27 | TLA+ model of rehoming and epoch fencing, checked exhaustively with TLC |

Not yet built, and why each is a real gap rather than an oversight: FoundationDB, multi-region homing, the namespace map, and rehoming (§7 — needs a running FDB cluster; `pkg/mds` is the client/authority boundary those would sit on, not the topology itself); byte-range locks and cross-node `flock`/`fcntl` forwarding (§17 — the RWX `disjoint-writers` carve-out only provides local mutual exclusion, see `TestRWXDisjointWritersFlockIsLocalOnly`); in-place random-access writes (§16.3 — both mounts buffer a file and commit it as a unit on flush); a CSI Controller service and dynamic provisioning (§22 — needs a real cluster); GC's open-but-unlinked handle tracking and client-side write-session `ESTALE` enforcement (§19.2/§19.3 — see `pkg/repo/gc.go`'s package doc); the §21.2 client performance work the benchmarks above quantify; §27's 24-hour `fsx` soak and its xfstests suite (see docs/conformance.md for what pjdfstest and the shorter fsx runs did cover); and the NFS export (§23 — needs `nfs-ganesha`, not installed here).

§27's conformance bar is met for one of its three suites: pjdfstest passes 8798/8798 with no exceptions, `fsx` is clean over 170k operations with mmap, and xfstests runs (docs/conformance.md). But that is minutes of fsx rather than §27's 24-hour soak — the one hour-long attempt ended in ENOSPC after the repo grew to 21 GB, and the cause was worse than the "GC was never run" first written here: superseded chunks were unreachable by *both* halves of mark-and-sweep, so running GC would have reclaimed nothing. That leak is fixed, and so is the constraint that made the soak unwinnable regardless — GC could not run at all while the mount was up, which `-gc-interval` now solves. The remaining cost is write amplification, which `-chunk-size` bounds but does not remove: the same 20,000 fsx operations store **2500 MiB at the default chunk size and 451 MiB at 16 KiB**, and 2500 MiB scaled to the soak's 170k operations is the 21 GB that was observed. GC-1's grace floor still means a window's worth of garbage sits on disk by design. Coverage is also only a handful of xfstests' generic tests plus a partial quick group, not the applicable groups §27 names. A Jepsen-style bounded-staleness harness remains untouched. The Quint model is simulated (5,000 traces), not exhaustively verified.

One pleasant surprise from manual testing: `O_APPEND` (`echo text >> file`) actually works correctly against a single local writer, verified through a real mount — the kernel computes the append offset and this build's `Write(off)` already handles arbitrary offsets correctly, so it falls out of the existing machinery rather than needing special-casing. §16.3 scopes append as best-effort outside `posix` because of concurrent multi-node writers, which this single-process build doesn't have to contend with yet; that qualifier still applies once a second holder exists.

| Reading for | Start at |
|---|---|
| Why this isn't Alluxio/Fluid/JuiceFS | §1, §3 |
| The core protocol | §10 (metadata cache coherence), §7 (regional authority) |
| Kubernetes | §22 (CSI/PVC), §23.5 (NFS as a PV) |
| Backends | §24 |
| What it costs to run | §12 |
| What it costs to build | §28 |
