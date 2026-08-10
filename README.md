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

**Phase 1 is done and Phase 2 is underway**: single-node/single-region, real reads and writes through real protocols end to end, for `immutable`, `relaxed`, and `session` classes. What's genuinely built and verified, not stubbed:

- Content-addressed chunking, the manifest format with single-chunk inlining, chunk/container/locator separation, small-file packing with whole-file dedup, and symlinks (§5, §14, §15).
- A `Backend` interface (§24.1) with **two working implementations**: local disk, and **S3** over the real `aws-sdk-go-v2` client — verified against an in-process fake S3 server speaking real HTTP/XML, including a genuine capability probe for conditional-PUT support (§24.2) rather than an assumption.
- A real **FUSE mount** (§21) — read-only for `immutable` (kernel-level reads verified byte-for-byte, writes rejected as `EROFS`); read-write for `relaxed`/`session` (create/write/overwrite/unlink/mkdir/rmdir/truncate, all verified through a real mount, including via an actual shell — `echo text > file` on the mount works).
- A **coherence.Manager** (§10) implementing the protocol modeled in `spec/coherence.qnt` for real: per-holder leases on a monotonic clock, directory-version-gated negative caching, and a best-effort/bounded push-invalidation registry. Wired into `pkg/fuseserver` as the one real consumer this build has — every `Node` caches its attrs and negative lookups, trusting them only while its lease holds. `posix`'s blocking recall is deliberately not implemented: it needs a second, remote holder to recall against, and this single-process build only ever has one (see `pkg/coherence`'s package doc).
- A **mutable write path** in `pkg/repo` (§16.1): buffer-then-commit-on-close, overwrite reuses the existing inode in place (so a cached lease on it is something real to invalidate), every mutation bumps the affected directory's coherence version.
- **Quotas** (§18.3): a bbolt-backed byte/inode counter charged in the same transaction as the inode/dentry write it guards, so a rejection aborts cleanly — no orphan inode, no dangling chunk locator. `Repo.SetQuota`/`QuotaUsage`, `ErrQuotaExceeded` mapped to `EDQUOT` in the FUSE layer, verified through a real mount (`echo` past the byte limit fails with `EDQUOT` at the shell).
- **Garbage collection** (§19): a real two-pass mark-and-sweep against metadb as ground truth. `Unlink`/`Rmdir` move an inode into a graveyard bucket instead of deleting it; `Repo.Sweep` marks every chunk reachable from the live namespace or a not-yet-expired graveyard entry, then reclaims a past-grace entry's chunks that mark didn't reach — verified to correctly *not* collect a chunk two files still share via dedup when only one is unlinked. Invariant GC-1 (`T_grace > T_write_max + D_max + ε`, §19.2) is a checked precondition, not a comment: `Sweep` refuses to run on a `graceDuration` that violates it. Real gaps, stated rather than hidden: no open-but-unlinked handle tracking (§19.3) — an inode is gravable immediately on unlink even if a FUSE client still has it open — no container compaction (§19.1 step 3, so backend bytes aren't actually reclaimed, only locator entries), and no client-side write-session-age `ESTALE` enforcement (the other half of GC-1).
- A **CSI node plugin** (§22.1's static ROX case) — real gRPC `Identity`/`Node` services, driven end to end by a real gRPC client through `NodePublishVolume`/`NodeUnpublishVolume`, mounting and reading a repo exactly as kubelet would for a statically-provisioned PV. Still ROX-only by design (§22.1 is the static-provisioning case); a writable PV path isn't wired up yet.
- A **Quint model** of the §10 metadata cache coherence protocol (Phase 0 per §10.8) — see [`spec/README.md`](spec/README.md). It's been run against the real `quint` checker, and the first draft's invariant was too strong and caught a real (non-)issue, fixed by narrowing it to what §10 actually promises.

Manual testing on this pass found and fixed two real bugs no unit test happened to catch on its own: a standalone `Commit` left its chunk unsealed in the packer's buffer (fixed by flushing per-commit), and `Flush` firing more than once in a single open-for-write session (observed: once right after a truncate, again at close) let a one-shot "committed" latch silently drop everything written after the first flush. Both now have regression tests.

```
go build -o atlas ./cmd/atlas -o atlas-csi ./cmd/atlas-csi

./atlas publish  [flags] <repo-dir> <src-dir> [dest-path]   # ingest a directory tree
./atlas ls       [flags] <repo-dir> [path]                  # list a directory
./atlas cat      [flags] <repo-dir> <path>                  # print a file's content
./atlas stat     [flags] <repo-dir> <path>                  # print an inode's metadata
./atlas mount    [flags] <repo-dir> <mountpoint>             # FUSE mount (Ctrl-C to unmount)

# every subcommand takes -backend=local|s3 (§24); AWS creds via the normal SDK chain
./atlas publish -backend=s3 -s3-bucket=my-bucket ./repo ./my-dataset /datasets/demo

# -class=immutable|relaxed|session on a brand-new repo (§8); ignored on reopen — the
# persisted class always wins, so this only matters the first time a repo-dir is used
./atlas mount -class=relaxed ./repo ./mnt   # now a real read-write mount

./atlas-csi -csi-endpoint=unix:///var/lib/kubelet/plugins/csi.atlas.io/csi.sock
```

`go test ./...` covers chunk/manifest round-trips, packing and locator resolution with corruption detection, dedup, metadb dentry/inode semantics, the coherence manager against all four properties the Quint model checks, the mutable write path (create/overwrite/unlink/mkdir/rmdir, including the sentinel-errno mapping), the S3 backend against a real fake-S3 server, the CSI driver against a real gRPC client, and full end-to-end FUSE tests — read-only and read-write — that mount over a real kernel connection and exercise it exactly as a real client would. All of `pkg/fuseserver`, `pkg/coherence`, and `pkg/repo` are additionally clean under `-race`.

Code layout:

| Package | DESIGN.md § | What it does |
|---|---|---|
| `pkg/chunk` | §5.1 | Fixed-size content-addressed chunking (BLAKE3) |
| `pkg/manifest` | §5.2/§5.3 | Canonical manifest encoding, content-addressed manifest IDs |
| `pkg/pack` | §5.4/§5.5/§14 | Container packing, chunk locators, hash-verified fetch |
| `pkg/store` + `local`/`s3` | §24 | Pluggable backend interface; local-disk and S3 implementations |
| `pkg/metadb` | §6/§7/§8 | Embedded (bbolt) stand-in for a per-region metadata cluster; persists each repo's class |
| `pkg/coherence` | §10 | Lease/negative-cache manager — the real implementation of `spec/coherence.qnt` |
| `pkg/repo` | §16, §8 | Publish/read paths, plus the mutable write path (Create/Write/Unlink/Mkdir/Rmdir) for non-immutable classes |
| `pkg/fuseserver` | §21, §10 | FUSE mount — read-only for `immutable`, read-write for `relaxed`/`session`, coherence-aware attr/negative caching |
| `pkg/repoopen` | §24, §8 | Shared backend + class selection logic (CLI flags and CSI `VolumeContext` both use it) |
| `pkg/csidriver` | §22.1 | CSI Identity + Node gRPC services for static ROX PVs |
| `cmd/atlas` | — | CLI |
| `cmd/atlas-csi` | — | CSI node plugin binary |
| `spec/coherence.qnt` | §10, §10.8 | Quint model of the metadata cache coherence protocol |

Not yet built, and why each is a real gap rather than an oversight: FoundationDB and multi-region homing (§7 — needs a running FDB cluster), GCS/Azure backends (§24.2 — additive against the same `Backend` interface the S3 backend already proves out), the `posix` class's blocking recall (§10.6 — needs a second, remote coherence holder this single-process build doesn't have), byte-range locks (§17 — `posix`-class territory), a CSI Controller service and a writable CSI path (§22 — needs a real cluster and RWX wiring neither built yet), GC (§19 — `Unlink`/overwrite drop bindings but never reclaim the orphaned chunks), and the NFS export (§23 — needs `nfs-ganesha`, not installed here).

One pleasant surprise from manual testing: `O_APPEND` (`echo text >> file`) actually works correctly against a single local writer, verified through a real mount — the kernel computes the append offset and this build's `Write(off)` already handles arbitrary offsets correctly, so it falls out of the existing machinery rather than needing special-casing. §16.3 scopes append as best-effort outside `posix` because of concurrent multi-node writers, which this single-process build doesn't have to contend with yet; that qualifier still applies once a second holder exists.

| Reading for | Start at |
|---|---|
| Why this isn't Alluxio/Fluid/JuiceFS | §1, §3 |
| The core protocol | §10 (metadata cache coherence), §7 (regional authority) |
| Kubernetes | §22 (CSI/PVC), §23.5 (NFS as a PV) |
| Backends | §24 |
| What it costs to run | §12 |
| What it costs to build | §28 |
