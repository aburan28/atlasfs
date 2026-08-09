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

**A first vertical slice of Phase 1 is implemented and working**, single-node/single-region, `immutable` class only, local-disk backend: content-addressed chunking (§5.1), the manifest format with single-chunk inlining (§5.2/§5.3), chunk/container/locator separation (§5.4/§5.5), small-file packing with whole-file dedup (§14/§15), a `Backend` interface with a local POSIX implementation (§24.1), and a real read-only FUSE mount (§21) — no FDB, S3, Kubernetes, or NFS yet; those are additive per the interfaces already in place (§24.5).

```
go build -o atlas ./cmd/atlas

./atlas publish  <repo-dir> <src-dir> [dest-path]   # ingest a directory tree
./atlas ls       <repo-dir> [path]                  # list a directory
./atlas cat      <repo-dir> <path>                  # print a file's content
./atlas stat     <repo-dir> <path>                  # print an inode's metadata
./atlas mount    <repo-dir> <mountpoint>             # read-only FUSE mount (Ctrl-C to unmount)
```

`go test ./...` covers chunk/manifest round-trips, packing and locator resolution, dedup, metadb dentry/inode operations, and an end-to-end test that publishes a tree, mounts it over a real FUSE connection, and reads it back through the kernel byte-for-byte.

Code layout:

| Package | DESIGN.md § | What it does |
|---|---|---|
| `pkg/chunk` | §5.1 | Fixed-size content-addressed chunking (BLAKE3) |
| `pkg/manifest` | §5.2/§5.3 | Canonical manifest encoding, content-addressed manifest IDs |
| `pkg/pack` | §5.4/§5.5/§14 | Container packing, chunk locators, hash-verified fetch |
| `pkg/store` + `pkg/store/local` | §24 | Pluggable backend interface; local-disk implementation |
| `pkg/metadb` | §6/§7 | Embedded (bbolt) stand-in for a per-region metadata cluster |
| `pkg/repo` | §16 | Publish and read paths tying the above together |
| `pkg/fuseserver` | §21 | Read-only FUSE mount for an `immutable` repo |
| `cmd/atlas` | — | CLI |

Not yet built: FoundationDB metadata (multi-region homing, §7), cloud backends (§24.2), the `relaxed`/`session`/`posix` consistency classes and their lease protocol (§10), Kubernetes CSI (§22), and the NFS export (§23).

| Reading for | Start at |
|---|---|
| Why this isn't Alluxio/Fluid/JuiceFS | §1, §3 |
| The core protocol | §10 (metadata cache coherence), §7 (regional authority) |
| Kubernetes | §22 (CSI/PVC), §23.5 (NFS as a PV) |
| Backends | §24 |
| What it costs to run | §12 |
| What it costs to build | §28 |
