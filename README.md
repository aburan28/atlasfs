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

**A Phase 1 vertical slice is implemented and working**: single-node/single-region, `immutable` class only, real reads and writes through real protocols end to end. What's genuinely built and verified, not stubbed:

- Content-addressed chunking, the manifest format with single-chunk inlining, chunk/container/locator separation, small-file packing with whole-file dedup, and symlinks (§5, §14, §15).
- A `Backend` interface (§24.1) with **two working implementations**: local disk, and **S3** over the real `aws-sdk-go-v2` client — verified against an in-process fake S3 server speaking real HTTP/XML, including a genuine capability probe for conditional-PUT support (§24.2) rather than an assumption.
- A real read-only **FUSE mount** (§21) — kernel-level reads, verified byte-for-byte against the source tree, with writes rejected as `EROFS`.
- A **CSI node plugin** (§22.1's static ROX case) — real gRPC `Identity`/`Node` services, driven end to end by a real gRPC client through `NodePublishVolume`/`NodeUnpublishVolume`, mounting and reading a repo exactly as kubelet would for a statically-provisioned PV.
- A **Quint model** of the §10 metadata cache coherence protocol (Phase 0 per §10.8) — see [`spec/README.md`](spec/README.md). It's been run against the real `quint` checker, and the first draft's invariant was too strong and caught a real (non-)issue, fixed by narrowing it to what §10 actually promises — that history is in the spec README because it's the point of doing this before building the rest.

```
go build -o atlas ./cmd/atlas -o atlas-csi ./cmd/atlas-csi

./atlas publish  [flags] <repo-dir> <src-dir> [dest-path]   # ingest a directory tree
./atlas ls       [flags] <repo-dir> [path]                  # list a directory
./atlas cat      [flags] <repo-dir> <path>                  # print a file's content
./atlas stat     [flags] <repo-dir> <path>                  # print an inode's metadata
./atlas mount    [flags] <repo-dir> <mountpoint>             # read-only FUSE mount (Ctrl-C to unmount)

# every subcommand takes -backend=local|s3 (§24); AWS creds via the normal SDK chain
./atlas publish -backend=s3 -s3-bucket=my-bucket ./repo ./my-dataset /datasets/demo

./atlas-csi -csi-endpoint=unix:///var/lib/kubelet/plugins/csi.atlas.io/csi.sock
```

`go test ./...` covers chunk/manifest round-trips, packing and locator resolution with corruption detection, dedup, metadb dentry/inode semantics, the S3 backend against a real fake-S3 server (ranged GET, conditional PUT, list pagination, delete), the CSI driver against a real gRPC client, and two full end-to-end tests that mount over a real FUSE connection and read back through the kernel byte-for-byte (one direct, one through the CSI flow).

Code layout:

| Package | DESIGN.md § | What it does |
|---|---|---|
| `pkg/chunk` | §5.1 | Fixed-size content-addressed chunking (BLAKE3) |
| `pkg/manifest` | §5.2/§5.3 | Canonical manifest encoding, content-addressed manifest IDs |
| `pkg/pack` | §5.4/§5.5/§14 | Container packing, chunk locators, hash-verified fetch |
| `pkg/store` + `local`/`s3` | §24 | Pluggable backend interface; local-disk and S3 implementations |
| `pkg/metadb` | §6/§7 | Embedded (bbolt) stand-in for a per-region metadata cluster |
| `pkg/repo` | §16 | Publish and read paths tying the above together |
| `pkg/fuseserver` | §21 | Read-only FUSE mount for an `immutable` repo |
| `pkg/repoopen` | §24 | Shared backend-selection logic (CLI flags and CSI `VolumeContext` both use it) |
| `pkg/csidriver` | §22.1 | CSI Identity + Node gRPC services for static ROX PVs |
| `cmd/atlas` | — | CLI |
| `cmd/atlas-csi` | — | CSI node plugin binary |
| `spec/coherence.qnt` | §10, §10.8 | Quint model of the metadata cache coherence protocol |

Not yet built, and why each is a real infrastructure dependency rather than an oversight: FoundationDB and multi-region homing (§7 — needs a running FDB cluster), GCS/Azure backends (§24.2 — additive against the same `Backend` interface the S3 backend already proves out), the `relaxed`/`session`/`posix` consistency classes' live lease protocol (§10 — modeled in Quint, not yet wired into `pkg/metadb`), a CSI Controller service for dynamic provisioning (§22 — needs a real cluster to provision against), and the NFS export (§23 — needs `nfs-ganesha`, not installed here).

| Reading for | Start at |
|---|---|
| Why this isn't Alluxio/Fluid/JuiceFS | §1, §3 |
| The core protocol | §10 (metadata cache coherence), §7 (regional authority) |
| Kubernetes | §22 (CSI/PVC), §23.5 (NFS as a PV) |
| Backends | §24 |
| What it costs to run | §12 |
| What it costs to build | §28 |
