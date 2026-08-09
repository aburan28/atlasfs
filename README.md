# atlasfs

A POSIX filesystem whose namespace spans regions and cloud providers, built on three claims:

1. **Content-addressed chunk identity is the unit of placement and replication.** Replicating a dataset across clouds is a set difference over hashes; verifying a replica is a hash check.
2. **Metadata authority is regional and explicit.** Every mutable subtree has a home region that owns its metadata. Operations spanning home regions return `EXDEV` rather than silently paying a cross-continent round trip.
3. **Consistency is a per-subtree property.** A published dataset and a shared scratch directory should not pay the same coordination cost.

Consumed through a **Kubernetes CSI driver** (PersistentVolumes and Claims, snapshots, quota-as-capacity) or an **NFSv4.1 export** for clients that cannot run FUSE. Storage backends are **pluggable** — S3, GCS, Azure Blob, S3-compatible, and POSIX/NFS.

Design stage — no implementation yet.

- [`DESIGN.md`](DESIGN.md) — the design.
- [`docs/review-response.md`](docs/review-response.md) — how the current draft answers the Draft 1 review.

## Status

Draft 3. Estimated at 4–6 engineer-years to reach the verification bar in DESIGN.md §27.

Phase 1 — immutable, content-addressed dataset and checkpoint storage with atomic publish, an S3 backend, and CSI read-only PVCs — is ~0.75 engineer-years and is where the value-to-effort ratio is best. See §28.

| Reading for | Start at |
|---|---|
| Why this isn't Alluxio/Fluid/JuiceFS | §1, §3 |
| The core protocol | §10 (metadata cache coherence), §7 (regional authority) |
| Kubernetes | §22 (CSI/PVC), §23.5 (NFS as a PV) |
| Backends | §24 |
| What it costs to run | §12 |
| What it costs to build | §28 |
