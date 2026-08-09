# atlasfs

A POSIX filesystem whose namespace spans regions and cloud providers, built on three claims:

1. **Content-addressed chunk identity is the unit of placement and replication.** Replicating a dataset across clouds is a set difference over hashes; verifying a replica is a hash check.
2. **Metadata authority is regional and explicit.** Every mutable subtree has a home region that owns its metadata. Operations spanning home regions return `EXDEV` rather than silently paying a cross-continent round trip.
3. **Consistency is a per-subtree property.** A published dataset and a shared scratch directory should not pay the same coordination cost.

Design stage — no implementation yet.

- [`DESIGN.md`](DESIGN.md) — the design.
- [`docs/review-response.md`](docs/review-response.md) — how Draft 2 answers the Draft 1 review.

## Status

Draft 2. Estimated at 3–5 engineer-years to reach the verification bar in DESIGN.md §25.
Phase 1 (immutable, content-addressed dataset and checkpoint storage with atomic publish) is ~0.5 engineer-years and is where the value-to-effort ratio is best — see §26.
