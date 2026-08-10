# AtlasFS — Design

**Status:** Draft 2. Supersedes Draft 1 in response to review.
**Scope of this revision:** general-purpose POSIX filesystem, full scope. Every point raised in review is resolved here rather than deferred.

---

## 1. Positioning

AtlasFS is a POSIX filesystem whose namespace spans regions and cloud providers, built on three claims that are not jointly available elsewhere:

1. **Content-addressed chunk identity is the unit of placement and replication.** A chunk's name is its hash. Replicating a dataset to another cloud is a set-difference over hashes, not a file-by-file copy; verifying a replica is a hash check, not a manifest diff; and a chunk fetched from any source is trustworthy by construction.
2. **Metadata authority is regional and explicit.** Every mutable subtree has a *home region* that owns its metadata. Mutations in the home region do not cross the WAN. Operations that would span home regions fail with `EXDEV` rather than silently paying a cross-continent round trip.
3. **Consistency is a per-subtree property, not a global one.** A published dataset and a shared scratch directory have different correctness requirements and should not pay the same coordination cost.

Draft 1 claimed (1) and (3) but contradicted (2): it dismissed JuiceFS for a metadata topology "not designed around regional autonomy" and then specified a single global FoundationDB cluster. A single FDB cluster has one transaction subsystem; every namespace mutation serializes through one region's proxies and resolvers regardless of client location. That is ~65 ms us-west-2 ↔ us-east-1, worse across continents, and operationally hostile spanning AWS and GCP. JuiceFS-on-TiKV is architecturally the same thing. The claim was false as designed.

§9 resolves this properly: **independent per-region metadata clusters, subtree home-region as first-class metadata, and no cross-region distributed transactions anywhere.** The differentiator is now real, and it is also narrower and more honest than Draft 1's phrasing.

### 1.1 What AtlasFS is not for

Stated up front, because a design that claims to suit everything describes nothing:

- **Single region, single cloud, caching over one bucket.** Use Alluxio, JuiceFS, or Mountpoint-with-caching. AtlasFS's home-region machinery is pure overhead here.
- **Small-scale general file serving.** Use NFS or EFS/Filestore.
- **Write-heavy shared-mutable workloads with fine-grained cross-node coordination** (databases, build caches with concurrent writers to the same file). The `posix` class supports this correctly but at the coordination cost such workloads imply; a purpose-built system will beat it.
- **Strict global linearizability across regions.** Not offered at any consistency class. §9 explains why we chose partition-tolerant regional authority instead.
- **NFS-only access at full scale.** The NFS export (§23) is a compatibility and reach path delivering 50–70% of native throughput, without node-local caching, `mmap` passthrough, or per-client authorization under `AUTH_SYS`. Where the CSI driver can be installed, it is the right answer.

---

## 2. Target Workloads

Three concrete workloads drive every number in this document.

**W1 — Dataset iteration (small files).** ImageNet-1k: 1.28M files, ~140 GB, mean ~107 KB. Many epochs, shuffled order. This workload is metadata- and IOPS-bound, not throughput-bound. Draft 1 optimized only the large-sequential case; §14 fixes that and it drove the format change in §6.

**W2 — Checkpoint write and restore (large sequential).** 100 GB–1 TB safetensors written every N steps, read back on restart or for evaluation. Loaded via `mmap`. Throughput-bound, latency-sensitive at restore.

**W3 — Cross-cloud dataset publication.** A 50 TB corpus produced in AWS, consumed by training in GCP. Bounded by egress cost, not by bandwidth. §17 exists because of this workload.

---

## 3. Competitive Landscape

Draft 1 compared against CephFS, JuiceFS CE, and raw S3. Those are not the competitors.

| System | Overlap | What it does not do |
|---|---|---|
| **Alluxio** | Unified namespace + caching over heterogeneous stores | No content-addressed identity; no per-subtree consistency; master is a scaling and availability chokepoint |
| **Fluid (CNCF)** | `Dataset` CRD, prefetch, locality-aware scheduling — near-identical to Draft 1 §20/§21 | Orchestration over an underlying engine; cannot express chunk-level cross-cloud placement because no engine beneath it has content-addressed chunks |
| **JuiceFS Enterprise** | Distributed metadata service, mirror regions | Mirrors are read replicas, not authorities; block IDs come from a counter, not content; one global consistency model |
| **Mountpoint for S3 (caching)** | S3-backed FUSE with local cache | Single bucket, single provider; not a general namespace; limited POSIX |
| **3FS** | High-performance DFS for AI, strong throughput story | Single-cluster, single-datacenter; no cross-cloud story |
| **CephFS / Lustre** | Real POSIX at scale | Datacenter-scoped; not object-store-native; operationally heavy across clouds |

**Why this isn't "contribute placement policy to Fluid and content-addressing to JuiceFS":**

Fluid's `Dataset` CRD delegates to a cache engine (Alluxio, JuiceFS, Vineyard). Chunk-level cross-cloud placement cannot be expressed at the Fluid layer because no engine underneath exposes a stable, content-derived chunk identity to place. The feature has nowhere to live.

JuiceFS's on-disk model is inode → chunk → slice → block, with block IDs allocated from a monotonic counter. Making block identity content-derived changes the format, the slice-compaction path, the GC algorithm, and the metadata schema simultaneously. That is a fork, not a patch — and it still would not give per-subtree consistency classes or regional metadata authority, both of which are cross-cutting rather than additive.

The three claims in §1 are load-bearing together. Content addressing is what makes cross-cloud replication a set operation and makes P2P chunk exchange trivially correct (§13). Home-region authority is what makes the namespace usable across a WAN. Consistency classes are what make the immutable case cost nothing. Any one alone is a feature; the combination is the system.

---

## 4. Architecture

Four layers, each addressable independently. This layering is unchanged from Draft 1 and remains the right bet.

**Namespace** — paths, directories, name→inode bindings. Owned by a home region (§9). Mutable.

**Inode** — POSIX attributes, link count, and a pointer to the current manifest. Owned by the same home region as its containing subtree.

**Manifest** — an ordered chunk list describing file content at a version. Content-addressed and immutable; a new version is a new manifest with a new hash. Stored as a blob, not in the metadata store (§5.3).

**Chunk** — content-addressed byte ranges. Immutable, globally deduplicable, location-independent. A chunk's *identity* is separate from its *location* (§5.4) — this indirection is what makes small-file packing, recompaction, and per-region placement possible without touching manifests.

Reads descend; the layers below the namespace are immutable and therefore trivially cacheable, replicable, and verifiable. Essentially all coordination cost lives in the top layer, which is why §9 and §10 are the substance of this design.

---

## 5. Naming and Formats

### 5.1 Chunk ID

`chunk_id = BLAKE3-256(plaintext)`, 32 bytes. BLAKE3 for tree-hashing (parallel verification of partial reads) and speed relative to SHA-256.

Chunk IDs name *plaintext*, not ciphertext or compressed bytes. Compression and encryption are properties of the stored representation, recorded in the locator (§5.4), so the same logical chunk deduplicates across subtrees with different codecs.

> **Convergent-encryption caveat.** Content-addressing plaintext means two tenants storing identical bytes share a chunk, and a tenant can test for the existence of a chunk they can guess. Where this matters, a subtree sets `encryption: keyed`, which mixes a per-subtree key into the chunk ID (`BLAKE3_keyed(subtree_key, plaintext)`), disabling cross-tenant dedup for that subtree. Default is `keyed` for user home subtrees and `convergent` for published datasets.

### 5.2 Manifest

```
manifest := {
  format_version: u16
  content_size:   u64
  chunk_size_hint: u32          # nominal; last chunk is short
  entries: [ { chunk_id: [32]u8, length: u32, flags: u32 } ]   # 40 bytes each
}
```

`manifest_id = BLAKE3-256(canonical_encoding(manifest))`.

### 5.3 Manifests are blobs, not metadata rows

FoundationDB limits: 100 KB per value, 10 MB per transaction, 5 s per transaction. Manifest sizes at 40 bytes per entry:

| File size | Chunk size | Entries | Manifest |
|---|---|---|---|
| 500 GB | 4 MiB | 119,209 | 4.8 MB |
| 500 GB | 16 MiB | 29,802 | 1.2 MB |
| 1 TB | 16 MiB | 59,605 | 2.4 MB |

A 4.8 MB manifest would need ~50 split keys and sits at half the transaction limit — writable, but every read of a large file's manifest becomes a multi-key range scan competing for transaction budget, and rewriting one on append is a 4.8 MB transaction.

**Draft 1 put the ordered chunk list in the metadata store. That is corrected here.** Manifests are content-addressed objects in the blob store. The metadata store holds only:

```
inode → { ..., manifest_id: [32]u8, content_size: u64, manifest_len: u32 }
```

Manifests are immutable and content-addressed, so they cache forever with no invalidation, and a manifest fetch is one GET against a key the client can compute. Small files (single chunk) inline the chunk ID directly in the inode and skip the manifest object entirely — the common case for W1 costs zero extra round trips.

### 5.4 Chunk locators: identity ≠ location

Draft 1 wrote chunks as `atlas/chunks/a8/a832…` — one object per chunk. That bakes in chunk = object and makes small-file packing a format migration. Corrected: manifests reference chunk IDs, and a separate map resolves identity to location.

```
locator: chunk_id → { region → { container: object_key,
                                 offset: u64, length: u32,
                                 codec: enum, uncompressed_length: u32 } }
```

This indirection buys four things that are otherwise unreachable:

- **Packing.** Thousands of small files' chunks live in one container object (§14).
- **Per-region placement.** The same chunk ID resolves to different containers in different regions. Cross-cloud replication rewrites locators, never manifests — so a manifest published in AWS is byte-identical to the one served in GCP, and its hash still verifies.
- **Recompaction.** Repacking cold containers rewrites locators only.
- **Codec migration.** Recompressing does not change chunk identity.

Locators live in the metadata store, sharded by `chunk_id` prefix, and are **globally replicated read-mostly state** rather than home-region-owned: a chunk is immutable, so its locator is append-mostly and conflict-free (§7.5).

### 5.5 Container objects

Append-only, sealed at 64–256 MiB (default 128 MiB), immutable once sealed. Written by a per-node packer. Files above `single_object_threshold` (default 64 MiB) get dedicated containers with 1:1 chunk-to-object mapping so that large sequential reads issue direct range GETs with no packing indirection cost.

Container key: `atlas/c/{region}/{shard}/{container_ulid}`. Not content-addressed — containers are mutable-until-sealed and their identity is a placement detail, not a correctness one. Chunk integrity comes from the chunk ID.

---

## 6. Metadata Store

FoundationDB, one **independent cluster per region**. Not one global cluster.

Keyspace within a region (tuple-encoded):

```
(ns, dir_inode, name)        → inode_id                 # dentry
(inode, inode_id)            → attrs | manifest_id      # inode record
(dirver, dir_inode)          → u64                      # directory version (§10.4)
(loc, chunk_id_prefix, ...)  → locator                  # chunk locators
(grave, delete_ts, inode_id) → tombstone                # GC (§19)
(quota, subtree_id)          → { bytes, inodes, limits } # §21.3
(lock, inode_id, range)      → lock record              # posix class only
```

Dentries are ordered lexicographically by name within a directory, which is what makes `readdir` at 10M entries paginate correctly (§21.1).

FDB is used for what it is good at — serializable transactions within a cluster, at low latency, on small values. It is not used for large values (manifests are blobs), and it is not used across the WAN (§9).

---

## 7. Metadata Topology: Home-Region Authority

### 7.1 Model

Every subtree has a `home_region`. It is inherited from the parent at creation and is otherwise immutable except by explicit rehoming (§7.4).

- The home region's FDB cluster is the **sole authority** for that subtree's dentries, inodes, and directory versions.
- A client in the home region mutates at LAN latency.
- A client outside the home region forwards the mutation to the home region's authority and pays one WAN RTT. This is correct, observable, and metered (`atlas_metadata_rtt_seconds{home_region}`) — not hidden.
- **Reads** outside the home region are served from cache under the lease protocol (§10) and usually cost nothing.

`/users/adam` homed in `us-west-2` means Adam's mutations never cross the WAN. `/datasets/imagenet`, once published, is immutable and needs no authority at all — its metadata is a sealed, content-addressed snapshot replicable to every region and readable locally with zero coordination. That is the property that makes the ML workload nearly free, and it falls out of the layering rather than being bolted on.

### 7.2 Why there are no cross-region transactions

**Any operation that would need atomicity across two home regions returns `EXDEV`.** This applies to `rename` and `link` whose source and destination resolve to different home regions.

This is the single most important simplification in the design. It eliminates cross-region two-phase commit, cross-region deadlock, and the entire class of partition-time availability decisions those create. The cost is that `mv /users/adam/x /datasets/x` fails with `EXDEV` — which applications already handle, because it is the ordinary cross-device case, and the fix is the same: copy then unlink. `coreutils mv` does this transparently.

**Rejected alternative: CRDT directory merge.** POSIX namespace operations do not have sensible commutative merges. Concurrent `rename(a,b)` in one region and `unlink(a)` in another has no merge that is not either a lost update or a resurrection, and concurrent creates of the same name cannot both win. A CRDT here would produce a filesystem that silently does something other than what POSIX says, which is worse than an honest `EXDEV`.

**Rejected alternative: global FDB.** Draft 1's design. Serializes every mutation through one region (§1).

### 7.3 The Namespace Map

The prefix → home region routing table. It is the one piece of genuinely global state.

```
namespace_map := [ { prefix: path, home_region: region, epoch: u64 } ]
```

Properties that make this affordable: it is small (one entry per homed subtree — hundreds, not millions), it changes rarely (subtree creation at mount-point granularity, and rehoming), and it is not on the data path.

- **Authority:** a Raft group of 5 members spread across regions. Writes take a WAN round trip; this is acceptable because they are rare and administrative.
- **Reads:** from a locally cached copy, revalidated on the same lease machinery as everything else (§10), with a long lease (default 300 s).
- **Fencing:** every metadata RPC carries the client's namespace-map epoch. An authority rejects RPCs carrying an epoch older than a rehoming that affects the target subtree, with `ATLAS_STALE_EPOCH`; the client refreshes and retries. This is what makes rehoming safe against in-flight writers.

### 7.4 Rehoming

Moving a subtree's authority between regions. Quiesce-based, not concurrent:

1. Namespace map entry marked `SEALING@epoch+1`. New mutations to the subtree return `EAGAIN`; reads continue from cache.
2. Old authority waits `D_max + ε` (§10.7) for outstanding leases to expire, then drains in-flight transactions.
3. Metadata range copied to the new region's cluster. Chunk locators are untouched (they are global, §7.5); actual data placement is a separate, asynchronous concern (§17).
4. Namespace map committed at `epoch+1` with the new home region. Old authority now rejects everything for that subtree with `ATLAS_STALE_EPOCH`.

Unavailability window for writes is the quiesce plus the copy — seconds to minutes depending on subtree size. Rehoming is an administrative operation, documented as such, and it is the only operation in the system with a planned write outage.

### 7.5 What is global, what is regional

| State | Scope | Why |
|---|---|---|
| Dentries, inodes, dirvers | Home region | Mutable, needs a single authority |
| Namespace map | Global (Raft) | Small, rare writes, off the data path |
| Chunk locators | Global, replicated per region | Immutable chunks ⇒ append-mostly ⇒ conflict-free |
| Manifests | Global (blob store) | Content-addressed, immutable |
| Chunks | Global (blob store) | Content-addressed, immutable |
| Quotas | Home region | Exact accounting is cheap with one authority (§21.3) |
| Locks | Home region | `posix` class only |

Chunk locators are the one mutable-ish structure that is global. They are safe because the only legal mutation is adding a `(region → location)` binding for a chunk ID, which commutes. Two regions concurrently ingesting identical bytes both add their own binding; both are correct; neither invalidates the other. Locator removal happens only via GC (§19), which is single-threaded per region and grace-period-bounded.

---

## 8. Consistency Classes

A per-subtree property, inherited at creation, changeable only on an empty or quiesced subtree.

| Class | Lease `D` | Guarantees | Intended use |
|---|---|---|---|
| `immutable` | ∞ after seal | Content never changes; namespace bindings frozen at publish | Published datasets, released checkpoints |
| `relaxed` | 30 s | Bounded staleness, monotonic reads, read-your-writes | Scratch, logs, intermediate artifacts |
| `session` | 5 s | Above, plus close-to-open | Shared working directories |
| `posix` | 0 (synchronous) | Linearizable metadata ops within the home region; byte-range locks; atomic `O_APPEND` | Anything that needs real POSIX |

`D` is the lease duration, and it is the only parameter that varies. The protocol is identical across classes (§10) — that uniformity is deliberate, because a protocol with four cases has four times the bug surface and cannot be model-checked as one thing.

### 8.1 Guarantee statements

These are written to be falsifiable and are the properties checked in §27.

Let `D` be the class lease duration and `ε` the clock-error bound (§10.7).

- **G1 (bounded staleness).** Any successful `lookup` or `stat` returns a namespace and attribute state that was the authoritative state at some instant within the preceding `D + ε` seconds.
- **G2 (monotonic reads).** Within a mount session, successive observations of the same object never return a version older than one already returned to that session.
- **G3 (read-your-writes).** A client's own mutations are immediately visible to itself, at every class.
- **G4 (close-to-open)** — `session` and `posix` only. If client A's `close()` returns before client B's `open()` begins, B observes A's data in full.
- **G5 (linearizable metadata)** — `posix` only. Metadata operations on objects within a single home region are linearizable.
- **G6.** No guarantee of cross-client write ordering outside `posix`. No guarantee of cross-home-region ordering at any class.

### 8.2 `immutable` is not "never invalidated"

Draft 1 said immutable subtrees need no invalidation after publication. That cannot be literally true: deletion, retention policy, and erasure requests all exist. Corrected:

> An `immutable` subtree's content bindings are invalidated only by an explicit `unpublish`, and unpublish is effective within a bounded window `T_unpublish` (default 24 h, configurable per subtree down to `D_relaxed`).

Precisely, after `unpublish(path)` returns:
- No `open()` of the path succeeds anywhere after `T_unpublish`.
- Already-open handles may complete their reads. Erasure workflows that require hard cutoff set `T_unpublish` low and accept that clients must revalidate that often, which is just the `relaxed` class with extra steps — stated so the tradeoff is visible rather than discovered.
- Chunk deletion follows GC (§19) and is bounded by `T_grace`, so **actual byte erasure completes no later than `T_unpublish + T_grace`**. For GDPR-style commitments this is the number to quote, not `T_unpublish`.

Subtrees with `retention: legal_hold` reject `unpublish` outright.

---

## 9. Class and Region Boundaries

Consistency classes and home regions are both per-subtree; POSIX operations cross subtrees. Draft 1 left this undefined. The rule is uniform:

**An operation requiring atomicity across a class boundary or a home-region boundary returns `EXDEV`.**

| Operation | Same class & region | Cross-class | Cross-home-region |
|---|---|---|---|
| `rename` | OK | `EXDEV` | `EXDEV` |
| `link` | OK | `EXDEV` | `EXDEV` |
| `symlink` | OK | OK (resolution crosses freely) | OK |
| `open`/`read`/`write` | OK | OK | OK (leases, §10) |
| `rename` into `immutable` | — | `EROFS` | `EROFS` |

`EXDEV` is the correct errno: it is what applications already handle for cross-device moves, and `mv`, `rsync`, Python's `shutil.move`, and Go's `os.Rename` callers all degrade to copy-then-unlink automatically.

**Mixed-class directories.** A directory may contain children of different classes only where a child is itself a subtree root (a class boundary is always a subtree root). Ordinary files inherit their parent's class and cannot deviate. `readdir` across a mixed directory is served at the *parent's* class — that is, at the weakest lease among the boundary's participants — and `stat` of each child is served at the child's own class. This means an `ls -l` over a mixed directory issues per-child revalidation for `posix` children, which is correct and slow, and is why mixing `posix` children under a `relaxed` parent is flagged by `atlas doctor`.

**Class of a boundary crossing during resolution.** Path resolution walking from a `relaxed` parent into a `posix` subtree switches classes at the boundary inode. Each component is validated at the class of the subtree containing it. Guarantees compose as the weakest along the path, and `atlas stat --explain <path>` prints the effective guarantee for a path so this is inspectable rather than folklore.

---

## 10. Metadata Cache Coherence Protocol

This is the core of the design. Draft 1 gave it one paragraph ("generation numbers plus invalidation") while naming it the central research problem. Metadata transactions are the easy part — FDB provides them. The design bugs live here.

### 10.1 What is cached

| Cache | Key | Invalidated by |
|---|---|---|
| Dentry (positive) | `(dir_inode, name)` → `inode_id` | Directory lease |
| Dentry (negative) | `(dir_inode, name)` → `ENOENT` | Directory lease + `dirver` |
| Attributes | `inode_id` → attrs, `manifest_id` | Inode lease |
| Manifest | `manifest_id` → chunk list | Never (content-addressed) |
| Chunk | `chunk_id` → bytes | Never (content-addressed) |
| Locator | `chunk_id` → locations | Never for existing bindings; additive only |
| Namespace map | prefix → region | Long lease + epoch fencing |

Only the first three require a coherence protocol. Everything below the inode layer is immutable and self-verifying — that is the payoff of the layering in §4.

### 10.2 Mechanism: leases, with push invalidation as a non-load-bearing optimization

**Correctness derives from time alone. Push invalidation only shortens the staleness window; losing every invalidation message violates no guarantee.**

*Grant.* The authority returns a lease duration `D` with every metadata response. The client's lease expires `D` seconds after **the instant it sent the request**, measured on `CLOCK_MONOTONIC` — not after receipt, and never against a timestamp supplied by the authority. This is conservative by exactly the request latency and removes any dependence on synchronized wall clocks.

*Use.* Within the lease the client serves reads from cache with no RPC. On expiry it revalidates.

*Push.* The authority keeps a **best-effort, bounded, lossy** registry of which clients hold leases on which inodes and directories: a capped LRU per object (default 64 holders; overflow marks the object `BROADCAST`). On mutation it fires invalidation messages to holders over the existing client connections. Delivery is not retried beyond the connection's own retry, and eviction from the registry is silent. If the message is lost, the client is stale for at most the remaining lease — which is exactly the guarantee already stated in G1.

This is the central design move, and it is worth stating why:

- **Pure callbacks (AFS-style)** are exact, but the authority must track every holder, and a partition forces a choice between blocking writes until the callback times out — which is a lease with extra steps and worse failure semantics — or breaking callbacks and losing exactness anyway. Across cloud boundaries, where connection loss is routine, pure callbacks convert a network blip into a write outage.
- **Pure TTL** has median staleness `D/2`. At `D = 30 s`, a `touch` on one node is invisible elsewhere for ~15 s on average, which makes interactive use feel broken.
- **Leases with best-effort push** get callback-like responsiveness in the common case (invalidation typically lands in one RTT) and lease-like partition behavior in the bad case (bounded staleness, no write outage). The failure mode degrades from "exact" to "bounded", never to "unavailable" and never to "unbounded".

The registry is capacity-bounded and lossy *by design*, not as an implementation shortcut — that is what keeps authority memory O(hot set) rather than O(clients × namespace), and what makes it safe to drop state under memory pressure.

### 10.3 Revalidation

```
REVALIDATE(dir_inode, dirver_seen, [inode_id, inode_ver_seen]...)
  → { dirver: u64, stale: [inode_id...], lease: D }
```

Batched: one RPC revalidates a directory and every inode the client has cached under it. A `ls -l` of a 1000-entry directory after lease expiry is one round trip, not 1001.

### 10.4 Negative caching

`stat()` on a not-yet-existing path is among the most common filesystem patterns — Python import resolution, `PATH` search, build systems, `.git` discovery walking to the root — and it is precisely what plain generation counters handle worst, because there is no object whose generation can be bumped.

A negative entry is leased **against the containing directory's version**:

```
negative_entry := (dir_inode, name, dirver_at_grant, expiry)
```

Any mutation that could create a name in that directory (`create`, `mkdir`, `link`, `rename` into it, `symlink`, `mknod`) bumps `dirver` atomically in the same transaction. A client validates **all** of its negative entries for a directory with a single `GETDIRVER(dir_inode)`:

- `dirver` unchanged ⇒ every cached negative entry for that directory is still valid, and the lease renews.
- `dirver` changed ⇒ only negative entries are dropped. Positive dentries and inode attributes are untouched.

This is what makes negative caching affordable. A Python interpreter probing 40 `sys.path` entries for a module costs **one RPC per directory**, not one per probe, and repeat probes cost zero. Without the directory-version amortization, negative caching is either unsound or as expensive as no caching at all.

Push invalidation for negatives is coarse — "directory X changed" — which is both cheaper and sufficient, since the client re-derives which negatives to drop from the new `dirver`.

Negative leases use `min(D_class, D_neg_max)` with `D_neg_max` defaulting to 5 s even in `relaxed`, because a stale negative (a file that exists but appears not to) breaks more software than a stale positive.

### 10.5 Granularity: two independent lease domains

**Directory lease** covers name→inode bindings and `dirver`. Bumped by `create`, `unlink`, `rmdir`, `rename`, `link`, `symlink` within that directory.

**Inode lease** covers attributes and `manifest_id`. Bumped by `write`, `truncate`, `setattr`, `chmod`, `chown`, `link`/`unlink` (via `nlink`).

**A parent `dirver` bump does not invalidate children's inode leases.** This is stated explicitly because the naive design — parent bump invalidates the subtree — makes a directory of 10,000 files re-fetch all 10,000 attribute entries on every single `create`, which is catastrophic for W1 (an ImageNet ingest touches one directory 1.28M times).

*Accepted consequence:* after `rename(a, b)`, a client holding a valid inode lease continues to serve the pre-rename `ctime` until that lease expires. `rename` changes `ctime` but not the data or any other attribute. We accept up to `D + ε` of `ctime` staleness outside `posix` and state it here rather than discovering it in a conformance run. In `posix`, `D = 0` and the question does not arise.

### 10.6 The `posix` class: `D = 0` and blocking recall

At `D = 0`, every lookup and attribute read revalidates synchronously. To keep this from being one RTT per `stat`, `posix` clients may hold **recallable leases**: a normal lease that the authority tracks in a *reliable* (not best-effort) registry and must recall before committing a conflicting mutation.

- Writer requests a mutation; authority sends `RECALL` to holders and waits for acknowledgement or for `D_recall + ε` to elapse (default `D_recall = 2 s`).
- A holder that does not acknowledge is presumed dead after the timeout; its lease is void and its next operation fails with `ATLAS_LEASE_LOST`, forcing a full revalidation. Clients treat `ATLAS_LEASE_LOST` on a dirty file as a data-loss condition and surface `EIO` on the next `write`/`fsync` — silently discarding a partition victim's writes is the failure mode this design is most concerned with avoiding.
- **This is the one place a partition causes a write stall**, bounded at `D_recall + ε`. It applies only to `posix` subtrees. That confinement is why classes exist.

### 10.7 Clocks

Leases are **durations on `CLOCK_MONOTONIC`**, never absolute wall-clock times, so the protocol has no NTP dependency and is immune to wall-clock jumps, leap seconds, and VM time warps.

`ε` bounds the *frequency* error between the client's and authority's monotonic clocks. Commodity TSCs are within ~100 ppm, so over a 30 s lease the divergence is ~3 ms; we set `ε = 500 ms` — three orders of magnitude of headroom — because the cost of a large `ε` is only slightly longer recall waits, while the cost of too small an `ε` is a correctness violation.

Two rules follow:
- The **client** must expire a lease no later than `D` after send. Suspended VMs are the hazard: on resume, `CLOCK_MONOTONIC` has advanced (Linux includes suspend time in `CLOCK_BOOTTIME`, not `CLOCK_MONOTONIC`), so the client additionally checks `CLOCK_BOOTTIME` and treats any gap exceeding `ε` as expiring **all** leases.
- The **authority** must not assume a lease is dead until `D + ε` after grant, measured on its own monotonic clock.

### 10.8 Model checking this first

The protocol in §10 is the highest-value thing to formally verify, and it is verifiable: it is small, it is entirely about message loss and timing, and its properties (G1–G3) are stateable as temporal formulas.

Phase 0 (§28) delivers a Quint specification covering: lease grant/expiry, best-effort push with arbitrary message loss and reorder, `dirver` bumps and negative-entry validity, the two lease domains, blocking recall with a dead holder, and clock skew bounded by `ε`. Checked properties: G1, G2, G3, and the recall-safety invariant that no conflicting mutation commits while a non-recalled `posix` lease may still be served.

**This is written before the FUSE mount.** The metadata transactions FDB gives us for free; the coherence protocol is where the design bugs live, and finding them in a model is orders of magnitude cheaper than finding them in a conformance run.

---

## 11. Read Path and Cache Hierarchy

Draft 1 listed a "regional cache" tier, which contradicts the principle (retained from Draft 1 §26, and correct) that **no design element may become a centralized bandwidth funnel**. A regional cache server on the data path is exactly such a funnel. Removed.

The hierarchy is:

1. **Kernel page cache** — via FUSE writeback cache and, on cache hits, passthrough (§21).
2. **Node NVMe cache** — content-addressed by `chunk_id`. Chunk immutability means no coherence protocol at all; the cache is a pure hash map with an LRU/LFU eviction policy.
3. **Peer NVMe (P2P)** — same-AZ peers first (§13).
4. **Origin object store** — range GET into a container at a known offset.

The only surviving "regional" construct is an **optional write-through replica bucket** in the object store — a bucket, not a server. It scales with the object store and funnels nothing.

### 11.1 P2P chunk exchange

Content addressing makes peer-to-peer trivially correct: a chunk hash is already a perfect P2P key, and any bytes that hash correctly are the right bytes regardless of who served them. No trust in peers is required.

- **Discovery:** a per-cluster tracker (`chunk_id` → holders), sharded and gossip-backed, colocated with the CSI driver's node plugin. Not on the data path — it returns peer addresses, not bytes.
- **Peer selection is AZ-aware and cost-aware**, which is not merely an optimization (§12.2): cross-AZ P2P can cost ~210× more than simply re-fetching from S3.
- Fetch policy: same-AZ peer → origin object store (if same-region) → cross-AZ peer → cross-region origin.

Multiple nodes starting the same job cold converge on the same chunks within a second or two, so P2P absorbs the thundering herd that would otherwise hit the object store's per-prefix request limits (~5,500 GET/s per prefix on S3), which is the real reason it is on the critical path.

---

## 12. Cost Model

Egress and request charges are the economics of a cross-cloud filesystem. Draft 1 mentioned them in one bullet. They belong in the architecture, because they change what the design must do — not just what it costs to run.

### 12.1 Egress

Cross-cloud egress runs $0.05–$0.09/GB (AWS) and $0.08–$0.12/GB (GCP). Replicating W3's 50 TB corpus from S3 to GCS is **$2,500–$4,500, one-time, per replica**. This is a budget line item requiring approval, not a config change, and the system must treat it that way (§12.3).

Pricing is not hardcoded: it comes from each backend's `Caps()` (§24.1), so the cost model evaluates whatever backends are actually configured. This matters more than it sounds, because the numbers are not uniform in kind — **Cloudflare R2 charges zero egress**, which does not shift the arithmetic below so much as delete it, and makes R2 a rational replication target for a corpus consumed from multiple clouds. A cost model with AWS prices baked in would never surface that.

### 12.2 Request charges, and why they are the design constraint

S3 GET is $0.0004 per 1,000 requests. For 1,000 nodes reading at 5 GB/s:

| Chunk size | GET/s per node | GET/s total | Cost/hour |
|---|---|---|---|
| 4 MiB, all cold | 1,192 | 1.19M | **$1,717** |
| 16 MiB, all cold | 298 | 298k | $429 |
| 4 MiB, 95% cache hit | 60 | 60k | $86 |

$1,717/hour in request charges alone, for a fleet whose compute might cost $30,000/hour — noticeable, and it is also 1.19M GET/s against a service that throttles around 5,500 GET/s per prefix, so the cold-4 MiB configuration does not merely cost more, **it does not work**.

Two conclusions the design is built around:

**Cache hit rate is the business case, not a performance metric.** The target is ≥95% steady-state for repeat-epoch training. The node cache, P2P layer, and packing (§14) exist to deliver that number; presenting them as latency optimizations understates them by an order of magnitude.

**Cross-AZ P2P is a cost regression unless it is free or replacing cross-region traffic.** For a 4 MiB chunk:

| Source | Cost per 4 MiB chunk |
|---|---|
| Same-AZ peer | $0 |
| Same-region S3 GET | $0.0000004 |
| Cross-AZ peer ($0.01/GB each way) | $0.0000839 — **210× S3** |
| Cross-region GET ($0.02/GB egress) | $0.0000839 |

So: same-AZ P2P always; same-region S3 in preference to cross-AZ P2P; cross-AZ P2P only when S3 is throttling or when the alternative is a cross-region fetch (where P2P and S3 cost the same per byte and P2P wins on latency). A naive P2P implementation that ignores AZ topology makes the bill worse while looking like an optimization — which is exactly the kind of error a design without a cost model produces.

### 12.3 Cost in the policy language

Draft 1's `placement:` accepted region lists and nothing else. Placement decisions with no cost dimension are not decisions.

```yaml
placement:
  subtree: /datasets/imagenet
  home_region: us-west-2
  class: immutable
  replicas:
    - region: us-west-2   # origin
    - region: europe-west4
      provider: gcp
      trigger: on_demand        # replicate on first access, not eagerly
  budget:
    egress_usd_per_month: 5000
    max_egress_gb_per_day: 2000
    request_usd_per_month: 500
    on_exceed: block            # block | degrade | alert
  cache:
    min_hit_rate: 0.90          # alert below; informs prefetch aggressiveness
```

Enforcement:
- **Admission control.** The replication controller estimates cost before executing and refuses (or defers, per `on_exceed`) work that would breach budget. `on_exceed: degrade` falls back to on-demand fetch instead of bulk replication.
- **Estimation up front.** `atlas placement estimate -f policy.yaml` prints projected one-time and recurring cost before anything runs. A 50 TB replication should never be a surprise on a bill.
- **Attribution.** Egress and request charges are attributed per subtree and exported as `atlas_egress_usd_total{subtree,src_region,dst_region}` and `atlas_requests_total{subtree,region,op}`. Without per-subtree attribution, budgets are unenforceable and the invoice is unactionable.

---

## 13. Placement and Replication

The replication controller reconciles observed placement against policy. Because chunks are content-addressed, replication is a set difference over chunk IDs, which makes it idempotent, resumable, and verifiable:

1. Enumerate chunk IDs reachable from the subtree's manifests.
2. Diff against locators already present in the destination region.
3. Estimate cost of the difference; check budget (§12.3).
4. Transfer, repack into destination-region containers, write destination locators.
5. Verify by hash. A replica that verifies is correct by construction — there is no manifest reconciliation step and no possibility of a silently divergent replica.

Interrupted replication resumes from the locator diff with no bookkeeping, because the diff *is* the bookkeeping.

---

## 14. Small Files, Packing, and Chunk Sizing

W1 is 1.28M files at ~107 KB. One object per file is the failure Draft 1's format baked in.

**Why it fails, in throughput terms rather than cost:** at ~20 ms S3 first-byte latency, one epoch of 1.28M individual GETs at concurrency `C` delivers `107 KB × C / 20 ms`. At `C = 256` that is **1.44 GB/s** — a hard ceiling well under the 5 GB/s target, reached not because of bandwidth but because of per-request latency. Hitting 5 GB/s would need ~1,000-way concurrency per node, which exhausts connection pools and file descriptors and collides with per-prefix request limits.

**Packed:** 140 GB in 128 MiB containers is 1,043 objects. A sequential pass is 1,043 GETs; 5 GB/s needs ~40-way concurrency. The workload stops being IOPS-bound.

### 14.1 The packer

- Files below `small_file_threshold` (default 1 MiB) are stored as a single whole-file chunk and packed into shared containers.
- **Packing order preserves directory locality.** Chunks are packed in the order their files appear in the directory's lexicographic dentry order, so a sequential directory scan reads contiguous container ranges and a single container GET satisfies thousands of consecutive files.
- Containers seal at 128 MiB or on publish, whichever comes first.

### 14.2 Shuffled access

Training shuffles, which appears to defeat locality packing. Two answers:

1. **Packing never makes random access worse.** A random single-file read is a range GET at a known offset into a container — the same one request as the unpacked case, with an identical latency profile. Packing is a strict improvement on sequential access and neutral on random.
2. **Container-granular shuffling is the recommended access pattern**, and it is what tf.data and WebDataset already do: shuffle container order, read whole containers, shuffle within a decode buffer spanning several containers. Statistically adequate for SGD and it keeps the sequential-read economics. `atlas dataset shuffle-plan` emits a container order for an epoch given a seed, so the sampler and the prefetcher agree on order and prefetch is never wasted.

### 14.3 Chunk size

The real tension, stated plainly: content-defined chunking wants small variable chunks for dedup; ML sequential reads want large aligned chunks; manifest size wants large chunks; small-file packing wants whole-file chunks. There is no single right answer, so it is a per-subtree policy with defaults chosen per workload.

| Subtree kind | `chunk_size` | Rationale |
|---|---|---|
| Default | 4 MiB | Balanced; aligns with S3 multipart and typical readahead |
| `kind: checkpoint` | 16 MiB | 500 GB → 29.8k entries, 1.2 MB manifest (§5.3) |
| `kind: dataset` (small files) | whole-file, packed | §14.1 |
| `dedup: aggressive` | 1 MiB | Only where measurement justifies it (§15) |

### 14.4 Migration risk this removes

Retrofitting the chunk → container indirection after Phase 1 would require rewriting every manifest ever published, which for `immutable` subtrees means changing content-addressed objects that other manifests and snapshots reference by hash. It would be a full-corpus rewrite with no incremental path. The indirection costs one map lookup now and is not negotiable later — which is why it lands in Phase 1 (§28) rather than being scheduled when packing is needed.

---

## 15. Deduplication

**Default: whole-file content hashing plus fixed-size aligned block hashing. Content-defined chunking (FastCDC) is off by default, behind a per-subtree flag, and gated on measurement.**

Draft 1 assumed CDC. For the headline workload the expected dedup gain is near zero, and it is worth being specific about why: consecutive safetensors checkpoints differ in nearly every byte, because training updates essentially every parameter. Dense float tensors do not dedup — the data has no repeated-substring structure — and CDC's variable boundaries buy nothing against data that changes uniformly. Meanwhile CDC costs a content-scan on every write, produces variable-length chunks that fragment the container layout, and inflates manifests.

The real dedup wins in these workloads are **whole-file identity**:
- Unchanged files across dataset versions (v1 → v2 of a corpus typically shares >95% of files).
- The same public dataset published independently by two teams.
- Base model weights shared across many fine-tunes.

Whole-file and fixed-block hashing capture all of these at a fraction of the complexity.

**Gate for enabling CDC on a subtree:** a measured dedup ratio > 1.15× on a representative sample, produced by `atlas dedup-analyze <path>`, which computes whole-file, fixed-block, and CDC ratios offline without writing anything. Measure before paying.

---

## 16. Write Path, Publish, and Durability Barriers

### 16.1 Write path

1. Client buffers writes; chunker emits chunks at the subtree's `chunk_size`.
2. Chunks are hashed, checked against local cache and the locator index, and uploaded only if absent — deduplication happens on the write path, not as a background pass.
3. Uploaded chunks accumulate into containers. Each carries an **upload epoch** (§19.2).
4. On `close` or barrier, the manifest is built, uploaded as a blob, and the inode's `manifest_id` is updated in a single home-region FDB transaction.

The manifest swap is atomic: a reader sees either the old manifest or the new one, never a partial file. This is the property that makes checkpoint writes safe without a write-to-temp-then-rename dance, though that dance also works.

### 16.2 `fsync(global)` is not expressible — and the fix

Draft 1 wrote `fsync(global)`. `fsync()` takes no flags; there is no such call. Real mechanisms, all provided, one canonical semantic:

- **Primary:** `ioctl(fd, ATLAS_IOC_PUBLISH, &atlas_publish_req)` where the request names the durability predicate to satisfy.
- **CLI:** `atlas publish <path>` — opens `O_PATH`, issues the ioctl. This is the interface most users touch and it matches how a dataset actually gets published.
- **Fallback:** writing to the extended attribute `user.atlas.publish` on the file or directory, for runtimes without ergonomic `ioctl` (notably JVM and Python without `fcntl` gymnastics). Same semantics, worse ergonomics.
- **Shell:** the per-mount control file `.atlas/control`.

The semantic split is fixed and small:

| Call | Meaning | Returns when |
|---|---|---|
| `fsync(fd)` | Durable in home region | Chunks in origin object store; manifest committed in home-region FDB |
| `ATLAS_IOC_PUBLISH` | Durable per placement policy | The subtree's placement policy durability predicate is satisfied (e.g. present in N regions) |

`fsync()` never blocks on cross-region replication. A checkpoint writer calling `fsync()` in a training loop must not stall for a transcontinental transfer, and a publisher who wants that guarantee must ask for it explicitly.

### 16.3 `O_APPEND`

Atomic append needs serialization at the authority.

- **`posix` class:** supported and atomic. `write` on an `O_APPEND` fd runs a home-region transaction that extends `content_size` by `n` and returns the allocated offset; the client then writes chunks covering `[offset, offset+n)`. Concurrent appenders get disjoint ranges. Cost is one metadata RTT per append, which is what atomicity costs.
- **Other classes:** best-effort. Concurrent appends from different nodes may interleave or lose data. Documented, and `atlas doctor` warns on `O_APPEND` opens outside `posix`. This is the same guarantee NFS gives, and for the same reason.

---

## 17. Locking

- **`posix` class:** `fcntl` byte-range locks (POSIX and OFD) and `flock` are held at the home-region authority, leased, and released on lease expiry. A client that loses its lease loses its locks and receives `ATLAS_LEASE_LOST` — surfaced as `EIO` on the next operation on that fd, never silently.
- **Mandatory locking:** not supported. It is deprecated in Linux and unimplementable across this topology.
- **Other classes:** governed by the mount option `lock=local|global|error` (default `local`).
  - `local` — locks are node-local. Correct for the common single-node case (SQLite in a pod, a lockfile guarding a local process group), silently insufficient for cross-node mutual exclusion. Chosen as the default because `ENOLCK` breaks a large amount of software that only ever locks locally, and because the alternative default silently breaks *more* software than it protects.
  - `global` — forwards to the authority; requires the subtree be `posix`, else `EINVAL` at mount.
  - `error` — returns `ENOLCK` for any lock attempt, for operators who want cross-node locking failures to be loud.

The default is documented in the mount man page, in `atlas doctor` output, and in a one-time log line at mount. Silent partial locking is the sharpest edge in this design and it is treated as such.

---

## 18. Directory Scale

### 18.1 `readdir` at 10M entries

Dentries are stored as `(ns, dir_inode, name) → inode_id` in FDB, so lexicographic key order gives natural pagination without a secondary index.

- **Page size:** 1,000 entries, additionally capped at 256 KB per page.
- **Cookie:** opaque, encoding the last name returned plus the `dirver` at scan start. It is **name-based, not snapshot-based**, and this is essential: a 10M-entry scan cannot run inside FDB's 5 s transaction limit, so it spans many transactions and cannot hold a read version across them. A snapshot-based cookie would be unresumable.
- **`dirver` change does not invalidate an in-progress scan.** The cookie survives concurrent mutation; the recorded `dirver` is advisory, used only to report whether the scan was concurrent-modified.

**Guarantee:** an entry present for the entire duration of the scan is returned exactly once. Entries created or removed during the scan may or may not appear. This is what POSIX permits, it is what NFS and every other paginated network filesystem provides, and it is the strongest property implementable without holding a snapshot across an unbounded scan.

### 18.2 `readdirplus`

`readdir` returns names plus inode IDs; attributes come from a batched `REVALIDATE` (§10.3) over the page. A `ls -l` of 10M entries is 10,000 dentry pages plus 10,000 batched attribute fetches — two RPCs per 1,000 files rather than one per file.

### 18.3 Quotas

Per-subtree byte and inode quotas, enforced at the home region.

Exact quota accounting is normally expensive in a distributed metadata store; **home-region ownership makes it cheap**, because a subtree has exactly one authority and the counter can be updated in the same FDB transaction as the mutation using an atomic add. No cross-region reconciliation, no approximation, no drift.

- Counters at `(quota, subtree_id)`, updated transactionally with each mutation.
- Large writes use reserve-then-commit: reserve at `open`, settle at `close`, release the reservation on lease expiry if the client dies.
- Over quota returns `EDQUOT` at commit.
- Nested subtree quotas roll up to ancestors within the same home region; a quota may not span home regions (rejected at policy validation).

---

## 19. Garbage Collection

Mark-and-sweep against the metadata store as ground truth, retained from Draft 1 unchanged in principle — it is the correct call, because any reference-counting scheme has to be correct across client crashes, partitions, and partial writes, and mark-and-sweep simply does not have that failure mode.

### 19.1 Algorithm

1. **Mark.** Take a read version `V` per region. Enumerate all reachable `manifest_id`s from inode records; enumerate the chunk IDs each manifest references. Reachability includes graveyard entries not yet past grace (§19.3).
2. **Sweep.** A chunk is collectable if it is unreferenced at `V` **and** its container was sealed before `V − T_grace`.
3. **Compact.** Containers below a liveness threshold (default 50%) are repacked; only locators change (§5.4).

Sweeping at container granularity, gated on the container's seal time, is what makes the grace-period reasoning tractable: there is one timestamp per container rather than one per chunk.

### 19.2 The grace-period invariant, stated

Draft 1 implied this and did not state it, which left GC free to collect a slow writer's chunks out from under it.

> **Invariant GC-1:** `T_grace > T_write_max + D_max + ε`
>
> where `T_write_max` is the maximum permitted age of an uncommitted write session and `D_max` is the longest lease granted on a *mutable* subtree (`immutable`'s unbounded lease is irrelevant here: a sealed subtree has no uncommitted writes, and its chunks are reachable from sealed manifests until `unpublish`).

The invariant is worthless unless enforced, so it is enforced on both sides:

- **Client-side.** Every chunk upload carries an **upload epoch**. A client MUST fail any write whose write session began more than `T_write_max` ago, with `ESTALE`, and MUST NOT reference in a commit any chunk it uploaded more than `T_write_max` ago. A writer that is too slow fails loudly rather than committing a manifest pointing at swept chunks.
- **Authority-side.** Commit validates every referenced chunk's upload epoch against `T_write_max` and rejects the transaction with `ATLAS_STALE_UPLOAD` if any is too old. The client's cooperation is an optimization; the authority's check is the enforcement, because a buggy or malicious client must not be able to construct a dangling manifest.

Defaults: `T_write_max = 1 h`, `D_max = 30 s`, `ε = 0.5 s`, `T_grace = 24 h`. The margin is enormous relative to the invariant's requirement, deliberately — `T_grace` costs only storage, while violating GC-1 costs data.

A writer legitimately needing more than an hour (a very large checkpoint on a slow link) raises `T_write_max` per subtree, which raises the required `T_grace` for that subtree's regions. The policy validator enforces GC-1 and rejects configurations that violate it, so the invariant cannot be broken by configuration.

### 19.3 Hardlinks, `nlink`, and open-but-unlinked files

Draft 1 did not address this; it interacts directly with GC.

- `nlink` lives in the inode record and is updated transactionally with `link`/`unlink`.
- When `nlink` reaches 0, the inode moves to `(grave, delete_ts, inode_id)` rather than being deleted. The mark phase treats graveyard entries as **reachable** until `delete_ts + T_grace`, so their chunks survive.
- **Open-but-unlinked** (`unlink` on an open file, which POSIX requires to keep working): the inode stays reachable while any client holds an open handle. Open handles are themselves leased. If the holder dies, the open lease expires and the inode falls into the graveyard normally. There is no orphan case and no silly-rename, because the graveyard already provides the delayed-delete semantics that silly-rename exists to fake.
- Hardlinks across home regions or classes are `EXDEV` (§9), so an inode never has links in two authorities and `nlink` never needs cross-region agreement.

---

## 20. Security and Identity

Draft 1 specified `uid`/`gid`/`mode` on inodes and SPIFFE/group ACLs for authorization, with no reconciliation between them. They are two authorization models and only one can be the security boundary.

**The security boundary is SPIFFE identity plus subtree ACLs, evaluated at the authority. `uid`/`gid`/`mode` are presentation-layer POSIX attributes.**

- Each mount has an identity binding: the mount's SPIFFE ID (from the workload API) determines what that mount may access. ACLs are attached to subtrees, keyed by SPIFFE ID or group, evaluated at the home-region authority, and cached under the same lease machinery as everything else (§10).
- `uid`/`gid`/`mode` are stored per-inode so that `ls -l`, `tar`, `rsync`, and build tools behave. Within a mount they are enforced by the kernel as usual. They are **not** the cross-cluster security boundary and a client that lies about them gains nothing, because the authority never consults them.
- **No global uid space.** Each mount has an idmap (NFSv4-style) translating on-disk owner identities to local uids, with a default squash to the mount's identity. Cross-cluster, cross-cloud uid reconciliation is a genuine swamp — it requires an organization-wide identity authority that most organizations do not have — and this design does not enter it. The cost is that a file's `uid` may render differently in two clusters; the benefit is that authorization is correct in both.

`atlas stat --explain <path>` prints both the POSIX bits and the effective ACL decision with the identity used, so the two models can be inspected side by side rather than confused.

---

## 21. Client and Performance

### 21.1 Targets, with mechanisms

Draft 1 claimed ">5 GB/s per node" with no mechanism. That number is achievable but not by default, and not on every path. Restated as two falsifiable targets:

- **Cache-hit read: > 5 GB/s per node**, via FUSE passthrough. This path is local NVMe speed minus overhead.
- **Cold read: > 2 GB/s per node**, at ≥64-way chunk concurrency, bounded by network and object-store behavior rather than by the client.

Splitting the target is the point: a single number hides the fact that the cold path has a different ceiling set by different hardware, and only the split version is testable.

### 21.2 Required mechanisms and kernel floor

**Minimum kernel 6.6 LTS. Recommended 6.9+. Full performance requires 6.14+.**

| Mechanism | Kernel | Effect |
|---|---|---|
| `FUSE_PASSTHROUGH` | 6.9+ | Cached files hand the kernel a backing fd; reads bypass the FUSE daemon entirely. Single largest lever. |
| FUSE over `io_uring` | 6.14+ | Removes per-request `/dev/fuse` read/write syscalls and context switches |
| `max_read`/`max_write` = 1 MiB | 4.20+ (`FUSE_MAX_PAGES`, 256 pages) | Fewer, larger requests |
| `writeback_cache` | 3.15+ | Kernel absorbs small writes |
| `parallel_dirops` | 4.7+ | Concurrent lookups in one directory |
| `splice_read`/`splice_write`/`splice_move` | long-standing | Avoids a copy on the non-passthrough path |
| `clone_fd` + per-core threads | long-standing | One `/dev/fuse` fd per core; one `io_uring` ring per core on 6.14+ |

Below 6.9, the client runs the splice path and the cache-hit target drops to roughly 2.5 GB/s. This is stated so the number is not quoted without its precondition.

Passthrough is what makes the cache-hit target reachable *and* what makes `mmap` work (§21.3), which is why the materialize-then-passthrough design in the next section is central rather than an optimization.

### 21.3 `mmap` and the framework fast path

Model loading is `mmap`-heavy — safetensors is `mmap`-based by design, and `torch.load`, `numpy.memmap`, and Arrow/Parquet readers all map files. This is how most bytes in W2 actually arrive, and `mmap` over a network-backed FUSE file is the worst case for this architecture:

- Page faults are synchronous and largely serialize; there is no concurrency for the client to exploit.
- Kernel readahead does not know about chunk boundaries, so a fault often triggers a partial chunk fetch and the rest is fetched again on the next fault.
- `MAP_POPULATE` helps but blocks for the whole mapping, converting a lazy load into a stall.
- Write-back of a dirty shared mapping has no natural commit point, so there is nowhere obvious to build a manifest.

**Design: materialize, then pass through.**

1. When a file in a subtree marked `prefetch: whole-file` is opened, or when an `mmap` is detected on a file below `mmap_materialize_threshold` (default 64 GiB), the client materializes the whole file into local NVMe with full chunk-level concurrency — the read pattern the client is good at.
2. It then enables passthrough on the materialized file. From that point the mapping is an ordinary mapping of an ordinary local file: real page cache, real readahead, no FUSE involvement in the fault path.

This converts the worst path into the best one, and it is the strongest argument for treating the 6.9 kernel floor as a requirement rather than a recommendation.

**Escape hatch.** A small number of libraries generate most bytes, so bypassing FUSE for them may be worth more than tuning FUSE:

- `libatlas` with `atlas_open_mapped(path)` returning an fd to the materialized local file, plus explicit prefetch and chunk-range hints. A framework integration is a few dozen lines.
- An `LD_PRELOAD` shim intercepting `open`/`mmap` for known patterns (safetensors, `torch.load`, `numpy.memmap`, Arrow) that routes through the same path with no application change.

The shim is a compatibility bridge, not the interface; the library is the supported path. Both are Phase 2 deliverables, ahead of general FUSE tuning, because the measured byte share justifies it.

---

## 22. Kubernetes: CSI, PersistentVolumes, and Claims

A CSI driver in two parts: a **controller plugin** (provisioning, quota, snapshots, placement reconciliation) and a **node plugin** DaemonSet (mounts, local NVMe cache, P2P tracker membership).

### 22.1 The volume model: a PVC is a subtree

A PersistentVolume maps to an AtlasFS **subtree**, which is exactly the unit that already carries a home region (§7), a consistency class (§8), a placement policy (§13), and a quota (§18.3). Nothing new is invented for Kubernetes; the CSI driver projects existing subtree properties onto Kubernetes primitives.

```yaml
apiVersion: storage.k8s.io/v1
kind: StorageClass
metadata:
  name: atlas-scratch
provisioner: csi.atlas.io
volumeBindingMode: WaitForFirstConsumer   # home_region follows the pod
allowVolumeExpansion: true                 # expansion is a quota change
reclaimPolicy: Delete
parameters:
  parentPath: /pvc/{namespace}
  class: session                           # §8
  chunkSize: 4Mi
  placementPolicy: default-single-region    # §13
  backend: s3-us-west-2                     # §24
```

**Dynamic provisioning** creates `\{parentPath\}/\{pvc-uid\}` with the StorageClass's class, chunk size, and placement, and sets the quota from `resources.requests.storage`.

**Static provisioning** binds a PV to a pre-existing subtree, which is the important case for ML:

```yaml
apiVersion: v1
kind: PersistentVolume
metadata: { name: imagenet }
spec:
  accessModes: [ReadOnlyMany]
  capacity: { storage: 140Gi }
  csi:
    driver: csi.atlas.io
    volumeHandle: /datasets/imagenet@v3    # subtree, optionally version-pinned
    volumeAttributes:
      readOnly: "true"
      prefetch: whole-dataset
```

A version-pinned handle (`@v3`) resolves to a sealed manifest set and is immutable for the life of the volume, so a job's input cannot change under it mid-run. This is what dataset reproducibility looks like at the Kubernetes layer, and it costs nothing because `immutable` subtrees are already content-addressed snapshots.

**Ephemeral inline volumes** are supported for read-only dataset mounts, so a training Job can name a dataset without a PVC round trip.

### 22.2 Access modes are a projection of consistency classes

Kubernetes access modes are a coarse, three-valued approximation of what §8 expresses precisely. The driver **enforces the mapping at provisioning and at `NodePublishVolume`, and rejects incoherent combinations rather than mounting something that will quietly misbehave.**

| Access mode | `immutable` | `relaxed` | `session` | `posix` |
|---|---|---|---|---|
| `ReadOnlyMany` (ROX) | **ideal** — zero coordination | allowed | allowed | allowed (wasteful) |
| `ReadWriteOnce` (RWO) | rejected — `EROFS` | allowed | allowed | allowed |
| `ReadWriteOncePod` (RWOP) | rejected | allowed | allowed | allowed |
| `ReadWriteMany` (RWX) | rejected — use ROX | **opt-in only** (below) | allowed | allowed |

The row that matters is **RWX on `relaxed`**. RWX advertises concurrent multi-node read-write, and `relaxed`'s 30 s staleness bound silently violates what most software means by that. A blanket rejection would be wrong too, because the dominant ML pattern under RWX — every rank writing its own file into a shared directory — is perfectly safe at `relaxed` and forcing it to `posix` would make it needlessly expensive.

So RWX on `relaxed` requires an explicit contract:

```yaml
  parameters:
    class: relaxed
    rwxContract: disjoint-writers   # required for RWX on relaxed; else provisioning fails
```

Under `disjoint-writers` the driver mounts with `lock=error` (§17), so any attempt at cross-node locking fails loudly with `ENOLCK` instead of succeeding locally and providing no mutual exclusion. The declared contract and its enforcement match: a workload that violates the contract discovers it immediately, at the lock, rather than as corruption discovered later.

### 22.3 Capacity is a quota, and expansion is a metadata write

Object-store-backed capacity is elastic, so PVC capacity maps to the subtree quota (§18.3) rather than to any allocation:

- `resources.requests.storage: 100Gi` → subtree quota 100 GiB, exact, enforced at commit via FDB atomic add.
- Volume expansion is a quota update: a single metadata transaction, online, no node-side work, no filesystem resize. `NodeExpandVolume` is a no-op and the driver reports `VolumeExpansion: ONLINE`.
- Shrink is equally trivial (Kubernetes does not permit it; `atlas quota set` does).
- `NodeGetVolumeStats` reports used bytes and inodes straight from the quota counters, so `kubelet_volume_stats_*` are exact rather than sampled — again a consequence of one authority per subtree.

For ROX volumes over `immutable` subtrees the capacity field is descriptive only; there is nothing to enforce.

### 22.4 Snapshots and clones are free

This is where content addressing pays off most visibly against block-based CSI drivers, for which snapshot and restore are data copies.

- **`VolumeSnapshot`** → seal the subtree's manifest set and record it. Data copied: **zero**. Time: a metadata transaction. Size: the manifest references, not the bytes.
- **Clone / restore from snapshot** → a new subtree whose files reference the same chunk IDs. Copy-on-write is inherent: chunks are immutable, so a write to the clone produces new chunks and new manifests, and the original is untouched by construction. There is no CoW machinery to implement.
- **Restore is instant and is not a data movement**, so recovering a 50 TB dataset to a previous version is a metadata operation, not a 50 TB read-write cycle.
- GC correctness holds because the mark phase (§19.1) walks all reachable manifests, and a snapshot is a root. Chunks shared between a snapshot and a live subtree are reachable from both and survive until neither references them.

### 22.5 Topology and scheduling

- Node topology keys `topology.atlas.io/region` and `topology.atlas.io/zone` are published by the node plugin, and drive both volume topology and AZ-aware P2P peer selection (§11.1).
- `volumeBindingMode: WaitForFirstConsumer` lets dynamic provisioning choose `home_region` from where the pod actually lands, which is the difference between metadata at LAN latency and metadata across the WAN (§7.1). This should be the default in every StorageClass and the driver warns when it is not.
- For static PVs over an existing subtree, the driver publishes `nodeAffinity` preferring regions where the subtree is placed. Mounting from elsewhere is permitted but surfaces a warning event carrying the estimated egress cost (§12.3), so a cross-region mount is a visible decision rather than a line on next month's bill.
- **Scheduler locality** — nodes advertise per-dataset cached-chunk coverage as an extended resource; a scheduler plugin scores nodes by coverage. Similar to Fluid, and there is no reason to be novel here. The difference is that coverage is computed over content-addressed chunk IDs, so it is exact and automatically shared between datasets that overlap.

### 22.6 `AtlasDataset` CRD

Declares a subtree, its placement policy, and prefetch intent; reconciled by the replication controller (§13) under the cost budget (§12.3). Prefetch and pinning are admission-controlled against cache capacity, so over-subscription is rejected up front rather than becoming cache thrash at runtime.

The CRD and the PV are two views of one subtree: `AtlasDataset` expresses *placement and lifecycle*, the PV expresses *how a pod mounts it*. A dataset typically has one `AtlasDataset` and many ROX PVs.

### 22.7 The FUSE-in-a-pod problem

The standard failure of FUSE-based CSI drivers: the mount lives in the node plugin's process, so upgrading or restarting the DaemonSet kills every mount on the node and every workload pod gets `ENOTCONN` — "transport endpoint is not connected" — from which POSIX offers no recovery.

Design:

- Mounts run in **dedicated mount pods whose lifecycle is independent of the CSI node plugin**, with `mountPropagation: Bidirectional` to the host and `HostToContainer` into workload pods. Upgrading the driver does not touch mounts.
- A small **supervisor process holds the `/dev/fuse` file descriptor** and passes it to the mount daemon over `SCM_RIGHTS`. The supervisor outlives daemon restarts, so the kernel-side FUSE connection survives a daemon upgrade or crash.
- On restart the daemon **rebuilds its entire in-memory state from the metadata store**. This works only because AtlasFS inode numbers are stable metadata-store identifiers rather than ephemeral pointers into daemon memory — a property of §6 that was not chosen for this reason but makes seamless restart possible. A daemon whose inode numbers were pointer-derived could not do this at all.

Honest limitation: FUSE has no request replay. Requests in flight across the restart window return `EINTR` or `EIO` to the caller. `libatlas` retries these transparently; applications using raw POSIX see a transient error on operations that happened to be in flight. This is a bounded, sub-second window on daemon restart, and it is a substantial improvement on losing the mount entirely — but it is not zero, and workloads that cannot tolerate a transient `EIO` should be drained before a driver upgrade.

### 22.8 `fsGroup`: why the driver declares `None`

The `CSIDriver` object sets `fsGroupPolicy: None`, deliberately.

The default `fsGroupChangePolicy: Always` makes kubelet **recursively `chown` the entire volume** at mount. On W1's 1.28M-file dataset that is 1.28M metadata mutations against the home-region authority on every pod start — minutes of latency, a quota-sized burst of writes, and a `dirver` bump storm that invalidates every cached dentry in the tree (§10.5). On an `immutable` subtree it is not merely slow but impossible, and would fail the mount.

Ownership is instead handled where §20 already puts it: the per-mount idmap, configured through volume attributes.

```yaml
      volumeAttributes:
        uid: "1000"
        gid: "1000"      # presentation-layer squash target; O(1), not O(files)
```

Authorization remains SPIFFE plus subtree ACLs (§20) and is unaffected by these values. This is the concrete payoff of separating the security boundary from the POSIX presentation layer: Kubernetes' ownership model becomes a mount-time constant instead of a recursive write.

---

## 23. NFS Export

Not every consumer can run FUSE. Managed Kubernetes control planes restrict privileged DaemonSets and `/dev/fuse` access; VMs and workstations outside the cluster have no CSI driver; non-Linux clients exist. An NFSv4.1 export makes AtlasFS mountable by anything.

### 23.1 Architecture, and the funnel problem

An NFS server on the data path is precisely the centralized bandwidth funnel that §11 removed and that this design forbids. That tension is real and is resolved by construction rather than waved at:

- **The export tier is horizontally scaled and shares no state on the data path.** Each instance is a full AtlasFS client with its own NVMe cache and P2P membership (§11.1). Because chunks are content-addressed and immutable, any instance can serve any read, and correctness never depends on which instance a client reaches.
- **Client affinity by consistent hash on export path** keeps cache locality high across a scaled tier.
- Aggregate bandwidth scales with instance count. There is no shared component that all bytes traverse.

Implementation is **nfs-ganesha with an `FSAL_ATLAS` layer over `libatlas`**, not a from-scratch NFS server. NFSv4.1 sessions, delegations, ACLs, and RPCSEC_GSS are a large amount of protocol surface that is not where this project's novelty lies.

**Honest cost:** the export is still a proxy hop. Expect **50–70% of native FUSE throughput**, an extra network round trip on the metadata path, and doubled east-west bandwidth for cache misses. The export is a compatibility and reach path, not the scale path, and §1.1 says so. Where both are available, CSI is the right answer.

**pNFS considered and rejected for v1.** A flexible-file layout would let clients read from data servers directly and remove the proxy hop, but AtlasFS's "data servers" are object stores requiring credentials and chunk-locator resolution that an NFS client cannot perform. Making this work would mean an AtlasFS-aware NFS client, which is a FUSE client with extra steps.

### 23.2 Consistency composes, it does not compound

An NFS client runs its own attribute cache (`acregmin`/`acregmax`, defaults 3–60 s; `acdirmin`/`acdirmax`, 30–60 s) and its own close-to-open rule. Naively stacked, this adds to AtlasFS's staleness: a `relaxed` subtree (D = 30 s) exported with default mount options can be stale by **up to ~90 s**, which is nobody's intent.

Two things prevent that:

1. **Delegations are driven by leases.** Ganesha grants an NFSv4 read delegation only while the instance holds a valid AtlasFS inode lease, and an AtlasFS invalidation (§10.2) or lease expiry triggers delegation recall. The NFS layer therefore inherits §10's coherence rather than layering a second, independent protocol on top of it. This is the whole reason to use NFSv4.1 rather than v3.
2. **The driver derives mount options from the class** and publishes them in the export, rather than leaving `actimeo` to chance:

| Subtree class | Exported with | Effective staleness |
|---|---|---|
| `immutable` | `actimeo=3600,nocto,ro` | none (content cannot change) |
| `relaxed` | `actimeo=30` | ≤ D + ε |
| `session` | `actimeo=5` | ≤ D + ε, close-to-open preserved |
| `posix` | `actimeo=0` + delegations | as G5, at the cost of `actimeo=0` |

The guarantee for an NFS client is the same G1–G5 as for a FUSE client of that class, provided the published mount options are used. A client that overrides `actimeo` upward gets correspondingly weaker guarantees; `atlas nfs check` inspects a live mount and reports the effective bound.

### 23.3 Locking and `O_APPEND`

NFSv4 byte-range locks map onto §17: they are honored only for `posix` subtrees, where the ganesha instance forwards them to the home-region authority. For other classes the export follows the subtree's `lock=` policy, and `lock=error` returns `NFS4ERR_NOTSUPP` rather than granting a lock that means nothing across instances. NFSv3/NLM locking is not offered — it is advisory, stateful in the wrong ways, and its failure modes across a scaled export tier are not defensible.

### 23.4 Identity: the real limitation

**With `AUTH_SYS`, per-client authorization collapses to per-export authorization.** The uid and gid on an `AUTH_SYS` request are asserted by the client and unauthenticated, so the only trustworthy identity is the ganesha instance's own SPIFFE identity — meaning every client of an export shares one authorization identity. This is a genuine reduction in the security model relative to the CSI path (§20), and it is a property of NFS, not something this design can engineer away.

Consequences, stated rather than buried:

- `AUTH_SYS` exports are safe only within a trust boundary, and only one authorization domain per export. The controller refuses to create an `AUTH_SYS` export over a subtree whose ACLs distinguish between principals, because such an export cannot honor those ACLs.
- **`RPCSEC_GSS` with krb5p restores per-client authorization**, via a Kerberos principal → SPIFFE identity mapping evaluated per request. This is required for multi-tenant exports and the controller enforces it there.
- NFSv4 `user@domain` idmapping composes with §20's per-mount idmap; the NFSv4 domain becomes the idmap namespace.

### 23.5 NFS as a Kubernetes PV

The two asks meet here. An AtlasFS NFS export can back a stock `nfs` PersistentVolume or `csi-driver-nfs`, so a cluster that cannot install the AtlasFS CSI driver — managed control plane, no privileged DaemonSet, no `/dev/fuse` — can still consume AtlasFS with zero cluster-side installation.

| | AtlasFS CSI (§22) | NFS export as PV |
|---|---|---|
| Install | Privileged DaemonSet, `/dev/fuse` | None cluster-side |
| Throughput | Full (§21.1) | 50–70% |
| Node-local NVMe cache, P2P | Yes | No — cache lives in the export tier |
| `mmap` passthrough (§21.3) | Yes | No |
| Snapshots, quota-as-capacity | Yes | No (plain NFS PV) |
| Per-pod authorization | Yes (SPIFFE) | Per-export only, unless krb5p |
| RWX | Per §22.2 | Native |

The honest summary: NFS-as-PV is the compatibility deployment. It trades the cache hierarchy and the authorization model for zero installation, and for W2's `mmap`-heavy checkpoint loading it gives up the single largest performance mechanism in the design. Use it where CSI is not possible, and know what it costs.

---

## 24. Pluggable Backends

### 24.1 The interface is small on purpose

The container-object design (§5.5) means AtlasFS never asks a backend for rename, directory semantics, partial overwrite, append, or listing on the read path. Objects are written once, sealed, read by range, and eventually deleted. That is the whole requirement, and it is why the interface is six methods and why a new backend is days of work rather than months.

```go
type Backend interface {
    Get(ctx context.Context, key string, off, length int64) (io.ReadCloser, error)
    Put(ctx context.Context, key string, r io.Reader, size int64, o PutOpts) (ObjectInfo, error)
    Delete(ctx context.Context, keys []string) ([]DeleteResult, error)
    List(ctx context.Context, prefix, cursor string, limit int) (ListPage, error)
    Stat(ctx context.Context, key string) (ObjectInfo, error)
    Caps() Caps
}

type Caps struct {
    ConditionalPut  bool     // If-None-Match: * / ifGenerationMatch=0
    MultipartUpload bool
    BatchDelete     int      // max keys per request; 0 = one at a time
    MaxObjectSize   int64
    StorageTiers    []string
    // feeds the cost model, §12
    EgressUSDPerGB, GetUSDPer1k, PutUSDPer1k float64
}
```

### 24.2 Capability matrix

| Backend | Conditional put | Multipart | Batch delete | Tiers | Notes |
|---|---|---|---|---|---|
| **S3** | `If-None-Match: *` | MPU | 1000/req | STANDARD, IA, GLACIER… | Reference implementation |
| **GCS** | `ifGenerationMatch=0` | Resumable + compose | Batch endpoint | STANDARD, NEARLINE, COLDLINE, ARCHIVE | |
| **Azure Blob** | `If-None-Match: *` | Block blobs | 256/batch | Hot, Cool, Cold, Archive | |
| **S3-compatible** (MinIO, Ceph RGW, Cloudflare R2, Wasabi) | Probe at startup | MPU | Varies | Varies | R2 egress is $0, which materially changes §12 |
| **POSIX** (local NVMe, NFS, EFS, Filestore, Lustre) | `O_EXCL` + rename | n/a — write in place | `unlink` loop | n/a | Covers NFS-as-backend, on-prem, and test fixtures |

Capabilities are **probed at startup, not assumed from the endpoint**, because S3-compatible implementations vary and a wrong assumption about conditional put is a correctness question, not a performance one.

### 24.3 Missing capabilities degrade, they do not disqualify

The metadata store — never the object store — is the source of truth for whether a container is sealed and what a chunk's locator is. That single decision makes every backend capability optional:

| Missing | Consequence |
|---|---|
| Conditional put | Seal races produce a duplicate container, which is wasted space reclaimed by GC compaction (§19.1), never a correctness problem. Conditional put is an optimization. |
| Multipart upload | Container target size drops to `MaxObjectSize`; more, smaller containers; request cost rises and is reflected in §12 |
| Batch delete | GC sweep issues one request per object; slower and costlier, modeled and reported, never incorrect |
| Storage tiers | Placement policy tier directives are rejected at validation rather than silently ignored |

### 24.4 Why a weakly-consistent backend is safe

Directly relevant to the POSIX/NFS backend, whose cache consistency is weak:

- **Reads are self-verifying.** Bytes either hash to the chunk ID or they do not. A failed verification falls through to the next source in the hierarchy (§11) exactly as a cache miss would. A backend cannot serve wrong data undetected.
- **`LIST` is used only by GC sweep**, and only ever to *reduce* the candidate set. A stale listing that omits an object delays its collection; a listing that includes an already-deleted object produces a harmless repeat delete. Neither can cause premature deletion, because deletion eligibility comes from the metadata store plus the grace period (§19.2), not from the listing.
- **No read-after-write dependency on the backend.** A chunk is referenced only after its locator commits in the metadata store, which happens after the `Put` returns.

So NFS's weak cache consistency, or an S3-compatible implementation with eventually-consistent listing, costs efficiency and never correctness. This is a property of content addressing rather than of careful coding, which is why it holds for backends nobody has written yet.

### 24.5 The metadata store is also an interface, within limits

Required: serializable transactions over an ordered keyspace, range scans, atomic add, and transactions of at least a few MB. Push invalidation (§10.2) is best-effort and runs in-process, so no watch primitive is needed.

| Store | Verdict |
|---|---|
| **FoundationDB** | Default. Per region (§7). |
| **TiKV** | Viable; same shape, different operational profile |
| **PostgreSQL** | Viable for single-region deployments below ~10M inodes, using `SERIALIZABLE`. Materially lowers the barrier for on-prem and small Kubernetes clusters that will not run FDB. |
| **etcd** | **Not viable.** ~8 GiB practical database limit, no range transactions at filesystem scale, and it is the cluster's own control plane — filesystem metadata there is an outage waiting to happen. Stated explicitly because it is the first thing Kubernetes users ask. |

The interface exists to make PostgreSQL possible, which is what makes a small on-prem deployment possible. It is not intended to make every KV store possible.

---

## 25. Observability

Non-negotiable metrics, because several are the only way to know whether the design's premises hold in production:

- `atlas_cache_hit_ratio{tier,subtree}` — the business case (§12.2).
- `atlas_egress_usd_total{subtree,src_region,dst_region}`, `atlas_requests_total{subtree,region,op}` — budget enforcement.
- `atlas_metadata_rtt_seconds{home_region}` — detects subtrees homed in the wrong region, the main operational failure of §7.
- `atlas_lease_staleness_seconds` histogram — measures actual staleness against G1's bound.
- `atlas_invalidation_delivery_ratio` — how often best-effort push actually lands. If it collapses, latency degrades toward `D/2` without any correctness alarm, so this is the early warning.
- `atlas_stale_epoch_total`, `atlas_lease_lost_total` — rehoming and partition health.

---

## 26. Failure Modes

| Failure | Behavior |
|---|---|
| Home-region metadata cluster down | That subtree: reads served from cache until leases expire, then `EIO`; writes `EIO`. Other subtrees unaffected — the blast radius of a regional failure is the subtrees homed there, which is the main operational argument for §7. |
| Client partitioned from authority | Reads from cache until lease expiry, then `EIO`. Dirty `posix` data → `ATLAS_LEASE_LOST` → `EIO` on next write/`fsync`; never silently discarded. |
| Object store unavailable in one region | Fetch falls back to P2P peers, then other regions (paying egress, subject to budget). |
| Namespace-map Raft group unavailable | Existing mounts continue on cached map; rehoming and new subtree creation blocked. |
| Invalidation messages all lost | Correctness unaffected (§10.2); staleness degrades toward `D`. Visible as `atlas_invalidation_delivery_ratio` collapse. |
| Slow writer exceeding `T_write_max` | `ESTALE` client-side, `ATLAS_STALE_UPLOAD` at commit. Never a dangling manifest (§19.2). |
| Clock anomaly / VM suspend | `CLOCK_BOOTTIME` gap > `ε` expires all leases (§10.7). |
| CSI node plugin upgraded or crashes | Mounts unaffected — they live in mount pods with an independent lifecycle (§22.7). |
| Mount daemon restarts | Supervisor retains the `/dev/fuse` fd; daemon rebuilds state from the metadata store using stable inode IDs. In-flight requests return `EINTR`/`EIO` and are retried by `libatlas`; raw-POSIX callers see a transient error (§22.7). |
| Node drained with a mount pod on it | Workload pods evict first; the mount pod terminates after the last unpublish. A stuck unpublish is force-released after `T_unpublish_force` (default 120 s), which is safe because a partitioned client's leases expire anyway. |
| NFS export instance fails | Client reconnects to another instance; any instance can serve any read because chunks are content-addressed. In-flight NFSv4.1 sessions are re-established; delegations are recalled and re-acquired (§23.1). |
| Backend returns corrupt or wrong bytes | Hash mismatch on verification; the read falls through to the next source in the hierarchy exactly like a cache miss (§24.4). Cannot be served undetected. |
| Backend lacks a probed capability | Degrades per §24.3 — duplicate containers reclaimed by GC, smaller containers, slower sweep. Never a correctness failure. |

---

## 27. Verification and Correctness Validation

Draft 1 had twelve success criteria and none of them was "passes a POSIX conformance suite." Conspicuous, and corrected: these are **gating** criteria, not aspirations.

**Formal (Phase 0, before implementation):**
- Quint specification of §10, checked for G1, G2, G3 and recall-safety under message loss, reorder, partition, and clock skew ≤ ε.
- TLA+ specification of the rehoming protocol (§7.4), checked for the property that no write is accepted by two authorities across an epoch change.

**POSIX conformance (gating for the `posix` class, Phase 3):**
- **pjdfstest** — 100% pass within `posix` subtrees. Documented, enumerated exceptions only, each with a rationale.
- **fsx** (`fsx-linux`) — 24 h soak, zero data mismatches, with `mmap` operations enabled.
- **xfstests** — the `generic/` groups applicable to a network filesystem. Groups that test on-disk layout, `fsck`, quota tooling specifics, or local-only semantics are explicitly listed as N/A with justification rather than silently skipped. A skip list nobody has justified is indistinguishable from a failure list.

**Distributed correctness:**
- A Jepsen-style harness checking G1 and G2 against real-time histories under partition. Bounded staleness is testable: record operation intervals, assert every read's returned version was authoritative within `D + ε` of the read's start.
- Rehoming under concurrent write load: no lost or duplicated writes across the epoch change.
- GC-1 stress: writers deliberately exceeding `T_write_max` against an aggressive GC, asserting no dangling manifest ever commits.

**Performance and cost (gating for release):**
- Cache-hit read ≥ 5 GB/s per node on kernel 6.14+; cold read ≥ 2 GB/s at 64-way concurrency.
- W1 epoch time within 1.2× of local NVMe after cache warm.
- **Cost regression test:** GET requests per GB delivered, tracked per release. A change that silently increases request amplification is a cost regression and fails CI — this is the only way §12's premises stay true over time.

---

## 28. Roadmap and Honest Effort Estimate

**Reaching the §27 bar is a 4–6 engineer-year system as now specified.** That is a product, not a prototype, and the phases below are ordered so that value lands early and each phase is independently useful.

| Phase | Content | Est. |
|---|---|---|
| **0** | Quint/TLA+ specs (§10, §7.4); format specs (§5); backend interface (§24) + POSIX backend; `atlas dedup-analyze` | 0.3 y |
| **1** | Single-region, `immutable` only: content-addressed publish and read, packing + locators, atomic publish, FUSE read path, S3 backend, **CSI static ROX provisioning** (§22.1–22.5). **No general write path.** | 0.75 y |
| **2** | Coherence protocol (§10), `relaxed` + `session`, node cache, P2P, `libatlas` + `LD_PRELOAD` fast path, dynamic provisioning + quota-as-capacity + mount-pod lifecycle (§22.3, §22.7) | 1.25 y |
| **3** | Mutable write path, `posix` class, locks, `O_APPEND`, quotas, full GC; RWX access modes (§22.2); pjdfstest/fsx/xfstests gating | 1.25 y |
| **4** | Multi-region homing, namespace map, rehoming, replication controller | 1.0 y |
| **5** | Cross-cloud: GCS + Azure backends, cost governance (§12), scheduler locality, `AtlasDataset` CRD | 0.9 y |
| **6** | NFS export tier: `FSAL_ATLAS`, delegation/lease binding, krb5p identity mapping, scaled tier (§23) | 0.5 y |

Two notes on how this moved from Draft 2's 4.75-year estimate:

**Kubernetes support is now load-bearing rather than a bullet, and it moved earlier.** CSI static ROX provisioning lands in Phase 1, because a content-addressed immutable dataset that cannot be mounted as a PVC is not consumable by the workload it targets. That raises Phase 1 from 0.5 to 0.75 engineer-years and makes it a *deployable* increment instead of a demo — the single best change to the plan here.

**Phase 6 is the honest place to cut.** The NFS export is a reach and compatibility path (§23.1), not the scale path, and it is the only phase whose removal costs no capability that CSI already provides to in-cluster consumers. If scope has to shrink, cut Phase 6 first, then Phase 5's Azure backend. Do not cut Phase 0.

**Phase 1 remains where the value-to-effort ratio is by far the best**: read-mostly, immutable, content-addressed dataset and checkpoint storage with atomic publish, explicit placement, and a working PVC needs no leases, no `posix` class, no in-place writes, almost no GC, and essentially none of §10 — because immutable data is trivially cacheable. It is roughly 12% of the total.

Phases 3 and 4 together remain more than half the cost. Committing to full scope is a legitimate choice; committing to it without having priced Phase 3 is not.

---

## 29. Open Problems

Genuinely unresolved, as distinct from unspecified:

1. **Optimal `D` per workload.** The staleness/RPC tradeoff should be adaptive — a directory being actively mutated wants a short lease, a cold one a long lease — but adaptive lease duration interacts with `ε` and recall in ways the Phase 0 model should explore before it is implemented.
2. **Automatic home-region placement.** Choosing a home region from observed access patterns, and deciding when the benefit of rehoming exceeds its write-outage cost. Currently manual.
3. **Cross-region read replicas for mutable subtrees.** Would let a WAN-remote reader avoid revalidation RTTs while keeping single-authority writes. Requires a replica staleness guarantee composed with G1; not attempted here.
4. **`EXDEV` ergonomics at scale.** Copy-then-unlink across home regions is correct but slow for large trees. A server-side assisted move — locator-level, since chunks need not move — would help, and is not designed.
5. **Dedup measurement across real corpora.** §15's 1.15× gate is a judgement, not a measurement. `atlas dedup-analyze` exists to replace it with data.
