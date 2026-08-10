# Response to Draft 1 Review

Maps each review point to its resolution in `DESIGN.md`. Written against Draft 2; section numbers updated for Draft 3, which added Kubernetes CSI/PVC (§22), NFS export (§23), and pluggable backends (§24) and renumbered the tail. Section numbers on the left are Draft 1's; on the right, current.

**Scope decision:** full general-purpose POSIX, all critiques resolved, with an honest effort estimate (§28: 4–6 engineer-years). The review's narrow-wedge recommendation is preserved as Phase 1 and explicitly costed, so the option remains visible without being the plan.

---

## 1. Differentiator contradicts the metadata design

**Accepted in full — this was a real error, not a presentation problem.** A single global FDB cluster has one transaction subsystem; every mutation serializes through one region. Draft 1's dismissal of JuiceFS was false as designed.

Resolved by taking the first branch (regional autonomy is real), using the review's own suggestion:

- **§7** — independent FDB cluster per region; `home_region` as first-class subtree metadata, inherited at creation.
- **§7.2** — the conflict model is *no conflicts*: exactly one authority per mutable object at any time. Cross-home-region `rename`/`link` return `EXDEV`, eliminating cross-region 2PC entirely. CRDT directory merge is considered and rejected with reasons (no sound commutative merge for `rename`/`unlink`).
- **§7.3** — the Namespace Map: the one piece of global state (5-member Raft), small, rarely written, off the data path, with epoch fencing.
- **§7.4** — rehoming as a quiesce-based operation with a stated write-outage window.
- **§7.5** — explicit table of what is global vs. regional, including why chunk locators can be global (immutable chunks ⇒ additive-only ⇒ commutative).

Your `/users/adam` / `/datasets` example is the worked example in §7.1.

## 2. §9 is one paragraph and it's the entire thesis

**Accepted.** §10 is now the longest section, with every decision you named made explicitly:

- **§10.2 — lease vs. callback.** Leases are load-bearing; push invalidation is best-effort and explicitly *not* load-bearing. Rationale for rejecting both pure callbacks (partition ⇒ write outage) and pure TTL (median staleness `D/2`) is stated. The registry is lossy by design, which bounds authority memory at O(hot set).
- **§10.4 — negative caching.** Leased against directory version; one `GETDIRVER` validates every negative entry in a directory at once. The Python `sys.path` case is the worked example. Negative leases capped at 5 s even in `relaxed`, because a stale negative breaks more software than a stale positive.
- **§10.5 — granularity.** Two independent lease domains. Parent `dirver` bump explicitly does **not** invalidate children's inode leases, with the ImageNet-ingest reason. The accepted consequence (`ctime` staleness after `rename`) is stated rather than left to be found in conformance testing.
- **§8.1 — falsifiable guarantees.** G1–G6, phrased for testability, with §27 describing the harness that checks them.
- **§10.7 — clocks.** `CLOCK_MONOTONIC` durations from *send* time, no NTP dependency; `ε = 500 ms` against ~3 ms of realistic drift; `CLOCK_BOOTTIME` gap detection for suspended VMs.
- **§10.8 — model-check first.** Quint spec is Phase 0, before the FUSE mount, for exactly your reason.

## 3. Data model bakes in chunk = object

**Accepted, and taken now rather than deferred.**

- **§5.4** — manifests reference chunk IDs; a separate `chunk_id → {region → (container, offset, length, codec)}` locator map resolves identity to location. Per-region locators are what let cross-cloud replication rewrite locations without touching manifests, so a manifest published in AWS is byte-identical in GCP and still verifies by hash.
- **§14.4** — why this cannot be retrofitted: it would require rewriting every published manifest, including content-addressed objects other manifests reference by hash.
- **§5.3 — manifests are blobs.** Your FDB limits are the argument, with arithmetic: 500 GB @ 4 MiB = 119,209 entries = 4.8 MB, half the transaction limit across ~50 keys. Metadata store holds only `manifest_id`. Single-chunk files inline the chunk ID and skip the manifest object.
- **§14.3 — chunk size.** The four-way tension stated as a tension; per-subtree policy with defaults (4 MiB general, 16 MiB checkpoints, whole-file packed for small-file datasets).
- **§15 — dedup.** Your analysis adopted directly: CDC off by default, whole-file + fixed-block as default, and a measurement gate (>1.15× via `atlas dedup-analyze`) before enabling CDC. Dense float tensors and consecutive safetensors checkpoints named as the reason.
- **§14** — the small-file fix in throughput terms: 1.28M individual GETs at 20 ms latency ceilings at **1.44 GB/s** at 256-way concurrency; packed into 128 MiB containers it is 1,043 objects needing ~40-way. Locality-preserving packing, plus §14.2 on why shuffled access does not defeat it (packing is strictly better sequential, neutral random) with container-granular shuffling as the recommended pattern.

## 4. Competitive framing misses the actual competitors

**Accepted.** §3 replaces the CephFS/JuiceFS-CE/raw-S3 comparison with Alluxio, Fluid, JuiceFS EE, Mountpoint-caching, and 3FS, and answers "why not contribute to Fluid and JuiceFS" directly: Fluid has nowhere to put chunk-level placement because no engine beneath it exposes content-derived chunk identity; JuiceFS block IDs come from a counter, so content-addressing is a fork rather than a patch. §1 leads with the three claims and argues they are load-bearing together. §1.1 adds a *when not to use this* section, including "single region, single cloud — use Alluxio or JuiceFS."

## 5. ">5 GB/s per node" with no mechanism

**Accepted.** §21.1 splits it into two testable targets — **>5 GB/s cache-hit** (passthrough) and **>2 GB/s cold** at ≥64-way concurrency — because the two paths have different ceilings set by different hardware.

§21.2 names every mechanism and the kernel floor: 6.6 LTS minimum, 6.9+ for `FUSE_PASSTHROUGH`, 6.14+ for FUSE-over-`io_uring`, plus `max_read` 1 MiB, `writeback_cache`, `parallel_dirops`, splice, per-core `clone_fd`. Below 6.9 the cache-hit target drops to ~2.5 GB/s, stated so the number is never quoted without its precondition.

§21.3 gives `mmap` its own subsection, as you asked. The four specific pathologies are named, and the design is **materialize-then-passthrough**: fetch the whole file into NVMe with chunk-level concurrency, then enable passthrough so the mapping becomes an ordinary mapping of an ordinary local file. This converts the worst path into the best one and is the strongest argument for the 6.9 floor. The escape hatch is there and prioritized ahead of general FUSE tuning: `libatlas` with `atlas_open_mapped()` as the supported path, `LD_PRELOAD` shim as the zero-change bridge, both Phase 2.

## 6. No cost model

**Accepted — promoted to a full section (§12) that changes the design, not just the operating cost.**

- 50 TB S3→GCS = **$2,500–$4,500 one-time per replica**.
- 1,000 nodes @ 5 GB/s, 4 MiB cold = 1.19M GET/s = **$1,717/hour** — and 1.19M GET/s against a ~5,500 GET/s-per-prefix limit, so that configuration *does not work*, not merely costs more. Table shows 16 MiB and 95%-hit variants.
- **Cache hit rate stated as the business case**, target ≥95%, with packing and P2P framed as cost mechanisms rather than latency optimizations.
- New finding while working the numbers: **cross-AZ P2P costs ~210× a same-region S3 GET** for a 4 MiB chunk ($0.0000839 vs $0.0000004). §11.1 makes peer selection AZ- and cost-aware; a topology-blind P2P implementation raises the bill while looking like an optimization.
- **§12.3** — `placement:` gains `budget:` with egress/request ceilings and `on_exceed: block|degrade|alert`, admission control in the replication controller, `atlas placement estimate` for up-front pricing, and per-subtree cost attribution (without which budgets are unenforceable).
- **§27** — a cost regression test (GETs per GB delivered) gates CI, so §12's premises stay true over time.

## 7. Consistency-class boundary semantics

**Accepted, including your recommended answer.** §9 makes one uniform rule: any operation needing atomicity across a class *or* home-region boundary returns **`EXDEV`**, for the reason you gave — applications already handle it as the cross-device case. Table covers `rename`, `link`, `symlink`, I/O, and `rename` into `immutable` (`EROFS`). Mixed-class directories: a class boundary is always a subtree root; `readdir` served at the parent's class, `stat` at each child's; `atlas doctor` flags `posix` children under `relaxed` parents. `atlas stat --explain` prints a path's effective guarantee.

**§8.2** corrects the immutable claim exactly as you framed it: invalidation only on explicit `unpublish`, effective within bounded `T_unpublish` (default 24 h). Added: actual byte erasure completes by `T_unpublish + T_grace`, which is the number to quote for GDPR commitments, and `retention: legal_hold` rejects `unpublish`.

## Smaller items

| Item | Resolution |
|---|---|
| `fsync(global)` not expressible | **§16.2** — `ioctl(ATLAS_IOC_PUBLISH)` primary, `atlas publish` CLI, `user.atlas.publish` xattr fallback, `.atlas/control` for shells. Semantics split: `fsync()` = durable in home region; publish = satisfies the placement policy's durability predicate. `fsync()` never blocks on cross-region replication. |
| GC grace invariant unstated | **§19.2** — stated as GC-1: `T_grace > T_write_max + D_max + ε`. Enforced client-side (`ESTALE` on stale write sessions) *and* authority-side (upload epochs validated at commit → `ATLAS_STALE_UPLOAD`), because a buggy client must not be able to construct a dangling manifest. Policy validator rejects configs violating GC-1. |
| §11 regional cache contradicts §26 (Draft 1) | **§11** — regional cache servers removed from the data path. Hierarchy is page cache → node NVMe → same-AZ peer → origin. Only surviving regional construct is an optional write-through replica *bucket* (a bucket, not a server). **§11.1** — P2P with chunk hash as the P2P key, AZ- and cost-aware peer selection; also the answer to object-store per-prefix rate limits. |
| uid/gid/mode vs SPIFFE unreconciled | **§20** — SPIFFE + subtree ACLs are the security boundary, evaluated at the authority; `uid`/`gid`/`mode` are presentation-layer POSIX attributes the authority never consults. Per-mount idmap with default squash. Global uid mapping explicitly not attempted, with the reason. |
| Hardlinks / `nlink` / GC | **§19.3** — `nlink` transactional; `nlink=0` → graveyard, reachable until `delete_ts + T_grace`. Open-but-unlinked held by leased open handles; graveyard subsumes silly-rename. Cross-region hardlinks are `EXDEV`, so `nlink` never needs cross-region agreement. |
| `readdir` at 10M entries | **§18.1** — lexicographic dentry keys give natural pagination; **name-based** cookie, not snapshot-based, precisely because a 10M-entry scan cannot hold a read version across FDB's 5 s limit. `dirver` change does not invalidate an in-progress scan. Guarantee: entries present throughout the scan appear exactly once; concurrent mutations may or may not appear. |
| Quotas | **§18.3** — per-subtree byte and inode quotas, exact via FDB atomic add in the mutation's own transaction. Home-region ownership is what makes exactness cheap. Reserve-then-commit for large writes; `EDQUOT` at commit; quotas may not span home regions. |
| Byte-range locks, `O_APPEND` | **§17** — `fcntl`/OFD/`flock` at the authority in `posix`; mandatory locking unsupported; `lock=local|global|error` mount option elsewhere, default `local` with the reasoning for that default and loud documentation. **§16.3** — atomic `O_APPEND` in `posix` via an authority transaction returning the allocated offset; best-effort elsewhere, documented, warned by `atlas doctor`. |
| Whose clock bounds lease expiration | **§10.7** — client's `CLOCK_MONOTONIC` from send time; authority waits `D + ε` on its own monotonic clock before presuming a lease dead; `CLOCK_BOOTTIME` gap expires all leases. |
| No correctness validation in §34 | **§27** — pjdfstest (100% in `posix`, enumerated justified exceptions), fsx 24 h soak with `mmap`, applicable xfstests `generic/` groups with N/A justifications listed rather than silently skipped, plus Quint/TLA+ model checking and a Jepsen-style bounded-staleness harness. All gating. |
| Numbering breaks at §28; §31 restates §5; §32–34 overlap | Renumbered single level (1–29 as of Draft 3). Duplicated content merged; §27 is the single verification section, §28 the single roadmap. |
| ~30% ASCII blocks listing nouns | Removed. Remaining diagrams-as-text are the two format definitions (§5.2, §6 keyspace) and the policy YAML (§12.3), all of which carry content. Space went to §10 and §12. |

## On scope

Your closing argument is recorded rather than dismissed. **§28** commits to full scope with a 4–6 engineer-year estimate and a per-phase breakdown, and states plainly that **Phase 1 — read-mostly, immutable, content-addressed, atomic publish, explicit placement — is ~0.5 engineer-years, roughly 10% of the system, and delivers most of the ML value**, because immutable data needs no leases, no `posix` class, almost no GC, and essentially none of §10.

Phases 3 and 4 are more than half the total cost. §28 says committing to full scope is legitimate, but committing without having priced Phase 3 is not.

## Open, not resolved

**§29** lists what remains genuinely unresolved rather than merely unwritten: adaptive lease duration and its interaction with `ε` and recall; automatic home-region placement; cross-region read replicas for mutable subtrees; `EXDEV` ergonomics for large cross-region moves (a locator-level assisted move looks tractable and is not designed); and the fact that §15's 1.15× dedup gate is a judgement awaiting measurement.
