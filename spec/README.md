# Quint spec: metadata cache coherence protocol (DESIGN.md §10)

Phase 0 deliverable per §10.8 — checked before the FUSE mount was built,
because the coherence protocol is where the design bugs live, not the
metadata transactions themselves (those come from FDB for free).

**This model now has a real implementation, including the recall half.**
`pkg/coherence` implements the same shape checked here — per-holder
leases on a monotonic clock, directory-version-gated negative caching, a
best-effort/bounded push registry — and is wired into `pkg/fuseserver`
as the read-path cache for `relaxed`/`session` mounts.

§10.6's blocking recall, which this model checks via
`RequestPosixMutate` / `AckRecall` / `CompletePosixMutate`, is now
implemented too: `coherence.Recall` in Go, driven over a real gRPC
boundary by `pkg/mds` so that the holders being recalled are separate
processes rather than the same address space. That is what previously
made it unimplementable — a recall you can only issue to yourself proves
nothing. `pkg/mds`'s tests exercise both branches the model has: the
mutation blocking until every holder acks, and the mutation proceeding
once `DRECALL` elapses.

## What's modeled

`coherence.qnt` models:

- **Leases as durations on the client's own clock** (§10.7): a cache hit
  is only served while `time <= expiry`, checked against the *client's*
  clock, never a server-stamped timestamp.
- **Best-effort, lossy push invalidation** (§10.2): `Mutate` drops a
  nondeterministic subset of leases immediately (simulating push landing
  for some holders and not others, including the "landed for nobody"
  case). Correctness must not depend on it — the model checks this by
  never assuming push fires at all.
- **Negative caching validated against a directory version** (§10.4): one
  `dirver` bump invalidates every negative entry at once, without
  per-entry messages.
- **Blocking recall for the `posix` class** (§10.6): a posix mutation
  (`o1` in the model) can only commit once every current lease holder has
  acknowledged the recall or blown its `DRECALL` deadline.

Two independent lease domains (directory vs. inode, §10.5) are collapsed
to one per-object lease here — that split doesn't change the coherence
argument being checked, only which cache entries a bump touches.

## Checked properties

Run via the simulator (`quint run`), which does bounded random
exploration of the transition system rather than exhaustive symbolic
verification — no Apalache/Java toolchain was available in the build
environment this was written in. That is real, load-bearing evidence
(a violation *was* found and fixed, see below) but it is evidence, not a
proof: a clean run over N steps and M samples means no violation was
found in that exploration, not that none exists. Widening `--max-steps`
and `--max-samples` (or getting Apalache running for `quint verify`,
symbolic and exhaustive up to a bound) is the natural next increment,
not a rewrite.

| Invariant | Checks |
|---|---|
| `invLeaseNeverTrustedPastExpiry` | A cache hit is never served from a lease past its expiry (+ε), i.e. the mechanism G1's bounded-staleness guarantee rests on. |
| `invRecallSafety` | The property named explicitly in §10.8: no posix mutation commits while a holder's lease is both granted and not yet recalled-or-timed-out. |
| `invNegativeCacheSound` | A negative entry is only trusted when its recorded `dirver` still matches the live one. |
| `invMonotonicClientView` | G2: a client's own view of an object's version never regresses. |

Run:

```
npx @informalsystems/quint typecheck spec/coherence.qnt
npx @informalsystems/quint run spec/coherence.qnt \
  --invariant=allInvariants --max-steps=100 --max-samples=5000 \
  --backend=typescript   # the rust backend needs a GitHub binary
                          # download this sandbox couldn't reach; use it
                          # instead where network access allows — it's
                          # meaningfully faster
```

**Actual results, this build:** `typecheck` passes cleanly. `allInvariants` combined: **no violation found across 5,000 traces × 100 steps** (78 traces/sec on the TypeScript backend). Each invariant individually, 1,500 traces × 60 steps: `invLeaseNeverTrustedPastExpiry` ok, `invRecallSafety` ok, `invNegativeCacheSound` ok, `invMonotonicClientView` ok. The TypeScript backend is the slow path (no Rust binary reachable here); with `--backend=rust` available, both step and sample counts should go up by roughly an order of magnitude for the same wall-clock budget.

## A violation the first draft actually found

The first version of `invLeaseNeverTrustedPastExpiry` checked something
strictly stronger than what §10 promises: that no lease record's
`granted` flag is ever `true` past its expiry, *anywhere in the model's
state*, at any time. The simulator found a counterexample in under a
second — a lease sitting expired-but-not-yet-revalidated, which is
completely normal (nothing revalidates a lease until the client actually
reads through it again) and not a violation of anything the design
claims.

The design's actual guarantee is narrower: a stale lease is never
*trusted to serve a read*. The fix was a ghost variable
(`staleServeFlag`) set only inside the one action where that trust
decision happens (`ClientRead`'s cache-hit branch), which is the same
technique `invRecallSafety` already used for the same reason —
checking "this transition respected its own precondition" without
threading full history. After the fix, `quint run` finds no violation.

This is the value Phase 0 is for: the bug was in the *specification* of
what to check, not the protocol, and it surfaced from actually running a
checker against a formal model rather than from prose review.

## Not modeled here (see DESIGN.md §29 / this build's own gaps)

- Real wall-clock skew between distinct client/authority clocks (§10.7's
  `ε` is a fixed slack constant here, not two independently-drifting
  clocks) — modeling that precisely needs per-agent clocks with bounded
  drift, which would roughly double the state space for a property this
  model already checks adequately in its current form.
- The directory/inode lease-domain split (§10.5) as two separate
  invalidation scopes — collapsed to one object-level lease here.
- Rehoming (§7.4) and the namespace-map epoch fencing — a separate TLA+
  deliverable per DESIGN.md §27. That deliverable now exists: see
  `spec/rehoming.tla` and `spec/rehoming-README.md`.
