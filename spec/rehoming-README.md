# TLA+ spec: subtree rehoming protocol (DESIGN.md §7.4)

Phase 0 deliverable per §27: "TLA+ specification of the rehoming protocol
(§7.4), checked for the property that no write is accepted by two
authorities across an epoch change." `spec/README.md`'s "Not modeled
here" section listed this as "a separate TLA+ deliverable, not attempted
in this build" — this is that deliverable.

**No Go implementation of rehoming exists yet.** Unlike `coherence.qnt`,
which models a protocol `pkg/coherence` actually implements, this spec
checks the protocol description in §7.4/§7.3 on its own; there is nothing
in `pkg/repo` or `pkg/metadb` yet that executes it. That is consistent
with the roadmap (§28 puts multi-region homing and rehoming in Phase 4)
and it's why this is pure model checking with no "wired into the real
code" claim to make.

## What's modeled

`rehoming.tla` models one subtree with two fixed candidate authorities,
`Old` and `New` (§7.4 only ever has a subtree moving between the region
it's currently homed in and the region it's moving to — modeling more
than two would test load-balancing policy, not the fencing protocol):

- **The namespace-map entry's state machine** (§7.3): an epoch counter
  and an `Active`/`Sealing` phase for the subtree, committed as a single
  piece of global state — matching §7.3's description of the namespace
  map as one small Raft-replicated table, not something regions can
  independently disagree about.
- **The four rehoming steps** (§7.4) as separate actions, each gated on
  the previous one having completed: `StartRehome` (step 1, marks
  `Sealing`), `Drain` (step 2), `CopyRange` (step 3), `CommitEpoch` (step
  4, bumps the epoch, swaps `home`, and returns the phase to `Active`).
  Rehoming can fire more than once — `Old → New → Old → …` — bounded by
  the `MaxEpoch` constant, so the property is checked across a sequence
  of epoch changes, not just the first one.
- **Client write RPCs, each carrying a region and an epoch** (§7.3: "every
  metadata RPC carries the client's namespace-map epoch"). A write is
  accepted only if it targets the region and epoch the namespace map
  currently names as home, and the phase is `Active`; every other
  combination just isn't an enabled transition — standing in for an
  RPC rejected with `EAGAIN` (phase is `Sealing`) or `ATLAS_STALE_EPOCH`
  (wrong region and/or epoch). The region and epoch a write carries are
  chosen freely, not constrained to be mutually consistent or to match
  reality, so the model also covers a client acting on an arbitrarily
  stale or corrupted cached map — the actual case the fencing check
  exists to guard against, not just the well-behaved case where a client
  refreshes promptly after a rehoming.

## What's checked

`NoDualAcceptance`, the property named in §27, is two conjuncts:

| Invariant | Checks |
|---|---|
| `AcceptedMatchesHistory` | Every accepted write's `(region, epoch)` pair is the pair the namespace map actually committed for that epoch — no write is ever attributed to a region that was not truly its authority at that epoch. |
| `NeverAcceptDuringSealing` | No write is ever accepted while the subtree is between step 1 and step 4 — the old authority never accepts a write after the epoch bump it should have observed is coming. This is the direct formalization of "new mutations to the subtree return `EAGAIN`" (§7.4 step 1). |

`TypeOK` is also checked as an ordinary type-correctness invariant.

Run:

```
java -cp tla2tools.jar tlc2.TLC -config spec/rehoming.cfg spec/rehoming.tla
```

**Actual result, this build:** exhaustive (breadth-first, not bounded-random
like the Quint run) model checking with `Clients = {c1, c2}` and
`MaxEpoch = 3` (two epoch changes: `Old → New → Old`) completes the entire
reachable state graph — **249 states generated, 144 distinct states found,
0 states left on queue** — in under a second, with no invariant violation.
Coverage stats confirm both `StartRehome` and `CommitEpoch` each fire 20
times across the explored paths, i.e. the run genuinely reaches and
re-traverses both epoch changes, not just the first one. This is a real
proof for this bound, not a sample: TLC's breadth-first search here is
exhaustive over the finite state space these constants produce, unlike
`quint run`'s random exploration in the sibling spec.

TLC also reports the standard fingerprint-collision probability estimate
for its hashing (`8.2E-16` here); this is not evidence of nondeterminism
or missed states, just how TLC bounds the (vanishingly small) chance its
disk-backed state fingerprinting collided.

## A violation the first draft actually found

The first draft of `ClientWrite` checked only that the request's region
and epoch matched the currently committed `(home, epoch)` pair — it did
not check the namespace map's phase. That looks reasonable in isolation
(region and epoch are, after all, what §7.3 says an RPC carries), but
§7.4 step 1 is specific: sealing happens strictly *before* the epoch
bump, precisely so the old authority stops taking new mutations before
the metadata range is copied — a write accepted after `Sealing` begins
but before `CommitEpoch` is exactly the kind of write that could race the
copy in step 3.

TLC found this immediately (depth 3, first invariant check after the
first `StartRehome`):

```
State 2: <StartRehome ...>
/\ epoch = 1
/\ mapPhase = "Sealing"
/\ home = Old
...
State 3: <ClientWrite ...>
/\ accepted = {[epoch |-> 1, client |-> c1, region |-> Old, duringSealing |-> TRUE]}
```

`region = Old` still equals `home = Old` and `epoch = 1` still equals the
current epoch — nothing about the `(region, epoch)` pair was stale, so
`AcceptedMatchesHistory` alone would not have caught this. It's exactly
why `NeverAcceptDuringSealing` is a separate conjunct rather than folded
into a single "matches history" check: the danger window §7.4 step 1
exists to close is a *phase* window, not a region/epoch mismatch, and
those are genuinely different failure modes that happen to be easy to
conflate into one invariant that would have missed this.

The fix was adding `mapPhase = "Active"` to `ClientWrite`'s guard — the
same fencing check the real authority has to make on every RPC, not just
on the epoch number. After the fix, `TLC` finds no violation across the
full state graph described above.

This is the same value Phase 0 buys here as it did for `coherence.qnt`:
the gap was in the first cut at the *specification* of the guard, not
some obscure edge case, and it surfaced from actually running a checker
against the model rather than from re-reading the prose describing it.

## Not modeled here

- **Real-time duration of the quiesce window** (`D_max + ε`, §10.7). The
  wait itself is exactly what `spec/coherence.qnt`'s lease-expiry model
  already checks; this spec collapses "wait, then drain" into one atomic
  `Drain` step because what it's checking is action *ordering* — can the
  protocol be made to commit an epoch before the old authority has
  actually stopped taking writes — not how long that wait needs to be.
- **Reads.** §7.4 step 1 says "reads continue from cache" during
  sealing; this spec only models writes because the named property
  (§27) is about write acceptance. Read staleness during rehoming is
  bounded by the same lease mechanism `coherence.qnt` already checks, not
  by anything specific to rehoming.
- **More than two candidate authorities**, or rehoming chosen by a
  placement policy rather than nondeterministically. §7.4 describes
  moving a subtree between exactly two regions (its current home and its
  new one); modeling a fleet of regions would test placement policy, a
  Phase 4/§29 open problem (§29.2), not the fencing protocol this spec
  checks.
- **The Raft group backing the namespace map itself** (§7.3: "a Raft
  group of 5 members spread across regions"). This spec treats a
  namespace-map commit as a single atomic global fact, which is exactly
  what a correctly functioning Raft group provides; Raft's own safety
  properties are a separately-solved problem this design deliberately
  builds on rather than re-verifies (§7.2's "rejected alternative: global
  FDB" note is about the data path, not this control-plane table).
- **No Go implementation to check the spec against.** As noted above,
  rehoming is Phase 4 work (§28); there is no `pkg/rehoming` this spec's
  actions correspond to the way `coherence.qnt`'s actions correspond to
  `pkg/coherence`. This is a specification of the protocol as designed,
  checked for the one property §27 names, not a model of running code.
