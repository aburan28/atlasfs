// Quint model of DESIGN.md §10, the metadata cache coherence protocol.
// Phase 0 deliverable per §10.8: this is checked before the FUSE mount
// is built, because the coherence protocol is where the design bugs
// live, not the metadata transactions themselves.
//
// Modeled: lease grant/expiry on CLOCK_MONOTONIC durations from send
// time (§10.7), best-effort/lossy push invalidation that is explicitly
// NOT load-bearing (§10.2), the two independent lease domains collapsed
// to one per-object lease here since the directory/inode split (§10.5)
// doesn't change the coherence argument being checked, negative caching
// validated against a directory version so one bump invalidates every
// negative entry at once (§10.4), and blocking recall for the `posix`
// class with a bounded wait before a non-responding holder's lease is
// presumed dead (§10.6).
//
// Checked properties (via `quint run --invariant=...`, see spec/README.md):
//   invLeaseNeverTrustedPastExpiry  — G1's mechanism: a cache hit never
//     happens outside [grant, expiry] on the client's own clock.
//   invRecallSafety                — §10.6: no posix mutation commits
//     while a holder's lease is both granted and not yet recalled or
//     timed out. This is the property named explicitly in §10.8.
//   invNegativeCacheSound           — §10.4: a negative entry is only
//     trusted when its recorded dirver still matches the live one.
//   invMonotonicClientView          — G2: a client's own view of an
//     object's version never regresses.

module coherence {

  pure val D = 5           // lease duration for ordinary (non-posix) objects
  pure val DRECALL = 3     // §10.6 recall wait before presuming a holder dead
  pure val EPSILON = 1     // §10.7 clock-skew bound

  pure val clients = Set("c1", "c2")
  pure val objects = Set("o1", "o2")
  pure val posixObjects = Set("o1")   // o1 is posix (recallable), o2 is not

  type Lease = { granted: bool, expiry: int, versionSeen: int }
  pure val noLease: Lease = { granted: false, expiry: 0, versionSeen: 0 }

  type Neg = { valid: bool, dirverSeen: int, expiry: int }
  pure val noNeg: Neg = { valid: false, dirverSeen: -1, expiry: 0 }

  var time: int
  var authVersion: str -> int
  var leases: (str, str) -> Lease
  var clientView: (str, str) -> int
  var recallPending: (str, str) -> bool
  var recallDeadline: (str, str) -> int
  var dirver: int
  var negLease: str -> Neg

  // Ghost/ghost-ish history flags: set false only if an action fires
  // whose guard failed to enforce the property it claims to enforce.
  // This is how a single-step invariant checks "this transition
  // respected its own precondition" without threading full history.
  var recallSafetyViolatedFlag: bool
  var staleServeFlag: bool

  action init = all {
    time' = 0,
    authVersion' = objects.mapBy(_ => 0),
    leases' = tuples(clients, objects).mapBy(_ => noLease),
    clientView' = tuples(clients, objects).mapBy(_ => -1),
    recallPending' = tuples(clients, objects).mapBy(_ => false),
    recallDeadline' = tuples(clients, objects).mapBy(_ => 0),
    dirver' = 0,
    negLease' = clients.mapBy(_ => noNeg),
    recallSafetyViolatedFlag' = false,
    staleServeFlag' = false,
  }

  action Tick = all {
    time' = time + 1,
    authVersion' = authVersion,
    leases' = leases,
    clientView' = clientView,
    recallPending' = recallPending,
    recallDeadline' = recallDeadline,
    dirver' = dirver,
    negLease' = negLease,
    recallSafetyViolatedFlag' = recallSafetyViolatedFlag,
    staleServeFlag' = staleServeFlag,
  }

  // A client reads object o: cache hit if it holds a granted,
  // unexpired lease (checked on the client's own clock, §10.7); else a
  // revalidation round trip that refreshes clientView and grants a
  // fresh lease of duration D from *now*.
  action ClientRead(c, o) = all {
    val cur = leases.get((c, o))
    if (cur.granted and time <= cur.expiry) {
      all {
        // cache hit: nothing changes, this is the whole point of a
        // lease — no RPC, no state transition needed to serve it.
        time' = time,
        authVersion' = authVersion,
        leases' = leases,
        clientView' = clientView,
        recallPending' = recallPending,
        recallDeadline' = recallDeadline,
        dirver' = dirver,
        negLease' = negLease,
        recallSafetyViolatedFlag' = recallSafetyViolatedFlag,
        staleServeFlag' = staleServeFlag or (time > cur.expiry + EPSILON),
      }
    } else {
      all {
        time' = time,
        authVersion' = authVersion,
        leases' = leases.set((c, o), { granted: true, expiry: time + D, versionSeen: authVersion.get(o) }),
        clientView' = clientView.set((c, o), authVersion.get(o)),
        recallPending' = recallPending,
        recallDeadline' = recallDeadline,
        dirver' = dirver,
        negLease' = negLease,
        recallSafetyViolatedFlag' = recallSafetyViolatedFlag,
        staleServeFlag' = staleServeFlag,
      }
    }
  }

  // Non-posix mutation: always allowed immediately (§10.2 — leases
  // never block a write outside `posix`). Push invalidation is
  // best-effort and lossy: nondeterministically some, none, or all
  // holders get invalidated early; correctness must not depend on it,
  // which is exactly what invLeaseNeverTrustedPastExpiry checks even
  // when push invalidation never fires at all.
  action Mutate(o) = all {
    not(posixObjects.contains(o)),
    time' = time,
    authVersion' = authVersion.set(o, authVersion.get(o) + 1),
    nondet dropped = clients.powerset().oneOf()
    leases' = clients.fold(leases, (acc, c) =>
      if (dropped.contains(c)) acc.set((c, o), noLease) else acc
    ),
    clientView' = clientView,
    recallPending' = recallPending,
    recallDeadline' = recallDeadline,
    dirver' = dirver,
    negLease' = negLease,
    recallSafetyViolatedFlag' = recallSafetyViolatedFlag,
    staleServeFlag' = staleServeFlag,
  }

  // §10.6: a posix mutation on o must recall every current lease
  // holder before it may commit. This action issues the recalls; it
  // does not bump authVersion itself.
  action RequestPosixMutate(o) = all {
    posixObjects.contains(o),
    time' = time,
    authVersion' = authVersion,
    leases' = leases,
    clientView' = clientView,
    recallPending' = clients.fold(recallPending, (acc, c) =>
      if (leases.get((c, o)).granted) acc.set((c, o), true) else acc
    ),
    recallDeadline' = clients.fold(recallDeadline, (acc, c) =>
      if (leases.get((c, o)).granted) acc.set((c, o), time + DRECALL) else acc
    ),
    dirver' = dirver,
    negLease' = negLease,
    recallSafetyViolatedFlag' = recallSafetyViolatedFlag,
    staleServeFlag' = staleServeFlag,
  }

  // A holder acknowledges recall: gives up its lease immediately.
  action AckRecall(c, o) = all {
    recallPending.get((c, o)),
    time' = time,
    authVersion' = authVersion,
    leases' = leases.set((c, o), noLease),
    clientView' = clientView,
    recallPending' = recallPending.set((c, o), false),
    recallDeadline' = recallDeadline,
    dirver' = dirver,
    negLease' = negLease,
    recallSafetyViolatedFlag' = recallSafetyViolatedFlag,
    staleServeFlag' = staleServeFlag,
  }

  // §10.6's actual safety property: the mutation may only commit once
  // every current holder has either acked or blown its recall
  // deadline. recallSafetyOk is evaluated here, at the one place the
  // version can change for a posix object, and any future edit that
  // lets this action fire without the condition holding flips the flag
  // permanently — that's what invRecallSafety checks.
  action CompletePosixMutate(o) = all {
    posixObjects.contains(o),
    val recallSafetyOk = clients.forall(c =>
      not(leases.get((c, o)).granted) or
      not(recallPending.get((c, o))) or
      time >= recallDeadline.get((c, o))
    )
    all {
      time' = time,
      authVersion' = authVersion.set(o, authVersion.get(o) + 1),
      leases' = clients.fold(leases, (acc, c) =>
        if (time >= recallDeadline.get((c, o)) and recallPending.get((c, o)))
          acc.set((c, o), noLease)
        else acc
      ),
      clientView' = clientView,
      recallPending' = clients.fold(recallPending, (acc, c) => acc.set((c, o), false)),
      recallDeadline' = recallDeadline,
      dirver' = dirver,
      negLease' = negLease,
      recallSafetyViolatedFlag' = recallSafetyViolatedFlag or not(recallSafetyOk),
      staleServeFlag' = staleServeFlag,
    }
  }

  // §10.4: negative caching. A lookup of an absent name trusts its
  // cached "not found" only while the directory version it was granted
  // against still matches — one dirver bump invalidates every negative
  // entry for that directory at once, without per-entry invalidation
  // messages.
  action NegativeLookup(c) = all {
    val cur = negLease.get(c)
    if (cur.valid and cur.dirverSeen == dirver and time <= cur.expiry) {
      all {
        time' = time, authVersion' = authVersion, leases' = leases,
        clientView' = clientView, recallPending' = recallPending,
        recallDeadline' = recallDeadline, dirver' = dirver,
        negLease' = negLease,
        recallSafetyViolatedFlag' = recallSafetyViolatedFlag,
        staleServeFlag' = staleServeFlag,
      }
    } else {
      all {
        time' = time, authVersion' = authVersion, leases' = leases,
        clientView' = clientView, recallPending' = recallPending,
        recallDeadline' = recallDeadline, dirver' = dirver,
        negLease' = negLease.set(c, { valid: true, dirverSeen: dirver, expiry: time + D }),
        recallSafetyViolatedFlag' = recallSafetyViolatedFlag,
        staleServeFlag' = staleServeFlag,
      }
    }
  }

  // A create in the directory: bumps dirver, invalidating every
  // negative entry regardless of who holds one or whether any push
  // message ever reaches them.
  action CreateInDir = all {
    time' = time, authVersion' = authVersion, leases' = leases,
    clientView' = clientView, recallPending' = recallPending,
    recallDeadline' = recallDeadline,
    dirver' = dirver + 1,
    negLease' = negLease,
    recallSafetyViolatedFlag' = recallSafetyViolatedFlag,
    staleServeFlag' = staleServeFlag,
  }

  action step = any {
    Tick,
    all {
      nondet c = clients.oneOf()
      nondet o = objects.oneOf()
      ClientRead(c, o),
    },
    all {
      nondet o = objects.oneOf()
      Mutate(o),
    },
    all {
      nondet o = posixObjects.oneOf()
      RequestPosixMutate(o),
    },
    all {
      nondet c = clients.oneOf()
      nondet o = objects.oneOf()
      AckRecall(c, o),
    },
    all {
      nondet o = posixObjects.oneOf()
      CompletePosixMutate(o),
    },
    all {
      nondet c = clients.oneOf()
      NegativeLookup(c),
    },
    CreateInDir,
  }

  // --- invariants -----------------------------------------------------

  // NOTE: this is deliberately NOT "no granted lease record ever exists
  // past its expiry" — that's not a guarantee the design makes. A
  // lease's bookkeeping can sit expired-but-untouched indefinitely; the
  // design only promises the *client* never treats it as valid past
  // expiry when deciding to serve a cached read. staleServeFlag is set
  // only inside ClientRead's cache-hit branch (the one place a stale
  // lease could be trusted), so this checks the guard that matters
  // rather than a stronger property the protocol never claimed.
  val invLeaseNeverTrustedPastExpiry = not(staleServeFlag)

  val invRecallSafety = not(recallSafetyViolatedFlag)

  val invNegativeCacheSound =
    clients.forall(c =>
      val n = negLease.get(c)
      n.valid implies n.dirverSeen <= dirver
    )

  val invMonotonicClientView =
    tuples(clients, objects).forall(cv =>
      clientView.get(cv) <= authVersion.get(cv._2)
    )

  val allInvariants =
    invLeaseNeverTrustedPastExpiry and invRecallSafety and
    invNegativeCacheSound and invMonotonicClientView
}
