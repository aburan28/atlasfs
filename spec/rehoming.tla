----------------------------- MODULE rehoming -----------------------------
(* TLA+ model of DESIGN.md Section 7.4, the subtree rehoming protocol, and
   the fencing rule from Section 7.3 that is supposed to make it safe.
   Phase 0 deliverable per Section 27: "TLA+ specification of the rehoming
   protocol (Section 7.4), checked for the property that no write is
   accepted by two authorities across an epoch change."

   Modeled: two fixed candidate authorities (Old, New) for one subtree, a
   namespace-map entry for that subtree with an epoch counter and an
   Active/Sealing phase (Section 7.3), the four rehoming steps of Section
   7.4 as separate actions with the ordering they must respect (seal,
   drain, copy, commit), and client write RPCs that each carry a region
   and an epoch the way Section 7.3 says every metadata RPC must. A write
   is accepted only if it targets the region and epoch the namespace map
   currently names as home for this subtree; every other combination is
   simply not an enabled transition, which stands for the RPC being
   rejected with EAGAIN (during Sealing) or ATLAS_STALE_EPOCH (wrong
   region and/or epoch) without needing separate reject actions.

   Rehoming can fire more than once (Old -> New -> Old -> ...), bounded by
   MaxEpoch, so the model exercises the property across a sequence of
   epoch changes, not just the first one. *)

EXTENDS Naturals, FiniteSets

CONSTANTS
    Clients,    \* the small set of client identities issuing writes
    MaxEpoch,   \* bound on how many epochs rehoming can advance through
    Old,        \* the subtree's authority at epoch 1
    New         \* the other candidate authority

ASSUME MaxEpoch \in Nat /\ MaxEpoch >= 1
ASSUME Old # New

Regions == {Old, New}

VARIABLES
    epoch,          \* current committed namespace-map epoch for the subtree
    home,            \* current committed home region
    mapPhase,        \* "Active" or "Sealing" -- the namespace-map entry's phase
    drained,         \* TRUE once the old authority has drained (step 2)
    copied,          \* TRUE once the metadata range has been copied (step 3)
    homeAtEpoch,     \* [1..MaxEpoch -> Regions]: home region as committed at each epoch reached so far
    accepted         \* set of write records the model has recorded as accepted

vars == <<epoch, home, mapPhase, drained, copied, homeAtEpoch, accepted>>

WriteRec == [client: Clients, region: Regions, epoch: 1..MaxEpoch, duringSealing: BOOLEAN]

TypeOK ==
    /\ epoch \in 1..MaxEpoch
    /\ home \in Regions
    /\ mapPhase \in {"Active", "Sealing"}
    /\ drained \in BOOLEAN
    /\ copied \in BOOLEAN
    /\ homeAtEpoch \in [1..MaxEpoch -> Regions]
    /\ accepted \subseteq WriteRec

Init ==
    /\ epoch = 1
    /\ home = Old
    /\ mapPhase = "Active"
    /\ drained = FALSE
    /\ copied = FALSE
    /\ homeAtEpoch = [e \in 1..MaxEpoch |-> Old]  \* only index 1 is meaningful until reached; see comment on WriteRec use
    /\ accepted = {}

(* Step 1 (Section 7.4): "Namespace map entry marked SEALING@epoch+1. New
   mutations to the subtree return EAGAIN; reads continue from cache."
   Bounded by MaxEpoch so the state space stays finite -- once epoch
   reaches MaxEpoch there is nowhere further to rehome to. *)
StartRehome ==
    /\ mapPhase = "Active"
    /\ epoch < MaxEpoch
    /\ mapPhase' = "Sealing"
    /\ UNCHANGED <<epoch, home, drained, copied, homeAtEpoch, accepted>>

(* Step 2: "Old authority waits D_max + epsilon (Section 10.7) for
   outstanding leases to expire, then drains in-flight transactions."
   The wait itself is a real-time duration that Section 10's coherence
   model already covers (spec/coherence.qnt); here it collapses to one
   atomic step because what this spec checks is action *ordering* --
   whether the protocol can be made to commit an epoch before the old
   authority has actually stopped -- not how long the wait takes. *)
Drain ==
    /\ mapPhase = "Sealing"
    /\ drained = FALSE
    /\ drained' = TRUE
    /\ UNCHANGED <<epoch, home, mapPhase, copied, homeAtEpoch, accepted>>

(* Step 3: "Metadata range copied to the new region's cluster." *)
CopyRange ==
    /\ mapPhase = "Sealing"
    /\ drained = TRUE
    /\ copied = FALSE
    /\ copied' = TRUE
    /\ UNCHANGED <<epoch, home, mapPhase, drained, homeAtEpoch, accepted>>

(* Step 4: "Namespace map committed at epoch+1 with the new home region.
   Old authority now rejects everything for that subtree with
   ATLAS_STALE_EPOCH." The old region is never named `home` again until
   (if ever) a later rehoming swaps back to it, which is exactly what
   makes ClientWrite's `reqRegion = home` guard reject it from this point
   on -- no separate "fenced" flag is needed. *)
CommitEpoch ==
    /\ mapPhase = "Sealing"
    /\ drained = TRUE
    /\ copied = TRUE
    /\ LET newHome == IF home = Old THEN New ELSE Old IN
        /\ epoch' = epoch + 1
        /\ home' = newHome
        /\ mapPhase' = "Active"
        /\ drained' = FALSE
        /\ copied' = FALSE
        /\ homeAtEpoch' = [homeAtEpoch EXCEPT ![epoch + 1] = newHome]
    /\ UNCHANGED accepted

(* A client write RPC, carrying a region (which authority it was routed
   to) and an epoch (its cached namespace-map epoch, Section 7.3). Both
   are chosen freely from Regions and 1..MaxEpoch by Next below, not
   constrained to be consistent with each other or with reality, so the
   model also covers a client acting on an arbitrarily stale or
   inconsistent cached map, which is the case the fencing check exists
   to guard against, not just the well-behaved case. *)
ClientWrite(c, reqRegion, reqEpoch) ==
    /\ mapPhase = "Active"
    /\ reqRegion = home
    /\ reqEpoch = epoch
    /\ accepted' = accepted \union
        {[client |-> c, region |-> reqRegion, epoch |-> reqEpoch,
          duringSealing |-> mapPhase = "Sealing"]}
    /\ UNCHANGED <<epoch, home, mapPhase, drained, copied, homeAtEpoch>>

Next ==
    \/ StartRehome
    \/ Drain
    \/ CopyRange
    \/ CommitEpoch
    \/ \E c \in Clients, r \in Regions, e \in 1..MaxEpoch : ClientWrite(c, r, e)

Spec == Init /\ [][Next]_vars

(* The property named in DESIGN.md Section 27: no write is accepted by
   two authorities across an epoch change. Formalized as two conjuncts:
   AcceptedMatchesHistory -- every accepted write's (region, epoch) pair
   is the pair the namespace map actually committed for that epoch, so no
   write is ever attributed to a region that was not truly its authority
   at that epoch; and NeverAcceptDuringSealing -- no write is accepted
   while the subtree is between step 1 and step 4, i.e. the old authority
   never accepts a write after the epoch bump it should have observed is
   coming. Together these rule out the old and new authority both having
   accepted writes for the same subtree with no epoch change separating
   them. *)
AcceptedMatchesHistory == \A w \in accepted : w.region = homeAtEpoch[w.epoch]

NeverAcceptDuringSealing == \A w \in accepted : ~w.duringSealing

NoDualAcceptance == AcceptedMatchesHistory /\ NeverAcceptDuringSealing

=============================================================================
