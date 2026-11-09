---- MODULE SolanaProcessingLease ----
EXTENDS Integers, FiniteSets, TLC
CONSTANT Fault
Workers == {0, 1}
NoOwner == 2
VARIABLES live, owner, staleMutation, liveSteal, reclaimed
vars == <<live, owner, staleMutation, liveSteal, reclaimed>>

\* live abstracts the conjunction of matching queue metadata and an existing
\* expiring lease key. Two valid scopes may refer to duplicate queue jobs for
\* one intent. This model covers new owner-bound locks, not opaque legacy locks.
Init == /\ live = Workers /\ owner = NoOwner
        /\ staleMutation = FALSE /\ liveSteal = FALSE /\ reclaimed = FALSE

Acquire(w) ==
    /\ w \in live
    /\ owner # w
    /\ (owner = NoOwner \/ owner \notin live \/ Fault = "steal_live")
    /\ owner' = w
    /\ liveSteal' = (liveSteal \/ (owner # NoOwner /\ owner \in live))
    /\ reclaimed' = (reclaimed \/ (owner # NoOwner /\ owner \notin live))
    /\ UNCHANGED <<live, staleMutation>>

Expire(w) ==
    /\ w \in live
    /\ live' = live \ {w}
    /\ UNCHANGED <<owner, staleMutation, liveSteal, reclaimed>>

Mutate(w) ==
    /\ owner = w
    /\ (w \in live \/ Fault = "ignore_queue")
    /\ staleMutation' = (staleMutation \/ w \notin live)
    /\ UNCHANGED <<live, owner, liveSteal, reclaimed>>

Release(w) ==
    /\ owner = w
    /\ owner' = NoOwner
    /\ UNCHANGED <<live, staleMutation, liveSteal, reclaimed>>

Next == \E w \in Workers: Acquire(w) \/ Expire(w) \/ Mutate(w) \/ Release(w)
Spec == Init /\ [][Next]_vars

TypeOK == /\ live \subseteq Workers /\ owner \in Workers \cup {NoOwner}
          /\ staleMutation \in BOOLEAN /\ liveSteal \in BOOLEAN /\ reclaimed \in BOOLEAN
AttemptMutationsHaveQueueOwner == ~staleMutation
LiveProcessingOwnerNotStolen == ~liveSteal
DeadOwnerReclaimNotReached == ~reclaimed
====
