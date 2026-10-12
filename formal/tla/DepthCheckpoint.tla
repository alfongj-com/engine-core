-------------------------- MODULE DepthCheckpoint ---------------------------
EXTENDS Naturals, FiniteSets

CONSTANTS Depth, MaxHeight, MaxReorgs, FaultTipAnchor,
          FaultIgnoreConflict, AllowQualifiedRollback

Heights == 1..MaxHeight
Hashes == {"A", "B"}
None == "none"
VARIABLE s
vars == <<s>>

Init == s = [
    ledger |-> [h \in Heights |-> "A"], head |-> Depth + 1, reorgs |-> 0,
    phase |-> "idle", observedHead |-> 0, observedHeadHash |-> None,
    boundary |-> 0, boundaryHash |-> None,
    checkpoint |-> 0, checkpointHash |-> None,
    qualified |-> 0, qualifiedHash |-> None,
    halted |-> FALSE, haltWasQualifiedConflict |-> FALSE,
    checked |-> FALSE, checkedQualifiedConflict |-> FALSE,
    sawShallowReorg |-> FALSE, survivedShallowReorg |-> FALSE]

Reset == [s EXCEPT !.phase = "idle", !.observedHead = 0,
    !.observedHeadHash = None, !.boundary = 0, !.boundaryHash = None]

Extend ==
    /\ s.head < MaxHeight
    /\ s' = [s EXCEPT !.head = @ + 1]

\* Canonical ledger changes are independent of the client's observation fields.
\* The positive case permits arbitrary changes before persistence, and shallow
\* changes above accepted qualified history afterward. The boundary case also
\* permits rollback of that qualified history: depth is only probabilistic.
Reorg(start) ==
    /\ start \in 1..s.head /\ s.reorgs < MaxReorgs
    /\ AllowQualifiedRollback \/ start > s.qualified
    /\ s' = [s EXCEPT
         !.ledger = [h \in Heights |->
             IF h >= start /\ h <= s.head
             THEN IF s.ledger[h] = "A" THEN "B" ELSE "A"
             ELSE s.ledger[h]],
         !.reorgs = @ + 1,
         !.sawShallowReorg = @ \/ (s.checkpoint > 0 /\ start > s.qualified)]

\* A fixed receipt at height 1 is assumed independently identity/canonical
\* checked by Finality.tla and implementation tests. This module isolates which
\* covering block is safe to retain for later continuity checks.
ReadHead ==
    /\ s.phase = "idle" /\ s.checkpoint = 0 /\ ~s.halted
    /\ s' = [s EXCEPT !.observedHead = s.head,
         !.observedHeadHash = s.ledger[s.head], !.phase = "boundary"]

ReadBoundary ==
    /\ s.phase = "boundary"
    /\ LET q == s.observedHead - Depth
       IN s' = [s EXCEPT !.boundary = q, !.boundaryHash = s.ledger[q],
                         !.phase = "recheckHead"]

RecheckHead ==
    /\ s.phase = "recheckHead"
    /\ s' = IF s.ledger[s.observedHead] = s.observedHeadHash
             THEN [s EXCEPT !.phase = "recheckBoundary"] ELSE Reset

RecheckBoundary ==
    /\ s.phase = "recheckBoundary"
    /\ s' = IF s.ledger[s.boundary] = s.boundaryHash
             THEN [s EXCEPT !.phase = "ready"] ELSE Reset

Persist ==
    /\ s.phase = "ready" /\ s.checkpoint = 0 /\ ~s.halted
    /\ s' = [s EXCEPT !.phase = "committed",
         !.checkpoint = IF FaultTipAnchor THEN s.observedHead ELSE s.boundary,
         !.checkpointHash = IF FaultTipAnchor THEN s.observedHeadHash ELSE s.boundaryHash,
         \* Semantic witness, not a guard: what the policy actually qualified.
         !.qualified = s.observedHead - Depth, !.qualifiedHash = s.boundaryHash]

CheckContinuity ==
    /\ s.checkpoint > 0 /\ ~s.halted
    /\ LET conflict == s.ledger[s.checkpoint] # s.checkpointHash
           qualifiedConflict == s.ledger[s.qualified] # s.qualifiedHash
           stop == conflict /\ ~FaultIgnoreConflict
       IN s' = [s EXCEPT !.checked = TRUE,
           !.checkedQualifiedConflict = qualifiedConflict,
           !.halted = stop,
           !.haltWasQualifiedConflict = IF stop THEN qualifiedConflict ELSE @,
           !.survivedShallowReorg = @ \/ (s.sawShallowReorg /\ ~conflict)]

Next == Extend \/ (\E start \in Heights : Reorg(start))
        \/ ReadHead \/ ReadBoundary \/ RecheckHead \/ RecheckBoundary
        \/ Persist \/ CheckContinuity
Spec == Init /\ [][Next]_vars

TypeOK ==
    /\ Depth \in 1..MaxHeight /\ MaxHeight >= Depth + 1
    /\ s.ledger \in [Heights -> Hashes]
    /\ s.head \in (Depth + 1)..MaxHeight
    /\ s.reorgs \in 0..MaxReorgs
    /\ s.phase \in {"idle", "boundary", "recheckHead", "recheckBoundary", "ready", "committed"}
    /\ s.observedHead \in 0..MaxHeight /\ s.observedHeadHash \in Hashes \cup {None}
    /\ s.boundary \in 0..MaxHeight /\ s.boundaryHash \in Hashes \cup {None}
    /\ s.checkpoint \in 0..MaxHeight /\ s.checkpointHash \in Hashes \cup {None}
    /\ s.qualified \in 0..MaxHeight /\ s.qualifiedHash \in Hashes \cup {None}
    /\ s.halted \in BOOLEAN /\ s.haltWasQualifiedConflict \in BOOLEAN
    /\ s.checked \in BOOLEAN /\ s.checkedQualifiedConflict \in BOOLEAN
    /\ s.sawShallowReorg \in BOOLEAN /\ s.survivedShallowReorg \in BOOLEAN

\* A shallow reorg above qualified history must not permanently fence the chain.
NoSpuriousHalt == s.halted => s.haltWasQualifiedConflict
\* A real contradiction must still halt at the next actual continuity check.
PositiveConflictHalts == (s.checked /\ s.checkedQualifiedConflict) => s.halted
\* Expected witness: progress is reachable through a shallow reorg and recheck.
ShallowReorgSurvivalNotReached == ~s.survivedShallowReorg
\* Expected negative boundary: depth does not prevent a future deep rollback.
AcceptedBoundaryRemainsCanonical ==
    s.checkpoint > 0 => s.ledger[s.qualified] = s.qualifiedHash
=============================================================================
