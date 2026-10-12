------------------------------- MODULE Finality ------------------------------
EXTENDS Naturals, FiniteSets

CONSTANTS Policy, MaxChanges, FaultEarly, FaultMissingHash,
          FaultRevertEarly, FaultLyingHead, AllowFinalizedRollback

Blocks == {"A", "B", "empty"}
Outcomes == {"success", "revert"}
None == "none"
Outcome(block) == IF block = "A" THEN "success" ELSE "revert"

VARIABLE s
vars == <<s>>

Init == s = [
    canonical |-> "empty", chainFinal |-> FALSE, depthReached |-> FALSE,
    changes |-> 0, phase |-> "idle", receipt |-> None,
    firstCanonical |-> None, secondCanonical |-> None, head |-> None,
    terminalBlock |-> None, terminalOutcome |-> None,
    canonicalAtCommit |-> TRUE, finalizedAtCommit |-> TRUE,
    seenInclusion |-> FALSE, seenLoss |-> FALSE, seenReinclusion |-> FALSE]

ResetObservation == [s EXCEPT !.phase = "idle", !.receipt = None,
    !.firstCanonical = None, !.secondCanonical = None, !.head = None]

\* The same signed intent can disappear and be re-included with a different
\* execution result. Identity is fixed; durable identity recovery is modeled
\* separately by EoaRecovery/SolanaRecovery, not assumed proved by this module.
ChainChange(block) ==
    /\ block \in Blocks /\ block # s.canonical
    /\ s.changes < MaxChanges
    /\ ~s.chainFinal \/ AllowFinalizedRollback
    /\ s' = [s EXCEPT !.canonical = block, !.chainFinal = FALSE,
         !.depthReached = FALSE, !.changes = @ + 1]

ChainFinalize ==
    /\ ~s.chainFinal
    /\ s' = [s EXCEPT !.chainFinal = TRUE]

DepthAdvance ==
    /\ ~s.depthReached
    /\ s' = [s EXCEPT !.depthReached = TRUE]

\* Every read is separate. Chain changes can interleave between reads, and
\* stale observations remain in local memory until the next attempt/crash.
ReadReceipt ==
    /\ s.phase = "idle"
    /\ s' = [s EXCEPT
         !.receipt = IF s.canonical = "empty" THEN None ELSE s.canonical,
         !.phase = IF s.canonical = "empty" THEN "idle" ELSE "canonical",
         !.seenInclusion = @ \/ s.canonical # "empty",
         !.seenLoss = @ \/ (s.seenInclusion /\ s.canonical = "empty"),
         !.seenReinclusion = @ \/ (s.seenLoss /\ s.canonical # "empty")]

ReadCanonical ==
    /\ s.phase = "canonical"
    /\ s' = [s EXCEPT !.firstCanonical = s.canonical, !.phase = "head",
         !.seenLoss = @ \/ s.canonical # s.receipt]

ReadHead ==
    /\ s.phase = "head"
    /\ IF ~FaultMissingHash /\ s.firstCanonical # s.receipt
       THEN s' = ResetObservation
       ELSE s' = [s EXCEPT !.phase = "recheckCanonical",
           !.head = IF FaultLyingHead
                       \/ (Policy = "finalized" /\ s.chainFinal)
                       \/ (Policy = "depth" /\ s.depthReached)
                    THEN s.canonical ELSE None]

RecheckCanonical ==
    /\ s.phase = "recheckCanonical"
    /\ s' = [s EXCEPT !.secondCanonical = s.canonical,
         !.phase = "recheckCheckpoint",
         !.seenLoss = @ \/ s.canonical # s.receipt]

\* A reverted receipt needs exactly the same finality gate as success.
RecheckCheckpoint ==
    /\ s.phase = "recheckCheckpoint"
    /\ LET early == FaultEarly \/
                      (FaultRevertEarly /\ Outcome(s.receipt) = "revert")
           hashOK == FaultMissingHash \/
                     (s.firstCanonical = s.receipt /\ s.secondCanonical = s.receipt)
           headOK == early \/ (s.head # None /\ s.head = s.canonical)
       IN s' = IF hashOK /\ headOK
               THEN [s EXCEPT !.phase = "ready"]
               ELSE ResetObservation

Commit ==
    /\ s.phase = "ready" /\ s.terminalBlock = None
    /\ s' = [s EXCEPT !.phase = "done", !.terminalBlock = s.receipt,
         !.terminalOutcome = Outcome(s.receipt),
         \* These are independent ledger facts recorded for invariants, never
         \* guards on Commit. A faulty observation can therefore violate them.
         !.canonicalAtCommit = s.canonical = s.receipt,
         !.finalizedAtCommit = s.chainFinal]

Crash ==
    /\ s.phase \notin {"idle", "done"}
    /\ s' = ResetObservation

Next == (\E block \in Blocks : ChainChange(block))
        \/ ChainFinalize \/ DepthAdvance \/ ReadReceipt \/ ReadCanonical
        \/ ReadHead \/ RecheckCanonical \/ RecheckCheckpoint \/ Commit \/ Crash
Spec == Init /\ [][Next]_vars

TypeOK ==
    /\ Policy \in {"finalized", "depth"}
    /\ s.canonical \in Blocks
    /\ s.chainFinal \in BOOLEAN /\ s.depthReached \in BOOLEAN
    /\ s.changes \in 0..MaxChanges
    /\ s.phase \in {"idle", "canonical", "head", "recheckCanonical",
                      "recheckCheckpoint", "ready", "done"}
    /\ s.receipt \in {None, "A", "B"}
    /\ s.firstCanonical \in Blocks \cup {None}
    /\ s.secondCanonical \in Blocks \cup {None}
    /\ s.head \in Blocks \cup {None}
    /\ s.terminalBlock \in {None, "A", "B"}
    /\ s.terminalOutcome \in Outcomes \cup {None}
    /\ s.canonicalAtCommit \in BOOLEAN /\ s.finalizedAtCommit \in BOOLEAN
    /\ s.seenInclusion \in BOOLEAN /\ s.seenLoss \in BOOLEAN
    /\ s.seenReinclusion \in BOOLEAN

CanonicalAtCommit == s.terminalBlock # None => s.canonicalAtCommit
TerminalRequiresFinality == s.terminalBlock # None => s.finalizedAtCommit
RevertNeedsFinality == s.terminalOutcome = "revert" => s.finalizedAtCommit
TerminalRemainsCanonical == s.terminalBlock # None => s.terminalBlock = s.canonical
OutcomeMatchesBlock == s.terminalBlock # None => s.terminalOutcome = Outcome(s.terminalBlock)
\* Expected witness failure proves the positive model can traverse loss,
\* re-inclusion and eventual terminal completion, rather than only parking.
ReinclusionWitnessNotReached == ~(s.terminalBlock # None /\ s.seenReinclusion)
=============================================================================
