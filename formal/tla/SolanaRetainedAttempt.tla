----------------------- MODULE SolanaRetainedAttempt -----------------------
EXTENDS Naturals, FiniteSets

CONSTANTS None, OriginalLimit, RecoveryLimit, MaxClaims,
          ExploreInterruptions, Fault
ASSUME /\ OriginalLimit > 0 /\ RecoveryLimit > 0 /\ MaxClaims > 0
VARIABLE s
vars == <<s>>
Wires == {"original", "other"}
MaxGrants == OriginalLimit + RecoveryLimit + 1
OwnStorage == s.local # 0 /\ s.storage = s.local
OwnQueue == s.local # 0 /\ s.queue = s.local
Healthy == s.healthy /\ s.admission = "active"
Grant(wire, pool) == [wire |-> wire, pool |-> pool,
    number |-> s.normalSpent + s.recoverySpent + 1,
    inRecoveryMode |-> s.mode]

\* One admitted, authenticated, signed attempt is already retained by authority.
\* Its Redis projection initially exists; losing it before any original send
\* includes the SQL-before-Redis crash cut. No action signs another transaction.
Init == s = [journal |-> "original", admission |-> "active", mode |-> FALSE,
    recoveryCount |-> 0, recoverySpent |-> 0, normalSpent |-> 0,
    projection |-> "original", projectionRecovery |-> FALSE, redisCount |-> 0,
    generation |-> 0, queue |-> 0, storage |-> 0, local |-> 0, phase |-> "idle",
    pendingGrant |-> None, permit |-> None, grants |-> {}, sends |-> {},
    healthy |-> TRUE, expired |-> FALSE, chain |-> "unseen", effects |-> {},
    restored |-> FALSE, restoredBeforeFirstSend |-> FALSE,
    restoredExpired |-> FALSE, restoredThenSent |-> FALSE,
    lostGrant |-> FALSE, restoredAgain |-> FALSE,
    badRestoreOwner |-> FALSE, overwrittenConflict |-> FALSE,
    badSendCheck |-> FALSE, staleTerminal |-> FALSE]

Claim ==
    /\ s.phase = "idle" /\ s.queue = 0 /\ s.generation < MaxClaims
    /\ s' = [s EXCEPT !.generation = @ + 1, !.local = s.generation + 1,
        !.queue = s.generation + 1, !.phase = "lock"]
Lock ==
    /\ s.phase = "lock" /\ OwnQueue /\ s.storage = 0
    /\ s' = [s EXCEPT !.storage = s.local, !.phase = "read"]
Read ==
    /\ s.phase = "read" /\ OwnStorage
    /\ s' = [s EXCEPT !.phase = IF s.projection = None THEN "mode"
        ELSE IF s.projection = s.journal \/ Fault = "substitute"
             THEN "check" ELSE "park"]

\* The durable mode is independent of the Redis flag. Repetition is idempotent;
\* it never clears the count. Terminal records may restore for read-only cleanup.
ActivateRecovery ==
    /\ s.phase = "mode" /\ OwnStorage /\ s.healthy
    /\ s.admission \in {"active", "terminal"}
    /\ s' = [s EXCEPT !.mode = TRUE, !.phase = "restore"]
Restore ==
    /\ s.phase = "restore" /\ (OwnStorage \/ Fault = "owner")
    /\ (s.projection = None \/ Fault = "overwrite")
    /\ s' = [s EXCEPT !.projection = s.journal, !.projectionRecovery = TRUE,
        !.redisCount = s.recoveryCount,
        !.recoveryCount = IF Fault = "reset_budget" THEN 0 ELSE @,
        !.permit = IF Fault = "restored_permission"
             THEN [wire |-> s.journal, pool |-> "recovery", number |-> 0,
                   inRecoveryMode |-> s.mode] ELSE @,
        !.phase = IF Fault = "restored_permission" THEN "validate" ELSE "check",
        !.badRestoreOwner = @ \/ ~OwnStorage,
        !.overwrittenConflict = @ \/ s.projection # None,
        !.restoredAgain = @ \/ s.restored, !.restored = TRUE,
        !.restoredBeforeFirstSend = @ \/ (s.sends = {} /\ s.normalSpent = 0),
        !.restoredExpired = @ \/ s.expired]

\* Stale/absent history is allowed even after execution. It never changes the
\* signature or proves non-execution. The finality model supplies receipt truth.
ObserveAbsent ==
    /\ s.phase = "check" /\ OwnStorage
    /\ s' = [s EXCEPT !.phase = IF s.expired \/ s.admission # "active"
                                        THEN "park" ELSE "grant"]
ObserveFinal ==
    /\ s.phase = "check" /\ OwnStorage /\ s.chain = "final"
    /\ s' = [s EXCEPT !.phase = "complete"]

OriginalGrant ==
    /\ s.phase = "grant" /\ OwnStorage /\ Healthy
    /\ ~s.projectionRecovery /\ (~s.mode \/ Fault = "ignore_mode")
    /\ (s.projection = s.journal \/ Fault = "substitute")
    /\ s.redisCount < OriginalLimit
    /\ s.normalSpent + s.recoverySpent < MaxGrants
    /\ LET grant == Grant(s.projection, "original")
       IN s' = [s EXCEPT !.redisCount = @ + 1, !.normalSpent = @ + 1,
          !.pendingGrant = grant, !.grants = @ \cup {grant}, !.phase = "return"]
RecoveryGrant ==
    /\ s.phase = "grant" /\ OwnStorage /\ Healthy /\ s.mode
    /\ s.projectionRecovery /\ (s.projection = s.journal \/ Fault = "substitute")
    /\ s.recoveryCount < RecoveryLimit /\ s.redisCount < RecoveryLimit
    /\ s.normalSpent + s.recoverySpent < MaxGrants
    /\ LET grant == Grant(s.projection, "recovery")
       IN s' = [s EXCEPT !.recoveryCount = @ + 1, !.recoverySpent = @ + 1,
          !.redisCount = @ + 1,
          !.pendingGrant = grant, !.grants = @ \cup {grant}, !.phase = "return"]

\* Durable claim/CAS/ack precede the response. Projection models refine these
\* steps; here a lost response consumes its claim without creating a permit.
GrantReturned ==
    /\ s.phase = "return" /\ s.pendingGrant # None /\ s.healthy
    /\ s' = [s EXCEPT !.permit = s.pendingGrant, !.pendingGrant = None,
                         !.phase = "validate"]
LoseGrantResponse ==
    /\ ExploreInterruptions /\ s.phase = "return"
    /\ s' = [s EXCEPT !.pendingGrant = None, !.phase = "check",
        !.lostGrant = @ \/ s.pendingGrant.pool = "recovery"]
ValidateSend ==
    /\ s.phase = "validate" /\ OwnStorage /\ s.permit # None
    /\ s' = [s EXCEPT !.phase = "dispatch", !.badSendCheck = @ \/ ~OwnStorage]
Dispatch ==
    /\ s.phase = "dispatch" /\ s.permit # None
    \* Lease/halt/expiry may change after the final check. Bytes already granted
    \* can escape; they remain the same signed identity and consume one grant.
    /\ s' = [s EXCEPT !.sends = @ \cup {s.permit}, !.permit = None,
        !.restoredThenSent = @ \/ (s.restored /\ s.permit.pool = "recovery"),
        !.phase = "check"]
Execute ==
    /\ ~s.expired
    /\ \E grant \in s.sends :
        /\ grant.wire \notin s.effects
        /\ s' = [s EXCEPT !.effects = @ \cup {grant.wire}, !.chain = "processed"]
Finalize == s.chain = "processed" /\ s' = [s EXCEPT !.chain = "final"]
Terminal ==
    /\ s.phase = "complete" /\ OwnQueue
    /\ s' = [s EXCEPT !.admission = "terminal", !.projection = None,
        !.queue = 0, !.storage = IF OwnStorage THEN 0 ELSE @,
        !.local = 0, !.phase = "idle", !.staleTerminal = @ \/ ~OwnQueue]

Retry ==
    /\ s.phase \notin {"idle", "return", "validate", "dispatch"}
    /\ s' = [s EXCEPT !.queue = IF OwnQueue THEN 0 ELSE @,
        !.storage = IF OwnStorage THEN 0 ELSE @, !.local = 0, !.phase = "idle"]
Crash ==
    /\ ExploreInterruptions /\ s.phase # "idle"
    /\ s' = [s EXCEPT !.local = 0, !.phase = "idle",
                         !.permit = None, !.pendingGrant = None]
ExpireQueue ==
    /\ ExploreInterruptions /\ s.queue # 0
    /\ s' = [s EXCEPT !.queue = 0]
ExpireStorage ==
    /\ ExploreInterruptions /\ s.storage # 0
    /\ s' = [s EXCEPT !.storage = 0]
LoseProjection ==
    /\ s.projection # None
    /\ ExploreInterruptions \/ (s.phase = "idle" /\ ~s.mode)
    /\ s' = [s EXCEPT !.projection = None, !.projectionRecovery = FALSE,
                         !.redisCount = 0]
ExpireHash ==
    /\ ExploreInterruptions /\ ~s.expired
    /\ s' = [s EXCEPT !.expired = TRUE]
Halt ==
    /\ ExploreInterruptions /\ s.healthy
    /\ s' = [s EXCEPT !.healthy = FALSE]
Quarantine ==
    /\ ExploreInterruptions /\ s.admission = "active"
    /\ s' = [s EXCEPT !.admission = "quarantined"]
InjectForeignProjection ==
    /\ ExploreInterruptions
    /\ s' = [s EXCEPT !.projection = "other"]
StripProjectionFlag ==
    /\ ExploreInterruptions /\ s.mode /\ s.projection = s.journal
    /\ s' = [s EXCEPT !.projectionRecovery = FALSE, !.redisCount = 0]

\* Explicit pre-existing boundary: an intact marker does not checksum every
\* Redis field. Rolling back the original counter before recovery mode can
\* replenish that original pool; the independent recovery pool still persists.
RollbackOriginalCounter ==
    /\ Fault = "original_counter_rollback" /\ ~s.mode
    /\ s.projection = s.journal /\ s.redisCount > 0
    /\ s' = [s EXCEPT !.redisCount = 0]

Next == Claim \/ Lock \/ Read \/ ActivateRecovery \/ Restore
    \/ ObserveAbsent \/ ObserveFinal \/ OriginalGrant \/ RecoveryGrant
    \/ GrantReturned \/ LoseGrantResponse \/ ValidateSend \/ Dispatch
    \/ Execute \/ Finalize \/ Terminal \/ Retry \/ Crash
    \/ ExpireQueue \/ ExpireStorage \/ LoseProjection \/ ExpireHash \/ Halt
    \/ Quarantine \/ InjectForeignProjection \/ StripProjectionFlag
    \/ RollbackOriginalCounter
Spec == Init /\ [][Next]_vars

TypeOK ==
    /\ s.journal \in Wires /\ s.projection \in Wires \cup {None}
    /\ s.admission \in {"active", "terminal", "quarantined"}
    /\ s.mode \in BOOLEAN /\ s.projectionRecovery \in BOOLEAN
    /\ s.recoveryCount \in 0..RecoveryLimit
    /\ s.normalSpent \in 0..MaxGrants /\ s.recoverySpent \in 0..MaxGrants
    /\ s.redisCount \in 0..(OriginalLimit + RecoveryLimit)
    /\ s.generation \in 0..MaxClaims
    /\ s.local \in 0..MaxClaims /\ s.queue \in 0..MaxClaims /\ s.storage \in 0..MaxClaims
    /\ s.phase \in {"idle", "lock", "read", "mode", "restore", "check",
                      "grant", "return", "validate", "dispatch", "park", "complete"}
OriginalAllowanceBound == s.normalSpent <= OriginalLimit
RecoveryAllowanceBound == s.recoverySpent <= RecoveryLimit
CombinedAllowanceBound == s.normalSpent + s.recoverySpent <= OriginalLimit + RecoveryLimit
ExactJournalIdentity == \A grant \in s.sends : grant.wire = s.journal
EverySendHasFreshGrant == s.sends \subseteq s.grants
RecoveryModeFencesOriginalGrants ==
    \A grant \in s.grants : grant.inRecoveryMode => grant.pool = "recovery"
AtMostOneEffect == Cardinality(s.effects) <= 1
RestoreOwned == ~s.badRestoreOwner
ConflictingProjectionNotOverwritten == ~s.overwrittenConflict
SendCheckOwned == ~s.badSendCheck
TerminalOwned == ~s.staleTerminal
RecoveryEvidenceRetained == s.journal = "original" /\ s.recoveryCount = s.recoverySpent
RestoredSendNotReached == ~s.restoredThenSent
PreFirstSendRecoveryNotReached == ~(s.restoredBeforeFirstSend /\ s.restoredThenSent)
ExpiredRestoreNotReached == ~s.restoredExpired
LostGrantRecoveryNotReached == ~(s.lostGrant /\ s.restoredThenSent)
BothAllowancesNotReached == s.normalSpent < OriginalLimit \/ s.recoverySpent < RecoveryLimit
RepeatedRestoreNotReached == ~s.restoredAgain
=============================================================================
