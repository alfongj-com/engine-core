--------------------- MODULE DisasterRecoveryProjection ---------------------
EXTENDS Naturals, FiniteSets

CONSTANTS None, MaxWrites, InitialSchema, FaultEarlyPermission,
          FaultForgetPending, FaultForgiveRollback, FaultAdoptRun,
          FaultResurrectCaller, FaultClearHalt

Token(version, epoch, checkpoint) ==
    [version |-> version, epoch |-> epoch, checkpoint |-> checkpoint]
Tokens == [version : {1, 2}, epoch : 0..1, checkpoint : 0..MaxWrites]
VARIABLE s
vars == <<s>>
Current == Token(s.schema, s.epoch, s.checkpoint)
MarkerKey == IF s.epoch = 0 THEN "original" ELSE "recovered"
Transition(old, target) ==
    [old |-> old, target |-> target, key |-> MarkerKey, run |-> s.expectedRun]
ValidPending == IF s.pending = None THEN FALSE ELSE
    /\ s.schema = 2 /\ s.pending.target = Current
    /\ s.pending.key = MarkerKey /\ s.pending.run = s.expectedRun
    /\ s.pending.old.epoch = s.epoch
    /\ (s.pending.old.version = 2 /\
        s.pending.old.checkpoint + 1 = s.checkpoint)
       \/ (s.pending.old.version = 1 /\
           s.pending.old.checkpoint = s.checkpoint)
ExactRun == s.run = s.expectedRun /\ s.primary
Stable == s.marker = Current /\ ExactRun
ObservedTarget == IF s.observed = None \/ s.pending = None THEN FALSE ELSE
    s.observed.marker = Current /\ s.observed.run = s.pending.run
        /\ s.observed.primary

Init == s = [schema |-> InitialSchema, epoch |-> 0, checkpoint |-> 0,
    marker |-> Token(InitialSchema, 0, 0), run |-> 0, expectedRun |-> 0,
    primary |-> TRUE, owner |-> FALSE, ready |-> FALSE, halt |-> None,
    pending |-> None, caller |-> None, returning |-> None, observed |-> None,
    writes |-> 0, acknowledged |-> {Token(InitialSchema, 0, 0)},
    requiredPending |-> {}, recoveryDiscarded |-> {}, returned |-> {},
    resumeOrigin |-> "none", resumedOld |-> FALSE, resumedNew |-> FALSE,
    resumedMigration |-> FALSE, uncertain |-> FALSE, resumedUncertain |-> FALSE,
    oldPendingLost |-> FALSE, badRepair |-> FALSE, adoptedRun |-> FALSE,
    resurrectedCaller |-> FALSE, clearedHalt |-> FALSE,
    offline |-> "none", offlineAlive |-> FALSE]

Start ==
    /\ ~s.owner /\ ~s.offlineAlive
    /\ s' = [s EXCEPT !.owner = TRUE, !.ready = FALSE, !.observed = None,
        !.resumeOrigin = IF s.pending = None THEN "none"
            ELSE IF s.pending.old.version = 1 THEN "migration"
            ELSE IF s.marker = s.pending.old THEN "old"
            ELSE IF s.marker = s.pending.target THEN "new" ELSE "none"]

Crash ==
    /\ s.owner
    /\ s' = [s EXCEPT !.owner = FALSE, !.ready = FALSE,
        !.caller = None, !.returning = None, !.observed = None,
        !.oldPendingLost = @ \/ (FaultForgetPending /\ s.pending # None),
        !.pending = IF FaultForgetPending THEN None ELSE @]

\* Cancellation loses the caller but leaves the owner and durable pending row.
Cancel ==
    /\ s.owner /\ (s.caller # None \/ s.returning # None)
    /\ s' = [s EXCEPT !.caller = None, !.returning = None,
                            !.observed = None, !.ready = FALSE]

\* Observations are independent Redis state. An error supplies no observation,
\* and cannot clear pending or release a new permission.
TransportUncertain ==
    /\ s.owner /\ s.halt = None /\ s.pending # None
    /\ s' = [s EXCEPT !.observed = None, !.ready = FALSE, !.uncertain = TRUE]

Observe ==
    /\ s.owner /\ s.halt = None
    /\ LET observed == [marker |-> s.marker, run |-> s.run, primary |-> s.primary]
       IN IF s.pending = None
          THEN IF Stable
               THEN s' = [s EXCEPT !.observed = observed, !.ready = TRUE]
               ELSE IF FaultForgiveRollback /\ ExactRun /\ s.marker # None
                       /\ s.marker.version = s.schema /\ s.marker.epoch = s.epoch
                       /\ s.marker.checkpoint < s.checkpoint
                    THEN s' = [s EXCEPT !.marker = Current, !.ready = TRUE,
                                               !.badRepair = TRUE]
                    ELSE s' = [s EXCEPT !.halt = "contradiction", !.ready = FALSE]
          ELSE IF ValidPending /\ ExactRun /\ s.marker \in {s.pending.old, Current}
               THEN s' = [s EXCEPT !.observed = observed, !.ready = FALSE]
               ELSE IF FaultAdoptRun /\ ValidPending /\ s.primary
                       /\ s.marker \in {s.pending.old, Current} /\ s.run # s.expectedRun
                    THEN s' = [s EXCEPT !.expectedRun = s.run, !.pending.run = s.run,
                        !.adoptedRun = TRUE, !.observed = None, !.ready = FALSE]
                    ELSE s' = [s EXCEPT !.halt = "contradiction", !.ready = FALSE]

\* One atomic FULL SQLite transaction commits authority and its exact pending
\* transition. There can be no second mutation until the first is acknowledged.
Commit ==
    /\ s.owner /\ s.ready /\ s.halt = None /\ s.schema = 2
    /\ s.pending = None /\ s.caller = None /\ s.returning = None
    /\ s.writes < MaxWrites
    /\ LET target == Token(2, s.epoch, s.checkpoint + 1)
       IN s' = [s EXCEPT !.checkpoint = @ + 1, !.writes = @ + 1,
           !.pending = Transition(Current, target), !.caller = target,
           !.requiredPending = @ \cup {target}, !.observed = None,
           !.ready = FALSE, !.resumeOrigin = "none", !.uncertain = FALSE]

\* v1 migration changes only the protocol schema, never inventing a past
\* pending transition from a checkpoint difference or an existing halt.
Migrate ==
    /\ s.owner /\ s.ready /\ s.halt = None /\ s.schema = 1
    /\ s.pending = None
    /\ LET target == Token(2, s.epoch, s.checkpoint)
       IN s' = [s EXCEPT !.schema = 2,
           !.pending = Transition(Current, target), !.requiredPending = @ \cup {target},
           !.ready = FALSE, !.observed = None, !.uncertain = FALSE]

\* CAS may have applied even when its response is lost. No permission follows
\* from this action; a fresh post-observation and SQL acknowledgement remain.
Publish ==
    /\ s.owner /\ s.halt = None /\ ValidPending
    /\ s.observed # None /\ s.observed.run = s.pending.run /\ s.observed.primary
    /\ s.observed.marker \in {s.pending.old, Current}
    /\ IF s.marker = s.pending.old
       THEN s' = [s EXCEPT !.marker = Current, !.observed = None]
       ELSE IF s.marker = Current
            THEN s' = [s EXCEPT !.observed = None]
            ELSE s' = [s EXCEPT !.halt = "contradiction", !.ready = FALSE]

\* The post-observation can itself race a later Redis fault. Acknowledgement
\* records that verified observation; this is not an atomic SQL/Redis lock.
Acknowledge ==
    /\ s.owner /\ s.halt = None /\ ValidPending /\ ObservedTarget
    /\ s' = [s EXCEPT !.acknowledged = @ \cup {Current},
        !.returning = IF s.caller = Current \/
                        (FaultResurrectCaller /\ s.pending.old.version = 2)
                      THEN Current ELSE None,
        !.resumedOld = @ \/ s.resumeOrigin = "old",
        !.resumedNew = @ \/ s.resumeOrigin = "new",
        !.resumedMigration = @ \/ s.resumeOrigin = "migration",
        !.resumedUncertain = @ \/ s.uncertain,
        !.pending = None, !.observed = None, !.ready = FALSE]

ReturnPermission ==
    /\ s.owner /\ s.returning # None
    /\ s' = [s EXCEPT !.returned = @ \cup {s.returning},
        !.resurrectedCaller = @ \/ s.caller # s.returning,
        !.returning = None, !.caller = None]

PrematurePermission ==
    /\ FaultEarlyPermission /\ s.owner /\ s.pending # None /\ s.caller # None
    /\ s' = [s EXCEPT !.returned = @ \cup {s.caller}]

RedisLoss == s' = [s EXCEPT !.marker = None]
RedisRollback ==
    /\ s.checkpoint > 0
    /\ \E previous \in 0..(s.checkpoint - 1) :
          s' = [s EXCEPT !.marker = Token(s.schema, s.epoch, previous)]
RedisRestart == s.run = 0 /\ s' = [s EXCEPT !.run = 1]
RedisRoleChange == s.primary /\ s' = [s EXCEPT !.primary = FALSE]
\* Wrong epoch/version is not a predecessor, even at the same numeric counter.
RedisForeignToken == s' = [s EXCEPT !.marker = Token(1, 1, 0)]

PermanentHalt ==
    /\ s.halt = None
    /\ \E cause \in {"operator", "storage", "evidence"} :
          s' = [s EXCEPT !.halt = cause, !.ready = FALSE]
IllicitHaltClear ==
    /\ FaultClearHalt /\ s.halt \in {"operator", "storage", "evidence", "contradiction"}
    /\ s' = [s EXCEPT !.halt = None, !.clearedHalt = TRUE]

\* Offline reattach accepts only an exact completed checkpoint. In this
\* focused model a run mismatch detected by Observe is conservatively a
\* contradiction halt, so reattach models the still-unlatched offline case.
Reattach ==
    /\ ~s.owner /\ ~s.offlineAlive /\ s.pending = None
    /\ s.halt = None /\ s.marker = Current /\ s.primary
    /\ s' = [s EXCEPT !.expectedRun = s.run, !.observed = None]

\* Full recovery retains/quarantines identity in DisasterRecovery.tla. Here
\* it discards the pending projection only as part of an explicit new epoch.
RecoverBegin ==
    /\ ~s.owner /\ ~s.offlineAlive /\ s.epoch = 0
    /\ s' = [s EXCEPT !.epoch = 1, !.schema = 2, !.checkpoint = 0,
        !.recoveryDiscarded = @ \cup
              (IF s.pending = None THEN {} ELSE {s.pending.target}),
        !.pending = None, !.caller = None, !.returning = None,
        !.observed = None, !.ready = FALSE, !.marker = None,
        !.expectedRun = s.run, !.halt = "recovery", !.offline = "sql",
        !.offlineAlive = TRUE]
RecoverPublish ==
    /\ s.offline = "sql" /\ s.offlineAlive /\ s.primary /\ s.marker = None
    /\ s' = [s EXCEPT !.marker = Current, !.offline = "redis"]
RecoverFinish ==
    /\ s.offline = "redis" /\ s.offlineAlive /\ Stable
    /\ s' = [s EXCEPT !.acknowledged = @ \cup {Current}, !.halt = None,
        !.offline = "none", !.offlineAlive = FALSE]
RecoveryCrash == s.offlineAlive /\ s' = [s EXCEPT !.offlineAlive = FALSE]

Next == Start \/ Crash \/ Cancel \/ TransportUncertain \/ Observe \/ Commit
    \/ Migrate \/ Publish \/ Acknowledge \/ ReturnPermission \/ PrematurePermission
    \/ RedisLoss \/ RedisRollback \/ RedisRestart \/ RedisRoleChange
    \/ RedisForeignToken \/ PermanentHalt \/ IllicitHaltClear \/ Reattach
    \/ RecoverBegin \/ RecoverPublish \/ RecoverFinish \/ RecoveryCrash
Spec == Init /\ [][Next]_vars

TypeOK ==
    /\ s.schema \in {1, 2} /\ s.epoch \in 0..1
    /\ s.checkpoint \in 0..MaxWrites /\ s.writes \in 0..MaxWrites
    /\ s.marker \in Tokens \cup {None}
    /\ s.run \in 0..1 /\ s.expectedRun \in 0..1
    /\ s.primary \in BOOLEAN /\ s.owner \in BOOLEAN /\ s.ready \in BOOLEAN
    /\ s.halt \in {None, "contradiction", "operator", "storage", "evidence", "recovery"}
    /\ s.acknowledged \subseteq Tokens /\ s.requiredPending \subseteq Tokens
    /\ s.recoveryDiscarded \subseteq Tokens /\ s.returned \subseteq Tokens
    /\ s.caller \in Tokens \cup {None} /\ s.returning \in Tokens \cup {None}
    /\ s.offline \in {"none", "sql", "redis"} /\ s.offlineAlive \in BOOLEAN

PermissionRequiresAcknowledgement == s.returned \subseteq s.acknowledged
PendingRetained == s.requiredPending \subseteq s.acknowledged \cup s.recoveryDiscarded
                    \cup (IF s.pending = None THEN {} ELSE {s.pending.target})
NoUnwitnessedRepair == ~s.badRepair
NoAutomaticRunAdoption == ~s.adoptedRun
NoResurrectedCaller == ~s.resurrectedCaller
NoArbitraryHaltClear == ~s.clearedHalt
OldMarkerRecoveryNotReached == ~s.resumedOld
NewMarkerRecoveryNotReached == ~s.resumedNew
MigrationRecoveryNotReached == ~s.resumedMigration
UncertainRecoveryNotReached == ~s.resumedUncertain
=============================================================================
