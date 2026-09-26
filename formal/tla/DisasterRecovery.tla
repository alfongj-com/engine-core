--------------------------- MODULE DisasterRecovery --------------------------
EXTENDS Naturals, FiniteSets

CONSTANTS IDs, Keys, Workers, Payloads, None, MaxWrites, FaultSendBeforeJournal,
          FaultIdBinding, FaultKeyBinding, FaultSharedLock, FaultFreshRecovery,
          FaultRetryUid, AllowAuthorityRollback

Token(epoch, checkpoint) == [epoch |-> epoch, checkpoint |-> checkpoint]
Attempt(id, key, payload) == [id |-> id, key |-> key, payload |-> payload]
Attempts == [id : IDs, key : Keys, payload : Payloads]
Auths == [attempt : Attempts, worker : Workers, epoch : 0..1]
States == {"absent", "admitted", "terminal", "quarantined"}

VARIABLE s
vars == <<s>>
Current == Token(s.epoch, s.checkpoint)
Continuity == s.marker = Current /\ s.run = s.expectedRun
Healthy(w) == w \in s.owners /\ s.halt = None /\ Continuity
Attempted(id) == \E a \in s.attempts : a.id = id
Unbound(key, id) == \A other \in IDs : other = id \/ s.binding[other] # key

Init == s = [
    ledger |-> [id \in IDs |-> None], state |-> [id \in IDs |-> "absent"],
    binding |-> [id \in IDs |-> None], attempts |-> {}, acceptedEver |-> {},
    requiredEvidence |-> {}, terminalEver |-> {}, recoveryQuarantine |-> {},
    epoch |-> 0, checkpoint |-> 0, marker |-> Token(0, 0),
    run |-> 0, expectedRun |-> 0, halt |-> None, writes |-> 0,
    owners |-> {}, ownerEpoch |-> [w \in Workers |-> 0], pending |-> None,
    offline |-> "none", offlineAlive |-> FALSE,
    projection |-> {}, permits |-> {}, messages |-> {}, effects |-> {},
    everySendJournaled |-> TRUE, noOldAuthorization |-> TRUE,
    noTerminalEnqueue |-> TRUE, retriesUseOriginal |-> TRUE, lateExecution |-> FALSE]

Start(w) ==
    /\ w \notin s.owners
    /\ (s.owners = {} /\ ~s.offlineAlive) \/ FaultSharedLock
    /\ IF s.halt = None /\ Continuity
       THEN s' = [s EXCEPT !.owners = @ \cup {w}, !.ownerEpoch[w] = s.epoch]
       ELSE s' = [s EXCEPT !.halt = IF s.halt # None THEN s.halt
                     ELSE IF s.marker # Current THEN "continuity" ELSE "restart"]

Crash(w) ==
    /\ w \in s.owners
    /\ s' = [s EXCEPT !.owners = @ \ {w},
         !.pending = IF s.pending # None /\ s.pending.worker = w THEN None ELSE @,
         !.permits = {a \in @ : a.worker # w}]

\* SQLite transactions commit independently of the Redis CAS mirror. The
\* serial mutex allows at most one such operation in progress in the real owner.
Admit(w, id, payload) ==
    /\ Healthy(w) /\ s.pending = None /\ s.writes < MaxWrites
    /\ s.ledger[id] = None \/ (FaultIdBinding /\ s.ledger[id] # payload)
    /\ s' = [s EXCEPT !.ledger[id] = payload, !.state[id] = "admitted",
         !.acceptedEver = @ \cup {[id |-> id, payload |-> payload]},
         !.checkpoint = @ + 1, !.writes = @ + 1,
         !.pending = [kind |-> "admit", worker |-> w, id |-> id,
                      key |-> None, payload |-> payload, old |-> Current]]

AttemptCommit(w, id, key) ==
    /\ Healthy(w) /\ s.pending = None /\ s.writes < MaxWrites
    /\ s.state[id] = "admitted" /\ s.ledger[id] # None
    /\ s.binding[id] \in {None, key} \/ FaultIdBinding
    /\ Unbound(key, id) \/ FaultKeyBinding
    /\ LET attempt == Attempt(id, key, s.ledger[id])
       IN s' = [s EXCEPT !.binding[id] = key, !.attempts = @ \cup {attempt},
           !.requiredEvidence = @ \cup {attempt},
           !.checkpoint = @ + 1, !.writes = @ + 1,
           !.pending = [kind |-> "attempt", worker |-> w, id |-> id,
                        key |-> key, payload |-> s.ledger[id], old |-> Current]]

TerminalCommit(w, id) ==
    /\ Healthy(w) /\ s.pending = None /\ s.writes < MaxWrites
    /\ s.state[id] \in {"admitted", "quarantined"}
    \* Finality/evidence validation is a separate model: this transition assumes
    \* a matching real execution has been conclusively reconciled.
    /\ \E a \in s.effects : a.id = id /\ a \in s.attempts
    /\ s' = [s EXCEPT !.state[id] = "terminal", !.terminalEver = @ \cup {id},
         !.checkpoint = @ + 1, !.writes = @ + 1,
         !.pending = [kind |-> "terminal", worker |-> w, id |-> id,
                      key |-> None, payload |-> s.ledger[id], old |-> Current]]

Mirror ==
    /\ s.pending # None /\ s.pending.worker \in s.owners
    /\ LET p == s.pending
           authorization == [attempt |-> Attempt(p.id, p.key, p.payload),
                             worker |-> p.worker, epoch |-> s.ownerEpoch[p.worker]]
       IN IF s.marker = p.old /\ s.run = s.expectedRun /\ s.halt = None
          THEN s' = [s EXCEPT !.marker = Current, !.pending = None,
              !.permits = IF p.kind = "attempt" THEN @ \cup {authorization} ELSE @,
              !.noOldAuthorization = @ /\
                  (p.kind # "attempt" \/ s.ownerEpoch[p.worker] = s.epoch)]
          ELSE s' = [s EXCEPT !.halt = "continuity", !.pending = None]

\* Retrying a matching unsent admission returns its original payload/UID,
\* regardless of a freshly generated candidate carried by the caller.
Enqueue(w, id, candidate) ==
    /\ Healthy(w) /\ s.pending = None /\ s.state[id] = "admitted"
    /\ s.ledger[id] # None /\ candidate \in Payloads
    /\ LET returned == IF FaultRetryUid THEN candidate ELSE s.ledger[id]
       IN s' = [s EXCEPT !.projection = @ \cup {[id |-> id, payload |-> returned]},
           !.noTerminalEnqueue = @ /\ id \notin s.terminalEver,
           !.retriesUseOriginal = @ /\ returned = s.ledger[id]]

Send(a) ==
    /\ a \in s.permits /\ a.worker \in s.owners
    \* No healthy guard: an already authorized call can race a halt. Its
    \* original identity is durable before this authorization was released.
    /\ s' = [s EXCEPT !.permits = @ \ {a}, !.messages = @ \cup {a.attempt},
         !.everySendJournaled = @ /\ a.attempt \in s.attempts]

UnjournaledSend(w, id, key) ==
    /\ FaultSendBeforeJournal /\ Healthy(w) /\ s.state[id] = "admitted"
    /\ LET a == Attempt(id, key, s.ledger[id])
       IN s' = [s EXCEPT !.messages = @ \cup {a},
           !.everySendJournaled = @ /\ a \in s.attempts]

\* Chain execution is independent of Engine/Redis health and may follow crash.
\* A replay key can execute once. Global binding must keep different intents
\* away from that key; changing key can execute the same intent twice.
Execute(a) ==
    /\ a \in s.messages
    /\ ~\E old \in s.effects : old.key = a.key
    /\ s' = [s EXCEPT !.effects = @ \cup {a}, !.lateExecution = @ \/ s.halt # None]

RedisLoss == s' = [s EXCEPT !.marker = None, !.projection = {}]
RedisRollback == s.checkpoint > 0 /\
    s' = [s EXCEPT !.marker = Token(s.epoch, 0), !.projection = {}]
RedisRestart == s.run = 0 /\ s' = [s EXCEPT !.run = 1]
RepairMarker == s.halt \in {"continuity", "restart"} /\
    s' = [s EXCEPT !.marker = Current]

DetectMismatch ==
    /\ s.owners # {} /\ s.pending = None /\ s.halt = None /\ ~Continuity
    /\ s' = [s EXCEPT !.halt = IF s.marker # Current THEN "continuity" ELSE "restart"]

Reattach ==
    /\ s.owners = {} /\ ~s.offlineAlive /\ s.pending = None
    /\ s.halt \in {None, "restart"} /\ s.marker = Current
    /\ s' = [s EXCEPT !.expectedRun = s.run, !.halt = None]

RecoverBegin ==
    /\ (s.owners = {} /\ ~s.offlineAlive) \/ FaultSharedLock
    /\ s.pending = None /\ s.epoch = 0
    /\ s' = [s EXCEPT
         !.state = [id \in IDs |-> IF s.state[id] # "terminal" /\ Attempted(id)
                                  THEN IF FaultFreshRecovery THEN "admitted" ELSE "quarantined"
                                  ELSE s.state[id]],
         !.binding = IF FaultFreshRecovery THEN [id \in IDs |-> None] ELSE @,
         !.recoveryQuarantine = {id \in IDs : Attempted(id) /\ s.state[id] # "terminal"},
         !.epoch = 1, !.checkpoint = 0, !.marker = None, !.projection = {},
         !.expectedRun = s.run, !.halt = "recovery", !.offline = "sql", !.offlineAlive = TRUE]

RecoverPublish ==
    /\ s.offline = "sql" /\ s.offlineAlive /\ s.marker = None
    /\ s' = [s EXCEPT !.marker = Current, !.offline = "redis"]
RecoverFinish ==
    /\ s.offline = "redis" /\ s.offlineAlive /\ Continuity
    /\ s' = [s EXCEPT !.halt = None, !.offline = "none", !.offlineAlive = FALSE]
RecoveryCrash ==
    /\ s.offlineAlive
    /\ s' = [s EXCEPT !.offlineAlive = FALSE]

\* OUTSIDE the supported protocol: restoring a coherent old copy of BOTH
\* authority and Redis can erase knowledge of messages still able to execute.
AuthorityRollback ==
    /\ AllowAuthorityRollback /\ s.attempts # {} /\ s.owners = {}
    /\ s' = [s EXCEPT !.ledger = [id \in IDs |-> None],
         !.state = [id \in IDs |-> "absent"], !.binding = [id \in IDs |-> None],
         !.attempts = {}, !.checkpoint = 0, !.marker = Token(s.epoch, 0),
         !.halt = None, !.pending = None, !.projection = {}]

Next == (\E w \in Workers : Start(w) \/ Crash(w))
        \/ (\E w \in Workers, id \in IDs, p \in Payloads : Admit(w,id,p) \/ Enqueue(w,id,p))
        \/ (\E w \in Workers, id \in IDs, key \in Keys : AttemptCommit(w,id,key) \/ UnjournaledSend(w,id,key))
        \/ (\E w \in Workers, id \in IDs : TerminalCommit(w,id))
        \/ (\E a \in s.permits : Send(a)) \/ (\E a \in s.messages : Execute(a))
        \/ Mirror \/ RedisLoss \/ RedisRollback \/ RedisRestart \/ RepairMarker
        \/ DetectMismatch \/ Reattach \/ RecoverBegin \/ RecoverPublish
        \/ RecoverFinish \/ RecoveryCrash \/ AuthorityRollback
Spec == Init /\ [][Next]_vars

TypeOK ==
    /\ s.ledger \in [IDs -> Payloads \cup {None}]
    /\ s.state \in [IDs -> States] /\ s.binding \in [IDs -> Keys \cup {None}]
    /\ s.attempts \subseteq Attempts /\ s.requiredEvidence \subseteq Attempts
    /\ s.acceptedEver \subseteq [id : IDs, payload : Payloads]
    /\ s.terminalEver \subseteq IDs /\ s.recoveryQuarantine \subseteq IDs
    /\ s.projection \subseteq [id : IDs, payload : Payloads]
    /\ s.epoch \in 0..1 /\ s.checkpoint \in 0..MaxWrites /\ s.writes \in 0..MaxWrites
    /\ s.marker \in [epoch : 0..1, checkpoint : 0..MaxWrites] \cup {None}
    /\ s.run \in 0..1 /\ s.expectedRun \in 0..1
    /\ s.halt \in {None, "continuity", "restart", "recovery"}
    /\ s.owners \subseteq Workers /\ s.ownerEpoch \in [Workers -> 0..1]
    /\ s.offline \in {"none", "sql", "redis"} /\ s.offlineAlive \in BOOLEAN
    /\ s.permits \subseteq Auths /\ s.messages \subseteq Attempts /\ s.effects \subseteq Attempts
    /\ s.everySendJournaled \in BOOLEAN /\ s.noOldAuthorization \in BOOLEAN
    /\ s.noTerminalEnqueue \in BOOLEAN /\ s.retriesUseOriginal \in BOOLEAN
    /\ s.lateExecution \in BOOLEAN

ImmutableAdmission == \A a,b \in s.acceptedEver : a.id = b.id => a.payload = b.payload
ReplayKeyUnique == \A a,b \in IDs : a # b /\ s.binding[a] # None => s.binding[a] # s.binding[b]
EverySendJournaled == s.everySendJournaled
EvidenceRetained == s.requiredEvidence \subseteq s.attempts
NoOldEpochAuthorization == s.noOldAuthorization
NoTerminalReenqueue == s.noTerminalEnqueue
OriginalPayloadOnRetry == s.retriesUseOriginal
AtMostOneEffectPerId == \A a,b \in s.effects : a.id = b.id => a.key = b.key
AttemptedRecoveryIsQuarantined == \A id \in s.recoveryQuarantine :
    s.state[id] \in {"quarantined", "terminal"}
LateExecutionWitnessNotReached == ~s.lateExecution
=============================================================================
