------------------------- MODULE SolanaRecovery -------------------------
EXTENDS Naturals, FiniteSets, Sequences, TLC

CONSTANTS Workers, Identities, MaxClaims, MaxBroadcasts, MaxChecks,
          MaxMessages, FaultRefreshOnAbsence, FaultSendBeforePersist,
          FaultStaleCompletion, FaultProviderLies, FaultRedisLoss, FaultReorg,
          ExploreInterruptions

ASSUME /\ Workers # {} /\ Identities # {} /\ 0 \notin Identities
       /\ MaxClaims > 0 /\ MaxBroadcasts > 0 /\ MaxChecks > 0

VARIABLE s
vars == <<s>>
Phases == {"Idle", "Load", "Recover", "Build", "Built", "PrepareSend",
           "Send", "Retry", "Check", "CheckAgain", "Observed", "ObservedAgain",
           "Validity", "Park", "Proof", "Complete"}
Observations == {"Unknown", "Absent", "Error", "Visible", "Success", "Failure"}
ChainStates == {"Unseen", "ProcessedOk", "ProcessedErr", "FinalOk", "FinalErr"}
OwnQueue(w) == s.token[w] # 0 /\ s.token[w] = s.queueOwner
OwnStorage(w) == s.token[w] # 0 /\ s.token[w] = s.storageOwner
Dispatched == {s.messages[n].identity : n \in 1..Len(s.messages)}

Init == s = [
  epoch |-> 0, queueOwner |-> 0, storageOwner |-> 0,
  queue |-> "Pending", admission |-> "Active", attempt |-> 0,
  broadcasts |-> 0, checks |-> 0, reservations |-> 0,
  phase |-> [w \in Workers |-> "Idle"], token |-> [w \in Workers |-> 0],
  local |-> [w \in Workers |-> 0], observed |-> [w \in Workers |-> "Unknown"],
  chain |-> [i \in Identities |-> "Unseen"], expired |-> {}, effects |-> {},
  persistedEver |-> {}, messages |-> <<>>,
  terminalIdentity |-> 0, terminalKind |-> "None", staleTerminal |-> FALSE]

Claim(w) ==
  /\ s.phase[w] = "Idle" /\ s.queue = "Pending" /\ s.queueOwner = 0
  /\ s.epoch < MaxClaims
  /\ s' = [s EXCEPT !.epoch = @ + 1, !.queueOwner = s.epoch + 1,
                    !.token[w] = s.epoch + 1, !.phase[w] = "Load", !.queue = "Active"]

Lock(w) ==
  /\ s.phase[w] = "Load" /\ OwnQueue(w) /\ s.storageOwner = 0
  /\ s' = [s EXCEPT !.storageOwner = s.token[w], !.phase[w] = "Recover"]

LoadAttempt(w) ==
  /\ s.phase[w] = "Recover" /\ OwnStorage(w)
  /\ s' = [s EXCEPT !.local[w] = s.attempt,
                    !.phase[w] = IF s.attempt = 0 THEN "Build" ELSE "Check"]

Build(w, i) ==
  /\ s.phase[w] = "Build" /\ OwnStorage(w) /\ s.attempt = 0
  /\ i \notin s.expired
  /\ s' = [s EXCEPT !.local[w] = i, !.phase[w] = "Built"]

Persist(w) ==
  /\ s.phase[w] = "Built" /\ OwnStorage(w) /\ s.attempt = 0
  /\ s' = [s EXCEPT !.attempt = s.local[w], !.broadcasts = 0, !.checks = 0,
                    !.persistedEver = @ \cup {s.local[w]}, !.phase[w] = "PrepareSend"]

PrepareSend(w) ==
  /\ s.phase[w] = "PrepareSend" /\ OwnStorage(w) /\ s.attempt = s.local[w]
  /\ s.broadcasts < MaxBroadcasts /\ s.reservations < MaxMessages
  /\ s' = [s EXCEPT !.broadcasts = @ + 1, !.reservations = @ + 1, !.phase[w] = "Send"]

NoSendAllowance(w) ==
  /\ s.phase[w] = "PrepareSend" /\ s.broadcasts = MaxBroadcasts
  /\ s' = [s EXCEPT !.phase[w] = "Retry"]

BypassPersistence(w) ==
  /\ FaultSendBeforePersist /\ s.phase[w] = "Built" /\ OwnStorage(w)
  /\ s' = [s EXCEPT !.phase[w] = "Send"]

\* Queue/storage expiry may occur AFTER the final ownership check and BEFORE
\* HTTP dispatch. The bytes already reserved remain identical; do not assert
\* that an external effect is fenced by a Redis lease at the instant it occurs.
Send(w) ==
  /\ s.phase[w] = "Send" /\ Len(s.messages) < MaxMessages
  /\ s' = [s EXCEPT !.messages = Append(@,
         [identity |-> s.local[w], persistedBefore |-> s.local[w] \in s.persistedEver]),
                    !.phase[w] = "Retry", !.observed[w] = "Unknown"]

\* Ledger changes do not depend on what Engine knows. A send can be lost,
\* accepted without execution, or execute after its response has been lost.
Execute(i, result) ==
  /\ i \in Dispatched /\ i \notin s.expired /\ s.chain[i] = "Unseen"
  /\ s' = [s EXCEPT !.chain[i] = result,
                    !.effects = IF result = "ProcessedOk" THEN @ \cup {i} ELSE @]

Finalize(i) ==
  /\ s.chain[i] \in {"ProcessedOk", "ProcessedErr"}
  /\ s' = [s EXCEPT !.chain[i] = IF @ = "ProcessedOk" THEN "FinalOk" ELSE "FinalErr"]

ExpireHash(i) ==
  /\ i \notin s.expired
  /\ s' = [s EXCEPT !.expired = @ \cup {i}]

AllowedObservations(i) ==
  {"Absent", "Error"}
  \cup (IF s.chain[i] # "Unseen" THEN {"Visible"} ELSE {})
  \cup (IF s.chain[i] = "FinalOk" \/ FaultProviderLies THEN {"Success"} ELSE {})
  \cup (IF s.chain[i] = "FinalErr" \/ FaultProviderLies THEN {"Failure"} ELSE {})

Observe(w, answer) ==
  /\ s.phase[w] \in {"Check", "CheckAgain"} /\ OwnStorage(w)
  /\ s.attempt = s.local[w] /\ s.checks < MaxChecks
  /\ answer \in AllowedObservations(s.local[w])
  /\ s' = [s EXCEPT !.checks = @ + 1, !.observed[w] = answer,
                    !.phase[w] = IF @ = "Check" THEN "Observed" ELSE "ObservedAgain"]

CheckBudget(w) ==
  /\ s.phase[w] \in {"Check", "CheckAgain"} /\ s.checks = MaxChecks
  /\ s' = [s EXCEPT !.phase[w] = "Park"]

Decide(w) ==
  /\ s.phase[w] \in {"Observed", "ObservedAgain"}
  /\ s' = [s EXCEPT !.phase[w] =
       IF s.observed[w] \in {"Success", "Failure"} THEN "Proof"
       ELSE IF s.observed[w] \in {"Visible", "Error"} THEN "Retry"
       ELSE IF s.phase[w] = "Observed" THEN "Validity" ELSE "Park"]

Validity(w) ==
  /\ s.phase[w] = "Validity" /\ OwnStorage(w)
  /\ s' = [s EXCEPT !.phase[w] = IF s.local[w] \in s.expired
                                        THEN "CheckAgain" ELSE "PrepareSend"]

\* Deliberately unsafe alternative: a second absent response after expiry
\* forgets the old bytes and creates another signature for the SAME intent.
RefreshOnAbsence(w, i) ==
  /\ FaultRefreshOnAbsence /\ s.phase[w] = "ObservedAgain"
  /\ s.observed[w] = "Absent" /\ OwnStorage(w)
  /\ i \notin s.persistedEver /\ i \notin s.expired
  /\ s' = [s EXCEPT !.attempt = 0, !.local[w] = i, !.phase[w] = "Built"]

ReleaseForCommit(w) ==
  /\ s.phase[w] = "Proof"
  /\ s' = [s EXCEPT !.storageOwner = IF OwnStorage(w) THEN 0 ELSE @,
                    !.phase[w] = "Complete"]

CommitTerminal(w) ==
  /\ s.phase[w] = "Complete" /\ (OwnQueue(w) \/ FaultStaleCompletion)
  /\ s' = [s EXCEPT !.terminalIdentity = s.local[w], !.terminalKind = s.observed[w],
                    !.staleTerminal = @ \/ ~OwnQueue(w),
                    !.admission = IF s.observed[w] = "Success" THEN "Completed" ELSE "Failed",
                    !.queue = "Terminal", !.queueOwner = 0, !.attempt = 0,
                    !.broadcasts = 0, !.checks = 0, !.phase[w] = "Idle",
                    !.token[w] = 0, !.local[w] = 0, !.observed[w] = "Unknown"]

Retry(w) ==
  /\ s.phase[w] = "Retry"
  /\ s' = [s EXCEPT !.storageOwner = IF OwnStorage(w) THEN 0 ELSE @,
                    !.queueOwner = IF OwnQueue(w) THEN 0 ELSE @,
                    !.queue = IF OwnQueue(w) THEN "Pending" ELSE @,
                    !.phase[w] = "Idle", !.token[w] = 0,
                    !.local[w] = 0, !.observed[w] = "Unknown"]

Park(w) ==
  /\ s.phase[w] = "Park" /\ OwnQueue(w)
  /\ s' = [s EXCEPT !.storageOwner = IF OwnStorage(w) THEN 0 ELSE @,
                    !.queueOwner = 0, !.queue = "Parked", !.phase[w] = "Idle",
                    !.token[w] = 0, !.local[w] = 0, !.observed[w] = "Unknown"]

DiscardStale(w) ==
  /\ s.phase[w] # "Idle" /\ ~OwnQueue(w)
  /\ s' = [s EXCEPT !.storageOwner = IF OwnStorage(w) THEN 0 ELSE @,
                    !.phase[w] = "Idle", !.token[w] = 0,
                    !.local[w] = 0, !.observed[w] = "Unknown"]

Crash(w) ==
  /\ s.phase[w] # "Idle"
  /\ s' = [s EXCEPT !.phase[w] = "Idle", !.token[w] = 0,
                    !.local[w] = 0, !.observed[w] = "Unknown"]

ExpireQueueLease ==
  /\ s.queueOwner # 0
  /\ s' = [s EXCEPT !.queueOwner = 0, !.queue = "Pending"]

ExpireStorageLease ==
  /\ s.storageOwner # 0
  /\ s' = [s EXCEPT !.storageOwner = 0]

Cancel ==
  /\ s.queue = "Pending" /\ s.queueOwner = 0
  /\ s' = [s EXCEPT !.queue = "Cancelled"]

Resume ==
  /\ s.queue \in {"Cancelled", "Parked"} /\ s.attempt # 0 /\ s.storageOwner = 0
  /\ s' = [s EXCEPT !.queue = "Pending", !.checks = 0]

LoseRedisAttempt ==
  /\ FaultRedisLoss /\ s.attempt # 0
  /\ s' = [s EXCEPT !.attempt = 0, !.broadcasts = 0, !.checks = 0]

Reorg(i) ==
  /\ FaultReorg /\ s.chain[i] \in {"FinalOk", "FinalErr"}
  /\ s' = [s EXCEPT !.chain[i] = "Unseen"]

WorkerStep(w) == Claim(w) \/ Lock(w) \/ LoadAttempt(w) \/ Persist(w)
       \/ PrepareSend(w) \/ NoSendAllowance(w) \/ BypassPersistence(w) \/ Send(w)
       \/ CheckBudget(w) \/ Decide(w) \/ Validity(w) \/ ReleaseForCommit(w)
       \/ CommitTerminal(w) \/ Retry(w) \/ Park(w) \/ DiscardStale(w)
       \/ (ExploreInterruptions /\ Crash(w))
       \/ (\E i \in Identities : Build(w, i) \/ RefreshOnAbsence(w, i))
       \/ (\E answer \in Observations : Observe(w, answer))
ChainStep(i) == Finalize(i) \/ ExpireHash(i) \/ Reorg(i)
       \/ (\E result \in {"ProcessedOk", "ProcessedErr"} : Execute(i, result))
Next ==
  \/ (\E w \in Workers : WorkerStep(w))
  \/ (\E i \in Identities : ChainStep(i))
  \/ (ExploreInterruptions /\ (ExpireQueueLease \/ ExpireStorageLease \/ Cancel \/ Resume))
  \/ LoseRedisAttempt

TypeOK ==
  /\ s.epoch \in 0..MaxClaims /\ s.queueOwner \in 0..MaxClaims /\ s.storageOwner \in 0..MaxClaims
  /\ s.queue \in {"Pending", "Active", "Parked", "Cancelled", "Terminal"}
  /\ s.admission \in {"Active", "Completed", "Failed"}
  /\ s.attempt \in Identities \cup {0} /\ s.broadcasts \in 0..MaxBroadcasts
  /\ s.checks \in 0..MaxChecks /\ s.reservations \in 0..MaxMessages
  /\ s.phase \in [Workers -> Phases] /\ s.token \in [Workers -> 0..MaxClaims]
  /\ s.local \in [Workers -> Identities \cup {0}] /\ s.observed \in [Workers -> Observations]
  /\ s.chain \in [Identities -> ChainStates] /\ s.expired \subseteq Identities
  /\ s.effects \subseteq Identities /\ s.persistedEver \subseteq Identities
  /\ Len(s.messages) <= MaxMessages
  /\ s.terminalIdentity \in Identities \cup {0}
  /\ s.terminalKind \in {"None", "Success", "Failure"} /\ s.staleTerminal \in BOOLEAN

EverySendDurable == \A n \in 1..Len(s.messages) : s.messages[n].persistedBefore
EverySendReserved == Len(s.messages) <= s.reservations
LifetimeSendLimit == s.reservations <= MaxBroadcasts /\ Len(s.messages) <= MaxBroadcasts
ImmutableSignedIdentity == Cardinality(Dispatched) <= 1
AtMostOneEffect == Cardinality(s.effects) <= 1
TerminalCommitOwned == ~s.staleTerminal
UnresolvedEvidenceRetained == (Dispatched # {} /\ s.admission = "Active") => s.attempt # 0
TerminalHasChainProof ==
  /\ (s.terminalKind = "Success" => s.chain[s.terminalIdentity] = "FinalOk")
  /\ (s.terminalKind = "Failure" => s.chain[s.terminalIdentity] = "FinalErr")
TerminalCleanupAtomic == (s.admission # "Active") => (s.attempt = 0 /\ s.queue = "Terminal")

\* Expected-negative reachability check, NOT a safety requirement. Its
\* counterexample proves the production-size send bound was actually reached.
BroadcastLimitWitnessNotReached == Len(s.messages) < MaxBroadcasts

Spec == Init /\ [][Next]_vars
=============================================================================
