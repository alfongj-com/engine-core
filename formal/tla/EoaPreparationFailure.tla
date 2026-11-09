----------------------- MODULE EoaPreparationFailure -----------------------
EXTENDS Naturals
CONSTANTS Fault, Mode
VARIABLE s
vars == <<s>>

ASSUME Fault \in {"none", "publish_without_reject", "ignore_attempt"}
ASSUME Mode \in {"rebuild", "retry", "race"}

\* One immutable intent, two independently scheduled preparations and one wire.
\* FULL journal authority survives process crashes and Redis projection loss.
\* The pending-checkpoint bit abstracts publication acknowledgement; its actual
\* CAS/ack/reattach protocol belongs to DisasterRecoveryProjection.
Init == s = [authority |-> "admitted", evidence |-> "none", attempt |-> FALSE,
    witness |-> "none", witnessPayload |-> "none", checkpoint |-> FALSE,
    running |-> TRUE, queue |-> TRUE, projection |-> "pending",
    rejector |-> "idle", observedPayload |-> "none", observedFailure |-> "none",
    sender |-> "idle", network |-> "empty", everFailed |-> FALSE,
    reportedWitness |-> "none", rebuilt |-> FALSE, crashAfterReject |-> FALSE,
    recoveryPublished |-> FALSE, broadcastRefusal |-> FALSE]

ObserveFailure ==
    /\ s.running /\ ~s.checkpoint /\ s.queue /\ s.projection = "pending"
    /\ s.authority = "admitted" /\ s.rejector = "idle"
    /\ \E payload \in {"original", "foreign"}, diagnostic \in {"gas", "fee"}:
        s' = [s EXCEPT !.rejector = "observed", !.observedPayload = payload,
                       !.observedFailure = diagnostic]

\* Checking absence of a replay binding/attempt and committing the rejection is
\* one journal transition. A stale earlier read cannot authorize this decision.
RejectInJournal ==
    /\ s.running /\ ~s.checkpoint /\ s.rejector = "observed"
    /\ s.authority = "admitted" /\ s.observedPayload = "original"
    /\ Fault # "publish_without_reject"
    /\ (~s.attempt \/ Fault = "ignore_attempt")
    /\ s' = [s EXCEPT !.authority = "terminal",
        !.evidence = "preparation_failure", !.witness = s.observedFailure,
        !.witnessPayload = s.observedPayload, !.checkpoint = TRUE,
        !.rejector = "authorized"]

RefuseFailure ==
    /\ s.running /\ ~s.checkpoint /\ s.rejector = "observed"
    /\ (s.observedPayload # "original"
        \/ (s.attempt /\ Fault # "ignore_attempt")
        \/ (s.authority = "terminal" /\ s.evidence # "preparation_failure"))
    /\ s' = [s EXCEPT !.rejector = "idle",
        !.broadcastRefusal = @ \/ (s.attempt /\ s.observedPayload = "original")]

\* A resumed worker reads the first exact terminal witness. It does not repeat
\* simulation/signing and cannot mistake a chain-finality terminal for failure.
RecoverFailure ==
    /\ s.running /\ ~s.checkpoint /\ s.queue /\ s.projection = "pending"
    /\ s.authority = "terminal" /\ s.evidence = "preparation_failure"
    /\ ~s.attempt /\ s.witnessPayload = "original"
    /\ (s.rejector = "idle"
        \/ (s.rejector = "observed" /\ s.observedPayload = "original"))
    /\ s' = [s EXCEPT !.rejector = "recovered"]

\* The first visible failed status OR failure webhook counts as publication.
\* Remaining Redis writes may be partial; safety cannot depend on their cleanup.
PublishFailure ==
    /\ s.running /\ ~s.checkpoint /\ s.queue /\ s.projection = "pending"
    /\ IF Fault = "publish_without_reject"
       THEN s.rejector = "observed" /\ s.observedPayload = "original"
       ELSE s.rejector \in {"authorized", "recovered"}
            /\ s.authority = "terminal" /\ s.evidence = "preparation_failure"
    /\ \E statusWritten \in BOOLEAN, removed \in BOOLEAN:
        s' = [s EXCEPT !.projection = IF statusWritten THEN "failed" ELSE @,
            !.queue = IF removed THEN FALSE ELSE @,
            !.everFailed = TRUE, !.rejector = "idle",
            !.reportedWitness = IF Fault = "publish_without_reject"
                                THEN s.observedFailure ELSE s.witness,
            !.recoveryPublished = @ \/ (s.crashAfterReject /\ s.rejector = "recovered")]

\* In recovery scenarios a later preparation succeeds after queue restoration.
\* Race mode also permits a successful preparation already in progress when
\* another worker observes failure. No nonce/wire mutation is abstracted here.
PrepareSender ==
    /\ s.running /\ ~s.checkpoint /\ s.queue /\ s.projection = "pending"
    /\ s.authority = "admitted" /\ s.sender = "idle"
    /\ (Mode = "race" \/ s.rebuilt)
    /\ s' = [s EXCEPT !.sender = "prepared"]

ReserveBroadcast ==
    /\ s.running /\ ~s.checkpoint /\ s.sender = "prepared"
    /\ s.authority = "admitted"
    /\ s' = [s EXCEPT !.attempt = TRUE, !.sender = "authorized"]

Dispatch == s.running /\ s.sender = "authorized" /\ s.attempt
            /\ s.network = "empty"
            /\ s' = [s EXCEPT !.network = "pending"]

\* A dispatched transaction can land after a crash, missing response or TTL.
Deliver == s.network = "pending" /\ s' = [s EXCEPT !.network = "delivered"]

Finalize ==
    /\ s.running /\ ~s.checkpoint /\ s.network = "delivered"
    /\ s.authority = "admitted" /\ s.attempt
    /\ s' = [s EXCEPT !.authority = "terminal", !.evidence = "chain_finality",
                       !.checkpoint = TRUE]

Mirror == s.running /\ s.checkpoint /\ s' = [s EXCEPT !.checkpoint = FALSE]

Crash ==
    /\ s.running
    /\ s' = [s EXCEPT !.running = FALSE, !.rejector = "idle", !.sender = "idle",
        !.observedPayload = "none", !.observedFailure = "none",
        !.crashAfterReject = @ \/ (s.evidence = "preparation_failure"
                                   /\ s.checkpoint /\ ~s.everFailed)]

Restart == ~s.running /\ s' = [s EXCEPT !.running = TRUE]

\* Selective projection loss/TTL does not erase the journal or the observer's
\* history. Initial unrelated unattempted loss is covered by QueueRebuild; here
\* loss follows rejection, a reported failure, or a durable broadcast attempt.
LoseProjection ==
    /\ (s.everFailed \/ s.authority = "terminal" \/ s.attempt)
    /\ s.projection # "missing"
    /\ s' = [s EXCEPT !.queue = FALSE, !.projection = "missing"]

Rebuild ==
    /\ Mode # "retry" /\ ~s.running /\ ~s.checkpoint
    /\ s.projection = "missing" /\ s.authority = "admitted"
    /\ s' = [s EXCEPT !.queue = TRUE, !.projection = "pending", !.rebuilt = TRUE]

RetrySameId ==
    /\ Mode = "retry" /\ s.running /\ ~s.checkpoint
    /\ s.projection = "missing" /\ s.authority = "admitted"
    /\ s' = [s EXCEPT !.queue = TRUE, !.projection = "pending", !.rebuilt = TRUE]

Next == ObserveFailure \/ RejectInJournal \/ RefuseFailure \/ RecoverFailure
        \/ PublishFailure \/ PrepareSender \/ ReserveBroadcast \/ Dispatch
        \/ Deliver \/ Finalize \/ Mirror \/ Crash \/ Restart \/ LoseProjection
        \/ Rebuild \/ RetrySameId
Spec == Init /\ [][Next]_vars

TypeOK ==
    /\ s.authority \in {"admitted", "terminal"}
    /\ s.evidence \in {"none", "preparation_failure", "chain_finality"}
    /\ s.attempt \in BOOLEAN /\ s.checkpoint \in BOOLEAN /\ s.running \in BOOLEAN
    /\ s.witness \in {"none", "gas", "fee"}
    /\ s.witnessPayload \in {"none", "original", "foreign"}
    /\ s.projection \in {"pending", "failed", "missing"} /\ s.queue \in BOOLEAN
    /\ s.rejector \in {"idle", "observed", "authorized", "recovered"}
    /\ s.observedPayload \in {"none", "original", "foreign"}
    /\ s.observedFailure \in {"none", "gas", "fee"}
    /\ s.sender \in {"idle", "prepared", "authorized"}
    /\ s.network \in {"empty", "pending", "delivered"}
    /\ s.everFailed \in BOOLEAN /\ s.reportedWitness \in {"none", "gas", "fee"}
    /\ s.rebuilt \in BOOLEAN /\ s.crashAfterReject \in BOOLEAN
    /\ s.recoveryPublished \in BOOLEAN /\ s.broadcastRefusal \in BOOLEAN

ReportedFailureCannotExecute == ~(s.everFailed /\ s.network = "delivered")
RejectedIntentHasNoAttempt == s.evidence = "preparation_failure" => ~s.attempt
PriorAttemptNeverReportedFailed == s.everFailed => ~s.attempt
FailureHasDurableEvidence == s.everFailed =>
    s.authority = "terminal" /\ s.evidence = "preparation_failure"
    /\ s.reportedWitness = s.witness /\ s.witness # "none"
ExactPayloadRejected == s.evidence = "preparation_failure" => s.witnessPayload = "original"

RecoveryPublicationNotReached == ~s.recoveryPublished
BroadcastWinnerNotReached == ~(s.broadcastRefusal /\ s.network = "delivered")
=============================================================================
