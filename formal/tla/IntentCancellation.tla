------------------------- MODULE IntentCancellation -------------------------
EXTENDS Naturals
CONSTANT FaultCachedRead
VARIABLE s
vars == <<s>>

Init == s = [authority |-> "admitted", attempt |-> FALSE, worker |-> FALSE,
             queue |-> TRUE, api |-> "idle", observedUnsigned |-> FALSE,
             emitted |-> FALSE]

\* Worker may be building/signing even after its Redis job is cancelled.
Build == /\ s.queue /\ ~s.worker
         /\ s' = [s EXCEPT !.worker = TRUE]

ReadForCancel == /\ s.api = "idle"
                 /\ s' = [s EXCEPT !.api = "checked",
                           !.observedUnsigned = ~s.attempt]

\* Fixed path checks the current authority and revokes before queue mutation.
\* Fault models the old API's earlier read followed by separate Redis cancel.
Cancel ==
    /\ s.api = "checked"
    /\ IF (IF FaultCachedRead THEN s.observedUnsigned ELSE ~s.attempt)
       THEN s' = [s EXCEPT !.api = "cancelled",
                   !.authority = IF FaultCachedRead THEN @ ELSE "quarantined"]
       ELSE s' = [s EXCEPT !.api = "refused"]

CancelQueue == /\ s.api = "cancelled" /\ s.queue
               /\ s' = [s EXCEPT !.queue = FALSE]

\* One FULL transaction binds permission. A later lost reply or process
\* restart does not erase either this binding or cancellation's revocation.
ReserveBroadcast == /\ s.worker /\ s.authority = "admitted" /\ ~s.attempt
                    /\ s' = [s EXCEPT !.attempt = TRUE]
Emit == /\ s.attempt /\ ~s.emitted
        /\ s' = [s EXCEPT !.emitted = TRUE]

Next == Build \/ ReadForCancel \/ Cancel \/ CancelQueue \/ ReserveBroadcast \/ Emit
Spec == Init /\ [][Next]_vars

TypeOK == /\ s.authority \in {"admitted", "quarantined"}
          /\ s.attempt \in BOOLEAN /\ s.worker \in BOOLEAN
          /\ s.queue \in BOOLEAN /\ s.observedUnsigned \in BOOLEAN
          /\ s.api \in {"idle", "checked", "cancelled", "refused"}
          /\ s.emitted \in BOOLEAN
CancellationExcludesAttempt == s.api = "cancelled" => ~s.attempt
EmissionRequiresAuthority == s.emitted => s.attempt
SuccessfulCancellationNotReached == s.api # "cancelled"
=============================================================================
