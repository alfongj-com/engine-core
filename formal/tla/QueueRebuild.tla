---------------------------- MODULE QueueRebuild ----------------------------
EXTENDS Naturals
CONSTANT Fault
VARIABLE s
vars == <<s>>
None == "none"

\* One intent, a trusted latest journal and one supported host. A process owns
\* the journal OR the offline tool does. The Redis lease may outlive the process.
Init == \E attempted \in BOOLEAN:
  s = [authority |-> "admitted", attempted |-> attempted,
       journalWire |-> IF attempted THEN "original" ELSE None,
       worker |-> TRUE, offline |-> FALSE, healthy |-> TRUE, lease |-> TRUE,
       queued |-> TRUE, wire |-> IF attempted THEN "original" ELSE None,
       restored |-> FALSE, budget |-> 0, grants |-> 0,
       badAuthority |-> FALSE, stolenLease |-> FALSE,
       changedWire |-> FALSE, madePermission |-> FALSE, repaired |-> FALSE]

Stop == s.worker /\ s' = [s EXCEPT !.worker = FALSE]
Start == ~s.worker /\ ~s.offline /\ s' = [s EXCEPT !.worker = TRUE]
Acquire == ~s.worker /\ ~s.offline /\ s' = [s EXCEPT !.offline = TRUE]
Release == s.offline /\ s' = [s EXCEPT !.offline = FALSE]
Expire == s.lease /\ s' = [s EXCEPT !.lease = FALSE]
LoseQueue == s.queued /\ s' = [s EXCEPT !.queued = FALSE,
                               !.wire = None, !.restored = FALSE]
Halt == s.healthy /\ s' = [s EXCEPT !.healthy = FALSE]
Finish == ~s.offline /\ s.authority = "admitted" /\ s.attempted
          /\ s' = [s EXCEPT !.authority = "terminal"]
Cancel == ~s.offline /\ s.authority = "admitted" /\ ~s.attempted
          /\ s' = [s EXCEPT !.authority = "quarantined"]
Sign == s.worker /\ s.healthy /\ ~s.offline /\ s.queued
        /\ s.authority = "admitted" /\ ~s.attempted
        /\ s' = [s EXCEPT !.attempted = TRUE,
                           !.journalWire = "original", !.wire = "original"]

Repair ==
  /\ s.offline /\ ~s.worker /\ ~s.queued
  /\ (s.healthy \/ Fault = "ignore_halt")
  /\ (s.authority = "admitted" \/ Fault = "revive")
  /\ (~s.lease \/ Fault = "steal_lease")
  /\ LET wire == IF Fault = "resign" /\ s.attempted THEN "different" ELSE s.journalWire
     IN s' = [s EXCEPT !.queued = TRUE, !.wire = wire,
          !.restored = s.attempted, !.repaired = TRUE,
          !.budget = IF Fault = "reset_budget" THEN 0 ELSE @,
          !.badAuthority = @ \/ s.authority # "admitted" \/ ~s.healthy,
          !.stolenLease = @ \/ s.lease,
          !.changedWire = @ \/ (s.attempted /\ wire # s.journalWire),
          !.madePermission = @ \/ Fault = "permission"]

\* Recovery grants consume the independent journal budget, not queue counters.
\* Their actual commit/CAS/ack and send checks belong to the retained-wire model.
Grant ==
  /\ s.worker /\ ~s.offline /\ s.healthy /\ s.authority = "admitted"
  /\ s.restored /\ s.queued /\ s.budget < 2 /\ s.grants < 3
  /\ s' = [s EXCEPT !.budget = @ + 1, !.grants = @ + 1]

Next == Stop \/ Start \/ Acquire \/ Release \/ Expire \/ LoseQueue
        \/ Halt \/ Finish \/ Cancel \/ Sign \/ Repair \/ Grant
Spec == Init /\ [][Next]_vars

AuthorityRequired == ~s.badAuthority
LeasePreserved == ~s.stolenLease
IdentityPreserved == ~s.changedWire
NoPermissionFromRepair == ~s.madePermission
RecoveryBudgetBounded == s.grants <= 2
RepairNotReached == ~s.repaired
=============================================================================
