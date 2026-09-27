------------------------------ MODULE Admission -----------------------------
EXTENDS Naturals, FiniteSets
CONSTANTS Fingerprints, MaxAdmissions, None, OverwriteConflict, ExpireActive,
          SplitAdmission, CheckLifetime

VARIABLES generation, identity, phase, queued, attempt, retainedHistory,
          accepted, effects
vars == <<generation, identity, phase, queued, attempt, retainedHistory,
          accepted, effects>>
Init ==
    /\ generation = 0 /\ identity = None /\ phase = "absent"
    /\ queued = FALSE /\ attempt = None /\ retainedHistory = FALSE
    /\ accepted = {} /\ effects = {}

\* One request ID, competing immutable request fingerprints. A generation is a
\* ghost variable: the implementation does not promise protection after BOTH
\* identity retention and job history disappear.
Admit(f) ==
    /\ identity = None /\ ~queued /\ attempt = None /\ ~retainedHistory
    /\ generation < MaxAdmissions
    /\ generation' = generation + 1
    /\ identity' = f /\ phase' = "active"
    /\ queued' = ~SplitAdmission
    /\ accepted' = accepted \cup {[gen |-> generation + 1, fp |-> f]}
    /\ UNCHANGED <<attempt, retainedHistory, effects>>

\* Retried identical requests are a stutter. Changed requests are rejected.
\* The faulty branch models overwriting an existing live/retained request.
ConflictingAdmission(f) ==
    /\ OverwriteConflict /\ identity # None /\ f # identity
    /\ identity' = f
    /\ accepted' = accepted \cup {[gen |-> generation, fp |-> f]}
    /\ UNCHANGED <<generation, phase, queued, attempt, retainedHistory, effects>>

PersistAttempt ==
    /\ phase = "active" /\ queued /\ attempt = None
    /\ attempt' = [gen |-> generation, fp |-> identity]
    /\ UNCHANGED <<generation, identity, phase, queued, retainedHistory,
                    accepted, effects>>

\* Includes an accepted send whose response is lost. Cancellation and crashes
\* cannot remove its identity: it may execute after either has happened.
Execute ==
    /\ attempt # None
    /\ effects' = effects \cup {attempt}
    /\ UNCHANGED <<generation, identity, phase, queued, attempt,
                    retainedHistory, accepted>>

Complete ==
    /\ phase = "active" /\ attempt # None /\ attempt \in effects
    /\ phase' = "terminal" /\ queued' = FALSE /\ attempt' = None
    /\ retainedHistory' = TRUE
    /\ UNCHANGED <<generation, identity, accepted, effects>>

Cancel ==
    /\ phase = "active" /\ queued
    /\ queued' = FALSE /\ phase' = "orphan"
    /\ UNCHANGED <<generation, identity, attempt, retainedHistory, accepted, effects>>

PruneHistory ==
    /\ retainedHistory /\ retainedHistory' = FALSE
    /\ UNCHANGED <<generation, identity, phase, queued, attempt, accepted, effects>>

ExpireIdentity ==
    /\ identity # None
    /\ phase = "terminal" \/ ExpireActive
    /\ identity' = None
    /\ phase' = IF phase = "terminal" THEN "absent" ELSE phase
    /\ UNCHANGED <<generation, queued, attempt, retainedHistory, accepted, effects>>

Next == (\E f \in Fingerprints : Admit(f) \/ ConflictingAdmission(f))
        \/ PersistAttempt \/ Execute \/ Complete \/ Cancel
        \/ PruneHistory \/ ExpireIdentity
Spec == Init /\ [][Next]_vars

TypeOK ==
    /\ generation \in 0..MaxAdmissions
    /\ identity \in Fingerprints \cup {None}
    /\ phase \in {"absent", "active", "terminal", "orphan"}
    /\ queued \in BOOLEAN /\ retainedHistory \in BOOLEAN
    /\ attempt \in [gen : 1..MaxAdmissions, fp : Fingerprints] \cup {None}
    /\ accepted \subseteq [gen : 1..MaxAdmissions, fp : Fingerprints]
    /\ effects \subseteq accepted
ImmutableWithinRetention == \A a,b \in accepted : a.gen = b.gen => a.fp = b.fp
ActiveIdentityRetained == phase \in {"active", "orphan"} => identity # None
AdmissionIsAtomic == phase = "active" => queued
AttemptBoundToIntent == attempt # None => attempt.fp = identity
\* This deliberately stronger claim is false once both retention records expire.
LifetimeAtMostOnce == ~CheckLifetime \/ Cardinality(effects) <= 1
=============================================================================
