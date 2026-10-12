----------------------------- MODULE QueueLease -----------------------------
EXTENDS Naturals, FiniteSets, Sequences, TLC

(***************************************************************************
 One queue ID, bounded generations and leases, concurrent completion calls.
 Queue data and indexes are independent variables: invariants check their
 agreement. Session WATCH, EXISTS and EXEC are distinct interleaving steps.
 This is a safety abstraction of twmq, not a translation of Rust or Redis.
 See ../queue.md for atomicity, error, cancellation and pruning boundaries.
 ***************************************************************************)
CONSTANTS Callers, Sessions, MaxLeases, MaxGenerations, MaxCalls,
          ActiveIdempotency, EnableCancel, EnablePrune,
          FaultNoWatch, FaultSharedSession, FaultNilIsSuccess,
          FaultReuseLease, FaultPruneLive, FaultDelayedGuard, FaultPruneRetained,
          FaultKeepCancellation,
          FaultHistoricalSuccess

VARIABLES generation, issued, tokenGeneration, leaseLive, activeToken,
          pending, delayed, active, data, meta, dedup,
          successes, failures, cancel, starts,
          pc, callToken, callKind, connection,
          owners, watched, dirty, commits, falseCommit, cancelReborrow

vars == <<generation, issued, tokenGeneration, leaseLive, activeToken,
          pending, delayed, active, data, meta, dedup,
          successes, failures, cancel, starts,
          pc, callToken, callKind, connection,
          owners, watched, dirty, commits, falseCommit, cancelReborrow>>
Tokens == 1..MaxLeases
Generations == 1..MaxGenerations
Kinds == {"ack", "fail", "nack", "delay"}
Busy == {"watch", "read", "exec"}
NoSession == "no-session"
Live == pending \/ delayed \/ active
LeaseKey(t) == IF FaultReuseLease THEN 1 ELSE t
Valid(t) == /\ LeaseKey(t) \in leaseLive
            /\ active /\ activeToken = t
            /\ tokenGeneration[t] = generation
Invalidate(t, except) ==
    [s \in Sessions |-> IF s = except THEN FALSE
                       ELSE dirty[s] \/ t \in watched[s]]
ClearWatch(s) == [watched EXCEPT ![s] = {}]
DropOwner(c,s) == [owners EXCEPT ![s] = @ \ {c}]

Init ==
    /\ generation = 0 /\ issued = {} /\ leaseLive = {}
    /\ tokenGeneration = [t \in Tokens |-> 0] /\ activeToken = 0
    /\ pending = FALSE /\ delayed = FALSE /\ active = FALSE
    /\ data = FALSE /\ meta = FALSE /\ dedup = FALSE
    /\ successes = {} /\ failures = {} /\ cancel = FALSE /\ starts = 0
    /\ pc = [c \in Callers |-> "idle"]
    /\ callToken = [c \in Callers |-> 0]
    /\ callKind = [c \in Callers |-> "ack"]
    /\ connection = [c \in Callers |-> NoSession]
    /\ owners = [s \in Sessions |-> {}]
    /\ watched = [s \in Sessions |-> {}]
    /\ dirty = [s \in Sessions |-> FALSE]
    /\ commits = <<>> /\ falseCommit = FALSE /\ cancelReborrow = FALSE

(* Lua push: dedup check plus data/meta and pending/delayed insertion. Old
   terminal list entries remain when Active idempotency permits ID reuse. *)
Push(delay) ==
    /\ ~dedup /\ generation < MaxGenerations
    /\ generation' = generation + 1
    /\ data' = TRUE /\ meta' = TRUE /\ dedup' = TRUE
    /\ pending' = (pending \/ ~delay) /\ delayed' = (delayed \/ delay)
    /\ UNCHANGED <<issued, tokenGeneration, leaseLive, activeToken, active,
                   successes, failures, cancel, starts, pc, callToken,
                   callKind, connection, owners, watched, dirty, commits,
                   falseCommit, cancelReborrow>>

(* TTL expiry is visible to WATCH even before pop's cleanup sees the job. *)
Expire(t) ==
    /\ t \in leaseLive
    /\ leaseLive' = leaseLive \ {t}
    /\ dirty' = [s \in Sessions |-> dirty[s] \/ t \in watched[s]]
    /\ UNCHANGED <<generation, issued, tokenGeneration, activeToken, pending,
                   delayed, active, data, meta, dedup, successes, failures,
                   cancel, starts, pc, callToken, callKind, connection,
                   owners, watched, commits, falseCommit, cancelReborrow>>

(* One-ID projection of the single pop Lua script, in source order:
   reap -> pending cancellation -> due delay -> optional borrow. No action
   can interleave inside this macrostep. Optional borrow abstracts the batch
   being filled by another ID; due=FALSE abstracts a future delayed score. *)
Poll(borrow, due) ==
    LET expired == active /\ LeaseKey(activeToken) \notin leaseLive
        a == active /\ ~expired
        p == pending \/ expired
        settle == cancel /\ ~a
        won == IF FaultHistoricalSuccess
               THEN successes # {}
               ELSE successes # {} /\ ~(p \/ delayed)
        kill == settle /\ ~won
        p2 == (p /\ ~kill) \/ (delayed /\ due /\ ~kill)
        d2 == delayed /\ ~due /\ ~kill
        take == borrow /\ p2 /\ data
        t == Cardinality(issued) + 1
    IN /\ (~take \/ (t \in Tokens))
       /\ (expired \/ settle \/ (delayed /\ due) \/ take)
       /\ pending' = (p2 /\ ~take)
       /\ delayed' = d2
       /\ active' = (a \/ take)
       /\ activeToken' = IF take THEN t ELSE IF expired THEN 0 ELSE activeToken
       /\ leaseLive' = IF take THEN leaseLive \cup {LeaseKey(t)} ELSE leaseLive
       /\ issued' = IF take THEN issued \cup {t} ELSE issued
       /\ tokenGeneration' = IF take THEN [tokenGeneration EXCEPT ![t] = generation]
                              ELSE tokenGeneration
       /\ failures' = IF kill THEN failures \cup {generation} ELSE failures
       /\ cancel' = (cancel /\ ~settle)
       /\ cancelReborrow' = (cancelReborrow \/
             (settle /\ p2 /\ generation \notin successes))
       /\ dirty' = IF take THEN [s \in Sessions |-> dirty[s] \/ LeaseKey(t) \in watched[s]]
                    ELSE dirty
       /\ UNCHANGED <<generation, data, meta, dedup, successes, starts,
                      pc, callToken, callKind, connection, owners, watched,
                      commits, falseCommit>>

(* cancel_job Lua: active cancellation is deferred; immediate pending/delayed
   cancellation keeps dedup even in Active mode, as the implementation does. *)
Cancel ==
    /\ EnableCancel /\ Live
    /\ pending' = FALSE /\ delayed' = FALSE
    /\ failures' = IF pending \/ delayed THEN failures \cup {generation} ELSE failures
    /\ cancel' = (cancel \/ active)
    /\ UNCHANGED <<generation, issued, tokenGeneration, leaseLive, activeToken,
                   active, data, meta, dedup, successes, starts, pc, callToken,
                   callKind, connection, owners, watched, dirty, commits,
                   falseCommit, cancelReborrow>>

(* Completion can hold an arbitrarily old borrowed job, or duplicate a call
   for the same token. Exclusive connection checkout is modeled separately
   from the lease; the shared-session mutant removes only this restriction. *)
Begin(c,t,k,s) ==
    /\ starts < MaxCalls /\ pc[c] = "idle" /\ t \in issued
    /\ (owners[s] = {} \/ FaultSharedSession)
    /\ starts' = starts + 1
    /\ pc' = [pc EXCEPT ![c] = "watch"]
    /\ callToken' = [callToken EXCEPT ![c] = t]
    /\ callKind' = [callKind EXCEPT ![c] = k]
    /\ connection' = [connection EXCEPT ![c] = s]
    /\ owners' = [owners EXCEPT ![s] = @ \cup {c}]
    /\ UNCHANGED <<generation, issued, tokenGeneration, leaseLive, activeToken,
                   pending, delayed, active, data, meta, dedup, successes,
                   failures, cancel, watched, dirty, commits,
                   falseCommit, cancelReborrow>>

Watch(c) ==
    LET s == connection[c] IN
    /\ pc[c] = "watch"
    /\ watched' = IF FaultNoWatch THEN watched
                   ELSE [watched EXCEPT ![s] = @ \cup {LeaseKey(callToken[c])}]
    /\ pc' = [pc EXCEPT ![c] = "read"]
    /\ UNCHANGED <<generation, issued, tokenGeneration, leaseLive, activeToken,
                   pending, delayed, active, data, meta, dedup, successes,
                   failures, cancel, starts, callToken, callKind, connection,
                   owners, dirty, commits, falseCommit, cancelReborrow>>

Read(c) ==
    LET s == connection[c] IN
    /\ pc[c] = "read"
    /\ IF LeaseKey(callToken[c]) \in leaseLive
       THEN /\ pc' = [pc EXCEPT ![c] = "exec"]
            /\ UNCHANGED <<owners, watched, dirty>>
       ELSE /\ pc' = [pc EXCEPT ![c] = "idle"]
            /\ owners' = DropOwner(c,s)
            /\ watched' = ClearWatch(s)
            /\ dirty' = [dirty EXCEPT ![s] = FALSE]
    /\ UNCHANGED <<generation, issued, tokenGeneration, leaseLive, activeToken,
                   pending, delayed, active, data, meta, dedup, successes,
                   failures, cancel, starts, callToken, callKind, connection,
                   commits, falseCommit, cancelReborrow>>

(* A nil EXEC executes no command. Correct code retries WATCH/EXISTS; old
   unit-valued decoding could treat it as committed and run post-completion. *)
ExecAbort(c) ==
    LET s == connection[c] IN
    /\ pc[c] = "exec" /\ dirty[s]
    /\ pc' = [pc EXCEPT ![c] = IF FaultNilIsSuccess THEN "idle" ELSE "watch"]
    /\ watched' = ClearWatch(s)
    /\ dirty' = [dirty EXCEPT ![s] = FALSE]
    /\ owners' = IF FaultNilIsSuccess THEN DropOwner(c,s) ELSE owners
    /\ falseCommit' = (falseCommit \/ FaultNilIsSuccess)
    /\ UNCHANGED <<generation, issued, tokenGeneration, leaseLive, activeToken,
                   pending, delayed, active, data, meta, dedup, successes,
                   failures, cancel, starts, callToken, callKind, connection,
                   commits, cancelReborrow>>

(* Only Redis WATCH decides whether EXEC runs. Valid(t) is recorded in a
   ghost audit event, NOT used as a guard: a missing fence has real bad paths. *)
ExecCommit(c) ==
    LET s == connection[c]
        t == callToken[c]
        k == callKind[c]
        terminal == k \in {"ack", "fail"}
    IN /\ pc[c] = "exec" /\ ~dirty[s]
       /\ commits' = Append(commits, [token |-> t, valid |-> Valid(t), kind |-> k])
       /\ leaseLive' = leaseLive \ {LeaseKey(t)}
       /\ active' = FALSE /\ activeToken' = 0
       /\ pending' = (pending \/ k = "nack")
       /\ delayed' = (delayed \/ k = "delay")
       /\ successes' = IF k = "ack" THEN successes \cup {generation} ELSE successes
       /\ failures' = IF k = "fail" THEN failures \cup {generation} ELSE failures
       /\ dedup' = (dedup /\ ~(terminal /\ ActiveIdempotency))
       /\ pc' = [pc EXCEPT ![c] = "idle"]
       /\ owners' = DropOwner(c,s)
       /\ watched' = ClearWatch(s)
       /\ dirty' = Invalidate(LeaseKey(t),s)
       /\ UNCHANGED <<generation, issued, tokenGeneration, data, meta, cancel,
                      starts, callToken, callKind, connection, falseCommit,
                      cancelReborrow>>

(* Cancelled completion future/transport failure: its exclusive physical
   connection is dropped, not returned with WATCH state. No RPC effect model. *)
DropConnection(c) ==
    LET s == connection[c] IN
    /\ pc[c] \in Busy
    /\ pc' = [pc EXCEPT ![c] = "idle"]
    /\ owners' = DropOwner(c,s)
    /\ watched' = IF owners[s] \ {c} = {} THEN ClearWatch(s) ELSE watched
    /\ dirty' = IF owners[s] \ {c} = {}
                THEN [dirty EXCEPT ![s] = FALSE] ELSE dirty
    /\ UNCHANGED <<generation, issued, tokenGeneration, leaseLive, activeToken,
                   pending, delayed, active, data, meta, dedup, successes,
                   failures, cancel, starts, callToken, callKind, connection,
                   commits, falseCommit, cancelReborrow>>

(* Historical sets contain ghost admission generations, so two physical list
   entries with the same ID are distinct elements here. Runtime checks only
   whether any ID reference survives in either trimmed list, not generations.
   Other IDs create pruning pressure; their entries are projected away. *)
Prune(g) ==
    LET remainingSuccess == successes \ {g}
        remainingFailure == failures \ {g}
        liveProtected == ~FaultPruneLive /\
                           (pending \/ active \/ (delayed /\ ~FaultDelayedGuard))
        historyProtected == ~FaultPruneRetained /\
                              remainingSuccess \cup remainingFailure # {}
        protected == liveProtected \/ historyProtected
    IN /\ EnablePrune /\ g \in successes \cup failures
       /\ successes' = remainingSuccess /\ failures' = remainingFailure
       /\ data' = (data /\ protected)
       /\ meta' = (meta /\ protected)
       /\ dedup' = (dedup /\ protected)
       /\ cancel' = (cancel /\ (protected \/ FaultKeepCancellation))
       /\ UNCHANGED <<generation, issued, tokenGeneration, leaseLive, activeToken,
                      pending, delayed, active, starts, pc, callToken,
                      callKind, connection, owners, watched, dirty, commits,
                      falseCommit, cancelReborrow>>

Next == \/ \E delay \in BOOLEAN : Push(delay)
        \/ \E t \in Tokens : Expire(t)
        \/ \E b,d \in BOOLEAN : Poll(b,d)
        \/ Cancel
        \/ \E c \in Callers, t \in Tokens, k \in Kinds, s \in Sessions : Begin(c,t,k,s)
        \/ \E c \in Callers : Watch(c) \/ Read(c) \/ ExecAbort(c) \/ ExecCommit(c) \/ DropConnection(c)
        \/ \E g \in Generations : Prune(g)
Spec == Init /\ [][Next]_vars

TypeOK == /\ generation \in 0..MaxGenerations /\ issued \subseteq Tokens
          /\ leaseLive \subseteq Tokens /\ activeToken \in 0..MaxLeases
          /\ tokenGeneration \in [Tokens -> 0..MaxGenerations]
          /\ <<pending,delayed,active,data,meta,dedup,cancel,falseCommit,cancelReborrow>> \in [1..9 -> BOOLEAN]
          /\ successes \subseteq Generations /\ failures \subseteq Generations
          /\ starts \in 0..MaxCalls
          /\ pc \in [Callers -> (Busy \cup {"idle"})]
          /\ callToken \in [Callers -> 0..MaxLeases]
          /\ callKind \in [Callers -> Kinds]
          /\ connection \in [Callers -> (Sessions \cup {NoSession})]
          /\ owners \in [Sessions -> SUBSET Callers]
          /\ watched \in [Sessions -> SUBSET Tokens]
          /\ dirty \in [Sessions -> BOOLEAN]
          /\ Len(commits) <= MaxCalls
FencedEffects == \A i \in 1..Len(commits) : commits[i].valid
AtMostOncePerLease == \A t \in Tokens : Cardinality({i \in 1..Len(commits) : commits[i].token = t}) <= 1
NoFalseCommit == ~falseCommit
SessionIsolation == \A s \in Sessions : Cardinality(owners[s]) <= 1
SessionCleanOnReturn == \A s \in Sessions : owners[s] = {} => watched[s] = {} /\ ~dirty[s]
LiveRecordIntegrity == Live => data /\ meta /\ dedup
RetainedHistoryIntegrity == successes \cup failures # {} => data /\ meta
ExclusiveLiveIndex == Cardinality({x \in {"p","d","a"} :
                          (x="p" /\ pending) \/ (x="d" /\ delayed) \/ (x="a" /\ active)}) <= 1
CancellationNoReborrow == ~cancelReborrow
=============================================================================
