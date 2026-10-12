---------------------------- MODULE EoaRecovery ----------------------------
EXTENDS Naturals, FiniteSets, TLC

CONSTANTS Intents, Workers, NonceCount, None,
          StaleReservation, RetryOnMissingReceipt, RevertIsSuccess,
          AllowReorg, FairProgress

Nonces == 0..(NonceCount - 1)
Fees == {0, 1}
Attempts == [intent : Intents, nonce : Nonces, fee : Fees]
Statuses == {"pending", "active", "confirmed", "failed"}

VARIABLES state, nextNonce, prepared, durable, sent, ledger, reverted,
          receipts, latest, running
vars == <<state, nextNonce, prepared, durable, sent, ledger, reverted,
          receipts, latest, running>>

Init ==
    /\ state = [i \in Intents |-> "pending"]
    /\ nextNonce = 0
    /\ prepared = [w \in Workers |-> None]
    /\ durable = {}
    /\ sent = {}
    /\ ledger = {}
    /\ reverted = {}
    /\ receipts = {}
    /\ latest = 0
    /\ running = TRUE

\* A stale read/signature can survive another reservation. Commit must validate
\* both pending membership and the optimistic nonce inside WATCH/EXEC.
Prepare(w, i) ==
    /\ running /\ prepared[w] = None
    /\ state[i] = "pending" /\ nextNonce < NonceCount
    /\ prepared' = [prepared EXCEPT ![w] =
          [intent |-> i, nonce |-> nextNonce, fee |-> 0]]
    /\ UNCHANGED <<state, nextNonce, durable, sent, ledger, reverted,
                    receipts, latest, running>>

Reserve(w) ==
    /\ running /\ prepared[w] # None
    /\ LET a == prepared[w] IN
       /\ StaleReservation \/ (state[a.intent] = "pending" /\ a.nonce = nextNonce)
       /\ state' = [state EXCEPT ![a.intent] = "active"]
       /\ nextNonce' = a.nonce + 1
       /\ durable' = durable \cup {a}
    /\ prepared' = [prepared EXCEPT ![w] = None]
    /\ UNCHANGED <<sent, ledger, reverted, receipts, latest, running>>

Discard(w) ==
    /\ prepared[w] # None
    /\ prepared' = [prepared EXCEPT ![w] = None]
    /\ UNCHANGED <<state, nextNonce, durable, sent, ledger, reverted,
                    receipts, latest, running>>

\* Sending and receiving a reply are deliberately not atomic. A crash cannot
\* undo sent, which represents transactions the network may still execute.
Broadcast(a) ==
    /\ running /\ a \in durable /\ a \notin sent
    /\ sent' = sent \cup {a}
    /\ UNCHANGED <<state, nextNonce, prepared, durable, ledger, reverted,
                    receipts, latest, running>>

\* Fee replacement changes only fee version, retaining intent and nonce.
Bump(a) ==
    /\ running /\ a \in durable /\ a.fee = 0
    /\ [a EXCEPT !.fee = 1] \notin durable
    /\ durable' = durable \cup {[a EXCEPT !.fee = 1]}
    /\ UNCHANGED <<state, nextNonce, prepared, sent, ledger, reverted,
                    receipts, latest, running>>

Mine(a, fails) ==
    /\ a \in sent /\ a.nonce = Cardinality(ledger)
    /\ ledger' = ledger \cup {a}
    /\ reverted' = IF fails THEN reverted \cup {a} ELSE reverted
    /\ UNCHANGED <<state, nextNonce, prepared, durable, sent,
                    receipts, latest, running>>

ObserveNonce(n) ==
    /\ running /\ n \in 0..Cardinality(ledger)
    /\ latest' = n
    /\ UNCHANGED <<state, nextNonce, prepared, durable, sent, ledger,
                    reverted, receipts, running>>

ReadReceipt(a) ==
    /\ running /\ a \in ledger
    /\ receipts' = receipts \cup {a}
    /\ UNCHANGED <<state, nextNonce, prepared, durable, sent, ledger,
                    reverted, latest, running>>

\* Null, wrong-hash and failed reads provide no positive evidence. Previously
\* observed receipts may disappear from a later read on a lagging provider.
MissingReceipt(a) ==
    /\ running /\ a \in receipts
    /\ receipts' = receipts \ {a}
    /\ UNCHANGED <<state, nextNonce, prepared, durable, sent, ledger,
                    reverted, latest, running>>

Confirm(a) ==
    /\ running /\ a \in durable \cap receipts
    /\ a.nonce < latest /\ state[a.intent] = "active"
    /\ state' = [state EXCEPT ![a.intent] =
          IF a \in reverted /\ ~RevertIsSuccess THEN "failed" ELSE "confirmed"]
    /\ durable' = {d \in durable : d.intent # a.intent}
    /\ UNCHANGED <<nextNonce, prepared, sent, ledger, reverted,
                    receipts, latest, running>>

\* Negative control for the old inference "nonce advanced => this missing
\* hash was replaced". A stale/null receipt does NOT justify this transition.
UnsafeRequeue(a) ==
    /\ RetryOnMissingReceipt /\ running /\ a \in durable
    /\ a.nonce < latest /\ a \notin receipts
    /\ state' = [state EXCEPT ![a.intent] = "pending"]
    /\ durable' = {d \in durable : d.intent # a.intent}
    /\ UNCHANGED <<nextNonce, prepared, sent, ledger, reverted,
                    receipts, latest, running>>

Crash ==
    /\ running
    /\ running' = FALSE
    /\ prepared' = [w \in Workers |-> None]
    /\ UNCHANGED <<state, nextNonce, durable, sent, ledger, reverted,
                    receipts, latest>>
Restart ==
    /\ ~running /\ running' = TRUE
    /\ UNCHANGED <<state, nextNonce, prepared, durable, sent, ledger,
                    reverted, receipts, latest>>

\* Inclusion is not finality. This explicit boundary is expected to violate
\* TerminalMatchesExecution; Engine currently does not roll back terminal jobs.
Reorg ==
    /\ AllowReorg /\ ledger # {}
    /\ ledger' = {} /\ reverted' = {} /\ receipts' = {} /\ latest' = 0
    /\ UNCHANGED <<state, nextNonce, prepared, durable, sent, running>>

\* A fair environment eventually performs a complete observation/commit while
\* the process is alive. This is stronger than arbitrary intermittent RPC reads
\* and is required ONLY by the optional progress specification below.
Progress(i) ==
    \/ /\ running /\ state[i] = "pending" /\ nextNonce < NonceCount
       /\ state' = [state EXCEPT ![i] = "active"]
       /\ durable' = durable \cup {[intent |-> i, nonce |-> nextNonce, fee |-> 0]}
       /\ nextNonce' = nextNonce + 1
       /\ UNCHANGED <<prepared, sent, ledger, reverted, receipts, latest, running>>
    \/ \E a \in Attempts : a.intent = i /\ Broadcast(a)
    \/ \E a \in Attempts, fails \in BOOLEAN : a.intent = i /\ Mine(a, fails)
    \/ /\ running /\ state[i] = "active"
       /\ \E a \in ledger :
            /\ a.intent = i
            /\ state' = [state EXCEPT ![i] = IF a \in reverted THEN "failed" ELSE "confirmed"]
            /\ durable' = {d \in durable : d.intent # i}
            /\ receipts' = receipts \cup {a}
            /\ latest' = Cardinality(ledger)
       /\ UNCHANGED <<nextNonce, prepared, sent, ledger, reverted, running>>

Next ==
    \/ \E w \in Workers, i \in Intents : Prepare(w, i)
    \/ \E w \in Workers : Reserve(w) \/ Discard(w)
    \/ \E a \in Attempts : Broadcast(a) \/ Bump(a) \/ ReadReceipt(a)
                          \/ MissingReceipt(a) \/ Confirm(a) \/ UnsafeRequeue(a)
    \/ \E a \in Attempts, fails \in BOOLEAN : Mine(a, fails)
    \/ \E n \in 0..NonceCount : ObserveNonce(n)
    \/ Crash \/ Restart \/ Reorg
    \/ /\ FairProgress /\ \E i \in Intents : Progress(i)

Spec == Init /\ [][Next]_vars
LiveSpec == Spec /\ WF_vars(Restart) /\ (\A i \in Intents : SF_vars(Progress(i)))

TypeOK ==
    /\ state \in [Intents -> Statuses]
    /\ nextNonce \in 0..NonceCount
    /\ prepared \in [Workers -> Attempts \cup {None}]
    /\ durable \subseteq Attempts /\ sent \subseteq Attempts
    /\ ledger \subseteq Attempts /\ reverted \subseteq ledger
    /\ receipts \subseteq ledger /\ latest \in 0..NonceCount
    /\ running \in BOOLEAN
OneIntentPerNonce == \A a, b \in durable : a.nonce = b.nonce => a.intent = b.intent
ActiveHasDurableIdentity == \A i \in Intents : state[i] = "active" => \E a \in durable : a.intent = i
AtMostOneEffect == \A i \in Intents : Cardinality({a \in ledger \ reverted : a.intent = i}) <= 1
TerminalMatchesExecution == \A i \in Intents :
    /\ (state[i] = "confirmed" => \E a \in ledger \ reverted : a.intent = i)
    /\ (state[i] = "failed" => \E a \in reverted : a.intent = i)
TerminalEventually == <> (\A i \in Intents : state[i] \in {"confirmed", "failed"})
=============================================================================
