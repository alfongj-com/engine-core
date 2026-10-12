--------------------------- MODULE NonceAllocator ---------------------------
EXTENDS Integers, FiniteSets, TLC

CONSTANTS NonceCount, IgnoreConsumedFloor
Nonces == 0..(NonceCount - 1)
VARIABLES nextNonce, borrowed, retained, consumed, observed, everReserved
vars == <<nextNonce, borrowed, retained, consumed, observed, everReserved>>

MaxOrNone(s) == IF s = {} THEN -1 ELSE CHOOSE n \in s : \A m \in s : m <= n

Init ==
    /\ nextNonce = 0 /\ borrowed = {} /\ retained = {}
    /\ consumed = 0 /\ observed = 0 /\ everReserved = {}

\* Preparation/reservation abstracts the WATCH/EXEC owner and pending-membership
\* checks already covered by EoaRecovery. It never reuses a journal replay key.
Reserve ==
    /\ nextNonce < NonceCount /\ nextNonce \notin everReserved
    /\ borrowed' = borrowed \cup {nextNonce}
    /\ everReserved' = everReserved \cup {nextNonce}
    /\ nextNonce' = nextNonce + 1
    /\ UNCHANGED <<retained, consumed, observed>>
Submit(n) ==
    /\ n \in borrowed
    /\ borrowed' = borrowed \ {n} /\ retained' = retained \cup {n}
    /\ UNCHANGED <<nextNonce, consumed, observed, everReserved>>
\* Chain effects are independent of Redis cleanup and its observation.
Mine ==
    /\ consumed \in borrowed \cup retained
    /\ consumed' = consumed + 1
    /\ UNCHANGED <<nextNonce, borrowed, retained, observed, everReserved>>
Observe ==
    /\ observed < consumed
    /\ observed' = consumed
    /\ nextNonce' = IF nextNonce < consumed THEN consumed ELSE nextNonce
    /\ UNCHANGED <<borrowed, retained, consumed, everReserved>>
\* Finalized receipt reads may arrive out of order. Newer records can be removed
\* while an older receipt remains absent/unavailable, despite a consumed nonce.
Settle(n) ==
    /\ n \in retained /\ n < observed
    /\ retained' = retained \ {n}
    /\ UNCHANGED <<nextNonce, borrowed, consumed, observed, everReserved>>
Clean ==
    LET outstanding == borrowed \cup retained
        floor == IF IgnoreConsumedFloor /\ outstanding # {}
                 THEN MaxOrNone(outstanding)
                 ELSE MaxOrNone(outstanding \cup (IF observed = 0 THEN {} ELSE {observed - 1}))
        candidate == floor + 1
    IN /\ nextNonce' = IF candidate < nextNonce THEN candidate ELSE nextNonce
       /\ UNCHANGED <<borrowed, retained, consumed, observed, everReserved>>

Next == Reserve \/ Mine \/ Observe \/ Clean
        \/ \E n \in Nonces : Submit(n) \/ Settle(n)
Spec == Init /\ [][Next]_vars
TypeOK ==
    /\ nextNonce \in 0..NonceCount /\ borrowed \subseteq Nonces
    /\ retained \subseteq Nonces /\ everReserved \subseteq Nonces
    /\ consumed \in 0..NonceCount /\ observed \in 0..consumed
    /\ borrowed \cap retained = {}
ObservedNonceFloor == nextNonce >= observed
OutstandingNonceFloor == \A n \in borrowed \cup retained : nextNonce > n
\* Reachability witness: an old record survives newer settlement, and a fresh
\* allocation is still possible beyond the entire consumed prefix.
OldReceiptCatchupNotReached ==
    ~(observed >= 2 /\ retained = {0} /\ nextNonce = observed /\ nextNonce < NonceCount)
=============================================================================
