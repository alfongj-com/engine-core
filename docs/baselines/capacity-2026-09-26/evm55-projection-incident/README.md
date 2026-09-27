# EVM55 interrupted verification

**Failed harness run; no capacity or complete safety claim.** At 59.529 seconds the host guard extrapolated a 24.25 MB/s observed allocation interval across the full remaining recovery budget, requiring 154.09 GB with 87.53 GB available. No actual disk floor breach was observed. The source of the host-wide allocation spike was not established.

All 3,274 accepted IDs match 3,274 unique signed attempts and proxy-accepted wires, with contiguous nonces 0–3,273. The journal contains 1,294 terminal proofs and 1,980 signed nonterminal intents. No requests had an unknown HTTP result. The remaining 16,526 offers were explicitly aborted.

The harness stopped and discarded its in-memory Anvil node before the independent receipt oracle. Its final canonical effects and fees cannot now be reconstructed for the unresolved intents. Journal proofs are not substituted for that missing oracle. All owned processes stopped; the failed supervisor state and review latch remain unchanged.

A private, hash-verified copy preserves all 18 runtime files (32,977,718 bytes). No original intent is replayed, reset or marked complete. Later tests may use independent fresh local chains only after explicit root review; they cannot repair or qualify this failed run. The follow-up harness fix preserves interrupted owned-node history and separates sustained disk consumption from short allocation spikes. Raw journal/AOF/wires and node data are excluded from this archive.
