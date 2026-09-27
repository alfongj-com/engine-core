# Reorg and pool-loss recovery: independent evidence review

**The new run supports automatic recovery using original signed wires.** The 225 admissions have exactly 225 durable attempts,225 terminal proofs and 225 independently reconciled canonical effects. The proxy recorded 265 forwarded and 265 accepted sends: nonces 57–96 were each sent twice; every other wire was sent once. SQL wire bytes, observation digests, replay keys and accepted identities match. No replacement attempt was created.

## What the fault and recovery exercised

- At load 19.435s, the harness orphaned five transactions in block 9, above the accepted depth boundary 7. Their original hashes were later included in block 10. The persisted checkpoint was 7=9−2, with the anchor block's hash; it did not pin the unstable tip. No chain halt occurred. These are local depth-two guarantees, not consensus finality.
- Before total pool deletion, the new snapshots saw 35 later **pending** transactions (nonces 62–96); their identity-set digest matches the 35 observed signed attempts. Immediately afterward both pending and queued counts were 0. The snapshots are not atomic, but the forwarded-send counter stayed 97 throughout them.
- RPC audit timestamps show 32 exact-wire repeats at 20.197–20.269s and eight at 25.199–25.224s. The report's `reorg_recovery_started` event at 29.457s is a later observer milestone, not actual replay onset.
- All 225 intents drained by 55.072s from load start, including the 45-second ongoing offer phase. Final `txpool_status` reports pending 0 and queued 0; Redis and journal unresolved counts are also 0. Recipient gain 225 and sender spend equal 225 plus independently summed receipt fees.
- One Engine process remained live through injection/recovery and stopped only after drain for consistent reconciliation. No restart, manual signed replay or offline journal recovery event is recorded. Harness/source review supports automatic recovery after fault injection; this is not proof against unrecorded outside activity. The harness deliberately controlled mining, rollback and pool deletion to inject the fault.

## Comparison limits

Both runs offered 225 transfers at 5/s for 45s, with local Anvil 2-second blocks and depth 2. The earlier depth-fixed binary left 185 intents unresolved at 645.029s;40 had settled. It recorded 234 attempts (nine IDs gained a second fee-bumped wire), and it did recover the five originally orphaned intents. Its main remaining failure was the later nonce gaps.

This is strong qualitative evidence that bounded exact-wire replay fixes consecutive gap recovery. It is **not a controlled speedup ratio**: the new fault occurred at 19.435s against nonces 57–61 after 27 prior terminals; the older fault occurred at 14.261s against nonces 31–35 after six. Genesis and harness hashes differ; the new harness adds pool instrumentation. The earlier report lacks before/after full-pool inventory, and its `node_pending:0` used a nonce delta that could miss queued transactions. Do not claim its pool was empty or that both faults removed an identical set. Neither 45-second chaos run qualifies sustainable capacity.

Exact input hashes, cross-checks and timings: `reorg-gap-evidence.json`. Sources: `/tmp/capacity-reorg-gap-fixed.json`, `/tmp/capacity-reorg-fixed.json`, both compressed per-ID observation files, and the new run's retained RPC audit plus SQLite journal. No network calls, source edits or heavy jobs were used for this review.
