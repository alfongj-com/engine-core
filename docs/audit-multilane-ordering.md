# Multilane queue ordering: open finding

**Finding:** `MultilaneQueue` pops a lane with `RPOP` (`twmq/src/multilane.rs`, helper `pop_job_from_lane`) while new admissions and `RequeuePosition::Last` append with `RPUSH`. `First` uses `LPUSH`, including delayed reentry. The single queue pops with `LPOP`, consistent with those insertion positions.

**Observed regression scenario:** after 107 continuation/error cycles, enqueue another job `tail`, then nack `target` with `Last`. Redis contains `[tail,target]`. Multilane immediately pops `target`, while the single queue pops `tail`. The diagnostics regression exposed this existing behavior and explicitly preserves it in that narrow change.

**Impact:** within one lane, a repeatedly nacked `Last` job can starve older pending jobs; continuous new arrivals can also prioritize newer work. This is a fairness/liveness issue, not evidence of duplicate execution or a lease-fencing defect. Cross-lane iteration does not remove starvation inside the affected lane.

**Current production scope:** repository search finds no `MultilaneQueue`, `push_to_lane` or `job_for_lane` caller outside the TWMQ library/tests. `server/src/queue/manager.rs` constructs ordinary `Queue` for EOA, Solana, webhooks and both account-abstraction paths. Therefore the current EOA/Solana capacity campaigns do not execute this faulty pop path. External library consumers may be affected.

**Follow-up:** separately define/test FIFO admission and immediate/delayed `First`/`Last` behavior with competing jobs, then align the pop direction. Do not silently combine that scheduling behavior change with diagnostic retention. A same-lane repeated-requeue regression should prove older jobs advance; existing lease/cancellation/pruning regressions must remain green.
