# Nonce cleanup with retained finality history

## Defect and contract

The load review found an allocator deadlock: older submitted receipts remained
while newer finalized records were removed. `CleanAndGetRecycledNonces` used the
highest remaining reservation and considered the observed chain count only when
no reservation existed. With consumed count 313, optimistic count 313 and an old
retained nonce 250, cleanup rewound allocation to 251. The independent journal
correctly rejected that already-bound replay key, stopping progress.

The cleanup floor is now the maximum of the highest borrowed nonce, highest
submitted nonce and **observed count minus one**, with explicit nonce-zero
handling. An unavailable observed count requires synchronization. This does not
permit sending an uncertain intent again or clearing retained attempts.

## Model and implementation

[`NonceAllocator.tla`](tla/NonceAllocator.tla) keeps chain consumption, observed
count, borrowed reservations, retained receipt records and the next allocation
separate. `Settle` can remove any consumed retained record; it need not remove the
oldest first. `Clean` runs independently. `everReserved` abstracts the journal's
permanent replay binding: an allocator rewind cannot bypass that guard, but
still violates the allocator floor and can block progress.

| Action/property | Runtime and regression |
|---|---|
| `Reserve`, `Submit` | EOA pending/borrowed transitions; existing stale-reservation and duplicate-intent Redis tests |
| `Observe` | `update_cached_transaction_count`: observed count and optimistic lower bound |
| `Settle` | Finality-qualified `CleanSubmittedTransactions` removes a proven nonce group |
| `Clean` / `ObservedNonceFloor` | [`submitted.rs`](../executors/src/eoa/store/submitted.rs), `CleanAndGetRecycledNonces::validation`; `retained_old_receipts_never_rewind_below_observed_chain_count` |
| `OutstandingNonceFloor` | Remaining borrowed and submitted evidence stays below the next allocation; existing crash/reservation cleanup tests |

Pinned TLC checked four nonce values. The positive model exhausted **645 states**;
removing the consumed floor failed `ObservedNonceFloor` after **184 explored
states**. The witness reached a remaining old receipt with newer consumed nonces
after **69 states**. The [full frozen-source gate](https://github.com/alfongj-com/engine-core/blob/load-tests/formal/evidence/throughput-review/report.json)
reproduced all three results.

## Limits

The observed count is coherent and nondecreasing in this small model. Reorgs,
malicious/stale RPC, finalized checkpoint continuity and retained signed identity
are covered separately by the finality/recovery models and runtime tests. This
model does not prove nonce recycling, manual reset, u64 exhaustion, external
signer use, receipt-pagination fairness or throughput. Four nonce values do not
establish a general population theorem. The separate models' composition is not
machine-checked.


## Gap-recovery high-water review

The later confirmation integration keeps the cached consumed-count high-water
when `latest` reports a lower value, and replays only original journaled wires.
This supports `ObservedNonceFloor`; it does not add a modeled rollback action.
The positive model assumes monotonic `consumed` and `observed <= consumed`, so it
does not establish availability or all allocator behavior across an actual chain
rollback. The real confirmation-flow regression preserves cached40 after
observing latest0, while journal membership and immutable replay keys remain the
independent safety fence. Cooldown state, ascending replay and mempool eviction
are implementation obligations, not a new proof from unchanged model states.
