# Durable unsigned EOA failures

## Contract

A deterministic preparation failure becomes a terminal journal decision before
Redis can expose `failed` or publish its failure webhook. That decision requires
the exact original payload, no replay binding and no attempted wire. A competing
broadcast reservation wins permanently once recorded. Failure publication can
resume from the original terminal witness after a process crash.

`EoaPreparationFailure` separates durable authority from Redis scheduling,
externally observed failure history and delayed network delivery. Losing a Redis
status or its TTL cannot erase a failure that a caller already observed.
Publication permits partial Redis cleanup and treats a visible failed status or
a failure webhook as an observed failure. Invariants check that observed failures
never later execute, preparation-rejected
intents never gain an attempt, prior attempts are never reported failed, and
failure reports carry the original payload's authoritative witness.

## Configurations

- Positive cases explore offline reconstruction, same-ID intake retry and a
  competing successful preparation. Crashes can occur between every action.
- The original-bug mutation publishes only to Redis. Its required counterexample
  reports failure, crashes, loses the projection, rebuilds the queue, then sends
  and delivers that same intent.
- The attempted-work mutation omits the current journal attempt check. Its
  required counterexample reserves a broadcast between failure observation and
  the terminal decision, then incorrectly reports failure.
- Witness cases require recovery publication after a crash between the terminal
  commit and checkpoint acknowledgement, and a broadcast winner that refuses
  the failure and subsequently lands. These ensure successful paths are reachable.

## Limits

One immutable intent, two preparation roles, one fixed wire and two diagnostic
labels are finite abstractions. A durable attempt bit represents the atomic
replay binding and attempted-wire record. Acknowledgement is a bit: the exact
checkpoint, CAS, ownership and reattach protocol remains in
`DisasterRecoveryProjection`.
FULL storage durability, exact serialization/cryptography and implementation
conformance remain assumptions tested separately. No model composition,
unconditional liveness, bounded recovery time or exactly-once webhook guarantee
is claimed. Full projection loss may prevent publication while terminal journal
evidence still prevents execution.

The model starts with no prior failure. Historical Redis-only failures whose
projections already expired left no negative fact in the old journal; this
protocol cannot reconstruct that missing history or certify those old intents.
