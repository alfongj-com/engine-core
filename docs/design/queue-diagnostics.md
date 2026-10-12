# Queue diagnostic history

## Contract

Each TWMQ job retains its 100 newest error/continuation diagnostics, newest first. Each newly serialized record is limited to 16 KiB, including JSON escaping. The limit applies to nack, terminal failure and deserialization-failure records in both queue variants. Normal `WorkRemaining` continuations remain inspectable.

`LPUSH` and `LTRIM` execute in the existing lease-owner-fenced completion transaction. This does not change retry metadata, requeue position/delay, hooks, lease ownership, job retention or key expiry. Retry decisions never use diagnostic-list length.

Ordinary records retain the existing `JobErrorRecord<E>` JSON byte-for-byte. Oversized records use a distinct omission envelope, for example:

```json
{
  "attempt": 17,
  "created_at": 42,
  "details": {"nack": {"delay": null, "position": "last"}},
  "diagnosticOmitted": {
    "reason": "serialized_record_exceeds_limit",
    "maxSerializedBytes": 16384
  }
}
```

Inspectors must recognize `diagnosticOmitted` before decoding an ordinary typed record. The marker preserves attempt/time/scheduling details; it does not retain a partial error string or fabricate a handler error variant. Serialization uses a bounded writer so an oversized error does not allocate a second unbounded JSON copy. A genuine handler serialization error retains its previous error behavior.

## Existing data and limits

The next diagnostic append trims an existing list to 100 entries. Legacy oversized entries remain until naturally displaced; the total serialized-payload bound of 100 × 16 KiB applies after 100 bounded replacements, not immediately to arbitrary old data. A very large legacy list can incur a one-time linear trim cost. There is no startup migration, age-based expiry or background rewrite.

This is a per-job diagnostic bound, not a global Redis memory bound. Redis object overhead, total job count, job payloads/results and retained legacy entries remain separate capacity concerns. Diagnostic history is not the authoritative transaction-intent/replay journal.

## Verification

Real Redis tests cover both queue variants: more than 100 alternating continuations/real errors, exact newest-first order, monotonic attempt metadata, existing TTL preservation, unchanged queue operations, oversized records at all six append sites and stale-owner fencing of both append and trim. Serializer tests cover exact 16 KiB boundary, escaped bytes, unchanged ordinary JSON and unrelated serialization failure. Removing LTRIM makes both count regressions fail (121 retained versus 100).

A separate pre-existing multilane queue-order issue is recorded in [the ordering audit](../audit-multilane-ordering.md); diagnostic retention does not alter it.
