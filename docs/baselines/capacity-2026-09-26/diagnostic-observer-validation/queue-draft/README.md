# Bounded queue diagnostic history draft

Status: isolated copy only; main repository is unchanged. Implementation files are in `twmq/src/`. Targeted build/tests use this standalone workspace and an explicitly disposable loopback Redis. There is no Engine build or live campaign.

Design:

- Keep newest100 records per job, each new serialized record at most16KiB.
- All six single/multilane nack, terminal-failure and deserialization-failure append sites add LPUSH then LTRIM to the existing owner-fenced transaction. Retry counts remain metadata, queue order/delay remains unchanged and key TTL is neither added nor refreshed.
- Ordinary JSON remains byte-for-byte JobErrorRecord<E>. Oversized records use a valid, explicit omission envelope with attempt, created_at, original nack/fail details and diagnosticOmitted reason/limit. It does not fabricate a generic ErrorData value. Existing generic consumers must detect the marker before decoding JobErrorRecord<E>.
- A capped writer stops the second serialized copy at16KiB, including JSON escaping. Only that deliberate overflow becomes a marker; unrelated handler Serialize errors preserve their prior failure semantics.
- Count bound applies on the next append. Legacy oversized records are not rewritten; the aggregate byte bound requires natural turnover of100 newer records. A huge legacy list can incur an O(existing history) one-time LTRIM cost. No startup migration or age TTL is added.
- These are per-job limits, not a global Redis memory bound: job count, payloads/results and existing histories remain separate capacity concerns. Error inspection history is not authoritative transaction intent/replay evidence.
- WorkRemaining records stay visible rather than being suppressed. Capping only these normal events would not bound repeated real errors and would alter the existing EOA diagnostic reader regression.

Test coverage planned/authored:

- Serializer exact-byte boundary and escaped oversized content; ordinary JSON unchanged; genuine serialization failure stays an error.
- RealRedis single/multilane mixed normal/remote-error nacks beyond100, newest-first order, attempts continue increasing, TTL preserved, tail scheduling unaffected.
- Stale-owner completion cannot append OR trim an overlong existing list or execute hooks/change current lease; current owner can.
- Oversized nack/fail/deserialization completion all retain bounded valid markers while preserving delayed/front semantics, terminal/dedupe transitions and no new expiry.

Only in-tree normal typed reader is `executors/src/eoa/worker/scheduling_tests.rs`; no retry/backoff code reads diagnostic length. The public generic JobErrorRecord type is preserved for ordinary records. External Redis inspection consumers need the marker contract documented before release.
