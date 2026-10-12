# Review follow-ups before production

Review date: October 11, 2026. Applies to PR #1; PR #2 remains unfinished.

## Changes made

- An intact Redis projection can be explicitly reattached after a live outage or
  primary-role change. Exact checkpoint and writable-primary checks still apply.
  Storage, checkpoint, operator and terminal-conflict halts cannot be cleared.
- Caller cancellation no longer splits journal commit and Redis publication.
  SIGTERM follows the coordinated shutdown path. A process or host crash can still
  leave the existing checkpoint gap; PR #2 addresses that separate protocol.
- Fee JSON preserves the full unsigned 128-bit range. RPC routing labels no longer
  corrupt revert data during credential redaction.
- Supplied-gas EVM transactions get node validation before signing; explicit gas
  and intentional contract reverts are preserved. This adds one estimate call per
  newly prepared supplied-gas transaction. Historical throughput/RPC-cost results
  do not measure this extra call. Identical-wire retries do not repeat it.
- Serialized Solana blockhash admission uses the processed bank; completion still
  requires finalized status and matching transaction evidence. An initially
  invisible hash retries before signing, within the existing unsigned retry limit.
  Parked attempts emit nonterminal NACK notifications with the signature and reason
  through the existing queue transaction; the hourly park cycle may repeat them.
- Webhook destinations are checked before admission and again at delivery. Read
  and encode POST routes no longer occupy mutation slots. Idle Redis transaction
  connections are checked before WATCH; an ambiguous EXEC is never replayed.

## Remaining production limits

| Area | Required work / current behavior |
| --- | --- |
| Deterministic EVM rejection | Estimation catches node-reported invalid gas/type/size rules before signing. Generic RPC does not expose every mempool cap or fork capability, and some estimators ignore admission rules. Define and qualify explicit chain/provider capability profiles before claiming comprehensive validation. A rejection after dispatch still retains its nonce and wire. |
| Unknown Solana outcome | A simulation error, even on Engine's first send, is not proof of non-execution: the caller may have submitted the same signed transaction elsewhere. Keep the attempt and idempotency reservation. Finalized evidence may resolve it; otherwise operator reconciliation is required. Do not turn expiry or a null lookup into terminal failure. |
| Operator reconciliation | There is no force-clear for a finality conflict and no CLI command to resolve an unknown Solana attempt. A future reconciliation command needs independently checked matching chain evidence and an audit record; clearing a flag is insufficient. |
| Journal retention | Admissions, bindings and terminal evidence have no TTL. Removing them can permit duplicate intent or nonce reuse. Design durable tombstones/archive lookup before adding deletion; monitor ledger/WAL size and free space meanwhile. |
| Startup cost | SQLite quick_check remains enabled. Measure startup against representative ledger sizes before changing the integrity check; this review does not establish a startup latency bound. |
| Upgrade | Follow [the legacy drain runbook](upgrade-from-legacy.md). Initialization needs an empty namespace and does not import pending work or historical idempotency records. |

These limits are not fixed by a passing local suite. Local nodes do not qualify
public RPC quotas, actual L2 settlement or mainnet operational recovery.
