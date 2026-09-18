# EOA receipt outcomes

**Scope:** EVM EOA transactions with an observed inclusion receipt. A receipt
does not establish long-term finality; reorganization recovery remains separate.

## Contract

| Receipt status | Stored status | Confirmation webhook | Worker cycle count |
| --- | --- | --- | --- |
| `1` | `confirmed` | `confirm` / `SUCCESS` | `confirmedTransactions` |
| `0` | `failed` | `confirm` / `FAILURE`, `TRANSACTION_REVERTED` | `failedTransactions` |
| No receipt | Remains unresolved in `submitted` | No terminal event | Neither |

A revert consumes the sender nonce and pays gas. It must not recycle that nonce
or retry the same intent at a new nonce. All submitted hashes for the completed
intent, including replacement fee attempts, leave the active set together.
Replaced competing intents remain a separate case: their retry requires a
receipt proving that another transaction consumed their nonce.

## Failure payload

The notification has `executorName: "eoa"`, `stageName: "confirm"`, and
`eventType: "FAILURE"`. Its `payload.error` contains:

- `errorCode: "TRANSACTION_REVERTED"`;
- `transactionId`, `transactionHash`, and `eoaAddress`;
- the full `receipt`, including `status: "0x0"` and inclusion block identity.

No `finalAttemptNumber` is emitted for this outcome: stored submission attempts
do not provide a reliable count of network broadcasts. The existing send-stage
failure and successful confirmation payloads are unchanged.

## Durability and compatibility

Terminal status, completion time, receipt, retention TTL, submitted-set cleanup,
and webhook enqueue share the fenced Redis transaction. The request identity
and attempt history remain for the configured completed-transaction retention
period. An identical ID retry within that period returns without requeueing;
an ID with different intent conflicts. Reusing an ID after retention expires
can create a new intent.

Previously, receipt status `0` was reported as confirmation success. Consumers
must now handle `TRANSACTION_REVERTED` as a terminal confirmation failure. The
worker's existing `failedTransactions` counter now counts mined reverts;
replacement retries remain in `replacedTransactions`. The queued-to-confirmed
latency metric still measures inclusion time for either receipt status.

## Verification

Real Redis regressions run the admission → borrowed → submitted → receipt
lifecycle for success and revert at nonce zero, with two signed attempts. They
check the terminal webhook, retained receipt/history, concurrent identical-ID
retries, conflicting intent rejection, repeated receipt cleanup, and rejection
of a stale borrow at a new nonce. No HTTP webhook or live-chain request is made
by these tests.

```sh
TEST_REDIS_URL=redis://127.0.0.1:16379/ \
  cargo test -p engine-executors eoa::store::atomic::tests -- --ignored
```
