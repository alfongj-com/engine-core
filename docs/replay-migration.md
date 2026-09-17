# Replay identity migration

## Behavior

New ERC-4337 jobs persist their UserOperation nonce before enqueue. New EIP-7702 jobs persist their wrapped-call UID in the existing `nonce` field. Workers reuse those values across retries. This prevents a lost broadcast response from creating a second operation with fresh replay protection. Different fees or signatures may still produce different UserOperation hashes, so nonce identity is the application safeguard, not a promise of identical raw bytes.

Jobs lacking either identifier stop before contacting a signer or bundler. They are reported as a worker failure with a reconciliation message. **This does not mean the transaction failed on-chain.** The old worker may have already submitted it.

EOA receipt polling also preserves unresolved submissions when a receipt is missing, fails to load, or has the wrong hash. A known receipt for another submitted intent at the same nonce is required before automatic replacement. The nonce must also be below the canonical `latest` transaction count; a preconfirmation at nonce zero while `latest` is zero cannot prove replacement. An external replacement that cannot be identified stays unresolved and requires operator reconciliation.

## Upgrade procedure

1. Stop intake and old workers. Preserve a snapshot of Redis and operational logs with restricted access; they contain signer credentials in the existing storage model.
2. Inventory active ERC-4337 and EIP-7702 jobs. For each missing replay identifier, reconcile the sender, chain, request, known hashes/IDs, bundler history, and on-chain events. A pending/unknown result is not permission to resubmit.
3. Drain or explicitly resolve those legacy jobs. Do not populate a fresh random nonce/UID into an ambiguous existing job and do not clear dedupe sets to bypass this guard.
4. Start the new workers and intake. Confirm that newly admitted jobs contain replay identity and that retries preserve it. Retain reconciliation records beyond queue retention.

## Solana admission cutover

New Solana requests bind the request ID to an immutable fingerprint of the normalized intent, public signer identity and options. Admission creates this record and the queue job atomically. Matching retries create no work; changed intent rejects. This identity survives ordinary queue pruning, but does not reconstruct missing legacy evidence.

1. Stop intake and old workers; snapshot Redis. Do not overlap old and new workers in the same namespace. Inventory existing Solana jobs, signed attempts and admission records before enabling new intake. Reconcile legacy signatures and chain outcomes; do not attach a new intent to an old attempt, clear dedupe state, refresh its blockhash or fabricate a fingerprint to bypass rejection.
2. The first new admission checks pending, active, delayed, success and failed indexes, then writes `twmq:{queue_name}:solana_admission_schema` with value `1` atomically with admission. The queue name is `{namespace}_solana_executor`, or `solana_executor` without a namespace. The check requires unique indexed IDs, job data and consistent metadata/dedupe membership. Wrong key types, orphan indexes and unknown schema versions reject before writes. More than **20,000 indexed records** exceeds the online check limit: drain/reconcile or migrate offline while stopped. No automatic offline migration tool is provided. Do not manually stamp or delete the marker to skip validation.
3. Once initialized, admission checks no longer scan the backlog. This relies on healthy queue transitions and stable Redis storage after cutover; the marker is not ongoing corruption detection. Existing metadata or signed attempts without an admission identity reject same-ID admission and require explicit reconciliation. An active identity whose queue job disappeared also rejects: retrying HTTP admission cannot resume it.
4. Start new workers and intake after resolving migration blockers. Verify that a new request creates one admission identity and that an identical retry creates no additional job. Check that crash recovery preserves the original signed bytes. Keep snapshots and reconciliation records; copying only queue jobs to a fresh namespace discards independent replay evidence.

Admission hashes use `{namespace}:solana_admission:{id}`, or `solana_admission:{id}` without a namespace. Active/uncertain records and signed attempts have **no TTL**, including cancellation and infrastructure failures. A queue failure alone is not proof of on-chain failure and does not authorize deletion or resubmission.

Only a committed terminal transition starts admission expiry, after checking the original fingerprint. Success and known on-chain failure remove the signed attempt in that same queue completion transaction. Deterministic pre-send failures may become terminal only when no attempt exists; aborted completion retains active evidence. `APP__QUEUE__COMPLETED_TRANSACTION_TTL_SECONDS` must be positive and defaults to **86400 seconds (24 hours)**. Terminal identity records protect matching retries for at least that configured period, independently of queue pruning.

Expiry is not a guaranteed reuse deadline: surviving queue evidence continues to reject until pruned. After both identity and history disappear, same-ID replay protection ends. Retain business outcome records externally, use fresh IDs for new intents, and define an operator process for indefinitely retained unresolved records. Lost/expired legacy evidence and Redis power-loss/failover durability remain outside this migration's guarantees. See the [signing and recovery design](design/rpc-and-solana-recovery.md).

## Verification

`server/tests/api_safety.rs` exercises actual HTTP admission and Redis persistence: duplicate requests keep the same stored nonce/UID, and legacy payloads fail before RPC access. `eip7702-core/src/transaction.rs` verifies that independently rebuilt owner and session-key calls have identical signed payload hashes under a persisted UID. `executors/src/eoa/worker/receipt_tests.rs` covers receipt RPC failures, missing/wrong-hash replies, and same-nonce replacement evidence against Redis.

`server/src/solana_admission_tests.rs` covers concurrent admission, completed queue pruning, cancellation, migration guards and aborted terminal commits against Redis. The local validator recovery test also retries completed requests after pruning their queue history and verifies no additional sends or on-chain effects.

These checks do not establish finality, deployed contract replay semantics, or cross-provider reconciliation. Validate the account implementation and chain finality policy before production rollout.

## Deployment lock migration

New ERC-4337 send results, errors, and confirmation jobs carry the unique deployment lock token. Completion changes the lock and deployment cache only if that token still owns the lock. Jobs created by old workers lack that token; they skip cleanup and let the lease expire. The confirmation result's `deploymentLockReleased` is now nullable because the result is produced before the ownership-checked Redis hook commits.

Lock and cache keys now follow the configured execution namespace. Stop old workers before upgrade, and reconcile/drain old active deployment work before using the new namespace keys. Redis snapshots remain the rollback boundary; do not run old and new deployment workers simultaneously against the same account. The existing 300-second lease bounds orphaned-lock delay but does not prove a slow bundler operation has stopped.

`executors/src/external_bundler/deployment_tests.rs` simulates lease expiry, acquisition by another worker, and late success/failure cleanup; only the current owner can mutate the lock/cache, and independent execution namespaces do not interfere.
