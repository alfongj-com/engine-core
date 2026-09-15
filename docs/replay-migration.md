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

## Verification

`server/tests/api_safety.rs` exercises actual HTTP admission and Redis persistence: duplicate requests keep the same stored nonce/UID, and legacy payloads fail before RPC access. `eip7702-core/src/transaction.rs` verifies that independently rebuilt owner and session-key calls have identical signed payload hashes under a persisted UID. `executors/src/eoa/worker/receipt_tests.rs` covers receipt RPC failures, missing/wrong-hash replies, and same-nonce replacement evidence against Redis.

These checks do not establish finality, deployed contract replay semantics, or cross-provider reconciliation. Validate the account implementation and chain finality policy before production rollout.

## Deployment lock migration

New ERC-4337 send results, errors, and confirmation jobs carry the unique deployment lock token. Completion changes the lock and deployment cache only if that token still owns the lock. Jobs created by old workers lack that token; they skip cleanup and let the lease expire. The confirmation result's `deploymentLockReleased` is now nullable because the result is produced before the ownership-checked Redis hook commits.

Lock and cache keys now follow the configured execution namespace. Stop old workers before upgrade, and reconcile/drain old active deployment work before using the new namespace keys. Redis snapshots remain the rollback boundary; do not run old and new deployment workers simultaneously against the same account. The existing 300-second lease bounds orphaned-lock delay but does not prove a slow bundler operation has stopped.

`executors/src/external_bundler/deployment_tests.rs` simulates lease expiry, acquisition by another worker, and late success/failure cleanup; only the current owner can mutate the lock/cache, and independent execution namespaces do not interfere.
