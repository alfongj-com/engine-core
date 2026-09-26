# To Alfonso

Updated September 26, 2026.

## Finality and Redis recovery

Implemented on `production-hardening`, in [draft PR #1](https://github.com/alfongj-com/engine-core/pull/1).

- **Final outcomes wait for finality.** Ethereum, Arbitrum and OP Stack/Base use the RPC's finalized checkpoint by default. Solana requires finalized status. Reverted execution follows the same rule. Explicit EVM block-depth policies remain probabilistic.
- **Reorgs retain the original intent.** Provisional receipts cannot release a nonce or create a fresh Solana signature. Conflicting retained EVM checkpoints halt that chain when detected during active polling.
- **Redis is no longer the only recovery record.** An independent SQLite journal stores admitted requests, signed attempts, replay bindings and final outcomes. Lost or stale Redis data closes writes. Offline recovery uses a new namespace and quarantines uncertain transactions.

### Evidence

Local process tests pass for Redis deletion, stale-backup restore, intact AOF restart, and successful/reverted EVM reorg recovery. Engine automatically recovered each orphaned transaction at its original nonce; no duplicate effects occurred. The local Solana validator test passed 12 lost-response/crash recoveries with identical signed bytes and 12 finalized effects.

The final full-suite and CI results will be recorded in [verification](docs/verification.md). The new TLA+ models cover finality and independent-journal recovery, including expected counterexamples for dishonest RPCs, finalized rollback and loss of the authoritative journal. These are bounded models, not a proof of the entire service.

### Operating limits

This version supports **one active Engine process on one host**, with the journal on a persistent local volume independent of Redis. Startup requires explicit journal initialization. Existing deployments need an offline cutover; old Redis jobs cannot establish missing journal evidence. [Setup and recovery commands](docs/design/redis-disaster-recovery.md).

Quarantined transactions need reconciliation; recovery does not automatically resend them. Losing or rolling back both the journal and Redis remains unsupported. EIP-7702 still relies on the bundler's transaction attribution. Production endpoints, KMS and deployed smart-account contracts remain unqualified.

No paid RPC calls were used. The earlier campaign estimate remains **$2.22**; its gateway is stopped and its $12 ceiling remains. Rotate the shared dRPC key when the campaign ends.

## Next work

1. Add evidence-based operator reconciliation for quarantined transactions and a shared durable authority before multi-host deployment.
2. Measure sustained throughput with the journal, finality backlog and production storage enabled; earlier Redis-only throughput figures do not apply to this path. Add intake/storage limits and production spending controls.
3. Integrate AWS KMS through credential references and independently verify smart-account execution, especially EIP-7702.

Nothing is needed from you to finish these checks. Deployment still needs target traffic, hosting/storage and KMS choices, endpoint qualification, and resolution of the upstream repository's missing license.
