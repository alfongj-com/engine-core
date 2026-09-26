# Invariant coverage and remaining work

This is a risk inventory, not a claim to enumerate every possible defect.
**Model** means finite TLA+ exploration of a reviewed abstraction. **Rust proof**
means Kani checks production code. **Test** means implementation evidence only.
Model composition and Rust-to-TLA+ refinement are not machine-checked.

| Invariant / failure | Current coverage | Remaining work |
|---|---|---|
| Only a live queue lease commits completion and hooks | Queue model + Redis races | Arbitrary worker populations; Redis failover |
| WATCH state belongs to one completion transaction | Queue model + shared-session regression | Redis/client implementation is trusted |
| EXEC conflict is retried; command error is not replayed | Model for conflicts; real Redis partial-error test | Model per-command partial execution and outbox repair |
| Queue indexes and payload agree across delay, cancel, prune and ID reuse | One-ID queue model + real Redis regressions | Multiple lane IDs, fairness, pruning liveness; cancellation remains ID-scoped |
| Accepted request has immutable intent; queue is its recoverable projection | Redis Admission model + independent DisasterRecovery model + HTTP/Redis/journal tests | Prove canonicalization and composition; offline recovery does not automatically rebuild queues |
| Identity outlives Redis terminal TTL while the independent ledger is retained | Admission TTL boundary + DisasterRecovery immutable/terminal binding + journal tests | Ledger retention/backup policy; the Redis-only model still permits replay after its own identity expires |
| EVM nonce reservation validates current pending state | EVM model + batch/stale-read Redis tests | Recycling, imported conflicts, manual reset, external signer use, u64 exhaustion |
| Old retained receipts cannot rewind below consumed nonce | NonceAllocator model + actual313/250 Redis regression | Reorg/lag policy, recycling and population generalization remain separate |
| Terminal evidence belongs to the same admitted signed attempt | DisasterRecovery terminal-attribution mutation + journal/EOA/AA/Solana identity regressions | Cryptographic wire derivation and model composition are not proved; bundled7702 disabled |
| Any post-dispatch EVM error preserves its nonce and wire | EVM abstraction + real HTTP/Redis/SQLite normal-path rejection/recovery regression + process crash tests | Message parsing is no longer a safety premise; NOOP automatic retry and bounded lifetime/backoff remain availability work |
| A reverted EVM execution cannot be reported as success | EVM model + Redis lifecycle + reverting-contract crash test | Contract/application-level success definitions |
| Fee recovery respects caller caps and integer bounds | Rust proofs + signed-wire tests | Whole transaction builder, gas estimate, provider replacement policy; global spending caps |
| Solana attempt is persisted before broadcast; retry bytes stay fixed | Solana model + wire/RPC/crash tests | All serialization and cryptographic code is not formally proved |
| Expired/absent Solana status cannot justify a fresh signature | Solana model + actual stale-history fixtures | Durable nonce intentionally unsupported; provider honesty is an assumption |
| Solana retry budgets survive crash/resume | Bounded model + real worker tests | All production read-budget counts, rate pacing and clock behavior |
| Ordinary pre-finality reorgs cannot produce a terminal outcome | Finality model + canonical RPC fixtures/executor tests + journal checkpoint CAS | Provider/chain qualification, continuous monitoring, independent consensus verification and immutable admission policy; dishonest RPC/finalized rollback remain negative boundaries |
| Redis loss/rollback cannot authorize a fresh identity when local authority survives | DisasterRecovery model + journal failure-cut/SIGKILL tests + integrated process harness | Real power-loss/fsync assurance, authority loss/rollback, multi-host fencing, projection repair availability; no SQLite/filesystem refinement proof |
| Every accepted request eventually terminates | Conditional EVM liveness only | Solana parked recovery/operator API, scheduler/lane fairness, bounded outage policy |
| ERC-4337 / EIP-7702 identifiers remain stable | Implementation regressions + original-payload retry in DisasterRecovery | Dedicated protocol/bundler/paymaster/authorization models; independent 7702 bundler-result attribution |
| Authentication binds the allowed signer, chain and request | HTTP/crypto/domain fixtures; prior audit | Formal policy model, key rotation and KMS credential-reference protocol |
| Signatures, ABI/wire encoding, hashes and randomness are correct | SDK vectors, signed-wire/cosignature tests | Cryptographic proofs and dependency assurance; primitives are trusted here |
| Webhook events follow durable transitions; retries preserve event identity | Queue fencing model for hook commit; implementation tests | End-to-end outbox proof; delivery is at least once and consumers must deduplicate |
| RPC/webhook URLs cannot reach forbidden networks or expose secrets | Transport/policy/redaction tests and audit | DNS/network-policy threat modeling; not represented by protocol models |
| Shutdown, deadlines and bounded concurrency preserve work | Process/queue tests | Tokio scheduling, wall-clock jumps, resource exhaustion and fairness models |
| Useful EOA send/recovery progress can continue without a timer delay while polling-only work stays delayed | Real Redis scheduling regression through production result decision and TWMQ lease completion | No wall-clock/fairness proof; mixed-progress unknown retries can run sooner; local matched load is measured separately; public-chain capacity remains unqualified |
| Throughput and resource use meet production targets | Local benchmarks + short public bursts; [pruning cost](evidence/pruning/README.md) | Bounded 20k page/churn and 10k cleanup-isolation tests, policy-head hint and false-high-count fixtures added; sustained finality-window load, memory/disk/backpressure and chain/provider quotas remain qualification work |
| Dependencies, compiler, Redis and operating system behave correctly | Pinned versions, tests and dependency audit | These components remain in the trusted computing base |

## Next verification work

1. Qualify actual chain/provider finality semantics, model immutable admission
   policy/migration, and connect the finality and journal models through executable
   traces. Do not treat a provider's returned tag as a consensus proof.
2. Exercise real power-loss/storage failures, specify authoritative-ledger backup
   and multi-host fencing, and improve safe projection repair. An older ledger
   copy cannot replace the current authority; quarantined outcomes may stay unknown.
3. Model ERC-4337/EIP-7702 admission and replay, then authorization/key rotation.
4. Add trace-driven conformance tests for model transitions with multiple real
   Redis clients. Existing counterexample regressions are a first connection;
   they do not establish general implementation refinement.
