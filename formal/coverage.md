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
| Accepted request has immutable intent and durable queue membership | Admission model + HTTP/Redis tests | Prove full fingerprint canonicalization and migration algorithm |
| Pending identity never expires; terminal replay protection has a finite lifetime | Admission model + TTL/retention tests | External business idempotency policy beyond retention |
| EVM nonce reservation validates current pending state | EVM model + batch/stale-read Redis tests | Recycling, imported conflicts, manual reset, external signer use, u64 exhaustion |
| Ambiguous EVM send does not permit a fresh nonce | EVM model + HTTP receipt tests + process crash tests | Provider-specific error classification and all deterministic rejection paths |
| A reverted EVM execution cannot be reported as success | EVM model + Redis lifecycle + reverting-contract crash test | Contract/application-level success definitions |
| Fee recovery respects caller caps and integer bounds | Rust proofs + signed-wire tests | Whole transaction builder, gas estimate, provider replacement policy; global spending caps |
| Solana attempt is persisted before broadcast; retry bytes stay fixed | Solana model + wire/RPC/crash tests | All serialization and cryptographic code is not formally proved |
| Expired/absent Solana status cannot justify a fresh signature | Solana model + actual stale-history fixtures | Durable nonce intentionally unsupported; provider honesty is an assumption |
| Solana retry budgets survive crash/resume | Bounded model + real worker tests | All production read-budget counts, rate pacing and clock behavior |
| Finality survives reorgs / provider disagreement | Expected counterexamples, prior chain research | **Open:** chain-specific finality policy, reorg rollback and independent provider reconciliation |
| Storage loss cannot duplicate accepted work | Expected counterexample, AOF process-crash tests | **Open:** host power loss, replica failover, backup rollback and disaster recovery protocol |
| Every accepted request eventually terminates | Conditional EVM liveness only | Solana parked recovery/operator API, scheduler/lane fairness, bounded outage policy |
| ERC-4337 / EIP-7702 identifiers remain stable | Implementation regressions and persisted admission IDs | Dedicated nonce/UID, bundler, paymaster, authorization and replay models |
| Authentication binds the allowed signer, chain and request | HTTP/crypto/domain fixtures; prior audit | Formal policy model, key rotation and KMS credential-reference protocol |
| Signatures, ABI/wire encoding, hashes and randomness are correct | SDK vectors, signed-wire/cosignature tests | Cryptographic proofs and dependency assurance; primitives are trusted here |
| Webhook events follow durable transitions; retries preserve event identity | Queue fencing model for hook commit; implementation tests | End-to-end outbox proof; delivery is at least once and consumers must deduplicate |
| RPC/webhook URLs cannot reach forbidden networks or expose secrets | Transport/policy/redaction tests and audit | DNS/network-policy threat modeling; not represented by protocol models |
| Shutdown, deadlines and bounded concurrency preserve work | Process/queue tests | Tokio scheduling, wall-clock jumps, resource exhaustion and fairness models |
| Throughput and resource use meet production targets | Local benchmarks + short public bursts; [pruning cost](evidence/pruning/README.md) | Retained-history indexing, sustained load, memory/backpressure, large queues and chain/provider quotas |
| Dependencies, compiler, Redis and operating system behave correctly | Pinned versions, tests and dependency audit | These components remain in the trusted computing base |

## Next verification work

1. Define the finality contract per chain, then model inclusion, rollback and
   terminal retention together. The current counterexamples show why this is
   necessary before claiming final success.
2. Specify operator recovery across lost Redis state and uncertain transactions.
   Model the decision before adding an API that can reset identity or nonce.
3. Model ERC-4337/EIP-7702 admission and replay, then authorization/key rotation.
4. Add trace-driven conformance tests for model transitions with multiple real
   Redis clients. Existing counterexample regressions are a first connection;
   they do not establish general implementation refinement.
