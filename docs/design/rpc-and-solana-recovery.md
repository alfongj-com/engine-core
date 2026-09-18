# Configured RPCs and Solana recovery

Status: implemented; initial public submission and process-recovery tests passed. Updated September 17, 2026. [Evidence and limits](../baselines/public-transactions.md).

## Purpose

Run ordinary EVM transactions and Solana transactions without Thirdweb Vault. Keep provider credentials and local signing keys out of queued jobs. A process crash or lost RPC response must not silently create a second Solana transaction.

## Configuration

Configure each EVM chain explicitly. For example, a local gateway serving Sepolia:

```sh
APP__EVM_RPC__ENDPOINTS__11155111__URL=http://127.0.0.1:8788/11155111
APP__EVM_RPC__REQUEST_TIMEOUT_MS=30000
APP__EVM_RPC__CONNECT_TIMEOUT_MS=5000
```

Provider headers use `APP__EVM_RPC__ENDPOINTS__<chain>__HEADERS__<name>`; YAML can also hold the endpoint map. Inject secrets at runtime. Clients reuse connections, refuse redirects, cap response bodies at 16 MiB, and redact endpoint URLs, headers and reflected credentials from RPC errors. Restart Engine after changing endpoint configuration.

Requests using configured providers authenticate with `x-engine-signing-token`, matching `ENGINE_SIGNING_TOKEN` (at least 32 bytes). Omit Thirdweb RPC headers. This path selects the local environment signer using `ENGINE_PRIVATE_KEY` before KMS credential extraction; configured-provider signing and EOA submission currently support that local signer only. Contract reads use the same token. KMS remains on legacy routing and is unqualified for configured providers; smart-account execution also rejects. Bundlers and paymasters still have separate configuration and qualification requirements.

Base uses the same canonical `latest` nonce lookup as other EVM chains. Only set `APP__EVM_RPC__ENDPOINTS__<chain>__USE_PENDING_FOR_PRECONFIRMATION=true` after verifying that endpoint's sequencer preconfirmation semantics. A normal mempool `pending` nonce is not evidence of inclusion or finality.

For Solana:

```sh
ENGINE_SOLANA_KEYPAIR_FILE=/private/path/test-solana-keypair.json
APP__SOLANA__DEVNET__HTTP_URL=http://127.0.0.1:8788/solana-devnet
```

The file contains a Solana CLI 64-byte JSON keypair. Protect it with owner-only permissions. EVM continues to use `ENGINE_PRIVATE_KEY`; Solana uses its own Ed25519 key. Both authenticate with the operator token. Queued local credentials contain only the public identity. Workers reject a different key at that path when signing admitted work. Attempts already signed retain their original bytes and identity for reconciliation and retransmission.

Solana uses a pooled HTTP sender with a 5-second connect timeout, 15-second request timeout and 16 MiB response limit, checked against both declared length and streamed bytes. It disables redirects, system proxies and hidden HTTP retries, including retries for HTTP 429. One RPC invocation makes at most one HTTP request; the worker owns retry decisions. The sender checks JSON-RPC version, matching response ID and mutually exclusive result/error fields. Errors withhold provider bodies and credential-bearing URLs while preserving numeric RPC error codes. These bounds do not impose a global rate or spending limit.

## Solana signing contract

- Instruction submission requests build a versioned transaction and add a memo containing the request ID. Execution supports omitted, manual or estimated priority fees.
- Serialized legacy and v0 transactions retain their message, blockhash and valid existing signatures. The configured signer must be the fee payer; every other required signature must already verify. Malformed wire data, extra bytes and missing signatures reject.
- Serialized requests must contain their own compute-budget instructions. Overrides reject instead of being silently ignored.
- Sign-only requests do not broadcast. Serialized sign-only requests need no RPC. Instruction sign-only requests support manual fees; automatic fee estimation is available through submission.
- Durable nonce submission is explicitly unsupported. Instruction requests do not resolve address lookup tables; serialized v0 requests can carry them.

## Solana admission and retention

An immutable SHA-256 fingerprint binds each request ID to its normalized queued request: chain, transaction contents, public signer identity, execution options and webhook options. A Redis Lua script validates key types and existing state before atomically storing that identity, enqueueing one job and registering it. Matching retries return the same ID without creating work; a different request under that ID rejects. An unresolved identity without a live queue job also rejects and requires reconciliation; resubmission is not a resume operation.

The admission hash is `{namespace}:solana_admission:{id}` (without the namespace prefix when unset), independent of bounded queue history. Its lifecycle is:

| State | Retention |
| --- | --- |
| Active, uncertain, cancelled or infrastructure failure | No expiry. Retain any signed attempt and reconcile explicitly. |
| Successful or known on-chain failed transaction | Mark terminal and remove the signed attempt in the same lease-fenced queue completion transaction. |
| Deterministic build/signing/attempt-limit failure before submission | Mark failed only if no persisted attempt exists. |

Terminal updates require the original fingerprint. Their retention clock starts only when queue completion commits: `APP__QUEUE__COMPLETED_TRANSACTION_TTL_SECONDS`, positive, defaults to `86400` (24 hours). A retained terminal record makes matching retries no-ops even after queue history is pruned. This is a minimum protection window at the configured duration, not a guaranteed ID-reuse deadline: surviving queue evidence still rejects after expiry. Once both identity and history are gone, same-ID replay protection is gone. Callers need durable business outcome records and fresh IDs for new intents.

The first admission initializes schema marker `twmq:{queue_name}:solana_admission_schema=1` only after a bounded queue consistency check. Later admissions use constant Redis work with respect to backlog. Stop old workers for this cutover; unknown schemas, inconsistent legacy evidence or more than 20,000 indexed records require reconciliation/offline migration. See [Solana admission cutover](../replay-migration.md#solana-admission-cutover).

## Recovery rules

| Observation | Action |
| --- | --- |
| New request | Sign once; persist exact bytes, signature, actual blockhash and its known last-valid height before any send. |
| Timeout, disconnect, `AlreadyProcessed`, unexpected send result | Retain the attempt. Query signature history and resend identical bytes while valid. |
| Signature visible below requested commitment | Wait; do not rebuild. |
| Successful status at requested commitment | Fetch details; require matching signature, slot and successful metadata before completing. |
| Failed status at requested commitment | Record the on-chain failure. |
| Signature absent and finalized height exceeds the recorded last-valid height | Query history again, then park if still absent. Provider absence is insufficient proof to create a new signature. |
| Serialized hash becomes invalid without a trustworthy expiry height | Park for reconciliation. Never invent a height or rewrite the message. |
| Legacy attempt lacks signed bytes | Reconcile its signature; otherwise park. |

Each attempt permits at most 20 broadcasts, spaced by at least two seconds. A processing call has a 90-second deadline, shorter than the storage lock. After 500 reconciliation checks the job parks with `RECOVERY_REQUIRED`; periodic wakeups perform no RPC or signing. The lock-protected storage resume method resets the check budget while preserving the signature and broadcast count. An authenticated operator HTTP resume endpoint is **not yet implemented**.

Attempt records have no expiry TTL. Successful completion and known on-chain failure delete the attempt inside the same Redis transaction as queue completion. Cancellation and infrastructure failure retain it because neither establishes the chain outcome. An aborted completion leaves the admission active and retains the attempt. Review retained evidence before deletion.

## Assumptions and limits

A load-balanced RPC may serve a finalized height from one node and incomplete history from another. Recovery therefore does not re-sign an expired transaction based on an absent status. New submissions reject `maxBlockhashRetries > 0` until a complete-history policy is qualified. Existing queued attempts still reconcile and can park. Separate-provider comparison, stronger finality policy and an operator resolution procedure remain release work.

This handles Engine process restarts while Redis retains its data. It assumes healthy queue transitions after schema initialization, no old/new worker overlap and no arbitrary Redis mutation. It does not establish durability across Redis power loss or failover. Missing legacy fingerprints or already-expired records cannot be reconstructed. Unresolved records may remain indefinitely; reconciliation alerts, storage capacity and operator recovery need production procedures.

## Verification

Local HTTP tests exercise pooling, header isolation, redirects, timeouts, oversized/malformed responses and Base nonce semantics. Real Redis plus injected RPC failures exercise persisted bytes, lost responses, expiry races, historical lookups, mismatched receipts, lease loss and terminal cleanup. Admission tests cover concurrent retries, changed intent, queue pruning, migration guards and aborted terminal commits. The local validator test verifies identical-byte crash recovery and no extra sends after completed queue history is pruned. Wire tests compare encoding with the Solana SDK and verify signatures independently. See [verification results](../verification.md) for executed commands and evidence.
