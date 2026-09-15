# Execution and API audit

**Baseline:** `b6b7a0bbdc737b3a2b09611305b71b1bf6aba6e8`  
**Date:** 2026-09-15  
**Scope:** HTTP boundary, signing, account abstraction, delegated accounts, Solana, webhooks, and their executor integration. Queue internals and EOA nonce management are reviewed separately.

## Decision

Do not treat the baseline as ready for custody or unrestricted network exposure. The strongest design elements are the separation of send/confirm workers, atomic queue hooks, chain-specific signer types, and the attempt-before-send pattern in Solana. The release blockers are unauthorized mutations, secret exposure, and a mismatch between durable queue retries and on-chain replay protection.

This is a source audit, not a claim of complete security assurance. Findings below describe the baseline; later fixes and tests must be checked separately. No production keys, funded transactions, or third-party services were used to reproduce issues.

## Architecture and trust boundaries

1. Axum extracts RPC and signer credentials and constructs transaction requests. There is no general principal/tenant model at this boundary.
2. The execution router writes credentials and requests to Redis, registers an ID, then queues the matching worker. The registry and enqueue are separate operations.
3. ERC-4337 workers determine/deploy an account, build a sponsored UserOperation, sign it, and send it to a bundler. EIP-7702 workers sign delegation and wrapped calls for Thirdweb's `tw_execute` extension. Success hooks enqueue confirmation work.
4. Solana workers save an attempt before sending, poll signature status, and rebuild after blockhash expiry. Attempt records contain a signature and blockhash, but not signed bytes.
5. Worker hooks enqueue webhooks containing transaction outcomes. URLs and webhook secrets originate in caller input.

Redis is part of the signing trust boundary in the baseline: compromise exposes serialized AWS/IAW credentials. Bundler/RPC replies are untrusted network input and can be delayed, duplicated, missing, or inconsistent across nodes. A queue ACK cannot atomically commit an external blockchain broadcast.

## Prioritized findings

Severity reflects plausible impact; exposure prerequisites are explicit. File line numbers refer to the baseline.

### S1 · High · Administrative mutation and cancellation lack authentication

**Evidence:** `server/src/http/routes/admin/queue.rs:47–84` empties a queue's dedupe set without an auth extractor. `server/src/http/routes/transaction.rs:52–79` cancels a supplied transaction ID without authentication or ownership validation. Both are mounted in `server/src/http/server.rs:65–102` without an auth layer.

**Failure:** anyone able to reach the service can remove replay protection or cancel another caller's known/predictable job ID. Browser access is additionally eased by wildcard CORS. Network isolation can reduce exposure but is not an API permission check.

**Required test:** exercise the actual mounted routes with missing, wrong, and valid administrative credentials against an isolated Redis instance; unauthorized requests must leave the queued job and dedupe set intact. Until a tenant ownership model exists, cancellation should require administrative authority.

### S2 · High · Diagnostics export signing credentials

**Evidence:** `server/src/http/routes/admin/eoa_diagnostics.rs:45,219–239` serializes `TransactionData` directly. `executors/src/eoa/store/mod.rs:73,91` nests the original request, including signing credentials. `core/src/credentials.rs:48–56` serializes AWS access and secret keys. RPC credentials and webhook secrets also travel in the original request.

**Failure:** a person or service authorized to inspect transaction health receives credentials that can sign unrelated transactions. Shared diagnostics responses, traces, and copied support bundles broaden exposure.

**Required test:** serialize a diagnostic response containing unique sentinel values for every secret; assert none occurs in response bytes while transaction status and safe request fields remain inspectable. Use explicit safe DTOs rather than redacting a growing generic JSON tree.

### S3 · High · Retried ERC-4337/EIP-7702 jobs can repeat execution

**Evidence:** `executors/src/external_bundler/send.rs:424–431` chooses a random nonce during each attempt unless one was pregenerated; broadcast occurs later in the same process call. `executors/src/eip7702_executor/send.rs:251–255,311–327` reconstructs and broadcasts wrapped calls each attempt; `eip7702-core/src/transaction.rs:153–154,179–180` creates a new random UID. Its job's optional `nonce` is unused.

**Failure:** bundler accepts a transfer, then the response or Redis completion is lost. Retrying the job creates a different nonce/UID and therefore a second independently valid transfer. Queue idempotency only deduplicates queue insertion. It does not make broadcasts idempotent. The same problem exists on worker death after broadcast even when a transport error is classified permanent.

**Required test:** a recording bundler accepts the first request then drops the connection; restart/reclaim the worker. The same semantic operation must retain its on-chain nonce/UID and signed identity, and execute once. Persist replay identity before any external effect, reconcile ambiguous outcomes, and version stored jobs for migration.

### S4 · High when exposed to untrusted submitters · Webhooks permit internal HTTP requests

**Evidence:** `core/src/execution_options/mod.rs:136–145` accepts an unrestricted URL. `executors/src/webhook/mod.rs:230–239` requests it directly. The client in `server/src/queue/manager.rs:105–114` has timeouts but no destination policy or redirect restriction.

**Failure:** a caller who can cause a webhook uses the server's network position to reach localhost/private services; an attacker-controlled redirect can change the destination. A failed transaction still produces a webhook, so valid signing authority is not necessarily needed to reach this path. Actual exploitability depends on egress and reachable services.

**Required test:** destination policy denies loopback, private, link-local, IPv6 equivalents, DNS answers in those ranges, and redirects to denied destinations; approved destinations work. Enforce policy at connection time or at an egress proxy to handle DNS rebinding. Bound response bytes as well as time.

### S5 · High for correctness · Solana expiry and cleanup lose replay evidence

**Evidence:** `executors/src/solana_executor/storage.rs:257,390` gives attempt records a ten-minute TTL. Worker confirmation/terminal hooks delete attempts before the queue transaction commits (`worker.rs:288,314`); signatures are queried before expiry decisions (`539–541,618–626`). `blockhash_last_valid_height` is stored but unused. Signed bytes are not persisted.

**Failure:** Redis attempt expiration or a crash after cleanup removes evidence of an already-broadcast transaction. Recovery can fail without reconciling it or construct another transaction. A crash between attempt storage and RPC send leaves a signature that can never confirm; recovery cannot resend the identical signed bytes. Status absence plus a hash invalid at a different commitment is insufficient proof that an earlier submission did not land.

**Required test:** kill at store-before-send, send-before-ACK, confirmation-before-hook-commit, and long RPC outage boundaries; assert no duplicate on-chain effect and eventual reconciliation. Retain signed bytes and replay evidence through terminal queue commit, use matching commitment and last-valid-block-height semantics, and consult historical status before replacing uncertain transactions.

### S6 · Medium · Caller-controlled inputs panic or silently change transaction semantics

**Evidence:** `server/src/execution_router/mod.rs` routes default/explicit `auto` to `todo!()` and indexes `transactions[0]` for empty EOA batches. `solana-core/src/transaction.rs:234` indexes an unvalidated deserialized account list. `core/src/transaction.rs:44–58` uses an untagged enum whose first variant has only optional fields; a legacy `{gasPrice: ...}` object can deserialize as empty EIP-7702 data and discard the requested fee.

**Failure:** accepted JSON aborts request tasks; a legacy fee selection is lost. Also, smart-account and delegated-account encoders replace `to: None` with the zero address (`aa-core/src/smart_account/mod.rs:38,52`; `eip7702-core/src/transaction.rs:130,172`), which does not perform contract creation and may burn value.

**Required test:** API-level empty/default-auto requests return a structured 4xx without Redis writes. Deserialize each supported fee variant, reject ambiguous combinations, and assert preserved fees. Reject unsupported contract creation for account execution. Feed malformed/truncated serialized Solana messages and require errors, never panics.

### S7 · Medium · ERC-6492 signatures are not ABI encoded

**Evidence:** `aa-core/src/signer.rs:282–298` concatenates a 20-byte address, lengths, and byte arrays. [ERC-6492](https://eips.ethereum.org/EIPS/eip-6492#signer-side) requires ABI encoding of `(address, bytes, bytes)` followed by the magic suffix; that includes address padding and dynamic offsets.

**Failure:** undeployed account signatures returned by the API cannot be decoded by conforming verifiers.

**Required test:** verify independent ABI fixture bytes, decode using the tuple type, and use the reference verifier against an undeployed local test wallet. Cover empty and non-word-aligned calldata/signatures.

### S8 · Medium · KMS/private-key UserOperation signing ignores the configured EntryPoint

**Evidence:** `core/src/userop.rs` KMS/private-key branches call `userop.hash(chain_id)` despite receiving `params.entrypoint`; `aa-types/src/userop.rs:190–198` hashes the standard EntryPoint. `hash_with_custom_entrypoint` already exists (`201–209`). IAW uses the supplied EntryPoint.

**Failure:** custom EntryPoints reject validly constructed operations because the signature commits to a different contract. Signer backend selection changes behavior.

**Required test:** recover signatures over independently computed hashes for both versions and two EntryPoints; cross-EntryPoint and cross-chain verification must fail.

### S9 · Medium · Deployment completion can delete a newer worker's lock

**Evidence:** `executors/src/external_bundler/deployment.rs:85–94,205–220` deletes the lock unconditionally; confirmation hooks use the pipeline path. Acquired lock IDs are not carried in send/confirmation job state. Stale-lock reclamation does use compare-and-delete, but completion does not.

**Failure:** worker A pauses, its lease expires, and B obtains a new deployment lock. A resumes and removes B's lock, allowing C to build competing account-deployment operations. Cache and lock keys also omit the configured execution namespace (`48–49,77–82`).

**Required test:** acquire A, replace with B, complete A, and assert B remains. Carry lock ownership tokens through all lifecycle states; test namespace separation in real Redis.

### S10 · Medium · Confirmation conflates inclusion, execution success, and finality

**Evidence:** `executors/src/external_bundler/confirm.rs:249–276` treats any UserOperation receipt as stage success, even `success: false`; default receipt polling gives up after about 100 seconds (`150–151,231–237`). EIP-7702 confirmation accepts the first successful receipt and removes registry state (`confirm.rs:319–378`). Neither checks canonicality or chain finality.

**Failure:** consumers can interpret a success webhook as business success, act on a transaction later removed by a reorg, or retry an operation declared failed solely because inclusion was delayed. EIP-7702 reports reverted receipt as failure, so event semantics differ across execution modes.

**Required test:** receipt disappears/reappears across a reorg, execution reverts, bundler is slow, and RPC returns stale data. Model `submitted`, `included`, `execution failed`, and policy-selected finality explicitly. Treat timeout as an unknown outcome until reconciled.

### S11 · Medium · Serialized Solana retry tracks a different blockhash

**Evidence:** `solana-core/src/transaction.rs:164–166` ignores the supplied recent blockhash for serialized transactions. `executors/src/solana_executor/worker.rs:773–775,845–853,884–889` fetches a fresh hash and stores it as the attempt hash anyway. The retry branch says unsigned serialized input can be refreshed but never rewrites its message hash.

**Failure:** expiry is checked against an unrelated hash; an unsigned serialized transaction is repeatedly signed with its original expired hash. Durable-nonce input requires a separate validity model.

**Required test:** submit a fixture whose original hash differs from the RPC's latest hash; recorded validity must refer to the signed message. Test unsigned refresh, preservation of existing signatures, and explicit durable-nonce rejection/support.

### S12 · Medium · HTTP work and response memory are incompletely bounded

**Evidence:** chain construction rebuilds reqwest clients per `get_chain` (`core/src/chain.rs:227–237`), without total request timeouts; ABI lookup does likewise. Contract routes use `join_all` on caller-sized batches. Webhook responses are read fully (`executors/src/webhook/mod.rs:251`), and `&s[..512]` truncation (`278`) can panic on a UTF-8 boundary.

**Failure:** a slow provider holds worker slots, high fan-out overwhelms RPC pools, or a webhook response exhausts memory/panics a worker. Connection reuse exists within one chain object, but not across independently rebuilt objects.

**Required test:** slow/never-ending peers terminate within budgets; oversized and multibyte responses remain bounded and valid; batch fan-out respects a concurrency limit. Benchmark end-to-end request reuse before introducing caches keyed by credentials.

## Other correctness and maintainability observations

- `ThirdwebAbiServiceBuilder.auth` is unused by `build`; default headers are empty. ABI lookup does not check HTTP status before parsing.
- Overload selection chooses the first ABI function whose arguments encode. Ambiguous overloads should require an explicit signature rather than depend on ABI order.
- AWS credentials have no session-token/role-provider path. Debug derivations on secret-bearing objects and error values containing original headers need systematic redaction.
- Chain routing is effectively Thirdweb-specific (hostname construction, mandatory paymaster, `tw_execute`), with a hardcoded local-chain exception. “All chains” needs explicit per-chain capability checks and configurable providers, not an arbitrary numeric chain ID.
- Request IDs are caller-controlled and scoped mainly by queue/namespace, without a tenant ID or payload digest. Cross-client collisions and reuse with different payloads need a defined conflict response.
- Registry writes precede enqueue; failed enqueue can leave ghost records, and repeated IDs can overwrite another queue's registry entry. Queue hooks log serialization/enqueue failures and still finish; an explicit transactional outbox error contract would be easier to reason about.
- Solana feature documentation promises address lookup tables, but instruction-built messages compile with an empty lookup-table list. The sign-only route drops requested priority fees. Max retries/fee percentile bounds are documented but not enforced.
- Public metrics have no auth and chain IDs appear in labels. Restrict observability exposure and label cardinality as part of deployment configuration.

## Coverage ledger

The following were read in detail for the findings above:

- `server/src/http/server.rs`, `extractors.rs`, `config.rs`, `chains.rs`, `execution_router/mod.rs`; admin queue/cancel handlers; Solana handlers; relevant transaction diagnostics handlers.
- `core/src/{credentials,chain,signer,userop,transaction}.rs`, all execution option modules, and the active transport implementation.
- `aa-types/src/userop.rs`; `aa-core/src/{signer.rs,smart_account/mod.rs,userop/builder.rs,userop/deployment.rs,account_factory/*}`.
- `eip7702-core/src/{delegated_account,transaction}.rs`; executor send/confirm/delegation cache paths.
- `solana-core/src/transaction.rs`; Solana executor transaction lifecycle and Redis storage.
- External bundler send/confirm/deployment; transaction registry; webhook dispatch/envelopes; Thirdweb auth/ABI/error code and IAW UserOperation signing.

The following received a structural or targeted review, not a line-by-line security review: crate manifests/module exports/constants/schema adapters; server boot/queue manager; contract encode/read/write and dynamic ABI parsing; general sign routes; IAW other signing endpoints; core RPC request/response types and error adapters; Solana RPC cache; metrics. Existing integration fixtures were inventoried, not treated as proof of coverage. Generated contract bytecode, dependencies, remote services, deployed account implementations, and infrastructure configuration are outside this source review.

## Verification approach

Use protocol fixtures for encoders/signatures, real isolated Redis for ownership and atomicity, mock RPC/bundler servers for ambiguous network outcomes, and local chain nodes for replay/reorg behavior. Assertions should target externally visible effects and durable state. Serialization round trips alone cannot validate wire compatibility; retry-delay bucket tests alone cannot validate recovery. A local pass does not establish public-chain throughput or operational finality.
