# Confirmation identity and capability boundaries

## Decision

A finalized receipt settles an admitted intent only when its identity belongs to
that intent's durable broadcast history. Redis supplies scheduling and cached
state; it must not redefine which effect the journal authorized.

This is an additional runtime check, not a proof that arbitrary Redis corruption
is recoverable or that a dishonest RPC provider reports consensus truth.

## Binding rules

| Executor | Admission and attempt binding | Terminal evidence |
| --- | --- | --- |
| EOA, including direct type-4 transactions | Exact admitted payload, signer, chain, calls and authorization list; recomputed signed-wire hash; durable wallet/nonce reservation | Requested receipt hash must be recorded for this ID. Redis sender/nonce must match the reservation before cleanup. Canonical receipt and configured finality policy still apply. |
| ERC-4337 | Stable admitted nonce, EntryPoint, account and signed UserOperation; confirmation credentials/webhooks must match admission | Independently derived UserOperation hash must match a recorded operation. Canonical EntryPoint event binds hash, sender, nonce, success and gas fields; outer transaction passes finality. |
| Solana | Exact admitted payload; signature and signed bytes recorded externally **before** saving the Redis attempt | Signature must belong to the recorded attempt. Finalized status and transaction receipt must agree on signature, slot and error. |
| Bundled EIP-7702 | **Disabled** | The current bundler queue-ID-to-transaction-hash reply does not independently prove execution of the admitted UID/calls. New requests reject before admission; existing send/confirm jobs park without signing or provider calls. |

The journal checks attempt membership again inside the terminal SQLite
transaction, including chain-halt state. A successful earlier read cannot bypass
a later durable halt. Checkpoint continuity and finality rules are described in
[finality and recovery](finality-and-recovery.md).

### EOA cached hashes

Alloy's `Signed` deserializer accepts an existing cached transaction hash. A
valid signature alone does not establish that the cached hash identifies the
encoded wire. Both EOA and NOOP authorization recompute `keccak256` of the
EIP-2718 bytes and reject a mismatch before journaling or emission.

### Solana first-attempt ordering

The first externally recorded signature follows payload validation and signing,
but precedes Redis attempt storage. A crash in that gap leaves a durable
reservation and no Redis projection: execution parks before fetching a new
blockhash or signing. A Redis-only attempt without that reservation also parks.

Later authorizations use the same durable payload. Broadcast count, last-send
clock and reconciliation count are zeroed in the journal copy; these mutable
Redis counters cannot create additional durable records for the same wire.
Original signed bytes and expiry context remain in the recovery export. Older
journal records with the former shape may add one new normalized record; they do
not gain permission to change the signature.

## Ambiguous AA outcomes

A bundler error, including nonce/preflight errors or retry exhaustion, is not
proof that a previous submission failed on chain. Send retries retain the
admitted nonce. A later deterministic build/configuration error after a recorded
attempt stays retryable; after 24 hours the job parks at hourly intervals. A
bundler success response naming a different operation hash cannot select the
confirmation identity.

This preserves safety but does not guarantee automatic progress: an accepted
operation whose response is lost may require operator reconciliation if every
resubmission is rejected. The implementation does not yet reconstruct every
confirmation job directly from journal attempt history.

## Remaining qualification and migration limits

- **Bundled EIP-7702:** enabling it requires a reviewed canonical witness binding
  UID, signer/account and admitted calls to execution, plus positive and
  substitution/reorg tests. A configuration override that trusts the bundler
  response would not satisfy this requirement. Direct EOA type-4 execution is a
  separate, verifiable capability.
- **ERC-4337:** tests cover local protocol boundaries, signed-hash construction
  and event corroboration. They do not establish deployed-account, paymaster,
  bundler and KMS interoperability on every supported chain. Account signing
  profiles remain limited as described in [UserOperation signing](userop-signing.md).
- **Old evidence:** legacy confirmation projections lacking a recorded attempt
  cannot acquire a new identity through confirmation. Older AA terminal records
  without operation-identity fields may require offline reconciliation rather
  than automatic completion under the stronger proof format. Existing historical
  journal records are not retroactively proven to have passed the new wire gate.
- **Trust:** control of the independent journal, signer or qualified RPC remains
  outside the selective Redis-projection checks. Full instruction/body truth is
  not independently established against a malicious consensus provider.

## Solana endpoint identity

Named devnet/mainnet endpoints must return the expected `getGenesisHash` before
worker signing/reconciliation or the sign-only route proceeds. Errors and wrong
clusters stay retryable for queued work and never authorize a new signature.
Only successful validation is cached, for five minutes, with at most three
entries keyed by endpoint URL and expected cluster. Errors are not cached;
concurrent checks for the same key coalesce. An endpoint or cluster change cannot
reuse another key's qualification.

| Cluster | Expected genesis hash |
| --- | --- |
| Devnet | `EtWTRABZaYq6iMfeYKouRu166VU2xqa1wcaWoxPkrZBG` |
| Mainnet-beta | `5eykt4UsFv8P8NJdTREpY1vzqKqZKvdpKuc147dw2N9d` |

The constants match the [Solana SDK's cluster definitions](https://github.com/solana-labs/solana/blob/master/sdk/src/genesis_config.rs#L47-L62).
They were independently confirmed with one `getGenesisHash` call to each
[official public endpoint](https://solana.com/docs/references/clusters) on
2026-09-26; no signing or transaction broadcast was involved. The RPC method
reports [the connected cluster's genesis hash](https://solana.com/docs/rpc/http/getgenesishash).

The explicit `solana:local` profile is exempt because isolated validators have
arbitrary genesis. It must not be used to declare an unknown remote endpoint
qualified. Serialized local signing remains offline; serialized public-cluster
signing now performs the cached identity check without changing its supplied
message or blockhash. This prevents ordinary endpoint misconfiguration, not a
malicious provider lying about its genesis or changing backends inside the cache
window. Stable endpoint routing remains an operator assumption.

## Operator access and cancellation

Legacy KMS/IAW signer requests now require `x-engine-access-token` matching a
separate `ENGINE_ACCESS_TOKEN` of at least 32 bytes, in addition to their provider
credentials. Missing, wrong or short/unconfigured tokens reject before durable
admission or provider signing. The access token authenticates the Engine caller;
it does not assert that the supplied KMS/IAW credentials are valid. Those remain
subject to the selected provider's validation.

The local signer continues to use `ENGINE_SIGNING_TOKEN` and
`x-engine-signing-token`. Mixed local/KMS/IAW signer headers reject instead of
silently changing backends. Operators migrating legacy clients must inject the
new access token explicitly; it is not copied into queued or journal payloads.
The two tokens should be independently generated and distributed according to
which signer capabilities a client needs. This is an operator boundary, not a
multi-tenant ownership or per-principal spending policy.

Administrative cancellation of an AA send-stage job with a durable broadcast
reservation now returns `CANNOT_CANCEL` and keeps tracking. That job may already
have executed even when its send response was lost. Cancelling an unreserved
pending job retains the existing queue semantics.

## Credentials at rest

Local EVM and Solana jobs store public key references. The legacy KMS and IAW
modes still serialize supplied credentials into queued requests. The permanent
journal now retains those same payloads: KMS access/secret keys, IAW authorization
tokens and some RPC/webhook credentials can therefore outlive Redis job TTLs.
Private file permissions and redacted diagnostics do not encrypt these values.
Backups and recovery exports require the same credential-level protection.

The current configured-provider HTTP mode selects the local environment signer;
it is not an instance-role KMS implementation. A production KMS redesign should
store an immutable key identity/reference, obtain short-lived credentials from a
worker role, and define key-rotation/recovery behavior. Removing old credentials
from durable history requires an explicit migration that preserves intent and
replay bindings; silently rewriting admission fingerprints would weaken them.

## Regression evidence

Local tests exercise:

- A valid EOA signature with a substituted serialized cached hash.
- A known EOA hash paired with the wrong nonce, chain or sender.
- Substituted AA confirmation hash/account/nonce/EntryPoint/chain/queue ID rejected
  before RPC; an unchanged operation proceeds to normal receipt polling.
- Nonce-error and mismatched-hash bundler replies remaining unresolved beyond the
  old retry limit; exact supplied operation/nonce retained at the network boundary.
- SQL-before-Redis Solana crash, unbound Redis attempt and different valid signed
  wire substitution causing no RPC/signing; changed counters retaining one
  durable wire record.
- Actual HTTP bundled EIP-7702 rejection and parked legacy workers with no RPC.
- Missing/wrong/unconfigured/short legacy access tokens causing zero journal or
  queue admissions; authenticated KMS/IAW requests retaining their selected
  backend; ambiguous reserved-AA cancellation preserving tracking.
- Wrong/unavailable Solana genesis preventing worker signing and send, transient
  failures being retried, success coalescing/cache reuse, and endpoint/cluster
  cache isolation.

Run the executor library tests, its ignored Redis tests with `TEST_REDIS_URL`, and
`thirdweb-engine --test api_safety` ignored tests with `REDIS_SERVER_BIN` pointing
to an isolated local Redis binary. The current bounded formal models do not
compose these new runtime checks into a machine-checked end-to-end proof.
