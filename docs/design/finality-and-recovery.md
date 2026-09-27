# Finality, reorganizations and recovery

Status: **implemented in the working tree; final integrated verification pending**.
This describes the shared EVM gate, executor integration and independent recovery
journal authored on 2026-09-26. Targeted implementation/model checks have passed;
the final frozen-source evidence belongs in [verification](../verification.md).
Official sources accessed **2026-09-26**. No production endpoint is certified here.

## Context and decision

An RPC receipt proves an observation of execution in a particular block. It does
not by itself justify deleting recovery evidence or announcing irreversible
success. Previously, matching EVM receipts could become terminal without a
canonical-block/finalized-head check, and Solana requests could select confirmed
as their terminal commitment. The [EVM flow](../../executors/src/eoa/worker/confirm.rs)
now validates canonical finality; [Solana recovery](../../executors/src/solana_executor/worker.rs)
requires finalized evidence even for existing confirmed requests.

**Default: terminal chain outcomes require finalized evidence.** Apply
the same requirement to successful and reverted execution. Before that boundary,
preserve the intent and every signed attempt. A timeout, missing receipt, nonce
movement, elapsed time or retry exhaustion cannot establish a terminal outcome.

Goals: handle ordinary pre-finality reorgs, preserve execution identity, expose
the guarantee actually achieved, and stop safely on conflicting finality
evidence. Non-goals: bridge settlement, consensus/light-client verification,
automatic recovery from a consensus-finality violation, and certifying arbitrary
chains by their numeric chain ID.

## Chain semantics: verified facts

| Family | Evidence and boundary |
| --- | --- |
| Ethereum PoS, including Sepolia | `latest` can reorganize; `safe` and `finalized` expose different consensus guarantees. Use the finalized block tag, not a fixed number of subsequent execution blocks. [Execution API](https://ethereum.github.io/execution-apis/1.0.0-beta.6/api/methods/eth_getBlockByNumber/) |
| Arbitrum Nitro | `safe`/`finalized` track the parent-chain blocks carrying the batch. Parent-finalized batch data differs from confirmed assertions and withdrawal settlement. AnyTrust adds data-availability assumptions. [Block tags and reorgs](https://docs.arbitrum.io/how-arbitrum-works/reference/finality-and-reorgs), [settlement distinction](https://docs.arbitrum.io/how-arbitrum-works/deep-dives/finality) |
| Custom Nitro / Orbit / L3 | Parent finality support and node configuration determine the tags' meaning; a successful RPC response alone is insufficient qualification. An L3 inherits its parent's guarantee, including that parent's assumptions. [Nitro configuration and inheritance](https://docs.arbitrum.io/how-arbitrum-works/reference/finality-and-reorgs) |
| Standard OP Stack, including OP Mainnet | `latest` is unsafe sequencer history; `safe` reflects L1-published data; `finalized` reflects finalized L1 data. Bridge dispute periods are separate. Batch or parent-chain stalls can delay finality without invalidating the admitted intent. [OP Stack finality](https://docs.optimism.io/op-stack/transactions/transaction-finality) |
| Base | Flashblocks, L2 inclusion, L1 batch inclusion and L1 batch finality are distinct stages. Flashblock evidence is provisional. Withdrawal availability is separate from ordinary L2 transaction finality. Published typical durations are not completion predicates. [Base finality](https://docs.base.org/specifications/transactions/transaction-finality) |
| Solana | `confirmed` means a supermajority has directly voted; `finalized` is the cluster's strongest commitment. `getTransaction` can return null when data is unavailable at the requested commitment. Signature history searches require `searchTransactionHistory: true` to go beyond the recent cache. [Commitments](https://solana.com/docs/rpc), [transaction lookup](https://solana.com/docs/rpc/http/gettransaction), [signature history](https://solana.com/docs/rpc/http/getsignaturestatuses) |

These are protocol/documentation facts, not evidence that an arbitrary provider
implements them correctly. OP alternative-DA deployments, custom consensus,
unknown EVM chains and L3 settlement paths require separate profiles. No depth
of child-chain blocks substitutes for parent finality.

## EVM contract

### Policy and evidence

Each chain profile identifies the chain, endpoint and finality policy. Default
to `finalized` for both known and unknown chain IDs; reject unsupported or malformed finality
responses without falling back to `safe`, `latest`, confirmations or a timer.
Unknown chains remain unqualified even when the RPC accepts this tag. An
operator may explicitly configure a depth policy for custom/local networks;
describe its outcome as probabilistic, disclose its weaker guarantee and
qualify reorg behavior separately. The shared assessment enum's `Finalized`
variant means the configured policy is satisfied; the evidence's `policy`
field determines whether consensus finality or a depth threshold was achieved.

Endpoint configuration:

```yaml
evm_rpc:
  endpoints:
    "11155111":
      url: https://operator-owned-endpoint.example
      finality: { mode: finalized } # Also the default when omitted.
    "31337":
      url: http://127.0.0.1:8545
      finality: { mode: depth, confirmations: 0 } # Explicit local-test exception.
```

Depth counts subsequent blocks (`head >= receipt block + confirmations`).
It must be positive except on local chain ID 31337; integer overflow rejects.
For observed latest height `H` and depth `D`, the durable checkpoint is the
canonical block at `H - D`. Reorganizations confined above that qualified
boundary do not contradict accepted history. The observed tip is rechecked during
assessment; a concurrent tip change delays completion without permanently halting
the chain. A positive conflict at the retained qualified boundary still halts.
Depth remains probabilistic. Numeric and unsigned decimal environment-string
depths are accepted; invalid values reject. Existing journals that retained the
old unqualified tip remain conservatively fenced; see [migration](../replay-migration.md#depth-checkpoint-correction).
The implementation is [the shared helper](../../core/src/finality.rs), exposed
through [chain configuration](../../core/src/chain.rs). No provider probe can
silently change its policy.

For a stored attempt hash, require all of the following before terminal cleanup:

1. A matching receipt with a valid execution status, block number and block hash.
2. A canonical block read at that number whose hash equals the receipt's hash.
3. A non-null finalized checkpoint `(number, hash)` covering the receipt height;
   an explicitly configured depth policy uses the canonical `latest - depth`
   boundary and requires that boundary to cover the receipt.
4. Consistency with the last durably accepted checkpoint: a lower candidate boundary
   cannot move it backward; a changed hash at its height cannot be accepted.
5. Revalidation of the selected checkpoint by number and of the receipt's block
   before the fenced terminal commit. Concurrent checkpoint updates must compare
   against the persisted predecessor or retry; a stale worker cannot overwrite it.
   Positive-depth assessments also re-read the observed tip. They use six block
   reads before executor caching; finalized-tag and local depth-zero use four.

The independent journal persists receipt block identity, checkpoint identity,
selected policy and outcome before Redis terminal status, retention start and
webhook work commit under the ownership fence. A crash between these commits
requires projection reconciliation; the writes are not a distributed atomic
transaction. A status-0 receipt that passes the gate means settled execution
failure under the selected policy, with its nonce consumed.

ERC-4337 additionally corroborates the operation's hash, sender, nonce and
EntryPoint event against the independently fetched canonical outer receipt. An
outer transaction's success alone is insufficient. Durable terminal identity must
also belong to this admission's recorded broadcast attempts. Bundled EIP-7702
is disabled at admission and existing send/confirm workers park: the proprietary
bundler has no qualified independent witness that its outer hash executed the
admitted UID/call. Direct EOA type-4 execution uses its separate signed-wire and
receipt gate; ordinary EOA tests do not qualify the disabled bundled protocol.

These cross-checks detect mismatches; they do **not** cryptographically establish
ancestry across a dishonest or internally inconsistent RPC. The initial design
trusts a qualified endpoint's canonical-chain and finality semantics. A JSON-RPC
batch is not an atomic chain snapshot. Missing or contradictory reads delay
completion; independent consensus/parent-chain verification remains separate work.

### Reorg and failure behavior

| Observation | Required behavior |
| --- | --- |
| Included receipt, checkpoint has not reached it | Remain provisional; retain attempts. Do not bump fees merely because finality is slow. |
| Previously observed receipt disappears or its block hash changes before finality | Invalidate that provisional observation, retain its audit history, inspect all attempt hashes, and reconcile the same intent/nonce. No new nonce or second intent follows from absence alone. |
| RPC error, unavailable tag, stale head or missing historical block | Retry with bounded polling/backoff; preserve evidence. The EOA retained-attempt budget limits new nonce allocation; EOA pending admission has its own finite cap; journal history still needs disk management. |
| Lower latest nonce after reorg or lag | Reconcile retained reservations; never recycle an uncertain nonce or silently reset the reservation ledger from this read. |
| An observed hash disagreement at the last accepted checkpoint | Durably halt the chain and stop new broadcasts and terminal transitions. Keep previous outcome evidence; require operator reconciliation. Do not automatically compensate or re-sign. |

An unfinalized receipt can guide scheduling, but must not erase the attempt or
become terminal success. Inclusion/nonce progress frees the ordinary mempool
window while submitted attempts remain retained. The EOA store applies a 100,000
retained-hash ceiling to new nonce allocation, separately from the configurable
inflight window and 25,000 unsigned-pending intake cap per signer/chain. Historical
journal rows and existing recovery work still need operational capacity controls.
Nonce cleanup always includes the observed consumed-count floor even if older
receipts remain; settling newer receipts cannot rewind allocation into old bindings.

EOA receipt polling runs at a five-second cadence with a rotating window of at
most 1024 hashes per wallet, fetched directly as bounded Redis rank pages.
Candidates use the signer count at finalized, or at latest height minus an
explicit depth (depth zero reuses latest count). This filters likely premature
receipts; it is never terminal evidence. Low/stale counts delay work, high counts
only request extra guarded reads, and unsupported reads have no latest fallback. Receipt
RPC concurrency is 32; distinct-block assessment concurrency is 8. Block evidence
is reused only within that worker's current batch for identical block identity;
each receipt still needs its own hash/status/durable reservation checks. Cleanup
reads only proven nonce groups (4096-record ceiling), preserving unrelated work.
There is no global per-chain checkpoint service or persistent assessment cache.
ERC-4337 retains its own retry/backoff; bundled 7702 is parked. See the
[50 TPS review](throughput-50tps.md) for finite capacity and polling-cost limits.

Checkpoint continuity is checked during active EOA receipt polling, before its
nonce-filter/empty-result return, and during ERC-4337 confirmation polling before
the bundler lookup. Bundled 7702 is disabled before that path. Assessment also checks it for pending/orphaned receipts.
This is **not continuous monitoring** of every previously finalized block: an
idle chain with no active work has no dedicated watcher. Null history delays
progress without retracting terminal outcomes. A positive contradiction that is
observed causes a halt; remote history/finality honesty remains an assumption.

## Solana contract

New durable Solana requests that explicitly select confirmed are rejected before
queue admission; finalized is the supported completion policy. Existing queued
confirmed requests are reconciled under the stronger floor without re-signing.
The durable executor currently also uses finalized for blockhash acquisition and
preflight; non-durable sign/simulate paths can still use their request preference.
A confirmed observation is provisional:
continue reconciliation and retain the attempt until finalized. Existing queued
confirmed requests wait for finalized under their original identity; never
rebuild their message or signature. Sign-only/preflight configuration does not
imply terminal completion. This strengthens the previous confirmed behavior and
must be disclosed to callers.

Reconcile the original signature before considering blockhash validity. Already
finalized execution remains successful or failed even after its blockhash expires.
Require finalized status plus matching transaction details, signature, slot and
execution outcome for both success and failure. A confirmed failure is still
provisional. A previously visible signature that later disappears is unresolved,
not permission to refresh its blockhash. Retain the existing broadcast/read
budgets and parking behavior; a parked attempt remains recoverable.

Blockhash acquisition/preflight and completion are distinct policies. Solana's
guide recommends confirmed blockhash acquisition with matching preflight to
maximize validity time; that does not require completing at confirmed. Splitting
these options is a later optimization, not an implicit change to already signed
bytes. [Confirmation and expiration guidance](https://solana.com/developers/cookbook/transactions/confirmation)

Persist finalized evidence before removing attempt bytes. Later null historical
lookups do not retract a retained finalized outcome: history can be unavailable.
Conflicting positive finalized evidence presented during reconciliation durably
halts execution. Completed signatures are not continuously queried afterward.
The guarantee assumes honest cluster/RPC finality and retained durable records;
it does not cover ledger resets, faulty consensus or loss of the outcome journal.

## API, migration and disaster-recovery boundary

- Preserve the existing acceptance response for supported requests; new durable
  Solana requests selecting confirmed receive a validation error. Terminal success/failure and their
  webhooks arrive later; document this behavior change. Optional early events
  must carry an explicit provisional stage and stable event/version identity.
- EVM terminal outcomes carry achieved finality and the selected policy. Retain
  receipt/block/checkpoint evidence so consumers can audit the guarantee.
- Stop old workers before cutover. Pending requests currently follow the
  operator's configured policy; policy is not pinned at admission. Before a
  chain's first persisted settlement checkpoint, configuration can change the
  threshold for pending work. After a checkpoint exists, a policy change conflicts
  with its retained policy and halts the chain when assessed; even strengthening
  depth to finalized requires a separately designed migration. There is no
  force-clear migration command. Admission-bound policy versions are future work.
  Never rewrite an
  original fingerprint, signed intent or lifetime send budget during migration.
- Previously terminalized records lack the new guarantee. Mark their finality
  unknown unless retained chain evidence can qualify them. Do not replay them.
- The [Redis disaster-recovery implementation](redis-disaster-recovery.md) uses
  an independent local SQLite authority for original intent, every possibly
  broadcast attempt, immutable replay reservations, checkpoints and outcomes.
  Missing/mismatched Redis continuity blocks execution. Offline recovery creates
  an empty new namespace/epoch, quarantines attempted nonterminal IDs, retains
  all bindings, and sends nothing. Original unsent requests may be retried;
  terminal IDs cannot requeue. A missing key or receipt never grants a new identity.

Recovery supports one active process on one Unix host with a durable local ledger.
Losing or rolling back that authority is outside the guarantee. Finality cannot
reconstruct lost admission identities or prove an unobserved transaction never
executed; a halt cannot retract a previously authorized network call.

## Acceptance tests and open decisions

| Fault or boundary | Required assertion |
| --- | --- |
| Receipt seen, then orphaned, then same attempt included elsewhere | One intent/nonce survives; no early terminal webhook; final result binds the new canonical block. |
| Reverted receipt before/after finality | Only finalized failure is terminal; never reported successful or retried as a fresh intent. |
| Finalized unsupported/null/stale; safe/latest continues | No downgrade or false completion; bounded resource use and retained evidence. |
| Receipt/checkpoint reads cross a fork or alternate backends | Mismatch prevents completion; accepted checkpoint cannot regress or change hash. |
| Restart/stale worker during checkpoint and terminal commits | Atomic outcome/evidence retention and fenced updates; no duplicate logical terminal event. |
| Solana confirmed success/failure disappears, or finalized signature outlives blockhash | No new signature; confirmed stays provisional; valid finalized evidence settles the original attempt. |
| Restore missing/old Redis data while chain contains accepted effects | Recovery remains closed to new sends until independent evidence establishes continuity. |

Targeted tests cover core RPC cross-checks/configuration, executor provisional
outcomes and final reverts, and journal checkpoint/terminal conflict handling.
The [Finality model](../../formal/finality.md) separates real canonical state from
observations; the [disaster-recovery model](../../formal/disaster-recovery.md)
separates the authority, Redis projection and network effects. Their negative
cases expose dishonest-provider, post-finality rollback and lost-authority limits.
Final process/Linux gates and frozen-source evidence remain required. Production
endpoint qualification, immutable admission policy, global backpressure and
independent 7702 bundler attribution remain open.

## Unknown EOA dispatch outcomes

All post-dispatch RPC errors retain the original signed borrowed attempt and its
nonce. Recovery first looks for a matching included receipt, then retransmits the
same wire when unresolved; error text never recycles the nonce. Unknown sends do
not emit a send-success webhook or terminal failure. The existing worker requeue
cadence uses a rounded one-second delay for unknown-only/no-progress work, with
32 concurrent recovery RPC tasks. Mixed successful cycles with send/recovery
progress and unsigned backlog can rejoin the queue tail immediately; retries can
continue indefinitely while evidence remains unknown. Admission and inflight
bounds limit retained work, not lifetime provider spend. Recovery visits all
borrowed records (potentially 4,096), so slow RPC can delay finality polling; the
256 new-reservation limit does not cap borrowed or recycled recovery work.

NOOP reserves its submitted hash before network I/O. An actually rejected NOOP
remains unresolved and may block nonce progress; its exact signed bytes survive
in the independent journal for explicit offline reconciliation. Automated NOOP
wire recovery is not implemented. Pre-sign deterministic validation failures
retain their existing behavior because no network attempt was authorized.
