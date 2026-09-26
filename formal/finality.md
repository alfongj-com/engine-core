# Finality gate: observations versus chain state

## Claim

[Finality.tla](tla/Finality.tla) explores one fixed transaction identity across
provisional inclusion, disappearance, re-inclusion, RPC reads and terminal
commit. It checks a finite abstraction of the [shared helper](../core/src/finality.rs),
not a refinement proof of Rust, a blockchain consensus proof, or a guarantee
against dishonest RPCs. The [runtime contract](../docs/design/finality-and-recovery.md)
defines the intended chain-specific meaning.

## Independent state and correspondence

The model keeps ledger state separate from observations. A block named `A`
contains successful execution; `B` contains reverted execution of the same
identity; `empty` contains neither. The chain can change its canonical block up
to three times, including disappearance and re-inclusion. Independent actions
advance actual finality or the explicit probabilistic depth threshold.

Receipt, canonical-block, checkpoint and revalidation reads are separate steps.
Chain changes and crashes can interleave between them. A recorded receipt can
therefore become stale. `Commit` records actual canonicality and finality as
ghost audit facts; they are **not guards** that make the safety assertions true
by construction. Chain changes remain possible after terminal commit in the
boundary configurations.

| Action | Source boundary |
| --- | --- |
| `ReadReceipt` | Executor receipt lookup; a missing receipt is unresolved. |
| `ReadCanonical`, `ReadHead` | `assess_receipt_finality`: compare receipt block hash and read finalized/explicit depth head. |
| `RecheckCanonical`, `RecheckCheckpoint` | Re-read the canonical receipt block and selected checkpoint before accepting the evidence. |
| `Commit` | Consumer of `FinalityAssessment::Finalized`: terminal success or execution failure, with policy/evidence retained. |
| `Crash` | Discard local observations; recover the same durable identity and reconcile again. Identity persistence is delegated to the separate recovery models. |

The model collapses block heights to a checkpoint that either covers the
receipt or does not. It does not model the arithmetic for a configured depth;
the actual Rust tests cover the threshold and overflow. Both block identities
and the weaker depth policy remain explicit.

## Configurations

All cases use one observer, one intent, three block identities and at most three
canonical changes. No fairness, liveness, state constraint or population
generalization is claimed. `CHECK_DEADLOCK FALSE` permits parked and exhausted
states without suppressing invariant checking.

| Config | Expected result |
| --- | --- |
| `Finality.cfg` | Pass: terminal commit was canonical and finalized, including reverted execution; its recorded block remains canonical under the normal finality assumption. |
| `Finality_early_inclusion.cfg` | `TerminalRequiresFinality`: an ordinary receipt cannot substitute for finality. |
| `Finality_missing_hash.cfg` | `CanonicalAtCommit`: a finalized height/checkpoint does not validate an old receipt's block identity. |
| `Finality_revert_early.cfg` | `RevertNeedsFinality`: failure must wait for the same gate as success. |
| `Finality_dishonest_provider.cfg` | `TerminalRequiresFinality`: a provider that invents finalized-head evidence defeats this RPC-trusting approach. |
| `Finality_finalized_rollback.cfg` | `TerminalRemainsCanonical`: a catastrophic rollback after real finality invalidates the ordinary guarantee. |
| `Finality_depth_boundary.cfg` | `TerminalRemainsCanonical`: depth confirmation remains vulnerable to later canonical changes. |
| `Finality_reinclusion_witness.cfg` | `ReinclusionWitnessNotReached`: an expected witness proving the model reaches terminal completion after observing inclusion, loss and re-inclusion. |

On 2026-09-26, targeted runs with the pinned TLC artifact passed the positive
configuration (752 distinct states) and produced every named negative/witness
counterexample. These targeted runs preceded final runtime source-map refresh;
the repository's complete runner/evidence gate must be repeated on frozen source.

## Assumptions and gaps

- The normal chain never changes a finalized canonical block. RPC block reads
  describe a coherent canonical chain, although observations can become stale
  between calls. The dishonest-provider and rollback cases remove these
  assumptions deliberately; no number of same-provider checks proves honesty.
- Each terminal commit is atomic and ownership-fenced. Queue fencing, Redis
  partial-command errors and journal durability are separate models/tests;
  their composition is not machine-checked here.
- One transaction identity is fixed throughout. Signature creation, fee
  replacements, nonce allocation and idempotency are not re-proved. Re-inclusion
  can change the outcome because execution state can change across branches.
- Cross-cycle checkpoint persistence, endpoint identity/chain qualification,
  policy changes, shared polling caches and reorg notification delivery are
  implementation obligations outside this finite gate model. Pending requests
  follow operator policy before the first chain checkpoint. Afterward, a policy
  change conflicts with the retained journal checkpoint and halts settlement;
  no immutable admitted policy version or automatic policy migration is claimed.
- Solana's finalized floor follows the same provisional/terminal distinction,
  but this module's block-hash read sequence represents EVM RPC. Solana signature
  commitment and attempt recovery have their own model and real worker tests.

## Frozen-source review

On 2026-09-26, after the coordinated runtime freeze and formatting pass,
[`source-map.json`](source-map.json) was refreshed for the shared helper/tests,
chain/configuration wiring, EOA/7702/external-bundler confirmation paths, executor
checkpoint adapter/tests and the real Anvil reorg harness. The core fixtures test
receipt/block identity, unavailable/lagging heads, revalidation, depth arithmetic
and checkpoint continuity. Executor regressions cover provisional success/revert,
active polling with no new receipt, a changed policy, and terminal projection
recovery. The journal regression checks the atomic chain-halt/terminal-write fence.

Active polling continuity and durable terminal/checkpoint guards are additional
source/test obligations, not states secretly added to this small one-observer
gate model. Solana's worker remains mapped to its separate commitment/recovery
model. Source hashes identify reviewed bytes and force reconsideration after a
change; they do not prove model composition or implementation refinement.
