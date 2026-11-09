# To Alfonso

Updated November 9, 2026. No credentials are needed for the local tests.

## Code and PRs

The open stack is **[#21](https://github.com/alfongj-com/engine-core/pull/21) →
[#18](https://github.com/alfongj-com/engine-core/pull/18) →
[#19](https://github.com/alfongj-com/engine-core/pull/19)**.

- **#21:** carries the earlier reviewed stack into `main` and fixes intake,
  cancellation and journal/Redis recovery gaps. Earlier merges into stack
  parents had not brought all of those changes into `main`.
- **#18:** repeated Linux capacity searches, individual/shared-chain checks and
  chaos tests, with explicit rate and duration limits.
- **#19:** read-only reconciliation of interrupted public runs and retries for
  throttled preparation. Temporary follow-up PRs #20/#22 are closed; their fixes
  are in the owning PR or the base integration.

Fixes go on their owning open PR; dependent PRs are rebased. This agent does not
merge PRs.
Merge each PR into `main` in this order; retarget the next PR to `main` after its
parent merges. Merging into an already merged parent branch does not update `main`.

Local code: **`/Users/alfongj/Code/engine-core-review-stack`**, branch
**`reconcile-public-read-failures`**. Record `git rev-parse HEAD` before testing.
Its `README.md` and `LOCAL_TESTING.md` contain setup, installed tool paths,
commands and a prompt for another local agent. The older
`/Users/alfongj/Code/engine-core` checkout is unchanged.

Local evidence: **`/Users/alfongj/Code/engine-core-review-evidence`**, local branch
**`review-evidence-oct11`**, tracking GitHub's **`load-tests`** branch. Read the
[source-specific results and limits](docs/baselines/pending-work-2026-11-08/README.md).
Private originals and unresolved fixtures are under
`/Users/alfongj/.local/share/engine-core/runs/pending-work-2026-11-08/`.
Old incident journals, failed fixtures and the Nitro VM remain intact.

## Results

- The latest review heads—`0783256` (#21), `164b4c5` (#18) and `7fd8fa7`
  (#19)—pass full CI: Rust/Redis/API/process checks, dependency audit,
  **122 finite model configurations**, **110 source guards**, five fee proofs
  and two required mutation counterexamples. All known open-PR review threads
  are addressed. Source guards and finite models are not whole-service proofs.
- Runtime `4d05bbf`: full Rust checks, **196 Redis/API regressions**, **15 process
  scenarios**, **115 finite model configurations** and five fee proofs pass.
  Those historical results remain separate from the latest review checks.
- The dependency audit reports **zero vulnerabilities and zero unsoundness
  warnings**. Five maintenance warnings remain; no advisory is suppressed.
- Small public correctness/recovery checks cover Sepolia, OP Sepolia, Arbitrum
  Sepolia, Base Sepolia and Solana Devnet. Four later EVM runs stop on provider
  read timeouts; all reconcile from their original retained state with finalized
  reads, **no new broadcasts and no duplicate effects**. Original failures stay
  recorded as failures.
- Public RPC use: **7,998 forwarded calls**, **about $0.048 at the configured
  accounting rate**. This is an estimate, not a verified provider invoice.
  Owned public services are stopped.
- Historical Linux runtime `4d05bbf` qualifies **50 TPS for the local OP-mode
  profile** and **25 TPS for local Agave**, each in three five-minute trials.
  Higher rates and the other EVM profiles are unqualified. These results do not
  establish a production or public-chain maximum. The Mac comparison stops
  incomplete and supports no performance conclusion.

## Remaining qualification

The [current local capacity campaign](https://github.com/alfongj-com/engine-core/actions/runs/42573447135)
uses source `7fd8fa7`, a 50-TPS screening ceiling, ten-minute trials and an
eight-minute observation window. Each individual rate needs three passes before
the shared-chain and chaos phase can run. It is still in progress; the PRs remain
draft while these measurements finish. No public RPC credits are used.

The earlier 100-TPS Agave trial finalizes all accepted transactions on chain but
leaves 17,397 Engine acknowledgements pending at its drain deadline. It does not
qualify 100 TPS. Journal serialization is the measured bottleneck in that trial.

Production hardware, public-provider limits, actual Nitro throughput, rollup
settlement, physical power loss and real AWS KMS operation still need
qualification. SDK and worker tests now cover transient and known permanent KMS
errors; IAM, quotas, credential persistence and workload identity remain open
deployment concerns.

Deployment still requires one independent journal and one active Engine process
on one Unix host. History is permanent; compaction reclaims free pages. Offline
queue reconstruction cannot infer outcomes of unknown transactions or recreate
missing Solana signing history. Follow the code branch's recovery/upgrade runbook;
schema 3 has no in-place downgrade.
