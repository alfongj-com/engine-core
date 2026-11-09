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

Local code: **`/Users/alfongj/Code/engine-core-review-stack`**, branch
**`reconcile-public-read-failures`**. Record `git rev-parse HEAD` before testing.
Its `README.md` and `LOCAL_TESTING.md` contain setup, installed tool paths,
commands and a prompt for another local agent. The older
`/Users/alfongj/Code/engine-core` checkout is unchanged.

Local evidence: **`/Users/alfongj/Code/engine-core-review-evidence`**, branch
**`load-tests`**. Read the [source-specific results and limits](docs/baselines/pending-work-2026-11-08/README.md).
Private originals and unresolved fixtures are under
`/Users/alfongj/.local/share/engine-core/runs/pending-work-2026-11-08/`.
Old incident journals, failed fixtures and the Nitro VM remain intact.

## Results

- Runtime `4d05bbf`: full Rust checks, **196 Redis/API regressions**, **15 process
  scenarios**, **115 finite model configurations** and five fee proofs pass.
  Later fixes have separate exact-head CI; these are not whole-service proofs.
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

Use the latest code's CI and capacity results before promoting it. Production
hardware, public-provider limits, actual Nitro throughput, rollup settlement,
physical power loss and real AWS KMS operation still need qualification. KMS
protocol vectors do not establish correct transient-error handling, IAM or quotas.

Deployment still requires one independent journal and one active Engine process
on one Unix host. History is permanent; compaction reclaims free pages. Offline
queue reconstruction cannot infer outcomes of unknown transactions or recreate
missing Solana signing history. Follow the code branch's recovery/upgrade runbook;
schema 3 has no in-place downgrade.
