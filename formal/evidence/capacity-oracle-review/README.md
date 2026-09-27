# Recovery oracle source review

[CI run 36281576684](https://github.com/alfongj-com/engine-core/actions/runs/36281576684)
at `5d602581dd0e2270a520fdba88ac3e71def00d86` stopped before TLC because the
reviewed hash of `scripts/local_eoa_reorg.py` had not been updated. The separate
fee-proof job passed. The artifact upload then failed because the stopped model
runner had produced no results. This was a source-review omission, not a model
counterexample. Preserve the [original log](source-guard-failure.log),
[run metadata](ci-run.json) and [hash summary](failure-summary.json).

The reviewed oracle requires a post-fault accepted Engine broadcast of the same
durable wire, independently matches that wire to the node transaction and
canonical receipt, and checks that success or revert remains provisional until
the configured depth. A repeated broadcast need not create another attempt.
Terminal-ID replay must not send again. Only an explicit legacy comparison flag
allows a same-nonce replacement with unchanged signed execution fields.

The [implementation evidence](../../../docs/baselines/capacity-2026-09-26/reorg-oracle/validation.json)
records seven passing offline unittest methods (positive witnesses and negative
controls) and two passing actual Engine/Anvil process cases, success and revert,
on unchanged binary `a3d360eba3dc470bd830ec9d3a32e0be8e11b6764f9bbf6efeb319f080eb8515`.
The offline synthetic signatures test evidence association, not cryptography.

The source map now also guards the new test module and shared wire/proxy helper.
No TLA+ transition changes: identical retransmission stutters in the EOA and
journal identity abstractions; `Finality` still separates provisional inclusion
from a qualified terminal outcome. These models do not prove mempool recovery
timing, wall-clock liveness, node encoding or implementation refinement. Mapping
the helper does not formally verify its unrelated capacity-campaign functions.
The suite remains 61 cases. This review does **not** claim a new TLC run; a later
combined source gate is required after the pending index change is frozen.
