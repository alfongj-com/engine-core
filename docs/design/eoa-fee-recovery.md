# EOA fee limits and stalled recovery

## Contract

Caller-supplied `gasPrice`, `maxFeePerGas`, and `maxPriorityFeePerGas` are hard per-gas ceilings, including recovery replacements. A supplied priority cap alone does not cap the total gas price. Missing fields remain estimated; estimated fees have no operator-configured spending ceiling today.

Recovery applies its existing 20% fee increase only within those ceilings, clamps the priority fee to the total fee, and uses saturating arithmetic. If neither fee changes, it skips signing and broadcasting a replacement. Fully specified fees normally already equal their ceilings, so such transactions cannot automatically increase their price.

If EIP-1559 estimation is unsupported, a request containing either explicit dynamic-fee field fails closed instead of falling back to legacy pricing and dropping that limit. Requests with no explicit fee values retain automatic legacy fallback. This intentionally changes prior behavior.

## Recovery and operator consequences

A failed stalled-transaction rebuild or replacement preserves the submitted intent and recovery evidence. Missing original request data also leaves the nonce unresolved. Neither case triggers the former fallback self-transfer or automatic nonce reset. The separate handling of deliberately recycled nonces is unchanged.

A capped or unrecoverable transaction can therefore hold later transactions behind its nonce. Operators must reconcile the original hash, nonce, and stored request before intervening; submitting the business intent under a new ID is not a safe recovery procedure. This change does not add a fee-edit or cancellation API.

These limits do not constitute a total-debit budget. Gas limits, value, L2 data charges, and other chain-specific costs also matter. Automatic estimates and deliberate recycled-nonce no-ops still require a separate operational spending policy.

## Verification

Run against a disposable local Redis:

```sh
cargo test --locked -p engine-executors --lib fee_tests
TEST_REDIS_URL=redis://127.0.0.1:PORT cargo test --locked -p engine-executors --lib fee_tests -- --include-ignored
```

All four regressions passed on 2026-09-17, including the normally ignored Redis case on its own disposable server. They cover legacy/EIP-1559/EIP-7702 ceilings, signed-wire fee and intent preservation with partial caps, overflow-safe arithmetic, and the real confirmation flow with Redis plus an HTTP RPC stub. The latter checks capped, unbuildable, missing-request, and unsupported partial-fee cases: no replacement broadcast, no fallback gas-price lookup or self-transfer, no reset, and unchanged submitted state. They do not contact a public chain or spend funds.
