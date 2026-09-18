# To Alfonso

Updated September 17, 2026.

## Done

The [fork](https://github.com/alfongj-com/engine-core/tree/production-hardening) builds without Thirdweb Vault. Changes are in [draft PR #1](https://github.com/alfongj-com/engine-core/pull/1).

- **332 public testnet transactions verified:** Ethereum Sepolia 58; Arbitrum, OP and Base Sepolia 78 each; Solana Devnet 40. **Zero duplicate effects.** Repeating request IDs produced no extra broadcasts.
- Crash tests passed on all five networks. EVM tests restarted both Engine and Redis; Solana recovered by resending identical signed bytes.
- Fixed two more bugs: recovery could exceed supplied EVM fee limits, and reverted transactions were reported as successful confirmations. A real reverting-contract test now proves terminal failure, consumed nonces and no duplicate retry after a crash.
- Full local workspace, 37 executor Redis regressions, HTTP tests, build and Clippy pass. **All three Linux CI workflows passed** on the final source. Warnings remain. [Verification](docs/verification.md) records exact results.

[Public results and receipts](docs/baselines/public-transactions.md) include the original OP fee-accounting test failure and its successful reconciliation.

## Speed and cost

Short bursts passed at **10 requests/s on Ethereum, 20/s on the three EVM L2s, and 5/s on Solana**. These are offered rates, not sustained production capacity. Gas/fees were precomputed for the EVM runs.

Estimated RPC spending is **$2.22 total**; this round added about **1.4 cents**. The gateway is stopped, and the persistent **$12 ceiling** remains. Keys and recovery data stay outside Git. Rotate the shared dRPC key when the campaign ends.

## Remaining work

1. Test reorgs, finality, provider disagreement and Redis failover.
2. Add an operator recovery API and production spending limits; measure sustained load and Solana polling before optimizing it.
3. Integrate AWS KMS through credential references and qualify deployed smart-account contracts.

**No action is needed from you for the completed tests.** Before deployment, we need your target traffic, AWS/KMS setup, hosting/Redis choice, and resolution of the upstream repository's missing license. This remains a draft, not a production release. Follow the [migration guide](docs/replay-migration.md) before upgrading existing queues.
