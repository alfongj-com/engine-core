# To Alfonso

Updated September 17, 2026.

## Done

The [fork](https://github.com/alfongj-com/engine-core/tree/production-hardening) builds without Thirdweb Vault. [Draft PR #1](https://github.com/alfongj-com/engine-core/pull/1) contains the changes.

- Added configurable EVM RPCs, connection reuse, authentication, credential redaction and response limits.
- Restored Solana signing with a separate local key. Queued local credentials contain public identities only.
- Fixed Solana crash recovery: persist signed bytes before sending, resend the same transaction after an uncertain response, and preserve evidence when the outcome is unknown. Duplicate request protection survives queue cleanup.
- Added tests for these failure cases. Actual local nodes confirmed **24 EVM transfers after an Engine crash**, **24 after an Engine + Redis crash**, and **12 Solana transfers after lost send responses and an Engine crash**. All had **zero duplicate effects**. The Redis test used durable AOF writes; it does not establish failover or power-loss safety.
- Verified actual Engine reads on Ethereum Sepolia, Arbitrum Sepolia, OP Sepolia and Base Sepolia. Solana signing works; the earlier simulation used an unfunded account. Faucet funding is now complete, and public Engine submissions are the next test.

**All three Linux CI workflows passed** on the final source commit. [Verification](docs/verification.md) records the tests and their limits. [RPC results](docs/baselines/rpc-results.md) records provider measurements and cost; [provider comparison](docs/design/rpc-test-plan.md) explains pricing and expected request demand.

## What the RPC results mean

All five networks passed a 30-second read test at 1,000 requests/second: 150,000 successful calls with no errors or local drops. Solana slowed at 2,000 and 4,000 requests/second and filled the client's concurrency limit. Those higher rates remain unqualified. Read capacity does not prove submission capacity or Engine transaction throughput.

Estimated dRPC usage is **$2.21** (conservative reservation bound: **$2.23**). The paid gateway is stopped. Its **$12 campaign ceiling** persists across restarts. The provider's bill remains authoritative.

## Funding is unblocked

I obtained and verified faucet funds on all five networks through OpenFaucet: **0.01 ETH each on Ethereum Sepolia, Arbitrum Sepolia and OP Sepolia; 0.012 ETH on Base Sepolia; 0.01 SOL on Solana Devnet**. No action is needed from you for initial wallet funding. [Receipts and balances](docs/testnet-funding.md).

The faucet round used no paid RPC calls. Mining and browser sessions are stopped. Keys remain outside Git in `~/.config/engine-core`, with owner-only permissions. Rotate the shared dRPC key when the campaign is finished.

## Next steps

1. Run funded transactions at one per second on each network; reconcile every effect and measure actual RPC calls and confirmation delay.
2. Increase rates in short steps. The single EVM signer has 50 outstanding nonce slots, so higher throughput may need multiple funded wallets and a reviewed signer-selection mechanism.
3. Reduce Solana polling cost, then repeat the same measurements. Current polling uses one signature per call.
4. Finish finality/reorg and Redis failover tests, qualify deployed smart-account profiles, integrate AWS KMS, and add an operator recovery endpoint for uncertain Solana outcomes.

This is **not ready for a production release**. The [security audit](docs/audit-security.md) tracks remaining issues. Follow the [migration guide](docs/replay-migration.md) before upgrading existing queues. The upstream repository's missing license also needs resolution before commercial use.
