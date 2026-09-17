# Test-wallet funding

Verified September 17, 2026. All five networks are funded through [OpenFaucet](https://openfaucet.org/). The earlier faucet-funding blocker is resolved.

| Network | Received and verified balance | Transaction |
| --- | ---: | --- |
| Ethereum Sepolia | 0.01 ETH | [Receipt](https://sepolia.etherscan.io/tx/0xced45bce948382aba9d2111dc1f8b0a70b01faa46a713a98eb5e5c003e2bd366) |
| Arbitrum Sepolia | 0.01 ETH | [Receipt](https://sepolia.arbiscan.io/tx/0xb90317418413da03234cf851ba08937a5a59e2d1cae648bf1895257aab7b5a21) |
| OP Sepolia | 0.01 ETH | [Receipt](https://sepolia-optimism.etherscan.io/tx/0x916b4626f0743ea2a0dfca2056669813818ded36cb98791f07502b2c2c4aa19d) |
| Base Sepolia | 0.012 ETH | [Receipt](https://sepolia.basescan.org/tx/0x6b453794f8b77bd08dc139413bc8d056aec201aa0632be78c4d2dace07613cc9) |
| Solana Devnet | 0.01 SOL | [Receipt](https://explorer.solana.com/tx/3eX6Wepq77dL1vbu7SKq66dLU3RN4pwjfxbvqQk8tgDvDq5dNXiatdPurcV7zcDqVcnV3VL56KEmDj1Z5W4bNc82?cluster=devnet) |

EVM wallet: `0x1AE5c03782552FA600eEe3d3ceFe42019a25AD17`. Solana wallet: `BymPLiErFxJBV47sc31B7VnMoePFnACshrMx3VxkjC69`.

## Verification

Balances started at zero. The EVM receipts returned success and the destination balances increased by the claimed amounts. Solana returned a finalized, error-free signature and a 10,000,000-lamport balance. Verification used public RPC endpoints. [Raw receipts, balances and attempt log](baselines/faucet-funding.json).

The normal browser proof-of-work flow used one worker per network and stopped at the first eligible claim. Base credited one additional proof while its worker was stopping, so its claim was 0.012 ETH. All mining and faucet browser sessions are stopped.

No paid RPC calls or purchases were made in this round. The dRPC campaign estimate remains $2.21. Private keys were never submitted to faucets.

## Other faucets checked

Google was inaccessible while the Mac was locked. QuickNode rejected the fresh addresses for lacking mainnet balances, despite contrary FAQ text. Other options were empty, paused, or required sign-in, prior wallet history or identity checks. The attempt log records each result.

## Next step

Use these small test balances for the planned low-rate Engine transaction tests, then increase rates only after reconciling each submitted intent. Receiving faucet funds does not establish Engine submission correctness or throughput.
