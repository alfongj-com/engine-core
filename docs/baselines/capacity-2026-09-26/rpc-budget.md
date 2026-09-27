# RPC cost and remaining test budget

Reviewed September 27, 2026 UTC. **The published dRPC price fits the method mix,
but local request counts are not a provider invoice or a sustainable-rate
measurement.** No credentials, account balance or paid RPC were accessed.

## Current price

dRPC lists 20 CU per ordinary method at $0.30/M CU: **$6 per million methods**.
This includes EVM send/receipt/estimate/fee/nonce/block calls and Solana
send/status/history/transaction/blockhash calls. No surcharge is listed for
historical signature searches or finalized commitment. `eth_chainId`,
`web3_clientVersion` and Solana `getHealth` are listed at zero CU. Counting every
gateway call at 20 CU is conservative for those exceptions. [Method prices](https://drpc.org/docs/pricing/compute-units).

Count each batch member, repeated poll and retry. Provider verification queries
multiple providers, and its cost model settles actual provider work; the current
gateway requests no quorum. Reprice if routing changes. The gateway limits
forwarded methods; its dollar conversion is not an independently verified invoice
cap. Paid service advertises no rate cap while funded, which does not guarantee
capacity or latency. [Cost model](https://drpc.org/docs/specs/balancing/costmodel),
[verification](https://drpc.org/docs/gettingstarted/verification),
[rate limits](https://drpc.org/docs/howitworks/ratelimiting).

These official sources were checked on the review date. Other providers' older
comparison prices were not re-researched.

## Observed local mix

The completed reports below include drain. Late RPC/s uses their approximately
120–180s samples. These local calls spent **no paid credit**; hourly dollars are
arithmetic equivalents. All four profiles failed their offered-rate capacity
criteria, so the late rates do not represent sustaining 100/75 transactions/s.

| Profile and report | Engine calls / unique intent | Late RPC/s | Equivalent $/hour |
|---|---:|---:|---:|
| [Anvil EVM12](capacity-evm12-100-candidate.json) | 3.027 | 107.84 | $2.33 |
| [Anvil OP execution](capacity-op-100-candidate.json) | 3.042 | 121.24 | $2.62 |
| [Native Nitro dev](capacity-nitro-100-candidate.json) | 4.293 | 156.85 | $3.39 |
| [Agave, 2s polling](capacity-solana-75-candidate-poll2.json) | 10.935 | 623.46 | $13.47 |

EVM12/OP supplied gas limits; automatic estimation adds roughly one call/intent.
Nitro already estimates gas and adds about 0.283 block reads/intent. Solana used
manual fees: three fixed calls plus 7.935 status calls/intent; automatic priority
fees add roughly one. Public finality and outages can raise these ratios.

Independent verification adds roughly two methods/intent, plus chain/setup
observations. That traffic, public fee guards, retries and drain must be budgeted
separately from the Engine-only columns. EVM12 used release `c0014d2a…`; the other
profiles used `a3d360e…`. [Exact counts, method deltas and hashes](rpc-budget-data.json)
preserve their source scope. Local depth-2 and single-validator timing do not
qualify public finality costs.

## Headroom and a conservative next-run estimate

Credential-free persisted counters, last written September 18, record:

- **370,084 observed calls:** $2.220504 at ordinary rates.
- **374,000 reserved:** $2.244; ceiling 2,000,000 calls.
- **Restart-safe remainder: 1,626,000 calls**, estimated $9.756. The saved live
  metrics included 707 unused in-memory reservations that are forfeited on
  restart; do not reclaim them or reset the budget.
- **Provider account balance: unknown.** The originally supplied $50 is historical
  information; other account usage and actual billing are outside these counters.

Preserve at least **25% for reconciliation**: new work may use at most
**1,219,500** of the remaining calls ($7.317 ordinary-rate estimate). All chains
share that allowance. Do not admit new work that consumes its drain reserve.

`USD = 0.30 / 1,000,000 × Σ(method count × CU weight × billed provider fanout)`.

At ordinary 20 CU and one provider, `$ / hour = 0.0216 × RPC/s`. For selected
successful rates, use the upper repeated full-lifecycle calls/intent, add verifier,
setup, fault and drain calls, then allow about 2× measured ordinary demand for
operational headroom. This is not a guarantee under arbitrary outages or fanout.

| Budgeted RPC/s, including allowances | $/hour | Minutes within new-work allowance |
|---:|---:|---:|
| 500 | $10.80 | 40.65 |
| 1,000 | $21.60 | 20.33 |
| 2,000 | $43.20 | 10.16 |
| 4,000 | $86.40 | 5.08 |

These are planning ceilings, not approval for a public run or evidence that the
provider balance can fund it. Final selected-rate measurements remain separate.
