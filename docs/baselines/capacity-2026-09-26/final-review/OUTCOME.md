# Production review outcome

27 September 2026. Read with the [measured results](../RESULTS.md).

| Area | Conclusion |
|---|---|
| Performance | With one signer per chain, 15-minute runs offered 50 EVM TPS and 58.75 aggregate TPS, then reconciled every accepted intent after drain. Individual 55–65 TPS screens locate useful candidates and overload. Shared 235 TPS overloaded badly. This does not establish 50 TPS per chain simultaneously or an absolute maximum. |
| Security | Available per-ID checks found no substituted signed identity or incorrect transaction effect. Lost-response and replay tests retain the original wire. This is scoped evidence, not a complete security audit. Bundled EIP-7702 stays disabled; legacy credentials in existing journal history still require migration. |
| Reliability | Mixed execution, response loss, injected send errors and the OP reorg recovered exactly. Engine and Redis crashes also exposed real operator-recovery requirements. A one-checkpoint SQL/Redis gap stopped Engine restart before workers. Preserved unknowns must not be described as recovered transactions. |
| Readability | The dispatcher separates ordered durable authorization from bounded RPC work; tests cover both new and recycled paths. The campaign now has one documented SQLite reader. Large borrowed/recycled recovery cycles and the large harness remain candidates for further separation; no runtime refactor was mixed into the final measurements. |
| Tests and proofs | Real Redis/SQLite/RPC regressions, compiled negative controls, actual node faults and independent effect reconciliation provide meaningful evidence. The 163 local harness tests pass. The finite models have 61 expected outcomes, with explicit counterexamples and source guards; they do not prove Rust refinement, model composition or wall-clock throughput. |

## Priorities

1. Design a recoverable journal-to-Redis update protocol before claiming automatic
   restart availability. Simply overwriting the Redis checkpoint or clearing the
   halt would remove the protection these tests exercised.
2. Measure time waiting for the journal lock, durable commits, Redis and RPC
   separately. Then improve fairness under shared load and evaluate bounded
   Solana status batching. The current data does not isolate the bottleneck.
3. Repeat on production hardware and full rollup stacks, then run a capped public
   testnet campaign with the actual RPC plan and signer backend. Local OP execution,
   Nitro dev L2 and a single Agave validator do not qualify public finality.

The retained-state cases and the earlier 1,980 unverified EVM outcomes remain
visible in the results. History capacity, authoritative-journal loss, multiple
hosts and automated NOOP recovery are still deployment limits. These results do not qualify the service for unattended production operation.
