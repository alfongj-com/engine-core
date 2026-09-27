# Shared pair with the production 600-second lease

One Engine, one SQLite FULL journal and Redis AOF every second served four local chains for 360 seconds each. One EVM signer was shared across distinct chain IDs; Solana used one payer. Both runs used the production 600-second queue lease, send concurrency 32, HTTP concurrency 128 and Solana polling every 5 seconds. These other settings are explicit campaign choices, not a claim that every production default was used.

| Offered vector EVM/OP/Nitro/Solana | Accepted and settled | Known HTTP 429s | Interpretation |
|---|---:|---:|---|
|60/60/50/65  = 235 TPS|55,105|29,495|Overload; original offered workload incomplete|
|12/12/10/13  = 47 TPS|16,920|0|Clean finite lower control; no sustainable maximum selected|

All accepted IDs have captured canonical/finalized receipt, original wire/replay and terminal evidence; fees/effects reconcile. Both runs drained all nine counters and stopped owned children. Independent audits compare captured observations against original private journals; they are not additional RPC sweeps. Normal reports retain aggregate HTTP counts, not a separate per-ID HTTP status map. Eventual accepted-work settlement does not erase the high run's admission rejection or backlog growth.

The earlier 10-second lease screens remain separate evidence. This pair changes that configuration and must not be silently merged with them. Ordered runs share a host and retained Nitro database; neither randomization nor infinite sustainability is established. Anvil OP execution lacks a real sequencer/L1 settlement, Nitro is dev L2-only, and Agave is local. No public-chain or paid RPC capacity claim follows.

Original reports and sanitized per-ID observations are preserved losslessly; RPC audits pass explicit event/key/value allowlists. Strict assessments and descriptive summaries remain unchanged. Source/report/audit hashes and copy provenance are in source-integrity.json and manifest.json. Private journals, AOF, node snapshots, raw signed wires, key material, environment and unstructured runtime logs are excluded. Partial archives have no manifest and are not trusted completed evidence.
