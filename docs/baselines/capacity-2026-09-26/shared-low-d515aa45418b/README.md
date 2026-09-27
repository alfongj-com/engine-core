# Shared four-chain 47 TPS control

All 16,920 offered transactions were accepted and reconciled exactly. One Engine and FULL journal served EVM 12, OP 12, Nitro 10 and Solana 13 TPS for 360 seconds. All queues drained and owned children stopped. This is one clean finite control, not a confirmed sustainable maximum.

The campaign used a **10-second queue lease**, while the service configuration defaults to 600 seconds. That distinction applies to comparisons with the overloaded 235 TPS run. Later tests use the production lease; original results are preserved unchanged.

Late completed TPS were EVM 12.001, OP 11.845, Nitro 9.951 and Solana 13.006. Unsigned backlog stayed small (4→3, 3→3, 4→1, 0→1 respectively). The unchanged strict assessment retains finality-cadence uncertainty rather than declaring capacity selected.

This archive preserves the exact report, sanitized observations, digest-only RPC audits, resource evidence and frozen source/config hashes. Independent audit and descriptive summaries may be appended. Private journal/AOF/raw wires/node data are excluded. All nodes and load share one Mac; these are local execution measurements, not public RPC or L1 settlement qualification.

The independent captured-evidence audit verified all 16,920 original IDs, effects, fees and nonces against the private journal. No RPC, transport or Engine errors were logged. This is a single clean control, not an indefinite throughput guarantee. [Audit](independent-audit.json).
