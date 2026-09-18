# Upstream baseline

- Repository: https://github.com/thirdweb-dev/engine-core
- Commit: `b6b7a0bbdc737b3a2b09611305b71b1bf6aba6e8`
- Fork: https://github.com/alfongj-com/engine-core
- Inspection started: 2026-09-15.

## Build exception before behavioral work

The workspace pins `vault-sdk` and `vault-types` to a private Git SSH dependency. The user explicitly requested removal. Vault appears in signing credentials, EOA/UserOperation/Solana signers, ERC-4337 request restrictions, HTTP extraction, startup configuration and integration fixtures. The public AWS KMS implementation and ephemeral local EVM signer already exist.

We preserve this upstream revision for comparisons. A queue-only baseline workspace copies the unchanged `twmq` source and lockfile, changing only workspace membership to avoid resolving the unavailable Vault packages. This measures queue mechanics, not chain transaction throughput. Full-engine upstream runtime measurements are unavailable until the dependency is removed; subsequent engine results must say so.

## Audit order

Static findings are recorded in `docs/audit-queue.md` and `docs/audit-security.md` before their associated fixes. Chain and test designs are under `docs/design/`. Baseline measurements precede queue changes. No claim of full chain compatibility or production readiness follows from local tests.
