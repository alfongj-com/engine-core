# Dependency security review — 2026-09-15

## Result

The inherited buildable lockfile had **26 known-vulnerability findings**. The revised lockfile has **0** against RustSec database commit `e2e640471715167f73e22eaf761f2e547adafeec` using cargo-audit 0.22.2. Informational warnings remain below; a zero vulnerability count is not a security certification. Final compile and runtime gates are recorded in the handoff.

## Changes

- Updated Alloy's compatible 1.x protocol/signing packages and core encoding/hash packages, including the remotely relevant [EIP-712 malformed-input panic](https://rustsec.org/advisories/RUSTSEC-2025-0073.html).
- Updated AWS configuration/KMS and TLS dependencies together. Selected the current HTTPS connector and removed the optional legacy TLS/HTTP connector, eliminating the old HTTP/2 and certificate-validation paths.
- Replaced the unused Solana umbrella client with its existing HTTP RPC client/API crates. The service does not use the umbrella's TPU or WebSocket clients; this removes their old TLS dependencies without changing the HTTP RPC implementation.
- Disabled Prometheus's optional protobuf feature. The service exports text metrics, so the vulnerable protobuf decoder was unnecessary. Metrics export tests remain applicable.
- Updated affected byte buffers, integer arithmetic, QUIC, logging, time, random-number and synchronization dependencies within compatible constraints.

`aws-smithy-types` is constrained to `1.6.3`: the current `aws-config 1.12.0` uses `aws-smithy-json 0.63.0`, which does not compile against the changed `Document` representation in types 1.7.0. Cargo resolves a coherent AWS set including KMS 1.118.0. Remove this temporary constraint only after AWS configuration catches up and the build/KMS tests pass.

These dependency updates are separate from the queue comparisons: the queue experiments preserve one identical lockfile across source variants. Do not attribute full-engine dependency changes to measured queue gains.

## Remaining informational findings

| Category | Package | Advisory | Finding |
| --- | --- | --- | --- |
| unmaintained | `bincode 1.3.3` | RUSTSEC-2025-0141 | Bincode is unmaintained |
| unmaintained | `bincode 2.0.1` | RUSTSEC-2025-0141 | Bincode is unmaintained |
| unmaintained | `derivative 2.2.0` | RUSTSEC-2024-0388 | `derivative` is unmaintained; consider using an alternative |
| unmaintained | `paste 1.0.15` | RUSTSEC-2024-0436 | paste - no longer maintained |
| unsound | `lru 0.16.4` | RUSTSEC-2026-0253 | Potential use-after-free due to lack of panic safety in `LruCache::pop()` |

The retained `lru 0.16.4` version is constrained by `alloy-provider 1.7.3`. The advisory requires a key with a panicking destructor in `LruCache::pop()`. Review of this resolved provider found `u64` block-number keys on its `pop` path and fixed-byte `B256` keys in the response cache; neither key has such a destructor. This makes the reported trigger appear unreachable in these call sites, but the dependency remains affected and should move to a patched compatible release. The finding is not suppressed. Unmaintained dependencies also need replacement planning. No deny-list exceptions were added.

## Reproduce

```sh
cargo install cargo-audit --locked
cargo audit --json
```

[Before](dependency-audit-before.json) and [after](dependency-audit-after.json) contain the full reports, patched-version ranges, dependency versions, and advisory metadata. A later database may report additional findings.
