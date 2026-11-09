# Signing dependency security

## Decision

Use released RustCrypto k256 0.14 / crypto-bigint >=0.7.6 and LRU >=0.18.2.
These fix [RUSTSEC-2026-0387](https://rustsec.org/advisories/RUSTSEC-2026-0387.html)
and [RUSTSEC-2026-0253](https://rustsec.org/advisories/RUSTSEC-2026-0253.html).
Alloy's published compatible versions still select the older crypto stack.
Until upstream releases compatible fixes, pin these small compatibility forks:

- [Alloy patch](https://github.com/alfongj-com/alloy/compare/v1.8.3...12003e896f89eb9c9263102a5d7c9ec58fb113b9):
  dependency/API migration; current LRU; remove unsupported Turnkey/YubiHSM
  feature graphs. Neither backend is exposed by Engine.
- [Alloy primitives patch](https://github.com/alfongj-com/alloy-core/compare/v1.7.3...36b85284347fcc45158f120e201492d3634acde8):
  k256 dependency and SEC1 point-trait rename, following upstream core PR #1147.

No local cryptographic arithmetic, advisory exceptions or version disguises.
Solana uses the same modular host-side types directly; the broad SDK's unused
on-chain secp256k1 recovery no longer enters the dependency graph.

## Compatibility and removal

Retained signed bytes remain authoritative. An upgrade may never re-sign an
unknown transaction to accommodate a dependency change. Environment keys use
native secp256k1; AWS signature decoding/recovery uses fixed upstream RustCrypto.
Validate published wire vectors, low-S recovery, hash boundaries, full functional
suites and cross-binary crash/replay before changing these pins.

Replace both forks once maintained upstream releases pass those checks and
`cargo audit` reports no vulnerabilities/unsoundness. Unmaintained
bincode (two versions), derivative, paste and pin-utils still produce warnings;
this decision does not certify those packages or every optional Alloy feature.
