# Signing dependency security

## Decision

Use released RustCrypto k256 0.14 / crypto-bigint >=0.7.6 and LRU >=0.18.2.
These fix [RUSTSEC-2026-0387](https://rustsec.org/advisories/RUSTSEC-2026-0387.html)
and [RUSTSEC-2026-0253](https://rustsec.org/advisories/RUSTSEC-2026-0253.html).
Alloy's published compatible versions still select the older crypto stack.
Until upstream releases compatible fixes, pin these small compatibility forks:

- [Alloy patch](https://github.com/alfongj-com/alloy/compare/v1.8.3...12003e896f89eb9c9263102a5d7c9ec58fb113b9):
  dependency/API migration; current LRU; remove unsupported Turnkey/YubiHSM
  feature graphs. Neither backend is exposed by Engine. The compatibility changes
  also adapt low-S normalization to the new return type, parse Anvil keys with
  `from_slice`, and preserve the rand 0.8 API by rejection-sampling 32-byte keys
  with the caller's CSPRNG and zeroizing temporary bytes. They do not use modulo
  reduction or change signing bytes for existing keys.
- [Alloy primitives patch](https://github.com/alfongj-com/alloy-core/compare/v1.7.3...36b85284347fcc45158f120e201492d3634acde8):
  k256 dependency and SEC1 point-trait rename, following upstream core PR #1147.

No local cryptographic arithmetic, advisory exceptions or version disguises.
Solana uses the same modular host-side types directly; the broad SDK's unused
on-chain secp256k1 recovery no longer enters the dependency graph.

Full commit pins protect source identity, not hosting availability. Production
builds must retain their verified dependency sources in a mirror or vendor
snapshot with `Cargo.lock`. `cargo vendor --locked --versioned-dirs` supplies the
source configuration; verify that snapshot with an offline `cargo build --frozen`
before retaining it as a build artifact. Discovery tags must never replace the
full `rev` pins. Both forks have an `engine-security-2026-11-08` discovery tag;
complete Git source bundles for the pinned revisions are retained with the local
review evidence independently of GitHub. Unused GCP signer features add lockfile
entries without compiling them; they remain outside Engine's qualified backends.

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
