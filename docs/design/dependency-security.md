# Dependency security review

Date: November 8, 2026. Scope: the workspace lockfile and enabled dependency graph.

## Decision

Apply available compatible patches. Keep `cargo audit` enabled and its failure visible.
The remaining cryptographic timing advisory blocks a claim of clean dependencies.
No advisory is ignored, no cryptographic code is vendored, and no major Alloy migration is included.

## Patches

| Package | Previous | Updated | Finding |
| --- | --- | --- | --- |
| ctutils | 0.4.2 | 0.4.8 | Constant-time mask/comparison fixes, RUSTSEC-2026-0385. |
| rustls | 0.23.45 | 0.23.46 | Four TLS advisories, RUSTSEC-2026-0346 through 0349. |
| rustls-webpki | 0.103.15 | 0.103.16 | Distrusted intermediate verification, RUSTSEC-2026-0382. |
| webpki-roots | 1.0.1 | 1.0.10 | Distrusted roots, RUSTSEC-2026-0381. |
| zstd / zstd-safe / zstd-sys | 0.13.3 / 7.2.4 / 2.0.16 | 0.13.4 / 7.4.0 / 2.1.2 | Stream reuse, decompression memory safety and allocator fixes. |
| tokio | 1.50.0 | 1.53.3 | AsyncFd I/O safety, RUSTSEC-2026-0386. |

Resolution also updates Tokio's runtime prerequisites. Workspace version requirements
and public APIs are unchanged. Against RustSec database commit
`309392b18f6b17022f82c701f300433126901068`, reported vulnerabilities fall from
12 to 1 and unsoundness warnings from 12 to 1. Five unmaintained-package warnings
remain, including both locked bincode versions. An audit count is not an exploitability assessment.

## Remaining findings

**crypto-bigint 0.5.5, RUSTSEC-2026-0387.** The enabled path is
Alloy signer → elliptic-curve 0.13.8 → crypto-bigint 0.5.5, also used through
k256 0.13.4. The patched crypto-bigint line is 0.7; this cannot be substituted
within the current upstream requirements. Alloy's current 2.5.0 release has the
[same unresolved dependency](https://github.com/alloy-rs/alloy/issues/4445).
The [advisory](https://rustsec.org/advisories/RUSTSEC-2026-0387.html) concerns
secret-dependent timing. Local private-key signing uses this stack; do not dismiss
it as a lockfile-only or unused dependency. KMS integration does not establish
that all local cryptographic operations are unaffected. Resolve this through a
reviewed upstream migration/backport, then rerun signing compatibility and recovery checks.

**lru 0.16.4, RUSTSEC-2026-0253.** Alloy provider 1.x requires this version line;
the patch starts at 0.18.2. The
[panic-safety defect](https://rustsec.org/advisories/RUSTSEC-2026-0253.html)
requires a key's `Drop` to panic during `pop`, followed by cache reuse after unwind.
The inspected provider caches use integer block numbers and B256 keys without
such destructors. This limits the demonstrated exposure; the dependency still
has an unsoundness advisory and is not marked fixed. An upstream version change
must preserve provider polling and cache behavior.

Unmaintained dependencies require planned replacement with explicit wire-format
compatibility, especially bincode/Solana; changing serialization during a security
patch would add recovery risk.

## Verification

Run `cargo audit` against a fresh database; it is expected to fail until the
crypto-bigint issue is resolved. Use [LOCAL_TESTING.md](../../LOCAL_TESTING.md)
for the workspace, Redis, process and chain checks. Dated results, binary hashes
and the matched load comparison are on the separate
[load-tests branch](https://github.com/alfongj-com/engine-core/tree/load-tests/docs/baselines/code-review-2026-11-08).
