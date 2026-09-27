# Review evidence

Read the [final conclusions](OUTCOME.md) first.

These files record the final capacity campaign's review boundaries. The Engine
binary remained `35e307e16cafa7cbb17caafcf5ed90a840d58dddd2454542566619266e7a4c07`.

- [Runtime review](quality-review-before-crashes.md): performance, security,
  reliability, readability and proof/test coverage before the final crash cuts.
  Its pending-work statements describe that review's date, not later outcomes.
- [Crash-test review](remaining-chaos-review.md): expected recovery assertions,
  retained-state rules and a separate SQLite dependency assessment.
- [Captured resources](captured-resources.json): sampled process memory and CPU
  time. These do not identify whole-host or storage bottlenecks.
- [RPC sizing](RPC-SIZING.md): measured request pressure and public-price
  assumptions; no paid RPC calls were made for this campaign.
- [CI metadata](ci-1ebe25c.json): four passing gates at the named commit.
  Additional capacity Python checks are separately recorded in the
  [journal-reader validation](../journal-reader/README.md); the workflow patch
  has not been installed because the GitHub token lacks workflow permission.

Use the [results](../RESULTS.md) for subsequent crash outcomes and the final
operating envelope. The earlier reviews do not override those measurements.

The three new unresolved/recovery fixtures are also copied to private durable
storage, with every regular file hash verified. [Publication metadata](private-custody-summary.json)
contains only paths, sizes and digests; private contents remain outside Git.
