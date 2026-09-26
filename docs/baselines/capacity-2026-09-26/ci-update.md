# CI integration

The current four workflows run on candidate `3a248ac`. New Rust and model
cases are discovered by those existing workflows. Their status is recorded
separately after completion.

[`ci.patch`](ci.patch) adds the capacity harness unit tests and a small, mixed
three-chain process test to `ci-hardening.yaml`. The patch was prepared and
reviewed locally. It was not published: the GitHub connector requires
reauthentication, the browser is signed out, SSH authentication is unavailable,
and the existing CLI credential does not grant workflow writes. Repository
code pushes remain available. No credentials or access settings were changed.

Apply this patch with an authorized workflow credential after reconnecting
GitHub, then rerun Rust correctness gates. Local harness and process results
remain separate evidence and do not substitute for an executed CI step.
