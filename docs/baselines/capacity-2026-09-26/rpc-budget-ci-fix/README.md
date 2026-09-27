# RPC budget CI fixture repair

[Original Linux CI run 36291670164](https://github.com/alfongj-com/engine-core/actions/runs/36291670164) failed on 6414d0e at the shared-budget fixture. [Original log](original-linux-ci-failure.log) is preserved. **Only the test changed; probe runtime and spending enforcement are unchanged.**

The old fixture offered five slots over 40ms with concurrency 1. Another caller consumed the remaining shared budget while the first successful response was pending. If that response finished after all later slots had been dropped locally, stage 1 never observed a refusal; stage 2 correctly received the first local HTTP 402 and stopped. The assertion that upstream received only one call passed. Requiring exactly one stage depended on response timing.

A [controlled reproduction](forced-late-original.test.mjs) holds the first response 100ms and reproduces the same `2 !== 1` failure: [before log](controlled-before-failure.log). It establishes the possible event order, not the unmeasured source of the original Linux delay.

The [test-only patch](test-only.patch) drains an explicit single-slot first stage, then requires one local HTTP 402, exactly one upstream forward, observed 20/reserved 20/remaining 0, no retry, and no third stage. A second controlled-clock regression delivers the refusal during final-slot drain and verifies that its stop reason survives.

Validation on Node v22.22.0: `node --test scripts/rpc/budget-gateway.test.mjs scripts/rpc/load-probe.test.mjs` passed **19 tests, 0 failures, 0 skips** in ~1.23 seconds ([log](after-full-suite.log)). A subsequent comment-only edit needed no rerun. Independent read-only security review found no blocker. No paid/public RPC, source build, stress loop, Python/runner/release change, or spending-limit relaxation occurred.

The forced reproduction uses explicit local checkout import paths and an injected fake upstream. It is historical evidence, not a runnable public-network probe. Source hashes and archive checksums are in [manifest.json](manifest.json).
