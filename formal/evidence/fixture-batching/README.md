# Linux fixture follow-up — September 26, 2026

Rust CI [run 36269984031](https://github.com/alfongj-com/engine-core/actions/runs/36269984031)
was cancelled for diagnosis after the ignored executor step stopped advancing.
Compilation had completed in 18.62 seconds; 57 tests started at 20:40:56 UTC. The
last completed test at 20:41:00 was `repeated_admission_preserves_pending_inflight_and_completed_state`.
The next sorted test was the 100,000-record retained-finality backlog fixture;
there was no further test output before cancellation at 20:50:51. Cancellation
is not a passing CI result or proof of the exact socket-level cause.

The original fixture sent all 100,002 commands in one async pipeline. It passed
locally on macOS, including the complete serial suite (57 tests in 4.73 seconds).
The test-only follow-up instead seeds the **same production cap** in batches of
1,000 commands, with a ten-second timeout per batch and an exact final ZCARD
assertion. Both cap and cap-minus-one allocation assertions remain unchanged.
This avoids large simultaneous request/response buffers and makes a stalled
setup fail with its batch offset. Runtime and model transitions are unchanged.

The corrected fixture passes the [exact serial CI command](executor-serial.log)
locally: **57/57 tests**, 4.83 seconds, against fresh disposable Redis. The
[original local serial run](original-local-serial.log) also passed, so local
passing alone does not diagnose the Linux buffering behavior. Test-only commit
`9537ac6ade9bd1acafe4d2e8bd1f2ceb919d1725` leaves measured runtime binaries unchanged.

The repeated [56-case formal gate](../throughput-review/report.json) passes on
the corrected test source, with all 65 reviewed hashes checked. Its runtime and
model transitions are unchanged from `025a3d1`. The complete executor regression
step, including the corrected fixture, passed on Linux in
[run 36271173982](https://github.com/alfongj-com/engine-core/actions/runs/36271173982).
This confirms the bounded setup runs there; it does not independently prove an
underlying Redis library defect.
