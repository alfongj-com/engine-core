# Linux harness qualification

All **163 tests passed, zero skips**, on Linux ARM64 (13.838s) and macOS
(12.915s) after a test-only portability correction. Both runs include actual
Ethereum/OP Anvil restores and all eight SQLite reader regressions.

The first Linux run is retained: 160 passed, three errors. Two configuration
tests implicitly required an existing release Engine binary. They now supply a
temporary placeholder file because argument validation never executes Engine.
The third error was a fixture-copy omission: the production-lease test needs two
tracked configuration YAMLs, which were then copied unchanged. This was not an
Engine failure. No application, benchmark or durability setting changed.

The isolated guest used Python 3.14.4, SQLite 3.46.1 and official Foundry 1.8.1
Linux ARM64. The archive digest and actual tool/source hashes are recorded.
All 37 code/config hashes matched their host sources. The existing Native
container remained paused with the same ID/PID; no test processes remained.
Added guest disk usage was 234.4 MB, below the predeclared 1 GiB bound.

`first-result.json` / `first-tests.log` retain the failed attempt. `result.json`
and `tests.log` contain the successful repeat; `macos-tests.log` is the host
repeat. Input-source hashes name repository files, not files in this archive.
`manifest.json` pins the archive itself. This is local Linux verification; the
additional GitHub Actions steps still require applying the pending CI patch.
