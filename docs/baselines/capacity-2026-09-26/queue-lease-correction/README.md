# Queue lease correction

Earlier capacity campaigns hardcoded a 10-second queue lease. Both service configuration layers default to 600 seconds. This matters under overload: the shared 235 TPS run logged 25 queue lease reaps, 15 wallet takeovers and 15 corresponding lost-lock errors. All accepted transactions eventually reconciled, but that run does not measure the default lease configuration.

The harness now defaults to 600 seconds and accepts an explicit bounded `--queue-lease-seconds` override. The actual environment value remains recorded in each report. A short lease remains available for deliberately labeled stress tests. No Engine code, ownership fence or durability setting changed.

All 147 integrated Python tests passed, including three new checks of production configuration agreement, the actual init/Engine environment and report, explicit override isolation from ambient settings, and invalid values. Actual Ethereum/OP snapshot restore tests also passed. The archived tested source includes the final readable CLI help text.

Single-profile OP60, Solana65 and updated Native50 had no lease expiry, takeover or lost-lock events under the older 10-second setting. EVM60 had one expiry without a wallet takeover or loss. Those results remain labeled with their actual configuration. Shared high/low runs with the 600-second lease are a new cohort. Longer leases can also delay crash reclamation; fault tests must report that latency and keep bounded drain time sufficient.
