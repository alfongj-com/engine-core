# Budgeted RPC screening

The gateway owns the provider credential and a durable campaign budget. The probe sends read-only requests to its literal loopback address. It never calls a provider directly.

## Run

Keep the existing state directory and `rpc-budget.json` across restarts. `drpc-key` lives in that private directory, outside the repository. The gateway refuses a second process using the same directory. If a process crashes, inspect the lock PID before removing the stale lock; unused reserved calls are forfeited.

```sh
node scripts/rpc/budget-gateway.mjs

# Prints the plan only; makes no requests.
node scripts/rpc/load-probe.mjs --chain 11155111

# Explicit execution saves a new report; refuses to overwrite an existing file.
node scripts/rpc/load-probe.mjs --chain 11155111 --execute \
  --output docs/baselines/rpc-sepolia.json
```

Supported routes: `11155111` (Sepolia), `421614` (Arbitrum Sepolia), `11155420` (OP Sepolia), `84532` (Base Sepolia), `solana-devnet`.

Default stages: 10, 50, 200, 500, and 1,000 requests/second for five seconds each, with one second between stages. This schedules 8,800 calls plus at most 32 discovery calls: at most $0.052992 per network under the conservative $6/million-call accounting. Five networks cost at most $0.26496 for this probe. No batching or automatic retries. SIGINT stops new admissions and drains requests already sent, each bounded by a timeout.

Options:

| Setting | Default / bound |
| --- | --- |
| `--rates 10,50,200` and `--seconds 5` | Five default rates above; maximum 5,000 RPS |
| `--rates 100 --count 500` | Explicit finite count at one rate |
| `--concurrency 256` | Maximum 1,024; excess scheduled work is dropped and counted locally |
| `--timeout-ms 20000` | Maximum 60,000 |
| `--reserve 10000` | Leave this many campaign calls available before starting |
| `--max-lag-ms 100` | Drop late schedule slots instead of producing catch-up bursts |
| `--pause-ms 1000` | Pause between completed stages |
| `--gateway http://127.0.0.1:8788` | Literal loopback HTTP only; no credentials/path/query |

Total planned calls cannot exceed 100,000 per invocation. The gateway remains the authoritative shared ceiling if another caller consumes budget after the probe precheck. A local budget rejection stops later admissions and stages. Reports distinguish local concurrency/scheduling drops, local gateway admission rejections, upstream HTTP errors, JSON-RPC errors, and transport failures. Each report includes per-method latency, aggregate error codes, CPU/event-loop measurements, and before/after budget counters. URLs, request parameters, and response text are omitted.

Gateway environment settings: `ENGINE_TEST_STATE_DIR` (default `~/.config/engine-core`), `ENGINE_RPC_GATEWAY_PORT` (8788), `ENGINE_RPC_MAX_CALLS` (2,000,000, cannot exceed this ceiling), `ENGINE_RPC_MAX_INFLIGHT` (256, maximum 1,024). A deliberate 512/1,024 concurrency follow-up must raise both the gateway setting and probe option. Restarting the gateway preserves the budget. Shutdown waits for admitted requests before persisting final counters; forced shutdown aborts remaining requests after 20 seconds.

## What the probe measures

EVM: equal weights of fee history, zero-value transfer gas estimation, latest nonce for recent senders, and receipts for recent transaction hashes. Discovery examines at most eight recent blocks and three sender balances. A sender with at least 0.001 ETH is preferred for estimation; if none is available, the report explicitly labels a zero-sender/zero-gas-price fallback. This fallback may be rejected by provider policy.

Solana: equal weights of confirmed blockhash, recent priority fees, historical signature status, and a recent transaction. Discovery requests up to eight recent System Program signatures and verifies that a transaction can be fetched.

These are short **read-only RPC responsiveness** measurements. They do not establish transaction submission capacity, chain inclusion throughput, sustainable capacity, or the Engine's end-to-end throughput. Small repeated target sets can benefit from provider caching. The method recipe and target-set hash are recorded; targets are rediscovered on each invocation. Probe CPU excludes the gateway and provider. Any local admission drops prevent interpreting that stage as an upstream capacity limit. `success_null` is recorded separately from populated results; HTTP 200 alone does not mean useful confirmation data was returned.

Budget pricing was checked against [dRPC compute-unit pricing](https://drpc.org/docs/pricing/compute-units) and its [getSignaturesForAddress method documentation](https://drpc.org/docs/solana-api/transactionsinfo/getSignaturesForAddress). Calls are conservatively counted at 20 CU ($0.000006), including any informational methods that may actually be free. Provider billing remains authoritative; the gateway limits requests dispatched by this campaign.

## Local verification

```sh
node --test scripts/rpc/budget-gateway.test.mjs scripts/rpc/load-probe.test.mjs
```

Tests create fresh loopback stubs and temporary budgets. They do not use the running campaign gateway or make paid requests.
