# Formal verification

## Decision

Use **TLA+/TLC for distributed state transitions** and **Kani for the actual Rust
fee arithmetic**. The models explore scheduling and failure combinations that
are difficult to exercise reliably in tests. Kani imports the same dependency-free
fee module used by the executor and checks machine-integer assertions.

[TLC](https://docs.tlapl.us/using:tlc:start) explores finite state spaces.
[Kani](https://model-checking.github.io/kani/) checks Rust assertions, arithmetic
and memory safety, but [does not model concurrent programs](https://model-checking.github.io/kani/limitations.html).
[Lean](https://lean-lang.org/doc/reference/latest/Introduction/) is a good fit for
inductive mathematical proofs. For this repository, proving a second handwritten
implementation in Lean would still leave the Rust/Redis correspondence to prove.
We chose direct Rust checks and explicit protocol models first. This is a project
decision, not a claim that Lean cannot verify systems code.

## What is checked

| Area | Properties | Evidence and limits |
|---|---|---|
| Queue ownership | Stale leases cannot commit; aborted EXEC is not success; physical WATCH sessions cannot interfere; ID reuse preserves live data and cancellation | [Queue model and Redis regressions](queue.md) |
| EVM recovery | One intent per active nonce; crash retains attempts; absent receipts cannot cause a second execution; reverted receipt is failure | [EVM model](eoa.md) |
| EVM allocator cleanup | Old retained receipts cannot rewind allocation below observed consumed count; outstanding reservations stay below the next nonce | [Allocator model](nonce-allocator.md) |
| Solana recovery | Persist before send; retries keep signed identity; expiry/absence preserves evidence; bounded send/check budgets; fenced terminal cleanup | [Solana model](solana.md) |
| Redis admission projection | Immutable request per retained Redis ID; atomic identity/queue admission; active identity survives cancellation; finite Redis retention boundary | [Admission model](admission.md); the server's independent journal adds longer-lived identity protection |
| Finality | Provisional inclusion, orphaning and re-inclusion; both success and revert wait for canonical finality evidence | [Finality model](finality.md); honest RPC/stable consensus assumptions, explicit depth and catastrophic boundaries |
| Redis disaster recovery | Durable attempt before authorization; immutable global replay binding; separate SQLite/Redis commits; restart/rollback halt; offline quarantine and original-payload retries | [Recovery authority model](disaster-recovery.md); one host/owner, retained local authority, not SQLite/filesystem refinement |
| Fee arithmetic | Supplied caps, priority/total ordering, nondecrease when permitted, overflow-safe computation | [Production Rust proofs](fees.md) |

Every model has negative checks. Fault configurations must fail the **named
property** and produce a counterexample. Syntax errors, timeouts and out-of-memory
failures never count as successful verification. Boundary configurations instead
disprove claims outside each module's assumptions: consensus finality surviving
a catastrophic rollback, truthful evidence from a dishonest RPC, the old
Redis-only projection surviving data loss, that projection deduplicating after
its retention expires, or progress during permanent outages. The new independent
journal addresses Redis loss and replay after Redis TTL expiry **while the
authoritative local ledger is retained**. Rolling back/copying that authority
remains an explicit failure boundary. These separate models are not a
machine-checked proof of their composition.

The [recorded run and raw evidence](evidence/README.md) identify the exact source,
state counts, counterexamples and implementation regressions for that run. The
manifest contains **56 configurations**: the previous 52, three allocator
checks and one terminal-attribution mutation. The prior 52-case run remains
historical evidence for its recorded source. The [latest runtime check](evidence/progress-scheduling/README.md)
passes all 56 expected outcomes at `f309177`, with 66 reviewed source hashes
and 3,164,046 summed positive states. It also tests real Redis scheduling after
useful EOA progress. The [preceding report](evidence/throughput-review/report.json)
retains its `9537ac6` scope. These finite models do not prove a 50 TPS target or
compose automatically into a whole-system proof.

## Run locally

Requires Python 3.9+, Java 11+, and Kani 0.68.0 for the Rust checks. TLC 1.7.4 is
downloaded from its official release and checked against a pinned SHA-256. Set
`JAVA` or `TLA_JAR` to use existing installations; the jar digest is still enforced.

```sh
python3 -m unittest discover -s formal -p 'test_*.py' -v
python3 formal/check.py
python3 formal/fees/check.py
```

Use `python3 formal/check.py --only EoaRecovery` for one model family. Results and
counterexample traces go to ignored `formal/results/`. The manifest
[`models.json`](models.json) defines the full required set; an unselected model
is not checked. CI runs both formal suites and the real Redis regressions. It needs no RPC service, wallets or paid API key.

## Keeping models tied to code

1. Each model document maps actions to source and implementation tests.
2. [`source-map.json`](source-map.json) fingerprints the mapped code. Changing it
   fails the model gate until a reviewer updates the model/assumptions/tests and
   records the new hashes. Hashes are a **review tripwire, not a refinement proof**.
3. Fee harnesses compile the production module directly; no copied fee algorithm
   stands in for it. Independent mathematical assertions provide the oracle.
4. Counterexamples that expose current bugs become real Redis/Rust regressions.

When changing a mapped file, explain the affected transition in the PR, update
its model or explain why the abstraction still applies, run the relevant formal
and implementation checks, then update its SHA-256 in `source-map.json` using
`shasum -a 256 path/to/file`. Do not refresh hashes merely to clear a gate.

## Scope of the result

**This does not prove the whole service correct.** TLC exhausts the configurations
in the manifest, using 64-bit state fingerprints with the collision estimates
recorded in its logs. It is not an unbounded inductive theorem. The models have
small, explicit populations and budgets; no state constraint silently discards
violations. Only the EVM progress configuration checks liveness, under stated
fairness and availability assumptions. Safety models intentionally disable
deadlock reporting because terminal, cancelled, parked and bounded states are
valid endpoints.

Redis Lua and MULTI/EXEC are modeled as successful atomic state transitions where
stated. [Redis command errors do not roll back earlier writes](https://redis.io/docs/latest/develop/using-commands/transactions/).
No model turns that into a rollback guarantee. Retained storage, trustworthy
matching receipts, collision-resistant identities and chain replay rules are
assumptions with explicit failure-boundary checks where feasible. DisasterRecovery
separates atomic SQLite commit from atomic Redis CAS; it deliberately permits a
crash between them. Successful durable filesystem writes and exclusive local
file locking are trusted components, not proved by TLC.

The [coverage map](coverage.md) lists the remaining work. Existing integration,
fault-injection and public-chain evidence in [verification](../docs/verification.md)
remains necessary; these models cannot replace it.
