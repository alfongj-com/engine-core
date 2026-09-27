# Single-signer capacity campaign: chain matrix

**Status:** native Nitro development node prepared; capacity results are separate. **Reviewed:** 2026-09-26.
**Question:** what rate can one signer sustain on each tested chain, through
faults, and what happens when those individually qualified rates share Engine?

## Decision and scope

Report a capacity for a **named binary, workload, host, node/provider and finality
policy**, not for “EVM” or “all chains.” Separate admission, signed dispatch,
canonical inclusion and durable terminal rates. A successful drain after the
producer stops does not establish steady-state capacity.

Run existing local screens first, then genuine private OP/Nitro stacks, then
small funded public corroboration. Anvil timing profiles exercise Engine against
a development node; they do not qualify Ethereum consensus or a rollup sequencer.
Use the same Engine process, independent chain IDs, Redis namespace and recovery
journal for the eventual simultaneous-chain test. Separate processes/journals
would hide contention in the shared durable writer.

## What is available now

Observed host: macOS ARM64, 10 logical CPUs, 16 GiB RAM; root reports about 25 GiB
free disk before installation. Existing executables and the subsequently prepared
Nitro node are:

| Component | Available path/version | Scope |
|---|---|---|
| Anvil | `/tmp/engine-finality/anvil`, 1.8.1, commit `982849d3140c01fd3b72905759581a132df7aa98` | EVM development execution, configurable interval mining |
| Agave | `/tmp/engine-finality/solana-release/bin/solana-test-validator`, 4.2.2 | Actual Solana validator software, single local validator |
| Redis | `/tmp/redis-7.4.2/src/redis-server` | Existing isolated test service |
| Native Nitro | Lima 2.2.0 VZ guest, Docker 29.8.1, Nitro 3.11.3 ARM64 | L2-only `--dev` sequencer funded and smoke-tested; no parent chain |
| Full rollup tooling | Kurtosis and full OP/Nitro stacks not installed | Auxiliary image/platform and settlement smoke checks still required |
| Remote tooling | `gh` available; AWS/GCP/Azure CLIs and their conventional config files absent | Existing GitHub CI is available; no configured paid VM identified |

Do not run downloads, builds, extra nodes or model checks alongside timed loads.
The 16 GiB Mac may run a small native stack, but combined capacity is unqualified
until measured memory/disk headroom is known. Put nodes on separate hosts for an
Engine-only capacity number; co-location measures the whole host.

## Chain qualification matrix

| Profile | Genuine behavior available / required | What must be measured or faulted |
|---|---|---|
| **Solana** | Installed Agave test-validator now; Devnet later for network corroboration. A single validator omits leader rotation, network contention and distributed consensus. | Shared writable payer/account pressure, compute budget and priority fees; finalized terminal outcomes; stale/absent history, blockhash expiry and identical-wire retries. One signer is also one fee-payer hotspot. [Fees](https://solana.com/docs/core/fees), [networks](https://solana.com/docs/references/clusters). |
| **Ethereum L1** | Anvil with 12s blocks is a timing/execution surrogate. A private execution+consensus network is available through [ethereum-package](https://github.com/ethpandaops/ethereum-package); choose and record its preset/slot/epoch/gas settings. | Account-pool limits, gas per block, ordered nonce allocation, replacement caps, empty slots, pre-finality reorgs, finalized-head stalls. Ethereum slots are 12s; a slot can be empty. [Blocks](https://ethereum.org/developers/docs/blocks/). |
| **OP Stack** | Anvil `--network optimism` covers OP-specific execution. Genuine rollup qualification requires op-geth/op-reth, op-node, batcher and L1 consensus/DA through [optimism-package](https://github.com/ethpandaops/optimism-package). | Unsafe→safe→finalized progression, batcher outage and L1 reorg, fee accounting including execution+L1 data+configured operator fee, sequencer restart and nonce gaps. A short block interval alone supplies none of these. [Fees](https://docs.optimism.io/op-stack/transactions/fees), [finality](https://docs.optimism.io/op-stack/transactions/transaction-finality). |
| **Base / OP variants** | Same OP-family harness, separately pinned forks, fee parameters and preconfirmation configuration. Base-specific subblock/preconfirmation paths need its actual node/provider. | Keep preconfirmation distinct from settlement; do not transfer an OP test result to Base automatically. Custom gas tokens/DA, operator fees and upgrade activation change the workload. [Base finality](https://docs.base.org/specifications/transactions/transaction-finality). |
| **Arbitrum Nitro** | Full [nitro-testnode](https://github.com/OffchainLabs/nitro-testnode) supplies Nitro sequencing/batch posting and L1. Its default L1 is dev Geth; `--pos` supplies Prysm consensus. | Deliberately delay nonce N while submitting N+1. Nitro has a short operator-configured retry buffer, not Ethereum's long-lived pending pool; `pending` count equals `latest`. Preserve signed identity after nonce-high errors. Measure L1 posting fees, sequencer/batch-poster outages and parent finality. [Nonce semantics](https://docs.arbitrum.io/arbitrum-essentials/arbitrum-vs-ethereum/nonce-management), [fees](https://docs.arbitrum.io/how-arbitrum-works/deep-dives/gas-and-fees), [finality](https://docs.arbitrum.io/how-arbitrum-works/reference/finality-and-reorgs). |
| **Other families** | Not automatically covered: Arbitrum AnyTrust/Orbit custom DA or gas tokens; BFT EVM chains; ZK rollups and proof publication; ERC-4337/paymasters. | Add a native node/provider profile, finality contract and fee reconciliation before naming a capacity. Engine's bundled EIP-7702 path is disabled; this campaign cannot qualify it. |

### Deployment sizing, not network maxima

These are necessary planning constraints; satisfying them does not guarantee
inclusion or capacity. Read the actual chain parameters and measure latency.

| Constraint | Formula / concrete implication |
|---|---|
| Ethereum block space | Required gas per block ≈ `rate × block interval × gas/transaction`. At 50/s and 12s: **12.6M gas** for 21k transfers, **60M** for 100k calls. The signer competes for that space. Do not assume the changing block gas limit, or that full blocks remain affordable: Ethereum's fee target is half the limit. [Gas and fees](https://ethereum.org/developers/docs/gas/) |
| EOA nonce window | Required inflight window ≳ `rate × observed inclusion latency`, plus gap/retry margin. A full 12s delay at 50/s needs at least **600** slots, versus Engine's default 50; the configurable ceiling is 4,096. Provider/account-pool limits can be tighter. |
| OP/Base retained finality | Retained hashes ≈ `rate × (finality lag + scan delay + outage margin) × replacement fanout`. At 50/s, OP's typical 15–30min requires **45k–90k** before margins. At 30min, the 100k cap leaves only **200s** extra at one hash/intent; at 100/s it requires **180k**, exceeding this implementation's cap. Other OP chains can post batches much less often. Fast local depth finality does not test this bound. [OP finality](https://docs.optimism.io/op-stack/transactions/transaction-finality) |
| Nitro native development profile | Measures native sequencing/RPC behavior with the defaults below, **not L1-backed finalization throughput**. The observed ~1.29M transfer estimate includes the native fee model; do not interpret it as 1.29M units of ordinary EVM execution or replace it with 21k. Real deployment must size parent-finality retention and posting fees separately. [Nitro fees](https://docs.arbitrum.io/how-arbitrum-works/deep-dives/gas-and-fees) |
| Solana inclusion window / fees | Recent-blockhash validity is finite; use the returned `lastValidBlockHeight`, not a fixed elapsed-time deadline. Lagging RPCs shorten usable time. Engine parks unresolved expired attempts; overload may therefore require reconciliation rather than automatic fresh signatures. One-signature base fees alone cost **0.9 SOL/hour at 50/s**, or **1.8 SOL/hour at 100/s**, before priority fees, rent and transfers. [Expiration](https://solana.com/developers/cookbook/transactions/confirmation), [fees](https://solana.com/docs/core/fees) |

All chains share Engine's durable writer, host and admission resources. Neither
the gas arithmetic nor isolated passing rates establish a simultaneous capacity
vector. Provider quotas, writable-account contention, polling costs and disk
growth remain deployment-specific constraints.

### Anvil's OP mode is useful but limited

The installed help exposes `--network optimism`, not the older `--optimism` flag.
Its [pinned backend](https://github.com/foundry-rs/foundry/blob/982849d3140c01fd3b72905759581a132df7aa98/crates/anvil/src/eth/backend/mem/optimism.rs)
uses `alloy_op_evm`/`op_revm`, and its [OP tests](https://github.com/foundry-rs/foundry/blob/982849d3140c01fd3b72905759581a132df7aa98/crates/anvil/tests/it/optimism.rs)
cover OP transaction handling. This is stronger than changing an ordinary Anvil
chain ID. It still provides no op-node derivation, live L1 attributes/batch
publication or genuine rollup finality. Label its result **Anvil OP execution
profile**; pin the OP hardfork and genesis/predeploy configuration, and verify
which fee components the fixture actually exercises.

Using an op-geth executable with plain `--dev` would not fix this scope gap:
the [pinned developer genesis](https://github.com/ethereum-optimism/op-geth/blob/v1.101702.2/core/genesis.go)
selects `AllDevChainProtocolChanges`, whose [configuration](https://github.com/ethereum-optimism/op-geth/blob/v1.101702.2/params/config.go)
does not enable Optimism. A real OP genesis and block builder are needed.
For geth-family pools, record `accountqueue`, `accountslots`, global limits and
any local-account exemption. `accountslots` is a **guaranteed minimum**, not a
hard per-signer executable cap; `accountqueue` bounds non-executable future
nonces. A nonce gap can therefore bind a high-rate signer even with ample block
gas. [Flag definitions](https://github.com/ethereum-optimism/op-geth/blob/v1.101702.2/cmd/utils/flags.go).

## Executed native Nitro setup

The official Lima 2.2.0 Darwin ARM64 archive was checksum-verified and extracted
under `/tmp/engine-capacity-tools/`. Its named `engine-nitro` VZ guest has **4
vCPUs, 4 GiB RAM, 12 GiB disk and no host-directory mounts**. Docker was installed
inside that guest. No host administrator password, Rosetta or OS-security change
was needed. After installation, the host had about **15 GiB free** and the guest
3.2 GiB free; this is insufficient headroom to casually add a full second stack.

| Item | Observed configuration |
|---|---|
| Native image | `offchainlabs/nitro-node@sha256:de2cc1f67c45a7ff75eaf8ee8e46639fbf6a897ccd8328e5f918e4b308bf71a4` |
| Client | `nitro/v3.11.3-beb2108/linux-arm64/go1.25.12` |
| RPC / chain | `http://127.0.0.1:18547`, chain ID **412346** |
| Container | `engine-nitro-dev`, `--dev --http.addr 0.0.0.0 --http.api net,web3,eth`, guest port published only on loopback |
| Sequencer defaults | Minimum block spacing 250ms; nonce-failure buffer 1s / 1,024 transactions; pending queue 1,024 / 12s timeout. No overrides. |
| Synthetic funds | Key-1 address `0x7e5f4552091a69125d5dfcb7b8c2659029395bdf` received **900 local ETH**, nonce still zero after setup |
| Idle footprint | Nitro about 103 MiB and <1% CPU in one snapshot; this is not a loaded resource measurement |

Native preflight for key-1 → `0x1111111111111111111111111111111111111111`,
1 wei and empty calldata returned **1,294,047 gas**, consistently with plain,
legacy-price and EIP-1559 estimate requests. The setup transfer actually used
997,000 gas. **Do not hardcode 21,000** for this profile: omit `gasLimit` so Engine
estimates and applies its existing 20% + 50,000 buffer. Observed base fee/gas
price was 0.1 gwei, priority fee zero, and `eth_feeHistory` succeeded. These are
fixture observations, not network-wide constants. No key-1 transaction was
broadcast during preflight; raw responses are in
`/tmp/engine-capacity-nitro-preflight.json`.

This is native **Nitro execution/sequencing**, with no parent L1, posting service,
DA or settlement. `finalized` returned `-32000: finalized block not found`.
Use an explicitly configured depth policy for this profile, never an implicit
fallback. Blocks are activity-driven: after admission stops, a separate public
development account must send a few self-transfers to advance trailing depth
checkpoints. Do not count those as Engine effects. The startup script's optional
chain-owner/L1-fee-zero/Stylus deployments were **not** applied; the native fee
estimator is retained, but its L1 inputs still do not come from a live parent.

Root-controlled local helper `/tmp/engine-capacity-tools/nitro-control.sh` offers
`start`, `stop`, `status`, and `ticker N` (1–60 self-transfers, once per second).
Two ticker smoke transfers advanced blocks 1→3 without changing key-1 effects.
For a campaign that may drain longer, use the no-argument executable
`/tmp/engine-capacity-tools/nitro-drain-hook.py`: it sends once per second until
the campaign terminates it, with a hard 600-second deadline. It validates chain
412346, logs its separate transaction hashes, and terminates/reaps an active
`cast` child on SIGTERM/SIGINT. A live smoke advanced block 3→4 while key-1
remained at nonce zero / 900 ETH; a blocked-child fixture verified SIGTERM
cleanup. Run either ticker only during drain and account for its RPCs separately.
Stop shuts down the container and VM. **Restarting the same preserved container
does not inherently reset its chain:** the exact running Nitro commit expands
`--dev` to the fixed `/tmp/dev-test` database path and normally reopens existing
state. The previous blanket reset statement was incorrect.
[Dev flags](https://github.com/OffchainLabs/nitro/blob/beb21087772a2668a1f13847697e1406305e4d89/cmd/util/confighelpers/configuration.go#L196-L235),
[existing-database initialization](https://github.com/OffchainLabs/nitro/blob/beb21087772a2668a1f13847697e1406305e4d89/cmd/nitro/init/init.go#L1016-L1047).

The data lives in this container's writable layer; deleting/recreating the
container or losing that directory can lose the chain. After an unclean stop,
back up the stopped state, repair the infrastructure, and verify the original
genesis, durable Engine checkpoint, signer nonce and transaction evidence before
resuming. Keep the existing Engine journal; a fresh journal is not a recovery
procedure. The helper's `start` may fund a zero-balance key-1 account and retains
an obsolete reset warning, so it must not be used for incident recovery. No
restart or crash-integrity guarantee follows solely from the persistence path.

To preserve this chain while other isolated profiles run:

```sh
/tmp/engine-capacity-tools/lima/bin/limactl shell engine-nitro docker pause engine-nitro-dev
/tmp/engine-capacity-tools/lima/bin/limactl shell engine-nitro docker unpause engine-nitro-dev
```

The container was paused after setup. The idle guest still reserves memory;
record that presence in co-located measurements. Safe local evidence is
`/tmp/engine-capacity-nitro-setup.json`, with funding receipt `/tmp/engine-capacity-nitro-funding.json` and CLI help
`/tmp/engine-capacity-tools/nitro-help.txt`. [Official dev-node recipe](https://github.com/OffchainLabs/nitro-devnode),
[Lima installation](https://lima-vm.io/docs/installation/).

## Native stack launch plan

These **full-stack** commands are launch recipes, not executed qualification.
The guest container runtime above is available; [Kurtosis supports macOS/Apple Silicon](https://docs.kurtosis.com/install/).
Use named, task-owned directories/volumes. Do not run global cleanup commands.

**OP:** start one minimal package before adding observers or other chains.

```sh
git clone https://github.com/ethpandaops/optimism-package.git
cd optimism-package
git checkout 7bef190d7c0b9f619438ed08b17bd5e5f51e72ff
kurtosis run --enclave engine-op-capacity . --args-file network_params.yaml
```

Record generated service endpoints and image digests, and fund an isolated Engine
signer using the local development allocation. Disable optional observability
services if needed, recording that change. The package defaults to a **minimal
Ethereum preset**, not mainnet timing. First prove finalized receipts advance;
then record the chosen production-like timing/gas parameters for the next run.
The [OP developer guide](https://devdocs.optimism.io/kurtosis-devnet/) uses the
same Docker/Kurtosis approach and warns that the tooling is actively changing.

**Nitro:** use the released environment, with its exact revision pinned because
the release branch can move.

```sh
git clone --branch release https://github.com/OffchainLabs/nitro-testnode.git
cd nitro-testnode
git checkout 72ac4bb1f07fadc108448ad5fae01ea2e044fe69
git submodule update --init --recursive
./test-node.bash --init --pos --detach
```

The [launch script](https://github.com/OffchainLabs/nitro-testnode/blob/72ac4bb1f07fadc108448ad5fae01ea2e044fe69/test-node.bash)
provides `--pos` and defaults to a combined sequencer/batch-poster/staker. Split
roles for batcher-only and sequencer-only fault injection; test that topology
before the measured run. Its current Nitro image `v3.11.3-beb2108` has both
Linux AMD64 and ARM64 manifests in the [publisher's registry](https://hub.docker.com/v2/repositories/offchainlabs/nitro-node/tags/v3.11.3-beb2108).
This verifies the Nitro image, **not every auxiliary image or complete ARM64
stack startup**. A simple [nitro-devnode](https://github.com/OffchainLabs/nitro-devnode)
is smaller but resets chain state on restart; it cannot substitute for this
recovery/parent-settlement test.

**Fallback:** use Linux CI for a one-stack smoke test, then a dedicated Linux
host for isolated native capacity. Public-repository Ubuntu runners currently
provide 4 CPUs, 16 GB RAM and 14 GB SSD; their shared hardware/limited disk make
absolute capacity claims weak. [Runner specifications](https://docs.github.com/en/actions/reference/runners/github-hosted-runners).
A dedicated 32 GiB machine with sufficient local SSD is a planning starting
point for multi-stack work, not a verified minimum. No remote VM was provisioned
and no paid infrastructure was purchased during this review.

## Public readiness and remaining budget

Private files exist under `~/.config/engine-core/`: EVM key/address, Solana
keypair/address, signing token, provider key, and budget/metrics. Only public
addresses and numeric budget metadata were read; no signing/provider secret was
opened or sent anywhere.

Six public read-only RPC calls on **2026-09-26 21:52 UTC** produced:

| Network | Balance / evidence |
|---|---|
| OP Sepolia | **0.009996935557456290 ETH**, current chain ID + balance from `sepolia.optimism.io` |
| Solana Devnet | **0.007849243 SOL**, finalized balance at slot 504553952 from `api.devnet.solana.com` |
| Ethereum Sepolia | Public identity request failed HTTP; current balance unverified. Last repository report: **0.008716341658908942 ETH**. |
| Arbitrum Sepolia | Public identity request failed HTTP; current balance unverified. Last report: **0.009615212427201922 ETH**. |
| Base Sepolia | Public identity request failed HTTP; current balance unverified. Last report: **0.011989726211696918 ETH**. |

Earlier balances come from `docs/baselines/testnet-*-final-source.json`; they are
not fresh readings. Safe raw results are in `/tmp/engine-capacity-chain-readiness.json`.
No public broadcasts, retries, faucet claims or paid RPC calls were made for this
review. The local Nitro funding/ticker transactions above use synthetic funds.
At the current 5,000-lamport base fee, the Solana balance covers at most **1,569
single-signature fees**, before rent/transfers/priority fees. Even 50/s for six
minutes needs 0.09 SOL for those fees alone. Requote before spending; existing
funding supports small public corroboration, not the maximum-capacity search.

The loopback paid gateway was down. Its retained file shows ceiling 2,000,000,
reserved 374,000 and observed 370,084 calls. Unused reservation is forfeited, so
**1,626,000 additional ordinary calls / $9.756 estimated guard headroom** remain
at $6/M. This is the campaign's local cap, not the provider's account balance;
other users of the key are outside it. Do not reset counters. Reserve at least
25% of the remaining calls for reconciliation before any new paid ramp. The
older [RPC plan](rpc-test-plan.md) contains an initial call model; current
finality and immediate-progress scheduling add/change calls, so use measured
per-method deltas instead of treating `4T + W/P` as a present exact formula.

## Sustainable rate, chaos and simultaneous chains

1. **Find a bracket.** Fix signer, transaction shape, fees, durability and worker
   limits. Increase offered rate in short screens until unsigned backlog grows,
   dispatch cannot track arrivals, errors rise or resource limits bind. Refine
   the passing/failing interval. Record declined admissions; do not hide them
   by reporting only accepted transactions.
2. **Confirm the candidate.** Repeat near the boundary, then soak for at least
   two observed finality-lag windows. Require stable unsigned/borrowed backlog,
   a bounded steady retained-finality backlog, and terminal rate tracking input
   after warm-up. Preserve exact effects, fees, nonce/signature uniqueness and
   final drain. Report the tested duration and rate interval; no finite run
   establishes an unlimited maximum. OP's usual 15–30min public finality makes
   a six-minute drain test insufficient for this claim.
3. **Inject faults near that rate.** Kill Engine; crash/restart Redis under the
   chosen persistence policy; lose an accepted send response; temporarily
   withhold history or return 429/timeout; stop a real sequencer/batcher; perform
   a native pre-finality reorg where supported. Pause new intake on ambiguous
   evidence and reconcile it. Redis-loss quarantine is a safe stop, not automatic
   recovery. Artificial Anvil snapshots/RPC faults remain labeled simulations.
4. **Run the individual rates together.** Offer the same per-chain rates to one
   Engine and journal while retaining per-chain identities and metrics. Expect
   interference: the durable writer, admission permits, CPU and provider budget
   are shared. If any backlog grows, report the failed rate vector and reduce
   jointly to find a sustainable vector; never assert that isolated maxima add.

Before each run calculate retained demand `rate × (finality lag + outage margin)
× replacement fanout` against the 100k per-signer hash ceiling, and size the
nonce window for inclusion delay (configured maximum 4,096). Set disk, RPC,
transaction-count, fee and runtime stops before admission starts. Save binary,
image, genesis, hardfork and configuration hashes with each result. See
[throughput constraints](throughput-50tps.md) and
[finality/recovery contract](finality-and-recovery.md) for implementation limits.
