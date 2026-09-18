# Public transaction qualification

September 17, 2026. These tests submit actual transactions through Engine, Redis,
the local signer, and the budgeted dRPC gateway. Each run retains its request IDs,
signed transactions and Redis AOF privately for recovery.

## What is checked

- Every admitted intent has a distinct successful receipt, the intended recipient
  and amount, and an independently queried transaction identity.
- Sender and recipient balances reconcile, including chain fees. EVM nonces form
  the exact expected contiguous range. Receipt blocks are checked for canonical
  membership at the end of the experiment.
- Repeating the original request IDs creates no additional broadcasts or effects.
- Fault runs discard successful send responses and restart Engine. EVM runs also
  kill Redis and restore its AOF. These are process-crash tests, not host power
  loss or replication failover tests.

## EVM results

Initial binaries use runtime source `3aad56d`, built at documentation commit
`6f24e89`. Reports record the exact commit and binary SHA-256. One funded signer
is used per chain, with a fresh recipient per run and transfers of one wei.

| Network | Initial run | Lost-response/crash run | Higher offered rates |
| --- | --- | --- | --- |
| Ethereum Sepolia | [10 at 1/s](testnet-sepolia-write.json), pass | [4 transfers](testnet-sepolia-crash.json), pass | [20 at 5/s](testnet-11155111-5tps.json), [20 at 10/s](testnet-sepolia-10tps.json), pass |
| Arbitrum Sepolia | [10 at 1/s](testnet-421614-write.json), pass | [4 transfers](testnet-arbitrum-crash.json), pass | [20 at 5/s](testnet-421614-5tps.json), [40 at 20/s](testnet-421614-20tps.json), pass |
| OP Sepolia | [10 original transfers reconciled](testnet-optimism-reconciled.json), pass after accounting correction | [4 transfers](testnet-optimism-crash.json), pass | [20 at 5/s](testnet-11155420-5tps.json), [40 at 20/s](testnet-11155420-20tps.json), pass |
| Base Sepolia | [10 at 1/s](testnet-84532-write.json), pass | [4 transfers](testnet-base-crash.json), pass | [20 at 5/s](testnet-84532-5tps.json), [40 at 20/s](testnet-84532-20tps.json), pass |

All passing runs have zero duplicate effects, zero duplicate-triggered sends and
zero forwarding-policy violations. The EVM crash runs recovered by querying
receipts; they did not need another raw broadcast. They therefore establish
reconciliation after response loss, not public identical-wire retransmission.

The initial [OP Sepolia report](testnet-11155420-write.json) deliberately retains
its failed result. All ten transfers succeeded, but the harness compared total
fees against an execution-only allocation. Receipts and balance changes explain
the discrepancy exactly: 210,052,500,000 wei execution fees plus 174,355,805,230
wei L1 posting fees. The harness now reserves separately for OP Stack chain fees.
That reserve is an experiment allocation, not a protocol-enforced fee cap.
The [reconciliation](testnet-optimism-reconciled.json) restored the original AOF,
refused every outbound broadcast, repeated the original ten IDs, and verified
unchanged balances and nonce. It passed with zero attempted broadcasts.

## Solana results

The [initial Devnet run](testnet-solana-write.json) finalized ten transfers with
zero duplicate effects, zero duplicate-ID broadcasts and exact balance checks.
Fees totaled 50,000 lamports. The first transfer funded a fresh account with the
live rent-exempt minimum, 650,240 lamports; the next nine transferred one lamport
each. Finalizing that first transfer is a separate setup phase before paced work.

The [crash run](testnet-solana-crash.json) also finalized ten unique transfers.
The nine post-setup intents each retransmitted their original signed bytes after
Engine restart. Across the scenario, 24 accepted-send responses were discarded
and 36 retransmissions occurred, with no duplicate effects. Redis stayed alive;
this run does not establish Solana recovery across Redis loss. Both runs retain
finalized receipt identities and exact sender/recipient/fee accounting.

The initial and crash runs used 123 and 152 paid RPC calls respectively, including
verification. The initial run made 66 signature-status calls for ten transactions.
This is a concrete polling cost to reduce before large sustained Solana workloads.

The [20-transfer run](testnet-solana-5tps.json) also passed. After rent setup, its
19 paced admissions achieved 5.009 requests/s. All twenty transactions finalized,
with zero duplicates, 100,000 lamports of fees, 150 total RPC calls and 44
signature-status reads. Finalized-result observation took 6.363–12.077 seconds
per intent. Its 48.465-second overall duration includes setup and twenty
sequential duplicate-request checks; it is not the transaction throughput window.

## Measurement limits

Rates in the table describe HTTP requests offered to Engine, not sustained chain
throughput. Each burst has only 10–40 transactions. The 40-transfer Arbitrum and
Base bursts completed in 4.152 and 5.168 seconds respectively, including admission
time. Ethereum's 20-transfer batches completed in 16.525–45.994 seconds. Inclusion
timing varies; these samples do not locate a capacity limit.

Gas and EIP-1559 fees are precomputed once per run. This intentionally tests
submission/recovery without repeating gas/fee estimation for every transaction.
It understates request demand for the automatic-estimation path. RPC totals also
include the harness's independent verification calls. The implementation uses a
debug Engine, one EOA worker and Redis `appendfsync=always` on a developer laptop.

The forwarding guard inspects signed EIP-1559 transactions, rejects changed
destinations/amounts/nonces, caps execution gas and fees, and permits only the
original signed bytes for a nonce. It durably reserves broadcasts before dispatch,
including ambiguous failures. These protections belong to the experiment;
production behavior needs its own tested policy.

EVM success here means observed inclusion and execution success. It does not
establish finality, reorg handling, sequencer-outage recovery, multiple-provider
agreement, multi-wallet capacity, or sustained production throughput.
