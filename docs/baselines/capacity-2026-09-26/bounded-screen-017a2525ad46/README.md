# Bounded screens and Nitro disk incident: 017a2525ad46

**Neither completed screen establishes a selected maximum.** EVM 55 TPS showed
persistent backlog growth. OP execution at 55 TPS remains a repeat candidate,
with mixed phase effects. Both completed screens reconciled all 19,800 admitted
intents to exactly one canonical, depth-qualified effect each. Native Nitro at
40 TPS was interrupted by a full VM disk; recovery of its original 14,400-ID
campaign was **not yet complete when this archive was recorded**. Solana did not
start in this sequence.

## Completed screens

| Local profile | Offered / admitted | Post-warmup attempted / terminal TPS | Drain reached | Interpretation |
|---|---:|---:|---:|---|
| Anvil, 12s blocks, 55 TPS | 19,800 / 19,800 | 54.869 / 53.069 | 400.313s | Persistent growth; test 50 next |
| Anvil OP execution, 2s blocks, 55 TPS | 19,800 / 19,800 | 54.410 / 55.410 | 370.126s | Strict result unconfirmed; repeat 55 before promotion |

Rates use observed sample times around 120–360s. Drain is elapsed time from the
start of arrivals, including the 360s offered phase. All requests returned 202;
there were no client drops or HTTP errors. Maximum HTTP start lateness was
26.737ms (EVM) / 34.189ms (OP), below the fixed 100ms limit.

EVM terminal backlog means rose across all four final 60s groups:
1,998, 2,090, 2,140, 2,258. Every nominal 180s phase comparison grew. OP's unsigned
endpoint increased by 141, but its grouped mean trend was slightly negative;
terminal backlog fell by 99 and phase comparisons had mixed signs. These
observations do not override either original strict verdict.

Both used Engine `300db978…858b23`, broadcast concurrency 32, HTTP concurrency
128, SQLite WAL `synchronous=FULL/fullfsync=ON`, and Redis `appendfsync=everysec`.
The report records source HEAD `dd9282a` **with uncommitted changes**; exact binary
and harness digests in [index.json](index.json) identify the tested artifacts.
Anvil cadence and OP execution are local surrogates, without native OP
sequencing, derivation, L1 fees, or settlement qualification. Explicit fixture
gas limits exclude `eth_estimateGas`; measured whole-run Engine RPC cost was
3.031 calls/intent (EVM) and 3.062 (OP), not a general provider-cost prediction.

## Native Nitro incident and continuity preflight

At 02:09:56 UTC on September 27, the Nitro container exited with code 2 while the
VM root filesystem was full. The journal retained 14,400 admissions: 4,502
terminal, 302 attempted nonterminal, and 9,596 unsigned. The interrupted report
had no complete final reconciliation or drain result and receives no capacity
or whole-campaign safety/liveness pass.

The coordinator cold-copied and hashed the stopped VM, expanded its disk from
12 to 24GiB, grew the root filesystem, and restarted the **same container**.
Nitro found persisted head 4099 but missing state, then rebuilt state by replaying
its stored sequencer messages to the same head. This was internal recovery of
stored history; Engine and the drain ticker remained stopped.

The [read-only preflight](nitro-continuity-observed.json) then passed: original
genesis and checkpoint 4083 matched; 297 canonical block anchors contained all
4,502 saved terminal transaction hashes; all 302 other attempted transactions
had successful canonical receipts. Signer latest/pending nonce was 94,803 and
recipient balance was exactly 89,999 initial wei plus 4,804 one-wei transfers.
Of the 302 receipts, 260 met depth 2 at head 4099 and 42 needed later blocks.
This check did not individually reread all 4,502 terminal receipts or recover
the 9,596 unsigned admissions. It does not authorize a fresh journal, new IDs,
or Engine restart. Later full recovery evidence must be recorded separately.

Pinned Nitro source `beb21087772a2668a1f13847697e1406305e4d89` uses the fixed
`/tmp/dev-test` chain directory and normally opens an existing database.
The earlier blanket claim that `--dev` always resets was incorrect. See
[persistence sources](nitro-persistence-research.json),
[repair metadata](nitro-disk-repair.json), and
[restart replay log](nitro-restart-replay.log).
Storage pressure and accumulated node history confound earlier ordered Native
comparisons; no causal index, concurrency, or disk performance claim follows.

## Evidence boundaries

Full reports, per-ID oracle rows, hash-only RPC audits, original assessments,
and descriptive analyses are retained for the **two completed screens only**.
Compressed files were checked against their original bytes; each screen has
19,800 unique per-ID rows with one attempt and one finalized canonical effect.
[manifest.json](manifest.json) hashes every archived file; [index.json](index.json)
also records original-file hashes. Native evidence contains sanitized summaries,
read-only observations, source links and backup hashes. It excludes databases,
AOF files, environment files, private backups, and signed wire payloads. Prior
failed runs and archives remain unchanged.
