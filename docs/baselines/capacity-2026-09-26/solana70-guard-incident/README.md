# Solana 70 TPS: interrupted verification and accepted-intent custody

**The 70 TPS capacity trial remains rejected. Its 25,155 accepted intents were subsequently reconciled exactly.** The original 25,200 offers include 45 known HTTP 429 rejections. No request had an unknown transport outcome. The failed report and supervisor stop remain unchanged.

## What stopped

The resource guard fired during reconciliation, 386.264 seconds after load began. Engine had already stopped and the campaign had recorded its drain target. A late host allocation raised the measured depletion rate; the guard projected that rate across 2,459.94 seconds of unused maximum drain time. It required 68.276 GB while 32.866 GB remained. This was a conservative guard interruption, not disk exhaustion or an observed Engine execution failure. See the [original report](original-failed-report.json.gz) and [resource samples](resource-guard.jsonl.gz).

The last original sample recorded 25,155 admitted, attempted, included, finalized and terminal intents, zero pending observer signatures, and six zero Redis queue indexes. Those are **historical observations**, not a fresh queue measurement made by the recovery runner.

The offer window also had 45 rejections and growing unsigned work: 2→364 over the late window, with a fitted growth of 0.982 intents/second. Completing accepted work later does not make this a sustainable 70 TPS result.

## Preserved ledger and two distinct restart observations

The coordinator preserved a verified private cold copy of all 65 regular campaign files, including the original ledger, journal/AOF and keys. These private data are not published here. [Cold-copy manifest](cold-backup-manifest.json).

1. A fresh inspection copy of the **original ledger** retained slots 0–815, rooted through 784. Original transaction slots 65 and 772 were full rooted blocks. The first restarted clone used the validator's default 10,000-shred retention; inspection later found only slots 803–1319. Its original receipt history had been pruned. Raising retention cannot recover already-pruned history from that used clone. [Original bounds](inspection-original-bounds.log), [used-clone bounds](inspection-used-bounds.log), [original slots](inspection-original-slots.log).
2. The coordinator created another hash-verified copy from preserved originals and restarted it with explicit finite retention of 1,000,000 shreds, without reset, funding or Engine. Its earliest health/root check still returned null for the first/last historical receipts. Subsequent read-only probes, with no further restart or resubmission, found both receipts, matching finalized statuses and complete block metadata. This demonstrates that initial health/root readiness was insufficient to establish historical RPC availability; the exact internal initialization cause was not established. [Initial check](clone-retained-first-check.json), [block/status diagnosis](retained-clone-read-diagnosis.json), [receipt recheck](retained-clone-transaction-recheck.json).

The second clone then served the complete independent receipt audit. Both clone processes were stopped afterward. No original file or failed supervisor state was cleared.

## Independent accepted-intent proof

The [runner](reconcile-original.py) pins the original report and harness modules, reconstructs all 25,200 original expected IDs, reads the retained journal and permits only six read-only RPC methods. It starts no process and sends no transaction. The current campaign's actual Solana adapter decodes each recorded signed wire, fetches its finalized transaction and historical status, and verifies effects and fees. A separate check binds each observed slot/signature/outcome to the original durable proof.

| Check | Result |
| --- | ---: |
| Accepted IDs / unique canonical finalized successes | 25,155 / 25,155 |
| Signed identities per accepted ID | 1 |
| Known 429 IDs absent from admission/attempt/proof | 45 |
| Unknown HTTP outcomes | 0 |
| Transfer per accepted ID | 1,000 lamports |
| Receipt fee per accepted ID | 5,000 lamports |
| Recipient increase | 25,155,000 lamports |
| Total receipt fees | 125,775,000 lamports |
| Payer debit | 150,930,000 lamports |
| Read RPC failures | 0 |
| Full receipt reconciliation duration | 13.687 seconds |

The accepted-only custody oracle passes. The original all-offer oracle remains incomplete for exactly the 45 rejected IDs. Queue counters supplied to both oracles are explicitly labeled historical, derived from the original complete response inventory, drain event, final sample and custody capture; neither Redis nor Engine was restarted.

An additional offline review checked **every** [per-ID observation](accepted-custody-observations.jsonl.gz) against the original journal: wire SHA256, replay binding, reconstructed intent digest, unique signature, durable slot/outcome, 1,000-lamport effect, 5,000-lamport fee, and aggregate conservation. Both owned clone PIDs were absent, the original failed report hash matched, and the global stop remained set. [Independent review](independent-audit.json), [full custody report](accepted-custody-report.json).

## Limits

This proves accepted-work custody using the preserved local chain and captured RPC evidence. It assumes an honest local validator; the final offline review did not query the node after shutdown. There was no new offered workload, no recovered throughput measurement and no public-cluster qualification. The accepted-only generic oracle eligibility flag does not override `capacity_qualification: false`.

The first failed receipt audit is retained with its error and source hash. That version's failure-report path omitted RPC counters; successful reconciliation includes all 50,314 read calls. The missing counters are not treated as zero. [Failed audit](reconciled-original-25155.json), [failure log](reconciled-original-25155.log).

[Manifest](MANIFEST.json) hashes every archived artifact. Original raw signed bytes, SQLite/WAL, Redis AOF, validator ledger and keypairs remain private.
