# Capacity and chaos campaign

Method: [capacity and failure testing](../../design/capacity-campaign.md).
Native chain support and simulation boundaries:
[chain matrix](../../design/capacity-campaign-chain-matrix.md).

## Initial screening

The original single-chain harness is used only to bracket candidate rates before
the shared-process campaign. [Source hashes](screen-source.json) preserve that
scope. Its `outcome: pass` checks eventual reconciliation; it does not mean the
offered rate was sustainable.

| Run | Offered | Late attempted / terminal TPS | Unsigned backlog at load end | Result |
|---|---:|---:|---:|---|
| [Solana, 120s](screen-solana-100-legacy-harness.json) | 100 TPS | 73.576 / 74.560 | 2,752 | All 12,000 accepted and reconciled by 160.580s; growing backlog means 100 TPS was not sustained. |

Solana admission p99 was 628.076ms. Final effects were exactly 12,000 transfers
and 12,000,000 recipient lamports. The next candidate must run long enough to
separate finality startup from steady completion and check backlog growth.

## Load-generator correction

The first shared-harness Solana 75TPS run offered 13,500 intents over 180 seconds.
It admitted and independently reconciled 13,177, with no incorrect effects.
It also recorded 267 client transport errors, 56 client-capacity drops and 667
proxy transport failures. The proxy opened a fresh connection per RPC request.
The host has 16,384 ephemeral ports and a 15-second TCP maximum segment lifetime;
connection churn was a plausible constraint, but the first harness did not
record errno, so that cause is not proven. This result **does not establish an
Engine capacity limit**. The corrected harness reuses connections, records
transport error categories and refuses a capacity claim after proxy errors.

[Original report](capacity-solana-75-v1.json),
[per-intent evidence](capacity-solana-75-v1.observations.jsonl.gz),
[assessment](capacity-solana-75-v1-assessment.json).
