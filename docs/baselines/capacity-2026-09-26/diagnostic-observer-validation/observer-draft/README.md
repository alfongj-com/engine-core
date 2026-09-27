# Solana live observer fairness draft

Outside-repository harness-only patch: `solana-observer.patch` changes
`scripts/capacity_campaign.py` and adds six regressions to its existing test
module. Production Engine code and post-drain exact reconciliation are unchanged.

## Defect and correction

The old numeric cursor follows the end of a continuously growing historical
signature list. Every new sample finds fresh arrivals after the prior cursor;
older signatures need not be queried again until arrivals stop. The completed
60 TPS report recorded live `finalized=0` while Engine had thousands of durable
finalized terminal results.

The draft maintains an insertion-ordered **pending** queue populated by the
existing incremental journal observer. Each newly discovered signature enters
once. Each sample snapshots at most 2,048 entries and performs at most eight
calls of at most 256 signatures. Immediately before each call, that batch moves
to the queue's tail. Finalized replies remove entries; other replies retain them.
A failed or malformed call raises the normal observer error and stops this wave;
its entries remain behind other waiting work, so repeated failures do not pin
the first batch forever. Unprocessed entries were never moved or discarded.

**Queue invariant:** entries not yet selected stay ahead of selected unresolved
entries; new arrivals cannot jump ahead of already waiting entries. Removing
finalized entries cannot shift a numeric index and skip another signature.
With continuing successful sampling, every finite-position entry receives
another turn. RPC outages cannot guarantee observation, and arrivals beyond
observer service capacity can still increase latency. No fixed wall-time bound
is claimed.

The pending scan now costs O(min(pending, 2,048)) per sample, plus O(1) queue
updates per result. It no longer builds an O(all historical signatures) list
every sample. Complete signature history remains for exact reconciliation, so
historical memory remains O(total signatures); existing incremental journal
reads are unchanged. Correct revisits can increase the average calls compared
with the broken tail-only observer, but the existing eight-call wave cap stays
unchanged and no extra retry wave is introduced.

## Evidence

- Six new tests exercise actual `Campaign.sample`: continuous append, finalized
  removals under a 4,100-entry backlog, bounded batches/waves, partial RPC failure,
  repeated failed/malformed first batches, wrong response shape, and incremental
  SQLite registration without reviving finalized work.
- Full existing campaign module: **26 tests passed**, including these six.
- Expected-negative control restores the original `sample` method; the new
  continuous-arrival test fails its fairness assertion (not fixture setup).
- Logs: `campaign-tests.log`, `legacy-starvation-control.log`.

Run from this draft directory:

```sh
PYTHONPATH=/tmp/engine-solana-observer-draft/scripts:/Users/alfongj/Code/engine-core/scripts python3 -m unittest capacity_campaign_test -v
```

No repository files have been changed. Root review/integration and the broader
Python gate precede a new frozen-harness capacity run. Historical reports keep
their original source hashes and lag caveat; no earlier outcome is upgraded.
