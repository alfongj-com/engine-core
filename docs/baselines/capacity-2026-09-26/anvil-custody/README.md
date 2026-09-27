# Failure-only owned Anvil custody

The interrupted campaign remains failed and requires operator review. This change preserves an original owned Anvil's mined chain before cleanup; it does not repair the already-lost EVM55 node, restart a campaign, replay a wire, or clear a guard.

## Change

`scripts/capacity_campaign.py::preserve_anvil_custody` runs only from interrupted custody capture, after Engine stops. It disables interval and automatic mining, records the head/genesis, requests `anvil_dumpState([true])`, and privately publishes decompressed state plus a separate `txpool_content` inventory. It verifies that the head did not change across capture. Every file uses no-overwrite publication, mode0600, file/directory fsync and SHA256. The directory is mode0700.

There is no periodic snapshot or extra nominal-load I/O. External Nitro and Solana lifecycle remain unchanged. Existing HTTP replies are capped at16MiB; decompressed state is capped at128MiB and pool JSON at16MiB. Capture retains the actual host floor, max(8GiB,2% of filesystem capacity), plus the file's remaining write allowance. The45-second deadline is cooperative: a started RPC or fsync may complete after it. No individual saved snapshot is labeled restore-verified automatically.

## What survives

Pinned Foundry commit `982849d3140c01fd3b72905759581a132df7aa98` serializes mined blocks, transaction receipts and, when requested, historical account states. [Serializer](https://github.com/foundry-rs/foundry/blob/982849d3140c01fd3b72905759581a132df7aa98/crates/anvil/src/eth/backend/mem/mod.rs), [state schema](https://github.com/foundry-rs/foundry/blob/982849d3140c01fd3b72905759581a132df7aa98/crates/anvil/src/eth/backend/db.rs).

**The pending/queued transaction pool is not restored by this state file.** Its separate private inventory and original journal wires remain evidence for later operator reconciliation. Already-dispatched RPCs may still arrive after a pool observation. Do not call the combined files an automatic full-process resume. Restore only into a new isolated node with the same binary/network configuration; do not merge state into a live unrelated chain. No raw state/pool files belong in the public evidence archive.

## Validation

Command (already executed):

```sh
PYTHONPATH=scripts ANVIL_BIN=/tmp/engine-finality/anvil \
  python3 -m unittest -v capacity_anvil_custody_test
```

`validation.log`: **6 passed,0 skipped,1.286s**. Anvil1.8.1 SHA256 `cad9ae8b74a37fd417e2b8d29b2caca869ba10fd015a1e92059ed15973188e66`.

For both Ethereum and OP execution, a fresh clone loaded the saved JSON and returned all3 original full receipts, original genesis/head/block hashes and historical balances. Each original also held1 pending and1 queued transaction: both disappeared from the restored pool and remained in the separately saved inventory. Other regressions cover the actual interrupted cleanup order, unchanged failed verdict, private no-overwrite/hash behavior, actual disk floor, expanded-state limit and expired budget. Test-owned node processes were stopped; no Engine or capacity workload ran. Independent source review found no scoped blocker.
