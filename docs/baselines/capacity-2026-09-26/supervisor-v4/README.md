# Sequential supervisor v4

This is the exact new runner source for future local campaigns. Existing v3 source, state, reports and review latches remain unchanged. It has not run a workload yet. `jobs-fixture.json` is historical input for tests, not an approved next campaign.

Changes from v3:

- Check host resource reserves before every child. Native Nitro adds guest disk/inode checks before container unpause. The campaign performs the same preflight and monitors at 60-second intervals.
- Require a host floor of `max(8 GiB, 2%)`, plus twice a provisional 2 MiB/s growth allowance over the complete job horizon and observation/cleanup margin. Reserve another 2 GiB for Solana setup, or 256 MiB otherwise. Native guest floors and growth allowances are recorded by the helper. Setup allocation is not extrapolated as recurring throughput: all floors/future reserves remain checked, then an actual end-of-setup measurement establishes the growth baseline. Load-period depletion spikes remain fully counted.
- Freeze the new resource-helper source along with Engine, campaign, proxy, runner and other existing inputs. A different runner-version state cannot be resumed here.
- Explicit infrastructure interruption or operator-review status rejects automatic continuation. A failed native pause is retained alongside the campaign's original incident rather than replacing it. No reset, deletion, reattach or replay occurs.

Use `runner.py --help` for the unchanged prepare/execute workflow. The runner retains explicit macOS/local-fixture paths; it is not a general deployment tool. The root task owns operator review, jobs, binary selection and execution. Never create a fresh state merely to bypass an unresolved old run.

Validation: 11 adapted prior runner regressions pass, including actual generated-argument parsing, finite bounds, private no-overwrite state, unknown/unresolved global-stop behavior, process timeout cleanup, and native pause ordering. Processes in those tests are only mocked campaign calls and one short, owned Python timeout fixture. The new repository infrastructure tests separately verify an outage plus failed pause persists the global stop and cannot resume.

A read-only final preflight also passed with the shared/Solana setup allowance: host available 32.204 GiB versus required 21.953 GiB; native guest available 13.408 GiB versus required 9.977 GiB, at a 2,880-second total horizon. No container unpause, funding, transaction, load, or node restart was performed. These observations establish current headroom only; provisional growth allowances are not a sustainable retention proof.

Run the archived tests with the repository scripts on `PYTHONPATH`:

```sh
PYTHONPATH=scripts python3 docs/baselines/capacity-2026-09-26/supervisor-v4/test_runner.py -v
```
