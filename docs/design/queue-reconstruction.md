# Reconstructing missing queue work

## Decision

Provide an explicit offline `thirdweb-engine --rebuild-queues` command. Read the
latest independent journal, restore eligible scheduling, then exit without
starting HTTP or workers. The existing journal owner lock, namespace and Redis
continuity checks must all succeed. A rebuild creates no broadcast permission.
Normal workers repeat admission, replay ownership, signed-wire and finality checks.

Use this after selective scheduling loss, or after the normal `recover` procedure
has activated a fresh namespace. A Redis process restart still needs `reattach`
first. Never change a checkpoint token or clear a durable halt to make repair pass.

```sh
# Stop Engine; use the deployment's ordinary configuration and persistent volume.
thirdweb-engine --rebuild-queues
# Inspect counts, then start Engine normally with the same configuration.
thirdweb-engine
```

The command is idempotent per intent. Interruption can leave a partial projection;
repeat it before starting workers. A conflict stops the command. It does not
silently overwrite unrelated payloads, live queue leases, cancelled work or a
known terminal job. Wait for dead workers' queue leases to expire. EOA store
updates use the existing owner-token protocol after acquiring the offline journal
lock; persistent EOA locks left by a dead process are reclaimed through that
protocol. No RPC request is made by reconstruction.

## Eligibility

| Journal record | Action |
| --- | --- |
| Admitted EOA, never attempted | Restore original request and pending scheduling. |
| Admitted EOA, retained signed history | Validate every wire/hash/sender/nonce; restore all hashes at the bound nonce and the latest borrowed wire. Raise the optimistic nonce floor; never lower it. |
| Admitted Solana | Restore original payload/admission and job. The worker restores missing wire through its existing cumulative journal budget. It never obtains a fresh blockhash/signature for attempted work. |
| Unattempted ERC-4337 with retained generated nonce | Restore the original send job. |
| Terminal or quarantined | Excluded from inventory. |
| Attempted ERC-4337, bundled EIP-7702, legacy NOOP | Count as requiring separate reconciliation; no automatic scheduling. |

A full Redis-loss recovery permanently quarantines possibly broadcast work under
its existing policy. This command does not clear that quarantine. A missing,
invalid or unsupported retained wire also stops reconstruction. Existing partial
EOA payload/attempt state that cannot demonstrate its scheduling remains blocked.

The journal does not retain original scheduling timestamps. Newly reconstructed
metadata uses repair time; surviving retry counters, creation times and delayed
positions are preserved. Solana send bounds remain in the independent journal.
Unknown queue age does not establish non-execution. Original intent and replay
identity remain unchanged; deadline-sensitive intents must encode their deadline.

## Bounds and compatibility

Inventory uses a consistent SQLite read transaction per page: at most 16 intents,
64 attempts per intent and 32 MiB of records. An oversized history requires export
and dedicated reconciliation. This is a memory bound, not a total recovery-time
or Redis-capacity guarantee. EOA pending capacity still applies. No journal schema
or checkpoint protocol changes; normal serving adds no inventory queries.

EOA submitted-index members with colon-containing IDs now use a tagged,
hex-encoded ID. Existing three/four-field members and ordinary IDs retain their
format. Previously malformed ambiguous legacy IDs are not guessed. Tests cover
Unicode, cleanup round trips, all retained replacement hashes, owner/type failures,
repeated repair, actual process crashes and exact on-chain hash/signature identity.

`QueueRebuild` checks a finite abstraction of owner exclusion, live leases,
terminal/quarantine/halt rejection, retained identity, no send permission from repair
and an independent recovery budget. Six faulty variants must fail and a witness
must reach repair. Existing retained-wire/checkpoint models remain separate;
there is no refinement or composition proof. Real Lua key types, byte encoding,
cryptography, file locks and crash durability are tested assumptions.
