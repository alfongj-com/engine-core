# Historical Nitro resume source

`resume-nitro-source.py` is the exact one-off script used to finish the original 18,000-intent Nitro run after an observer connection failed. SHA-256: `d3d17085bfe792cec8d743cabc82d0de03f2d90721821b4873a6bcb31e27e2f9` (matches the successful resume report's `resume_script_sha256`).

Executed from the repository with:

```sh
PYTHONPATH=/Users/alfongj/Code/engine-core/scripts python3 /tmp/engine-capacity-next/resume_nitro.py
```

It reopened the original journal and AOF only after exact-checkpoint reattach, used the pinned baseline binary, and only unpaused/paused the existing native dev node. The guard allowed existing signed wires or first signatures for originally unsigned IDs; it allowed no new intents or replacement identities. All 18,000 outcomes passed independent reconciliation. This separate drain is not a capacity measurement.

The source retains historical local paths and depends on that exact saved state; it is evidence, not a general recovery utility. It contains the public dev key 1 and generated runtime auth only, with no user secret literals. The private forensic journal snapshot is not included.
