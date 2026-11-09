# Solana processing-lock recovery

## Problem

Solana jobs have a fixed queue lease and a separate 120-second processing
lock. After Engine dies, the queue can reassign a job before that lock expires.
A signed transaction can expire while its replacement worker waits. A local
three-chain crash run reproduced this delay and parked an unknown outcome.

## Ownership rule

New processing locks store a random lock token and the queue metadata key,
lease key and lease token. A queue owner is live only when **both** the metadata
token matches and its expiring lease key exists.

One Redis Lua operation checks the claimant and prior owner before replacing a
lock. It refuses a dead claimant and a live prior owner. It can replace a known
owner record immediately after the prior queue lease is lost. Concurrent
claimants compete in that same operation; only one wins.

Attempt reads, writes and the final pre-RPC ownership check require the exact
processing-lock value and a live queue lease. A stale worker cannot update the
replacement's attempt or remove its lock. Release remains compare-and-delete.
The durable journal still controls broadcast permission and retains the exact
signed wire. This change creates no signature, changes no retry budget and
relaxes no expiry, finality or recovery rule.

## Compatibility and limits

Queue payloads, job IDs and journal schema do not change. Opaque, unknown-version
and legacy UUID locks carry no usable ownership evidence, so early takeover is
refused. Legacy locks retain their existing release/expiry behavior. Drain old
workers gracefully before upgrading where possible; a crash of an old worker
can still impose the old TTL. Never delete ambiguous locks to force progress or
sign a replacement for an expired transaction with an unknown outcome.

The queue sets the lease TTL on pop and does not renew it. Keep
`queue.lease_duration_seconds` above the worker's 90-second processing deadline
with time for journal writes and fenced completion; the server default is 600
seconds. A shorter lease can expire while the original worker is alive. Its
stale writes/completion are refused, and only a live replacement can reconcile
the retained attempt; this prevents stale ownership but may delay progress.

A long queue lease, Redis outage or slow restart can still exceed Solana's
blockhash validity window. This is no deadline guarantee. Redis is the existing
single-instance queue authority; this does not add Redis Cluster or multi-host
journal support.

## Verification

Real-Redis regressions cover immediate takeover after lease expiry, mismatched
metadata, stale attempt writes, concurrent claimants, stale release and legacy
lock refusal. `SolanaProcessingLease` models the new ownership rule, with required
counterexamples for stale mutation and live-owner stealing, plus a reachable
takeover witness. The finite model abstracts Redis's atomic scripts and does
not prove Lua parsing, timing, RPC behavior or whole-service correctness. Those
remain regression and process-test obligations.
