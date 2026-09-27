# Native Nitro 50 TPS / send concurrency 32: tail diagnosis

The remaining 40 transactions were a short finality/poll tail after a longer dispatch drain. No recovery intervention was needed. [Extracted evidence](nitro50-send32-tail.json) retains source paths and hashes.

At the 360.009-second offered-phase sample, 17,999 requests were admitted, 14,567 had durable attempts, 14,439 were accepted/included, and 14,376 were terminal. The final admission completed immediately afterward. The last 40 sends were accepted at **01:02:46.620–46.871 UTC on 27 September 2026**. At 450.008 seconds all 18,000 were included and 17,960 were terminal; by 455.014 seconds all were terminal. Thus the final 40 waited under approximately 10 seconds after acceptance, not the entire roughly 95-second drain.

Those 40 occupy consecutive nonces 71,959–71,998. Their receipts are in blocks 3,188/3,189, with the retained depth-2 checkpoint at block 3,193. There are exactly 18,000 durable attempts, forwarded sends, accepted wires, and terminal intents, with no global or chain halt. The final report verifies zero retained workload and owned children stopped. This supports a finality/polling explanation; it does not independently measure the exact instant each terminal SQLite row committed.

No direct/proxy node queries, sends, restarts, or state changes were used for this diagnosis. The native node was left paused. This successful drain does not establish sustainable 50 TPS during the offered phase.
