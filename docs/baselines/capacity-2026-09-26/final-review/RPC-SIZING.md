# RPC sizing from the local screens

Measured Engine calls, excluding independent verification traffic:

| Local workload | Late Engine RPC/s | 15-minute equivalent at the cited price |
| --- | ---: | ---: |
| EVM 55 TPS |165.80|$0.90|
| OP 55 TPS |170.47|$0.92|
| Native Nitro 65 offered TPS (overload) |245.52|$1.33|
| Solana 60 TPS |419.88|$2.27|
| Shared 58.75 TPS |287.74|$1.55|

These are sizing calculations, not paid tests or estimates of the full test bill.
The equivalent is RPC/s × 900s × 20 CU × $0.30 / 1,000,000 CU.
Relevant ordinary EVM and Solana methods are listed at 20 CU, with $0.30/M CU,
on [dRPC's official pricing page](https://drpc.org/docs/pricing/compute-units),
checked September 27, 2026. Free methods, account-specific terms, retries,
independent reconciliation and longer public finality can change the cost.
No paid RPC calls were made in this campaign; the current account balance was
not queried.

Solana at 60 TPS used exactly seven Engine calls per accepted intent in the full
run. At the measured late rate, eight continuous hours would cost about $72.56
before verification traffic. That is why this campaign stayed local. Bounded
signature-status batching is worth measuring before a long public run; neither
public latency nor savings from that unimplemented change are established here.
The older [public RPC screen](../../rpc-results.md) is a separate short read-only
capacity result, not current Engine transaction or public-finality qualification.
