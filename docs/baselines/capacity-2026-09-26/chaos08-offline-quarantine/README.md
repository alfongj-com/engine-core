# Closed chaos evidence

Exact original report, one-shot state, sanitized observations, digest-only full RPC audits, resources and supplied independent audit/analysis are preserved here. Read full-report.json.gz and the independent audit for the actual fault and outcome. Privacy/provenance checks do not independently prove execution safety or successful recovery.

The original active custody and review-required fields remain unchanged. This helper never clears them, selects capacity, resumes work or treats deliberate quarantine as settled. Same-ID retries may first admit original requests; the independent review must reconcile that union, not merely original HTTP202 counts. Lost HTTP envelopes can contain more accepted identities than the configured selected fault count.

Private journals, AOF, node state, keys, environment, raw signed payloads and unstructured logs are excluded. Original sources are untouched. Hashes and source paths are in provenance.json; manifest.json is written last. A partial directory without that manifest is not a completed archive. These are local-node chaos results, not public-chain capacity certification.

## Outcome

Explicit offline recovery preserved all 605 admissions, 600 attempts and terminal/finality tables. The new namespace retained 413 terminal records, quarantined 187 possibly broadcast requests and kept five unsigned requests. All 331 API probes returned their expected result (200 terminal 202s and 131 quarantine 503s), with zero new sends. The independent audit verified all captured execution effects and fees.

This is a successful guarded projection-recovery test, not completed original work. The original oracle remains safety=true/liveness=false; 192 journal records and 13 pending node transactions remain unresolved. Their private state remains retained, and no new same-signer liveness is claimed. All owned children stopped.
