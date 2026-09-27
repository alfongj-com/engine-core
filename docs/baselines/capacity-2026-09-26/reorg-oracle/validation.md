# Reorg CI oracle repair

Both real Engine + Anvil cases pass using unchanged release `a3d360eba3dc470bd830ec9d3a32e0be8e11b6764f9bbf6efeb319f080eb8515`: success in18.929s and revert in19.388s. Each preserves one durable signed attempt, independently re-encodes the observed node transaction to that wire, and records one accepted original-wire resend after restart. The canonical receipt, reserved nonce, finality witness, and outcome agree. Terminal-ID retry causes no additional send or journal attempt. All owned children stopped.

The default oracle now requires exact-wire recovery. An explicit legacy comparison flag permits a fee replacement only when independently verified execution fields remain identical. Seven offline tests reject missing resend evidence, unexpected replacement, nonce/signer/winner substitution, changed original wire, duplicate attempts, and wrong receipt/block/outcome. These test evidence association; the process cases supply real signed transactions.

The first run stopped before fault injection because the fixture assumed lowercase Redis address formatting. The final fixture discovers the sole namespaced cached-count key and validates its signer case-insensitively. The failed log/report are preserved and are not included in passing results.

Exact commands and log/report paths: [validation.json](validation.json). No production source edit or build occurred. Existing CI success/revert process steps exercise the repaired oracle; the new offline unittest module should be added to the pending workflow update.
