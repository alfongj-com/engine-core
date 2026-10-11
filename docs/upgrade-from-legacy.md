# Upgrade an existing Engine deployment

## Decision

The recovery journal cannot import a legacy Redis queue. Initialization requires
a fresh journal and an empty destination namespace; draining pending jobs alone
does not make the old namespace empty because retained jobs and indexes remain.
Use a new namespace after reconciling the old deployment. Do not delete old data
or manually write recovery markers to make initialization pass.

This procedure supports one Engine process on one Unix host with persistent
local journal storage. It is a maintenance-window cutover, not a rolling upgrade.
The [recovery runbook](design/redis-disaster-recovery.md) governs deployments that
already have a journal; do not initialize another one for those deployments.

## Before the window

- Save the old binary version, configuration, Redis backup and a transaction
  inventory. Protect these as secrets. Include request IDs, signer/chain, nonce
  or Solana signature, and pending/active/delayed/failed work. Account for all
  queue types and EOA/Solana executor storage, not just the top-level queue count.
- Check the [replay compatibility changes](replay-migration.md), configured
  signers/RPCs, chain finality policy and [webhook origins](design/webhook-egress.md).
  Qualify these against disposable state first. Bundled EIP-7702 and automatic
  Solana blockhash replacement are disabled.
- Choose a unique new execution namespace and a private local journal directory
  outside the Redis backup volume. Prepare a caller-side boundary that prevents
  old request retries from entering the new deployment: legacy idempotency
  records are not imported, and the new journal cannot recognize those IDs.

## Cutover

1. Stop external admission and automatic client retries at the ingress. Keep
   the old workers running only to finish already accepted work. Stop any other
   software that can submit with the same signers before the final inventory.
2. Reconcile every accepted request against its original chain evidence and
   required finality. Wait for normal completion or record why a request was
   never signed/sent. Include failed and delayed jobs, fee replacements, pending
   EOA nonces and Solana attempts. Empty queues, a null receipt or an expired
   blockhash do not prove a transaction did not execute. Unknown outcomes block
   reuse of that signer/request; do not turn them into fresh submissions.
3. Stop all old Engine workers and verify the processes are gone. Preserve a
   final consistent Redis backup and the reconciled inventory. A worker shutdown
   acknowledgement alone does not prove that every chain transaction finalized.
4. Initialize once against the **new empty namespace**, using the new binary and
   the same Redis endpoint the service will use. For example, after creating a
   private directory `data` with mode 0700:

   ```sh
   export REDIS_URL=redis://127.0.0.1:6379/
   engine-recovery init --path data/recovery.sqlite --namespace engine_v2
   engine-recovery status --path data/recovery.sqlite
   ```

   Set `APP__REDIS__URL` to that endpoint,
   `APP__RECOVERY__JOURNAL_PATH` to the same persistent journal path and
   `APP__QUEUE__EXECUTION_NAMESPACE=engine_v2` for ordinary server startup.
   `--initialize-recovery` is the equivalent server command using those settings.
5. Start one new Engine process. Check `/health`, perform one controlled new
   request per enabled chain and reconcile its terminal evidence. Enable new
   traffic only after those checks. Keep the old-request retry boundary active
   and retain the old inventory/backup for investigation.

## Stop and rollback

SIGTERM and SIGINT enter the same graceful HTTP and worker shutdown path. Give
the service time to finish current handlers; a forced kill still requires the
normal crash-recovery checks. Shutdown does not drain all queued chain work.

Before the first new admission or signature, reverting to the stopped old
deployment is possible only after ensuring the new process is gone and its
signers produced no new effects. After new execution begins, restoring the old
Redis snapshot or starting the old binary against the same signer can lose
idempotency and replay history. Stop admission and reconcile both inventories;
there is no supported automatic downgrade or legacy-journal migration command.
