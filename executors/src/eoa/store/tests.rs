//! Real Redis regressions. Run with an explicitly selected disposable Redis:
//! TEST_REDIS_URL=redis://127.0.0.1:16379/ cargo test -p engine-executors eoa::store::atomic::tests -- --ignored
//! No node, account credentials, or live chain is used.
use super::*;
use crate::{eoa::store::EoaTransactionRequest, metrics::EoaMetrics, webhook::WebhookRetryConfig};
use alloy::primitives::{Bytes, U256};
use engine_core::{chain::RpcCredentials, credentials::SigningCredential};
use std::{
    sync::atomic::{AtomicUsize, Ordering},
    time::Duration,
};
use thirdweb_core::auth::ThirdwebAuth;
use twmq::redis::{Client, aio::ConnectionManager};

struct Fixture {
    client: Client,
    shared: ConnectionManager,
    namespace: String,
}

impl Fixture {
    async fn new() -> Self {
        let url = std::env::var("TEST_REDIS_URL")
            .expect("set TEST_REDIS_URL to a disposable Redis instance");
        let client = Client::open(url).unwrap();
        let shared = client.get_connection_manager().await.unwrap();
        Self {
            client,
            shared,
            namespace: format!("eoa-regression:{}", uuid::Uuid::new_v4()),
        }
    }

    fn store(&self) -> EoaExecutorStore {
        EoaExecutorStore::new(
            self.shared.clone(),
            Some(self.namespace.clone()),
            Address::ZERO,
            31337,
            3600,
        )
    }

    async fn owner(&self) -> AtomicEoaExecutorStore {
        self.store()
            .acquire_eoa_lock_aggressively("owner-a", EoaMetrics::new(10, 60, 60), &self.client)
            .await
            .unwrap()
    }

    async fn webhook_queue(&self) -> Arc<twmq::Queue<WebhookJobHandler>> {
        Arc::new(
            twmq::Queue::builder()
                .redis_connection_manager(self.shared.clone(), self.client.clone())
                .name(format!("{}:webhooks", self.namespace))
                .handler(
                    WebhookJobHandler::new(
                        crate::webhook::WebhookDestinationPolicy::default(),
                        Arc::new(WebhookRetryConfig::default()),
                    )
                    .unwrap(),
                )
                .build()
                .await
                .unwrap(),
        )
    }

    async fn cleanup(self) {
        // The random namespace is owned exclusively by this fixture.
        let mut conn = self.shared;
        let keys: Vec<String> = twmq::redis::cmd("KEYS")
            .arg(format!("{}:*", self.namespace))
            .query_async(&mut conn)
            .await
            .unwrap();
        if !keys.is_empty() {
            let _: usize = conn.del(keys).await.unwrap();
        }
    }
}

// Change observed state at the exact validation/EXEC boundary, without sleeps.
struct ConflictingOperation {
    state_key: String,
    observer: ConnectionManager,
    attempts: AtomicUsize,
    steal_lock: bool,
    clear_shared_watch: bool,
}

impl SafeRedisTransaction for ConflictingOperation {
    type ValidationData = u64;
    type OperationResult = u64;

    fn name(&self) -> &str {
        "regression conflict"
    }
    fn watch_keys(&self) -> Vec<String> {
        vec![self.state_key.clone()]
    }

    async fn validation(
        &self,
        conn: &mut MultiplexedConnection,
        store: &EoaExecutorStore,
    ) -> Result<u64, TransactionStoreError> {
        let observed: u64 = conn.get(&self.state_key).await?;
        if self.attempts.fetch_add(1, Ordering::SeqCst) == 0 {
            let mut observer = self.observer.clone();
            if self.steal_lock {
                let _: () = observer.set(store.eoa_lock_key_name(), "owner-b").await?;
            } else {
                let _: () = observer.set(&self.state_key, observed + 10).await?;
            }
            if self.clear_shared_watch {
                twmq::redis::cmd("UNWATCH")
                    .query_async::<()>(&mut store.redis.clone())
                    .await?;
            }
        }
        Ok(observed)
    }

    fn operation(&self, pipeline: &mut Pipeline, observed: u64) -> u64 {
        pipeline.set(&self.state_key, observed + 1);
        observed + 1
    }
}

#[tokio::test]
#[ignore = "requires disposable Redis in TEST_REDIS_URL"]
async fn aborted_exec_revalidates_before_reporting_commit() {
    let fixture = Fixture::new().await;
    let owner = fixture.owner().await;
    let state_key = format!("{}:counter", fixture.namespace);
    let mut observer = fixture.client.get_connection_manager().await.unwrap();
    let _: () = observer.set(&state_key, 0).await.unwrap();
    let operation = ConflictingOperation {
        state_key: state_key.clone(),
        observer: observer.clone(),
        attempts: AtomicUsize::new(0),
        steal_lock: false,
        clear_shared_watch: false,
    };

    let committed = owner
        .execute_with_watch_and_retry(&operation)
        .await
        .unwrap();
    assert_eq!(
        committed, 11,
        "must retry against the value that invalidated EXEC"
    );
    assert_eq!(operation.attempts.load(Ordering::SeqCst), 2);
    assert_eq!(observer.get::<_, u64>(&state_key).await.unwrap(), committed);
    fixture.cleanup().await;
}

#[tokio::test]
#[ignore = "requires disposable Redis in TEST_REDIS_URL"]
async fn shared_unwatch_cannot_disable_a_stale_owner_fence() {
    let fixture = Fixture::new().await;
    let owner = fixture.owner().await;
    let state_key = format!("{}:counter", fixture.namespace);
    let mut observer = fixture.client.get_connection_manager().await.unwrap();
    let _: () = observer.set(&state_key, 0).await.unwrap();
    let operation = ConflictingOperation {
        state_key: state_key.clone(),
        observer: observer.clone(),
        attempts: AtomicUsize::new(0),
        steal_lock: true,
        clear_shared_watch: true,
    };

    assert!(matches!(
        owner.execute_with_watch_and_retry(&operation).await,
        Err(TransactionStoreError::LockLost { .. })
    ));
    assert_eq!(observer.get::<_, u64>(&state_key).await.unwrap(), 0);
    assert_eq!(
        observer
            .get::<_, String>(owner.eoa_lock_key_name())
            .await
            .unwrap(),
        "owner-b"
    );
    fixture.cleanup().await;
}

#[tokio::test]
#[ignore = "requires disposable Redis in TEST_REDIS_URL"]
async fn simple_lock_checked_write_rejects_takeover_before_exec() {
    let fixture = Fixture::new().await;
    let owner = fixture.owner().await;
    let key = format!("{}:result", fixture.namespace);
    // A separate synchronous connection pins takeover between GET and EXEC.
    let observer = std::sync::Mutex::new(fixture.client.get_connection().unwrap());
    let result: Result<(), TransactionStoreError> = owner
        .with_lock_check(|pipeline| {
            twmq::redis::cmd("SET")
                .arg(owner.eoa_lock_key_name())
                .arg("owner-b")
                .query::<()>(&mut *observer.lock().unwrap())
                .unwrap();
            pipeline.set(&key, "stale-write");
        })
        .await;
    assert!(matches!(
        result,
        Err(TransactionStoreError::LockLost { .. })
    ));
    assert!(
        !fixture
            .shared
            .clone()
            .exists::<_, bool>(&key)
            .await
            .unwrap()
    );
    fixture.cleanup().await;
}

fn pending(id: &str) -> PendingTransaction {
    PendingTransaction {
        transaction_id: id.to_owned(),
        queued_at: 1,
        user_request: EoaTransactionRequest {
            transaction_id: id.to_owned(),
            chain_id: 31337,
            from: Address::ZERO,
            to: Some(Address::ZERO),
            value: U256::ZERO,
            data: Bytes::new(),
            gas_limit: None,
            webhook_options: vec![],
            signing_credential: SigningCredential::random_local(),
            rpc_credentials: RpcCredentials::Thirdweb(ThirdwebAuth::SecretKey(
                "unused-test-fixture".into(),
            )),
            transaction_type_data: None,
        },
    }
}

async fn assert_failure_fenced(batch: bool) {
    let fixture = Fixture::new().await;
    let owner = fixture.owner().await;
    let request = pending("intent");
    let mut observer = fixture.client.get_connection_manager().await.unwrap();
    let data_key = owner.transaction_data_key_name("intent");
    let borrowed_key = owner.borrowed_transactions_hashmap_name();
    let _: () = observer
        .hset(&data_key, "status", "submitted")
        .await
        .unwrap();
    let _: () = observer
        .hset(&borrowed_key, "intent", "new-owner-signed-bytes")
        .await
        .unwrap();
    let _: () = observer
        .set(owner.eoa_lock_key_name(), "owner-b")
        .await
        .unwrap();
    let webhook_queue = fixture.webhook_queue().await;
    let error = EoaExecutorWorkerError::TransactionBuildFailed {
        message: "old result".into(),
    };
    let result = if batch {
        owner
            .fail_pending_transactions_batch(vec![(&request, error)], webhook_queue)
            .await
    } else {
        owner
            .fail_pending_transaction(&request, error, webhook_queue)
            .await
    };
    assert!(matches!(
        result,
        Err(TransactionStoreError::LockLost { .. })
    ));
    assert_eq!(
        observer
            .hget::<_, _, String>(&data_key, "status")
            .await
            .unwrap(),
        "submitted"
    );
    assert_eq!(
        observer
            .hget::<_, _, String>(&borrowed_key, "intent")
            .await
            .unwrap(),
        "new-owner-signed-bytes"
    );
    assert_eq!(
        observer.ttl::<_, i64>(&data_key).await.unwrap(),
        -1,
        "stale failure cannot expire live data"
    );
    fixture.cleanup().await;
}

#[tokio::test]
#[ignore = "requires disposable Redis in TEST_REDIS_URL"]
async fn stale_owner_cannot_fail_single_pending_request() {
    assert_failure_fenced(false).await;
}

#[tokio::test]
#[ignore = "requires disposable Redis in TEST_REDIS_URL"]
async fn stale_owner_cannot_fail_pending_batch() {
    assert_failure_fenced(true).await;
}

#[tokio::test]
#[ignore = "requires disposable Redis in TEST_REDIS_URL"]
async fn pending_failure_requires_current_pending_membership() {
    let fixture = Fixture::new().await;
    let owner = fixture.owner().await;
    let request = pending("already-borrowed");
    let mut observer = fixture.shared.clone();
    let data_key = owner.transaction_data_key_name(&request.transaction_id);
    let _: () = observer
        .hset(&data_key, "status", "submitted")
        .await
        .unwrap();
    let result = owner
        .fail_pending_transaction(
            &request,
            EoaExecutorWorkerError::UserCancelled,
            fixture.webhook_queue().await,
        )
        .await;
    assert!(matches!(
        result,
        Err(TransactionStoreError::TransactionNotInPendingQueue { .. })
    ));
    assert_eq!(
        observer
            .hget::<_, _, String>(&data_key, "status")
            .await
            .unwrap(),
        "submitted"
    );
    fixture.cleanup().await;
}

#[tokio::test]
#[ignore = "requires disposable Redis in TEST_REDIS_URL"]
async fn pending_failure_commits_status_removal_and_retention() {
    let fixture = Fixture::new().await;
    let owner = fixture.owner().await;
    let request = pending("pending");
    let mut observer = fixture.shared.clone();
    let data_key = owner.transaction_data_key_name(&request.transaction_id);
    let _: () = observer
        .zadd(
            owner.pending_transactions_zset_name(),
            &request.transaction_id,
            1,
        )
        .await
        .unwrap();
    owner
        .fail_pending_transaction(
            &request,
            EoaExecutorWorkerError::UserCancelled,
            fixture.webhook_queue().await,
        )
        .await
        .unwrap();
    assert_eq!(owner.get_pending_transactions_count().await.unwrap(), 0);
    assert_eq!(
        observer
            .hget::<_, _, String>(&data_key, "status")
            .await
            .unwrap(),
        "failed"
    );
    assert!(observer.ttl::<_, i64>(&data_key).await.unwrap() > 0);
    fixture.cleanup().await;
}

#[tokio::test]
#[ignore = "requires disposable Redis in TEST_REDIS_URL"]
async fn command_errors_are_not_retried_as_watch_conflicts() {
    let fixture = Fixture::new().await;
    let owner = fixture.owner().await;
    let key = format!("{}:wrong-type", fixture.namespace);
    let _: () = fixture.shared.clone().set(&key, "string").await.unwrap();
    let calls = AtomicUsize::new(0);
    let result: Result<(), TransactionStoreError> = tokio::time::timeout(
        Duration::from_secs(2),
        owner.with_lock_check(|pipeline| {
            calls.fetch_add(1, Ordering::SeqCst);
            pipeline.hset(&key, "field", "value");
        }),
    )
    .await
    .unwrap();
    assert!(result.is_err());
    assert_eq!(
        calls.load(Ordering::SeqCst),
        1,
        "server errors cannot be replayed blindly"
    );
    fixture.cleanup().await;
}

#[tokio::test]
#[ignore = "requires disposable Redis in TEST_REDIS_URL"]
async fn trailing_recycled_nonce_resets_to_chain_count_including_zero() {
    for chain_count in [0u64, 1, 37] {
        let fixture = Fixture::new().await;
        let owner = fixture.owner().await;
        let mut observer = fixture.shared.clone();
        let _: () = observer
            .set(owner.last_transaction_count_key_name(), chain_count)
            .await
            .unwrap();
        let _: () = observer
            .set(
                owner.optimistic_transaction_count_key_name(),
                chain_count + 1,
            )
            .await
            .unwrap();
        let _: () = observer
            .zadd(owner.recycled_nonces_zset_name(), chain_count, chain_count)
            .await
            .unwrap();
        assert!(
            owner
                .clean_and_get_recycled_nonces()
                .await
                .unwrap()
                .is_empty()
        );
        assert_eq!(
            owner.get_optimistic_transaction_count().await.unwrap(),
            chain_count,
            "failed trailing allocation must be reusable, including a fresh account's nonce zero"
        );
        assert_eq!(owner.get_recycled_nonces_count().await.unwrap(), 0);
        fixture.cleanup().await;
    }
}

fn borrowed(id: &str, nonce: u64) -> BorrowedTransactionData {
    use alloy::{
        consensus::{SignableTransaction, TxLegacy},
        signers::{SignerSync, local::PrivateKeySigner},
    };
    let signer = PrivateKeySigner::random();
    let transaction = TypedTransaction::Legacy(TxLegacy {
        chain_id: Some(31337),
        nonce,
        ..Default::default()
    });
    let signature = signer
        .sign_hash_sync(&transaction.signature_hash())
        .unwrap();
    let signed_transaction = transaction.into_signed(signature);
    BorrowedTransactionData {
        transaction_id: id.to_owned(),
        hash: signed_transaction.hash().to_string(),
        signed_transaction,
        queued_at: 1,
        borrowed_at: 2,
    }
}

async fn seed_pending(owner: &AtomicEoaExecutorStore, ids: &[&str], next_nonce: u64) {
    let mut observer = owner.redis.clone();
    let _: () = observer
        .set(owner.optimistic_transaction_count_key_name(), next_nonce)
        .await
        .unwrap();
    for id in ids {
        let _: () = observer
            .zadd(owner.pending_transactions_zset_name(), id, 1)
            .await
            .unwrap();
    }
}

#[tokio::test]
#[ignore = "requires disposable Redis in TEST_REDIS_URL"]
async fn incremented_batch_order_does_not_move_nonce_backward() {
    let fixture = Fixture::new().await;
    let owner = fixture.owner().await;
    seed_pending(&owner, &["one", "zero"], 0).await;
    let transactions = [borrowed("one", 1), borrowed("zero", 0)];
    assert_eq!(
        owner
            .atomic_move_pending_to_borrowed_with_incremented_nonces(&transactions)
            .await
            .unwrap(),
        2
    );
    assert_eq!(owner.get_optimistic_transaction_count().await.unwrap(), 2);
    let stored = owner.peek_borrowed_transactions().await.unwrap();
    for transaction in transactions {
        assert!(
            stored
                .iter()
                .any(|entry| entry.transaction_id == transaction.transaction_id
                    && entry.hash == transaction.hash)
        );
    }
    assert_eq!(owner.get_pending_transactions_count().await.unwrap(), 0);
    fixture.cleanup().await;
}

#[tokio::test]
#[ignore = "requires disposable Redis in TEST_REDIS_URL"]
async fn duplicate_intent_cannot_reserve_two_incremented_nonces() {
    let fixture = Fixture::new().await;
    let owner = fixture.owner().await;
    seed_pending(&owner, &["same"], 0).await;
    assert!(
        owner
            .atomic_move_pending_to_borrowed_with_incremented_nonces(&[
                borrowed("same", 0),
                borrowed("same", 1)
            ])
            .await
            .is_err()
    );
    assert_eq!(owner.get_optimistic_transaction_count().await.unwrap(), 0);
    assert_eq!(owner.get_borrowed_transactions_count().await.unwrap(), 0);
    assert_eq!(owner.get_pending_transactions_count().await.unwrap(), 1);
    fixture.cleanup().await;
}

#[tokio::test]
#[ignore = "requires disposable Redis in TEST_REDIS_URL"]
async fn recycled_nonce_cannot_be_reserved_by_two_intents() {
    let fixture = Fixture::new().await;
    let owner = fixture.owner().await;
    seed_pending(&owner, &["first", "second"], 1).await;
    let _: () = fixture
        .shared
        .clone()
        .zadd(owner.recycled_nonces_zset_name(), 0, 0)
        .await
        .unwrap();
    assert!(
        owner
            .atomic_move_pending_to_borrowed_with_recycled_nonces(&[
                borrowed("first", 0),
                borrowed("second", 0)
            ])
            .await
            .is_err()
    );
    assert_eq!(owner.get_borrowed_transactions_count().await.unwrap(), 0);
    assert_eq!(owner.get_recycled_nonces().await.unwrap(), vec![0]);
    assert_eq!(owner.get_pending_transactions_count().await.unwrap(), 2);
    fixture.cleanup().await;
}

struct PendingRemoval<'a> {
    inner: MovePendingToBorrowedWithIncrementedNonces<'a>,
    observer: ConnectionManager,
    attempts: AtomicUsize,
}

impl SafeRedisTransaction for PendingRemoval<'_> {
    type ValidationData = Vec<String>;
    type OperationResult = (usize, Option<u64>);
    fn name(&self) -> &str {
        "pending removal race"
    }
    fn watch_keys(&self) -> Vec<String> {
        self.inner.watch_keys()
    }
    async fn validation(
        &self,
        conn: &mut MultiplexedConnection,
        store: &EoaExecutorStore,
    ) -> Result<Self::ValidationData, TransactionStoreError> {
        let result = self.inner.validation(conn, store).await?;
        if self.attempts.fetch_add(1, Ordering::SeqCst) == 0 {
            let _: () = self
                .observer
                .clone()
                .zrem(
                    store.pending_transactions_zset_name(),
                    &self.inner.transactions[0].transaction_id,
                )
                .await?;
        }
        Ok(result)
    }
    fn operation(
        &self,
        pipeline: &mut Pipeline,
        data: Self::ValidationData,
    ) -> Self::OperationResult {
        self.inner.operation(pipeline, data)
    }
}

#[tokio::test]
#[ignore = "requires disposable Redis in TEST_REDIS_URL"]
async fn removed_pending_request_cannot_be_borrowed_from_a_stale_read() {
    let fixture = Fixture::new().await;
    let owner = fixture.owner().await;
    seed_pending(&owner, &["cancelled"], 0).await;
    let transactions = [borrowed("cancelled", 0)];
    let operation = PendingRemoval {
        inner: MovePendingToBorrowedWithIncrementedNonces {
            transactions: &transactions,
            keys: &owner.keys,
            eoa: owner.eoa(),
            chain_id: owner.chain_id(),
        },
        observer: fixture.client.get_connection_manager().await.unwrap(),
        attempts: AtomicUsize::new(0),
    };
    assert!(matches!(
        owner.execute_with_watch_and_retry(&operation).await,
        Err(TransactionStoreError::TransactionNotInPendingQueue { .. })
    ));
    assert_eq!(owner.get_borrowed_transactions_count().await.unwrap(), 0);
    assert_eq!(owner.get_optimistic_transaction_count().await.unwrap(), 0);
    fixture.cleanup().await;
}

fn serializable_request(id: &str) -> EoaTransactionRequest {
    let mut request = pending(id).user_request;
    request.signing_credential = SigningCredential::Environment {
        address: Address::ZERO,
    };
    request
}

#[tokio::test]
#[ignore = "requires disposable Redis in TEST_REDIS_URL"]
async fn repeated_admission_preserves_pending_inflight_and_completed_state() {
    for status in ["pending", "borrowed", "submitted", "confirmed", "failed"] {
        let fixture = Fixture::new().await;
        let store = fixture.store();
        let request = serializable_request("idempotent");
        store.add_transaction(request.clone()).await.unwrap();
        let mut observer = fixture.shared.clone();
        let data_key = store.transaction_data_key_name("idempotent");
        let created_at: u64 = observer.hget(&data_key, "created_at").await.unwrap();
        if status != "pending" {
            let _: () = observer
                .zrem(store.pending_transactions_zset_name(), "idempotent")
                .await
                .unwrap();
            let _: () = observer.hset(&data_key, "status", status).await.unwrap();
        }
        let _: () = observer.expire(&data_key, 3600).await.unwrap();
        for result in
            futures::future::join_all((0..16).map(|_| store.add_transaction(request.clone()))).await
        {
            result.unwrap();
        }
        assert_eq!(
            observer
                .hget::<_, _, String>(&data_key, "status")
                .await
                .unwrap(),
            status
        );
        assert_eq!(
            observer
                .hget::<_, _, u64>(&data_key, "created_at")
                .await
                .unwrap(),
            created_at
        );
        assert_eq!(
            store.get_pending_transactions_count().await.unwrap(),
            u64::from(status == "pending")
        );
        assert!(
            observer.ttl::<_, i64>(&data_key).await.unwrap() > 0,
            "retry must preserve retention"
        );
        fixture.cleanup().await;
    }
}

#[tokio::test]
#[ignore = "requires disposable Redis in TEST_REDIS_URL"]
async fn conflicting_admission_cannot_overwrite_a_live_intent() {
    let fixture = Fixture::new().await;
    let store = fixture.store();
    let original = serializable_request("same-id");
    store.add_transaction(original.clone()).await.unwrap();
    let data_key = store.transaction_data_key_name("same-id");
    let mut observer = fixture.shared.clone();
    let original_json: String = observer.hget(&data_key, "user_request").await.unwrap();
    let mut conflicting = original;
    conflicting.value = U256::from(1);
    assert!(matches!(
        store.add_transaction(conflicting).await,
        Err(TransactionStoreError::TransactionConflict { .. })
    ));
    assert_eq!(
        observer
            .hget::<_, _, String>(&data_key, "user_request")
            .await
            .unwrap(),
        original_json
    );
    assert_eq!(store.get_pending_transactions_count().await.unwrap(), 1);
    fixture.cleanup().await;
}

#[tokio::test]
#[ignore = "requires disposable Redis in TEST_REDIS_URL"]
async fn concurrent_conflicting_admissions_choose_one_immutable_request() {
    let fixture = Fixture::new().await;
    let store = fixture.store();
    let first = serializable_request("race");
    let mut second = first.clone();
    second.value = U256::from(1);
    let (first_result, second_result) =
        tokio::join!(store.add_transaction(first), store.add_transaction(second));
    assert_ne!(
        first_result.is_ok(),
        second_result.is_ok(),
        "exactly one conflicting intent is admitted"
    );
    let rejected = if first_result.is_err() {
        first_result
    } else {
        second_result
    };
    assert!(matches!(
        rejected,
        Err(TransactionStoreError::TransactionConflict { .. })
    ));
    assert_eq!(store.get_pending_transactions_count().await.unwrap(), 1);
    fixture.cleanup().await;
}
