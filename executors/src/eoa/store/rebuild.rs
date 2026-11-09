//! Offline restoration of intent projections and their nonce/hash indexes.
//! Retained scheduling must prove its binding; conflicting state fails closed.
use super::*;
use alloy::{
    consensus::{Transaction, TxEnvelope},
    eips::eip2718::{Decodable2718, Encodable2718},
};
use serde_json::Value;
use twmq::redis::Script;

impl super::atomic::AtomicEoaExecutorStore {
    /// The caller must hold the journal owner lock with no workers/HTTP running.
    /// Exact journal attempts are restored as borrowed, never as fresh pending work.
    /// Returns whether retained intent scheduling or its indexes needed repair.
    pub async fn rebuild_missing_intent(
        &self,
        request: &EoaTransactionRequest,
        replay_key: Option<&str>,
        retained: &[Value],
    ) -> Result<bool, TransactionStoreError> {
        let conflict = || TransactionStoreError::TransactionConflict {
            transaction_id: request.transaction_id.clone(),
        };
        if request.from != self.eoa || request.chain_id != self.chain_id {
            return Err(conflict());
        }
        let payload = serde_json::to_string(request)?;
        let now = EoaExecutorStore::now();
        let mut attempts = Vec::new();
        let mut nonce = None;
        for (index, value) in retained.iter().enumerate() {
            let wire = value
                .get("signedTransaction")
                .and_then(Value::as_str)
                .and_then(|wire| wire.strip_prefix("0x"))
                .ok_or_else(conflict)?;
            let bytes = hex::decode(wire).map_err(|_| conflict())?;
            let mut rest = bytes.as_slice();
            let envelope = TxEnvelope::decode_2718(&mut rest).map_err(|_| conflict())?;
            if !rest.is_empty() || envelope.encoded_2718() != bytes {
                return Err(conflict());
            }
            let tx = envelope.into_signed();
            crate::recovery::validate_eoa_wire(request, &tx).map_err(|_| conflict())?;
            let n = tx.nonce();
            if n >= (1u64 << 53) - 1
                || nonce.is_some_and(|old| old != n)
                || replay_key != Some(format!("evm:{}:{:#x}:{n}", self.chain_id, self.eoa).as_str())
                || value.get("transactionHash").and_then(Value::as_str)
                    != Some(tx.hash().to_string().as_str())
                || value.get("nonce").and_then(Value::as_u64) != Some(n)
                || value.get("chainId").and_then(Value::as_u64) != Some(self.chain_id)
            {
                return Err(conflict());
            }
            nonce = Some(n);
            attempts.push(TransactionAttempt {
                transaction_id: request.transaction_id.clone(),
                details: tx,
                sent_at: now,
                attempt_number: u32::try_from(index + 1).map_err(|_| conflict())?,
            });
        }
        if replay_key.is_some() != !attempts.is_empty() {
            return Err(conflict());
        }
        // Initial submissions need not have an attempt-list entry. Read a
        // bounded literal submitted member, then recheck that exact witness in
        // Lua under the same EOA owner token; never infer it from nonce progress.
        let mut submitted_witness = String::new();
        if let Some(nonce) = nonce {
            let rows: Vec<SubmittedTransactionStringWithNonce> = twmq::redis::cmd("ZRANGEBYSCORE")
                .arg(self.submitted_transactions_zset_name())
                .arg(nonce)
                .arg(nonce)
                .arg("WITHSCORES")
                .arg("LIMIT")
                .arg(0)
                .arg(129)
                .query_async(&mut self.redis.clone())
                .await?;
            if rows.len() > 128 {
                return Err(conflict());
            }
            for row in rows {
                let parsed =
                    SubmittedTransactionDehydrated::from_redis_strings(std::slice::from_ref(&row));
                let Some(member) = parsed.first() else {
                    return Err(conflict());
                };
                if member.transaction_id != request.transaction_id
                    || !attempts
                        .iter()
                        .any(|a| a.details.hash().to_string() == member.transaction_hash)
                {
                    return Err(conflict());
                }
                submitted_witness = row.0;
            }
        }
        let borrowed = attempts.last().map(|a| BorrowedTransactionData {
            transaction_id: request.transaction_id.clone(),
            signed_transaction: a.details.clone(),
            queued_at: now,
            borrowed_at: now,
            hash: a.details.hash().to_string(),
        });
        let encoded_attempts: Vec<String> = attempts
            .iter()
            .map(serde_json::to_string)
            .collect::<std::result::Result<_, _>>()?;
        let submitted: Vec<String> = attempts
            .iter()
            .map(|attempt| {
                SubmittedTransactionDehydrated {
                    nonce: attempt.details.nonce(),
                    transaction_hash: attempt.details.hash().to_string(),
                    transaction_id: request.transaction_id.clone(),
                    queued_at: now,
                    submitted_at: now,
                }
                .to_redis_string_with_nonce()
                .0
            })
            .collect();
        let borrowed = borrowed
            .map(|b| serde_json::to_string(&b))
            .transpose()?
            .unwrap_or_default();
        let script = Script::new(
            r#"
            local types = {'hash','zset','hash','list','zset','string','zset','string'}
            for i, expected in ipairs(types) do
                local actual = redis.call('TYPE', KEYS[i]).ok
                if actual ~= 'none' and actual ~= expected then return -1 end
            end
            -- Every mutation is fenced by the current EOA owner token.
            if redis.call('GET',KEYS[8]) ~= ARGV[9] then return -1 end
            -- Validate the entire retained hash namespace before any write,
            -- including aliases for older fee-replacement attempts.
            local missing_alias=false
            for i=9,#KEYS do
                local actual=redis.call('TYPE',KEYS[i]).ok
                if actual~='none' and actual~='string' then return -1 end
                local mapped=redis.call('GET',KEYS[i])
                if mapped and mapped~=ARGV[1] then return -1 end
                if not mapped then missing_alias=true end
            end
            local high = redis.call('GET',KEYS[6])
            if high and (not string.match(high,'^%d+$')
                or tonumber(high) > 9007199254740991) then return -1 end
            local next_nonce = tonumber(ARGV[5])
            local attempts = cjson.decode(ARGV[6])
            local submitted = cjson.decode(ARGV[7])
            local function restore_binding()
                local changed=missing_alias
                if not high or tonumber(high)<next_nonce then
                    redis.call('SET',KEYS[6],ARGV[5]);changed=true
                end
                if redis.call('ZREM',KEYS[7],next_nonce-1)>0 then changed=true end
                for i=9,#KEYS do
                    if redis.call('EXISTS',KEYS[i])==0 then redis.call('SET',KEYS[i],ARGV[1]) end
                end
                return changed and 1 or 0
            end
            local existing = redis.call('HGET',KEYS[1],'user_request')
            if existing then
                if existing ~= ARGV[2] then return -1 end
                local status = redis.call('HGET',KEYS[1],'status')
                if status ~= 'pending' and status ~= 'submitted' then return -1 end
                -- A durable binding must still have a borrowed/submitted projection.
                -- Do not let a partial pending record allocate another nonce.
                if next_nonce and redis.call('HEXISTS',KEYS[3],ARGV[1]) == 0 then
                    local score = ARGV[10] ~= '' and redis.call('ZSCORE',KEYS[5],ARGV[10]) or false
                    if score == false or tonumber(score) ~= tonumber(ARGV[5])-1 then return -1 end
                end
                if next_nonce then return restore_binding() end
                if redis.call('HEXISTS',KEYS[3],ARGV[1]) == 0 then
                    if status ~= 'pending' or redis.call('EXISTS',KEYS[4]) == 1 then return -1 end
                    local created = redis.call('HGET',KEYS[1],'created_at')
                    if not created or not string.match(created,'^%d+$') then return -1 end
                    if redis.call('ZSCORE',KEYS[2],ARGV[1]) == false
                        and redis.call('ZCARD',KEYS[2]) >= tonumber(ARGV[8]) then return -2 end
                    return redis.call('ZADD',KEYS[2],'NX',created,ARGV[1])
                end
                return 0
            end
            if redis.call('EXISTS',KEYS[1]) == 1
                or redis.call('ZSCORE',KEYS[2],ARGV[1]) ~= false
                or redis.call('HEXISTS',KEYS[3],ARGV[1]) == 1
                or redis.call('EXISTS',KEYS[4]) == 1 then return -1 end
            -- All type/collision/number checks precede the first write. Lua errors
            -- do not roll back earlier commands.
            if not next_nonce and redis.call('ZCARD',KEYS[2]) >= tonumber(ARGV[8]) then return -2 end
            redis.call('HSET',KEYS[1],'user_request',ARGV[2],'created_at',ARGV[3],
                'status',next_nonce and 'submitted' or 'pending')
            if not next_nonce then
                redis.call('ZADD',KEYS[2],ARGV[3],ARGV[1])
            else
                redis.call('HSET',KEYS[3],ARGV[1],ARGV[4])
                for _,a in ipairs(attempts) do redis.call('LPUSH',KEYS[4],a) end
                for _,s in ipairs(submitted) do redis.call('ZADD',KEYS[5],next_nonce-1,s) end
                restore_binding()
            end
            return 1
        "#,
        );
        let mut invocation = script.prepare_invoke();
        invocation
            .key(self.transaction_data_key_name(&request.transaction_id))
            .key(self.pending_transactions_zset_name())
            .key(self.borrowed_transactions_hashmap_name())
            .key(self.transaction_attempts_list_name(&request.transaction_id))
            .key(self.submitted_transactions_zset_name())
            .key(self.optimistic_transaction_count_key_name())
            .key(self.recycled_nonces_zset_name())
            .key(self.eoa_lock_key_name());
        for attempt in &attempts {
            invocation
                .key(self.transaction_hash_to_id_key_name(&attempt.details.hash().to_string()));
        }
        let result: i32 = invocation
            .arg(&request.transaction_id)
            .arg(payload)
            .arg(now)
            .arg(borrowed)
            .arg(nonce.map(|n| (n + 1).to_string()).unwrap_or_default())
            .arg(serde_json::to_string(&encoded_attempts)?)
            .arg(serde_json::to_string(&submitted)?)
            .arg(MAX_PENDING_TRANSACTIONS)
            .arg(self.worker_id())
            .arg(submitted_witness)
            .invoke_async(&mut self.redis.clone())
            .await?;
        if result == -2 {
            return Err(TransactionStoreError::CapacityExceeded);
        }
        if result < 0 {
            return Err(conflict());
        }
        Ok(result == 1)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use alloy::{
        consensus::{SignableTransaction, TxEip1559},
        primitives::{Address, B256, Bytes, TxKind, U256},
        signers::{SignerSync, local::PrivateKeySigner},
    };
    use engine_core::{chain::RpcCredentials, credentials::SigningCredential};
    use serde_json::json;
    use twmq::redis::AsyncCommands;

    async fn fixture() -> (
        super::super::atomic::AtomicEoaExecutorStore,
        EoaTransactionRequest,
        Vec<Value>,
        String,
    ) {
        let client = twmq::redis::Client::open(std::env::var("TEST_REDIS_URL").unwrap()).unwrap();
        let redis = client.get_connection_manager().await.unwrap();
        let signer = PrivateKeySigner::from_bytes(&B256::repeat_byte(1)).unwrap();
        let namespace = format!("rebuild_{}", uuid::Uuid::new_v4());
        let request = EoaTransactionRequest {
            transaction_id: "invoice:2026:11".into(),
            chain_id: 31337,
            from: signer.address(),
            to: Some(Address::repeat_byte(2)),
            value: U256::from(1),
            data: Bytes::new(),
            gas_limit: Some(21000),
            webhook_options: vec![],
            signing_credential: SigningCredential::Environment {
                address: signer.address(),
            },
            rpc_credentials: RpcCredentials::Configured,
            transaction_type_data: None,
        };
        let store = EoaExecutorStore::new(redis, Some(namespace), signer.address(), 31337, 3600);
        let store = store
            .acquire_eoa_lock_aggressively(
                "repair-test",
                crate::metrics::EoaMetrics::new(10, 60, 60),
                &client,
            )
            .await
            .unwrap();
        let mut history = Vec::new();
        for fee in [2, 3] {
            let tx = TxEip1559 {
                chain_id: 31337,
                nonce: 5,
                gas_limit: 21000,
                max_fee_per_gas: fee,
                max_priority_fee_per_gas: 1,
                to: TxKind::Call(request.to.unwrap()),
                value: request.value,
                ..Default::default()
            };
            let signature = signer.sign_hash_sync(&tx.signature_hash()).unwrap();
            let wire: TxEnvelope = tx.into_signed(signature).into();
            history.push(json!({"chainId":31337,"nonce":5,"sender":signer.address(),"transactionHash":wire.tx_hash().to_string(),"signedTransaction":format!("0x{}",hex::encode(wire.encoded_2718()))}));
        }
        let key = format!("evm:31337:{:#x}:5", signer.address());
        (store, request, history, key)
    }
    async fn cleanup(store: super::super::atomic::AtomicEoaExecutorStore) {
        let keys: Vec<String> = store
            .redis
            .clone()
            .keys(format!("{}:*", store.namespace.as_ref().unwrap()))
            .await
            .unwrap();
        if !keys.is_empty() {
            let _: usize = store.redis.clone().del(keys).await.unwrap();
        }
    }

    #[tokio::test]
    #[ignore = "requires disposable Redis in TEST_REDIS_URL"]
    async fn missing_projection_restores_all_hashes_one_nonce_and_latest_borrowed_wire() {
        let (store, request, history, key) = fixture().await;
        assert!(
            store
                .rebuild_missing_intent(&request, Some(&key), &history)
                .await
                .unwrap()
        );
        assert_eq!(store.get_pending_transactions_count().await.unwrap(), 0);
        assert_eq!(store.get_optimistic_transaction_count().await.unwrap(), 6);
        let borrowed = store.peek_borrowed_transactions().await.unwrap();
        assert_eq!(borrowed.len(), 1);
        assert_eq!(
            borrowed[0].signed_transaction.hash().to_string(),
            history[1]["transactionHash"]
        );
        let (submitted, _) = store
            .get_submitted_transaction_page(6, 0, 10)
            .await
            .unwrap();
        assert_eq!(submitted.len(), 2);
        assert!(
            submitted
                .iter()
                .all(|s| s.transaction_id == request.transaction_id && s.nonce == 5)
        );
        let hydrated = store.hydrate_all_submitted(submitted).await.unwrap();
        assert_eq!(hydrated.len(), 2);
        assert!(
            !store
                .rebuild_missing_intent(&request, Some(&key), &history)
                .await
                .unwrap()
        );
        assert_eq!(store.get_borrowed_transactions_count().await.unwrap(), 1);
        assert_eq!(store.get_submitted_transactions_count().await.unwrap(), 2);
        let original = store
            .get_transaction_data(&request.transaction_id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(original.attempts.len(), 2);
        for (attempt, retained) in original.attempts.iter().zip(history.iter().rev()) {
            assert_eq!(
                attempt.details.hash().to_string(),
                retained["transactionHash"]
            );
            assert_eq!(
                store
                    .get_transaction_id_for_hash(&attempt.details.hash().to_string())
                    .await
                    .unwrap(),
                Some(request.transaction_id.clone())
            );
        }
        let _: () = store
            .redis
            .clone()
            .hdel(
                store.borrowed_transactions_hashmap_name(),
                &request.transaction_id,
            )
            .await
            .unwrap();
        let _: () = store
            .redis
            .clone()
            .del(store.transaction_attempts_list_name(&request.transaction_id))
            .await
            .unwrap();
        store
            .rebuild_missing_intent(&request, Some(&key), &history)
            .await
            .unwrap();
        assert_eq!(store.get_submitted_transactions_count().await.unwrap(), 2);

        assert_eq!(
            original.attempts[0].details.hash().to_string(),
            history[1]["transactionHash"]
        );
        cleanup(store).await;
    }

    #[tokio::test]
    #[ignore = "requires disposable Redis in TEST_REDIS_URL"]
    async fn surviving_bindings_repair_nonce_and_hash_indexes_without_resigning_or_lowering_floor()
    {
        for borrowed_survives in [true, false] {
            for floor in [None, Some(4u64), Some(9u64)] {
                let (store, request, history, key) = fixture().await;
                assert!(
                    store
                        .rebuild_missing_intent(&request, Some(&key), &history)
                        .await
                        .unwrap()
                );
                let mut redis = store.redis.clone();
                let original: Vec<String> = redis
                    .lrange(
                        store.transaction_attempts_list_name(&request.transaction_id),
                        0,
                        -1,
                    )
                    .await
                    .unwrap();
                let _: () = redis
                    .del(store.optimistic_transaction_count_key_name())
                    .await
                    .unwrap();
                if let Some(floor) = floor {
                    let _: () = redis
                        .set(store.optimistic_transaction_count_key_name(), floor)
                        .await
                        .unwrap();
                }
                for attempt in &history {
                    let _: () = redis
                        .del(store.transaction_hash_to_id_key_name(
                            attempt["transactionHash"].as_str().unwrap(),
                        ))
                        .await
                        .unwrap();
                }
                if !borrowed_survives {
                    let _: () = redis
                        .hdel(
                            store.borrowed_transactions_hashmap_name(),
                            &request.transaction_id,
                        )
                        .await
                        .unwrap();
                }
                let _: () = redis
                    .zadd(store.recycled_nonces_zset_name(), 5, 5)
                    .await
                    .unwrap();
                assert!(
                    store
                        .rebuild_missing_intent(&request, Some(&key), &history)
                        .await
                        .unwrap()
                );
                assert_eq!(
                    store.get_optimistic_transaction_count().await.unwrap(),
                    floor.unwrap_or(0).max(6)
                );
                assert_eq!(store.get_pending_transactions_count().await.unwrap(), 0);
                assert_eq!(
                    store.get_borrowed_transactions_count().await.unwrap(),
                    u64::from(borrowed_survives)
                );
                assert_eq!(
                    redis
                        .lrange::<_, Vec<String>>(
                            store.transaction_attempts_list_name(&request.transaction_id),
                            0,
                            -1
                        )
                        .await
                        .unwrap(),
                    original
                );
                assert_eq!(
                    redis
                        .zscore::<_, _, Option<u64>>(store.recycled_nonces_zset_name(), 5)
                        .await
                        .unwrap(),
                    None
                );
                for attempt in &history {
                    assert_eq!(
                        store
                            .get_transaction_id_for_hash(
                                attempt["transactionHash"].as_str().unwrap()
                            )
                            .await
                            .unwrap(),
                        Some(request.transaction_id.clone())
                    );
                }
                assert!(
                    !store
                        .rebuild_missing_intent(&request, Some(&key), &history)
                        .await
                        .unwrap()
                );
                cleanup(store).await;
            }
        }
    }

    #[tokio::test]
    #[ignore = "requires disposable Redis in TEST_REDIS_URL"]
    async fn conflicting_hash_aliases_and_invalid_nonce_floors_fail_before_any_repair() {
        for existing in [true, false] {
            for conflict in [
                "foreign-alias",
                "wrong-alias-type",
                "NaN",
                "-1",
                "1.5",
                "9007199254740992",
            ] {
                let (store, request, history, key) = fixture().await;
                let mut redis = store.redis.clone();
                if existing {
                    store
                        .rebuild_missing_intent(&request, Some(&key), &history)
                        .await
                        .unwrap();
                    let _: () = redis
                        .del(store.optimistic_transaction_count_key_name())
                        .await
                        .unwrap();
                    for attempt in &history {
                        let _: () = redis
                            .del(store.transaction_hash_to_id_key_name(
                                attempt["transactionHash"].as_str().unwrap(),
                            ))
                            .await
                            .unwrap();
                    }
                }
                let alias = store.transaction_hash_to_id_key_name(
                    history[1]["transactionHash"].as_str().unwrap(),
                );
                match conflict {
                    "foreign-alias" => {
                        let _: () = redis.set(&alias, "another-intent").await.unwrap();
                    }
                    "wrong-alias-type" => {
                        let _: () = redis.hset(&alias, "other", "type").await.unwrap();
                    }
                    invalid_floor => {
                        let _: () = redis
                            .set(store.optimistic_transaction_count_key_name(), invalid_floor)
                            .await
                            .unwrap();
                    }
                }
                assert!(
                    store
                        .rebuild_missing_intent(&request, Some(&key), &history)
                        .await
                        .is_err(),
                    "{existing} / {conflict}"
                );
                assert_eq!(
                    redis
                        .exists::<_, bool>(store.transaction_data_key_name(&request.transaction_id))
                        .await
                        .unwrap(),
                    existing
                );
                assert_eq!(
                    store.get_submitted_transactions_count().await.unwrap(),
                    if existing { 2 } else { 0 }
                );
                assert_eq!(
                    store.get_borrowed_transactions_count().await.unwrap(),
                    u64::from(existing)
                );
                assert_eq!(
                    store
                        .get_transaction_id_for_hash(
                            history[0]["transactionHash"].as_str().unwrap()
                        )
                        .await
                        .unwrap(),
                    None
                );
                let remaining: Option<String> = redis
                    .get(store.optimistic_transaction_count_key_name())
                    .await
                    .unwrap();
                assert_eq!(
                    remaining.as_deref(),
                    if conflict.ends_with("alias") || conflict == "wrong-alias-type" {
                        None
                    } else {
                        Some(conflict)
                    }
                );
                cleanup(store).await;
            }
        }
    }

    #[tokio::test]
    #[ignore = "requires disposable Redis in TEST_REDIS_URL"]
    async fn mismatched_wire_wrong_types_and_live_owner_cannot_partially_restore() {
        let (store, request, mut history, key) = fixture().await;
        history[0]["transactionHash"] = json!(B256::ZERO);
        assert!(
            store
                .rebuild_missing_intent(&request, Some(&key), &history)
                .await
                .is_err()
        );
        assert!(
            !store
                .redis
                .clone()
                .exists::<_, bool>(store.transaction_data_key_name(&request.transaction_id))
                .await
                .unwrap()
        );
        let _: () = store
            .redis
            .clone()
            .set(store.eoa_lock_key_name(), "live-owner")
            .await
            .unwrap();
        assert!(
            store
                .rebuild_missing_intent(&request, None, &[])
                .await
                .is_err()
        );
        let _: () = store
            .redis
            .clone()
            .set(store.eoa_lock_key_name(), "repair-test")
            .await
            .unwrap();
        let _: () = store
            .redis
            .clone()
            .set(
                store.transaction_attempts_list_name(&request.transaction_id),
                "wrong-type",
            )
            .await
            .unwrap();
        assert!(
            store
                .rebuild_missing_intent(&request, None, &[])
                .await
                .is_err()
        );
        assert!(
            !store
                .redis
                .clone()
                .exists::<_, bool>(store.transaction_data_key_name(&request.transaction_id))
                .await
                .unwrap()
        );
        cleanup(store).await;
    }
}
