use super::*;

/// Model a lease expiring while the old sender is still waiting on its bundler.
/// The resumed sender's success/failure hooks must not delete the replacement
/// owner's lock or publish stale cache state.
#[tokio::test]
#[ignore = "requires disposable Redis in TEST_REDIS_URL"]
async fn stale_completion_cannot_release_or_cache_for_a_new_owner() {
    let client = twmq::redis::Client::open(
        std::env::var("TEST_REDIS_URL").expect("select disposable Redis"),
    )
    .unwrap();
    let namespace = format!("deployment-owner:{}", Uuid::new_v4());
    let lock = RedisDeploymentLock::new(client.clone())
        .await
        .unwrap()
        .with_namespace(Some(namespace.clone()));
    let cache = RedisDeploymentCache::new(client.clone())
        .await
        .unwrap()
        .with_namespace(Some(namespace.clone()));
    let address = Address::repeat_byte(1);
    let chain_id = 31337;
    let old_id = match lock.acquire_lock(chain_id, &address).await.unwrap() {
        AcquireLockResult::Acquired(id) => id,
        _ => panic!("isolated account must be unlocked"),
    };
    let mut conn = lock.conn().clone();
    let ttl: i64 = conn.ttl(lock.lock_key(chain_id, &address)).await.unwrap();
    assert!((1..=LOCK_TTL_SECONDS as i64).contains(&ttl));

    // Expire the first lease without sleeping or relying on a timing race.
    let _: bool = conn
        .expire(lock.lock_key(chain_id, &address), 0)
        .await
        .unwrap();
    let new_id = match lock.acquire_lock(chain_id, &address).await.unwrap() {
        AcquireLockResult::Acquired(id) => id,
        _ => panic!("expired lock must be available"),
    };
    assert_ne!(old_id, new_id);
    let mut stale_completion = twmq::redis::pipe();
    stale_completion.atomic();
    lock.release_lock_and_update_cache_with_pipeline(
        &mut stale_completion,
        chain_id,
        &address,
        &old_id,
        true,
    );
    stale_completion.query_async::<()>(&mut conn).await.unwrap();
    assert_eq!(lock.check_lock(chain_id, &address).await.unwrap().0, new_id);
    assert_eq!(cache.is_deployed(chain_id, &address).await, None);

    let mut stale_failure = twmq::redis::pipe();
    stale_failure.atomic();
    lock.release_lock_with_pipeline(&mut stale_failure, chain_id, &address, &old_id);
    stale_failure.query_async::<()>(&mut conn).await.unwrap();
    assert_eq!(lock.check_lock(chain_id, &address).await.unwrap().0, new_id);

    // Identical chain/account in another execution namespace has independent
    // ownership and deployment knowledge.
    let other_namespace = format!("{namespace}:other");
    let other_lock = RedisDeploymentLock::new(client.clone())
        .await
        .unwrap()
        .with_namespace(Some(other_namespace.clone()));
    let other_cache = RedisDeploymentCache::new(client)
        .await
        .unwrap()
        .with_namespace(Some(other_namespace));
    let other_id = match other_lock.acquire_lock(chain_id, &address).await.unwrap() {
        AcquireLockResult::Acquired(id) => id,
        _ => panic!("namespaces must not share deployment locks"),
    };
    let mut current_completion = twmq::redis::pipe();
    current_completion.atomic();
    lock.release_lock_and_update_cache_with_pipeline(
        &mut current_completion,
        chain_id,
        &address,
        &new_id,
        true,
    );
    current_completion
        .query_async::<()>(&mut conn)
        .await
        .unwrap();
    assert!(lock.check_lock(chain_id, &address).await.is_none());
    assert_eq!(cache.is_deployed(chain_id, &address).await, Some(true));
    assert_eq!(
        other_lock.check_lock(chain_id, &address).await.unwrap().0,
        other_id
    );
    assert_eq!(other_cache.is_deployed(chain_id, &address).await, None);

    let keys: Vec<String> = conn.keys(format!("{namespace}:*")).await.unwrap();
    let _: () = conn.del(keys).await.unwrap();
}

#[test]
fn queued_error_retains_lock_identity_and_legacy_error_cannot_unlock() {
    use crate::external_bundler::send::ExternalBundlerSendError;
    let address = Address::repeat_byte(2);
    let error = ExternalBundlerSendError::UserOpBuildFailed {
        account_address: address,
        nonce_used: alloy::primitives::U256::ZERO,
        had_deployment_lock: true,
        deployment_lock_id: Some("attempt-owner".into()),
        stage: "BUILDING".into(),
        message: "synthetic build failure".into(),
        inner_error: None,
    };
    let mut persisted = serde_json::to_value(error).unwrap();
    let restored: ExternalBundlerSendError = serde_json::from_value(persisted.clone()).unwrap();
    assert_eq!(restored.acquired_lock(), Some((address, "attempt-owner")));
    persisted
        .as_object_mut()
        .unwrap()
        .remove("deployment_lock_id");
    let legacy: ExternalBundlerSendError = serde_json::from_value(persisted).unwrap();
    assert_eq!(
        legacy.acquired_lock(),
        None,
        "a historical boolean cannot authorize an unlock"
    );
}
