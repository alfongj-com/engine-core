//! Abort each public mutation after its FULL commit and before the Redis CAS.
use super::{
    tests::{Fixture, admit, request},
    *,
};
use serde_json::json;

#[tokio::test]
#[ignore = "requires disposable Redis in TEST_REDIS_URL"]
async fn cancelled_mutations_finish_mirroring_and_retain_owner_and_serialization() {
    for operation in [
        "admission",
        "attempt",
        "terminal",
        "checkpoint",
        "chain_halt",
    ] {
        let f = Fixture::fresh();
        f.initialize().await;
        let j = f.open().await;
        let replay = "evm:31337:0x1111111111111111111111111111111111111111:0";
        let hash = alloy::primitives::B256::repeat_byte(1);
        if operation != "admission" {
            admit(&j, "eoa", "a", request("a")).await;
        }
        if operation == "terminal" {
            j.before_broadcast(
                "eoa",
                "a",
                replay,
                json!({"transactionHash":hash,"wire":"fixture"}),
            )
            .await
            .unwrap();
        }
        let prior = RecoveryJournal::status(&f.path).unwrap().checkpoint;
        let (entered, entering) = tokio::sync::oneshot::channel();
        let (resume, resuming) = tokio::sync::oneshot::channel();
        *j.before_mirror.lock().unwrap() = Some((entered, resuming));
        let caller_journal = j.clone();
        let caller = tokio::spawn(async move {
            match operation {
                "admission" => { admit(&caller_journal, "eoa", "a", request("a")).await; Ok(()) }
                "attempt" => caller_journal.before_broadcast("eoa", "a", replay, json!({"transactionHash":hash,"wire":"fixture"})).await,
                "terminal" => caller_journal.record_terminal("eoa", "a", json!({"chainId":31337,"transactionHash":hash,"outcome":"success","finality":{"blockNumber":10,"blockHash":"0xdef","checkpointNumber":12,"checkpointHash":"head","policy":{"mode":"finalized"}}})).await,
                "checkpoint" => caller_journal.commit_checkpoint(31337, None, crate::finality::FinalityEvidence {
                    block_number:10, block_hash:alloy::primitives::B256::repeat_byte(10),
                    checkpoint_number:12, checkpoint_hash:alloy::primitives::B256::repeat_byte(12),
                    policy:crate::finality::FinalityPolicy::Finalized,
                }).await.map(|_| ()),
                "chain_halt" => caller_journal.halt_chain(31337, "fixture").await,
                _ => unreachable!(),
            }
        });
        tokio::time::timeout(Duration::from_secs(5), entering)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            RecoveryJournal::status(&f.path).unwrap().checkpoint,
            prior + 1
        );
        caller.abort();
        assert!(matches!(caller.await, Err(error) if error.is_cancelled()));
        assert!(
            j.serial.try_lock().is_err(),
            "{operation} released serialization before mirror"
        );
        if operation == "admission" {
            assert_eq!(
                j.admission_slots.available_permits(),
                MAX_CONCURRENT_ADMISSIONS - 1
            );
        }
        let weak = Arc::downgrade(&j);
        drop(j);
        assert!(matches!(
            owner_lock(&f.path, false),
            Err(RecoveryError::Locked)
        ));
        let observer = weak
            .upgrade()
            .expect("detached mutation retains journal owner");
        let health = tokio::spawn(async move { observer.ensure_healthy().await });
        resume.send(()).unwrap();
        tokio::time::timeout(Duration::from_secs(5), health)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        // Opening a fresh handle also verifies the marker exactly equals the
        // independent authority, with no reattach/recover or new namespace.
        let reopened = tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                match RecoveryJournal::open(
                    &f.path,
                    &f.url,
                    RecoveryJournal::status(&f.path).unwrap().namespace,
                )
                .await
                {
                    Ok(j) => break j,
                    Err(RecoveryError::Locked) => tokio::task::yield_now().await,
                    Err(error) => panic!("{operation}: {error}"),
                }
            }
        })
        .await
        .unwrap();
        assert_eq!(
            reopened.admission_slots.available_permits(),
            MAX_CONCURRENT_ADMISSIONS
        );
        assert_eq!(
            RecoveryJournal::status(&f.path).unwrap().checkpoint,
            prior + 1
        );
        if operation == "chain_halt" {
            assert!(reopened.check_chain_healthy(31337).await.is_err());
        }
        drop(reopened);
        f.cleanup(&[]).await;
    }
}

#[tokio::test]
#[ignore = "requires disposable Redis in TEST_REDIS_URL"]
async fn cancelled_db_task_retains_owner_until_its_blocking_closure_finishes() {
    let f = Fixture::fresh();
    f.initialize().await;
    let j = f.open().await;
    let (entered, entering) = tokio::sync::oneshot::channel();
    let (resume, resuming) = std::sync::mpsc::channel();
    let caller = tokio::spawn(async move {
        j.db(move |conn| {
            let _ = entered.send(());
            resuming.recv_timeout(Duration::from_secs(5)).unwrap();
            control(conn).map(|_| ())
        })
        .await
    });
    tokio::time::timeout(Duration::from_secs(5), entering)
        .await
        .unwrap()
        .unwrap();
    caller.abort();
    assert!(matches!(caller.await, Err(error) if error.is_cancelled()));
    assert!(matches!(
        owner_lock(&f.path, false),
        Err(RecoveryError::Locked)
    ));
    resume.send(()).unwrap();
    let owner = tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            match owner_lock(&f.path, false) {
                Ok(owner) => break owner,
                Err(RecoveryError::Locked) => tokio::task::yield_now().await,
                Err(error) => panic!("{error}"),
            }
        }
    })
    .await
    .unwrap();
    drop(owner);
    let j = f.open().await;
    j.ensure_healthy().await.unwrap();
    drop(j);
    f.cleanup(&[]).await;
}

#[tokio::test]
#[ignore = "requires disposable Redis in TEST_REDIS_URL"]
async fn graceful_shutdown_drains_cancelled_mutations_through_redis_publication() {
    let f = Fixture::fresh();
    f.initialize().await;
    let j = f.open().await;
    // On this current-thread runtime, the spawned task cannot be polled until
    // we yield. Registration must already prevent a shutdown write guard.
    let mut unpolled = Box::pin(j.run_owned(|_| async { Ok(()) }));
    std::future::poll_fn(|cx| {
        assert!(unpolled.as_mut().poll(cx).is_pending());
        assert!(j.owned_operations.try_write().is_err());
        std::task::Poll::Ready(())
    })
    .await;
    drop(unpolled);
    j.drain_mutations().await;
    let (entered, entering) = tokio::sync::oneshot::channel();
    let (resume, resuming) = tokio::sync::oneshot::channel();
    *j.before_mirror.lock().unwrap() = Some((entered, resuming));
    let caller_journal = j.clone();
    let caller = tokio::spawn(async move {
        admit(&caller_journal, "eoa", "shutdown", request("shutdown")).await
    });
    tokio::time::timeout(Duration::from_secs(5), entering)
        .await
        .unwrap()
        .unwrap();
    caller.abort();
    assert!(matches!(caller.await, Err(error) if error.is_cancelled()));
    let mut drain = Box::pin(j.drain_mutations());
    std::future::poll_fn(|cx| {
        assert!(
            drain.as_mut().poll(cx).is_pending(),
            "shutdown returned before the cancelled mutation published Redis"
        );
        std::task::Poll::Ready(())
    })
    .await;
    resume.send(()).unwrap();
    tokio::time::timeout(Duration::from_secs(5), drain)
        .await
        .unwrap();
    j.ensure_healthy().await.unwrap();
    assert!(j.admission("eoa", "shutdown").await.unwrap().is_some());
    drop(j);
    let j = f.open().await;
    j.ensure_healthy().await.unwrap();
    drop(j);
    f.cleanup(&[]).await;
}
