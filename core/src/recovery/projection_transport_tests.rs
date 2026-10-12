//! A local RESP proxy loses a successful Redis response after the server has
//! executed the actual CAS. The production connection manager sees EOF.
use super::*;
#[path = "../../../test-support/redis_reply_loss.rs"]
mod redis_reply_loss;
use redis_reply_loss::ReplyLossProxy;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires disposable Redis in TEST_REDIS_URL"]
async fn lost_successful_cas_reply_retains_pending_without_returning_permission() {
    let f = Fixture::fresh();
    f.initialize().await;
    let proxy = ReplyLossProxy::start(&f.url).await;
    let namespace = RecoveryJournal::status(&f.path).unwrap().namespace;
    let j = RecoveryJournal::open(&f.path, &proxy.url, namespace)
        .await
        .unwrap();
    admit(&j, "eoa", "a", request("a")).await;
    let before = RecoveryJournal::status(&f.path).unwrap();
    proxy.armed.store(true, Ordering::SeqCst);
    let outcome = tokio::time::timeout(
        Duration::from_secs(10),
        j.before_broadcast("eoa", "a", replay(), attempt()),
    )
    .await
    .unwrap();
    assert!(matches!(outcome, Err(RecoveryError::PublicationPending)));
    assert!(proxy.dropped.load(Ordering::SeqCst));
    let pending = RecoveryJournal::status(&f.path).unwrap();
    assert!(!pending.halted && pending.projection_pending);
    assert_eq!(pending.attempts, 1);
    assert_eq!(pending.checkpoint, before.checkpoint + 1);
    let conn = connection(&f.path, true).unwrap();
    let current = control(&conn).unwrap();
    drop(conn);
    let mut direct = redis_connection(&f.url).await.unwrap();
    assert_eq!(
        direct.get::<_, String>(current.key()).await.unwrap(),
        current.token(),
        "Redis must have applied the CAS whose response was lost"
    );
    // Health may finish acknowledgement, but it has no attempt payload or
    // network dispatcher and cannot return the failed caller's permission.
    tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            match j.ensure_healthy().await {
                Ok(()) => break,
                Err(RecoveryError::PublicationPending) => {
                    tokio::time::sleep(Duration::from_millis(10)).await
                }
                Err(error) => {
                    panic!("unexpected error repairing exact pending transition: {error}")
                }
            }
        }
    })
    .await
    .unwrap();
    assert!(!RecoveryJournal::status(&f.path).unwrap().projection_pending);
    j.before_broadcast("eoa", "a", replay(), attempt())
        .await
        .unwrap();
    assert_eq!(
        RecoveryJournal::status(&f.path).unwrap().checkpoint,
        pending.checkpoint
    );
    let changed = replay().replace(":0", ":1");
    assert!(
        j.before_broadcast("eoa", "a", &changed, attempt())
            .await
            .is_err()
    );
    drop(j);
    drop(proxy);
    f.cleanup(&[]).await;
}
