use super::*;
use crate::recovery::tests::{Fixture, admit, request};
use serde_json::json;

const REPLAY: &str = "evm:31337:0x1111111111111111111111111111111111111111:0";

fn failure() -> Value {
    json!({"errorCode":"TRANSACTION_BUILD_FAILED","message":"gas limit cannot be represented"})
}

#[tokio::test]
#[ignore = "requires disposable Redis in TEST_REDIS_URL"]
async fn unsigned_preparation_failure_survives_restart_and_excludes_rebuild_and_retry() {
    let fixture = Fixture::fresh();
    fixture.initialize().await;
    let journal = fixture.open().await;
    let payload = request("rejected");
    admit(&journal, "eoa", "rejected", payload.clone()).await;
    let checkpoint = RecoveryJournal::status(&fixture.path).unwrap().checkpoint;
    assert_eq!(
        journal
            .record_eoa_preparation_failure("rejected", &payload, failure())
            .await
            .unwrap(),
        Some(failure())
    );
    // Cut the process before any business projection/status/webhook is written.
    // A fresh owner recovers the first error without reauthorizing work.
    drop(journal);
    let journal = fixture.open().await;
    assert_eq!(
        journal
            .eoa_preparation_failure("rejected", &payload)
            .await
            .unwrap(),
        Some(failure())
    );
    assert_eq!(
        journal
            .record_eoa_preparation_failure(
                "rejected",
                &payload,
                json!({"different":"cleanup error"})
            )
            .await
            .unwrap(),
        Some(failure())
    );
    let reservation = admit(&journal, "eoa", "rejected", payload.clone()).await;
    assert!(reservation.terminal);
    let record = journal.admission("eoa", "rejected").await.unwrap().unwrap();
    assert_eq!(record.state, AdmissionState::Terminal);
    assert!(record.replay_key.is_none());
    assert!(journal.rebuild_page(None).await.unwrap().is_empty());
    assert!(matches!(
        journal
            .before_broadcast("eoa", "rejected", REPLAY, json!({"wire":"must not send"}))
            .await,
        Err(RecoveryError::RecoveryRequired(_))
    ));
    let status = RecoveryJournal::status(&fixture.path).unwrap();
    assert_eq!(status.attempts, 0);
    assert_eq!(
        status.checkpoint,
        checkpoint + 1,
        "retries must not append evidence or advance authority"
    );
    assert!(matches!(
        journal
            .eoa_preparation_failure("rejected", &json!({"changed":true}))
            .await,
        Err(RecoveryError::Conflict)
    ));
    assert!(matches!(
        journal
            .record_eoa_preparation_failure("rejected", &json!({"changed":true}), failure())
            .await,
        Err(RecoveryError::Conflict)
    ));
    journal.halt("negative authority fixture").await;
    assert!(matches!(
        journal.eoa_preparation_failure("rejected", &payload).await,
        Err(RecoveryError::RecoveryRequired(_))
    ));
    drop(journal);
    fixture.cleanup(&[]).await;
}

#[tokio::test]
#[ignore = "requires disposable Redis in TEST_REDIS_URL"]
async fn preparation_failure_and_broadcast_have_one_durable_winner() {
    for failure_first in [true, false] {
        let fixture = Fixture::fresh();
        fixture.initialize().await;
        let journal = fixture.open().await;
        admit(&journal, "eoa", "race", request("race")).await;
        let checkpoint = RecoveryJournal::status(&fixture.path).unwrap().checkpoint;
        let (entered, entering) = tokio::sync::oneshot::channel();
        let (resume, resuming) = tokio::sync::oneshot::channel();
        *journal.before_mirror.lock().unwrap() = Some((entered, resuming));
        let operation = |journal: Arc<RecoveryJournal>, reject| async move {
            if reject {
                journal
                    .record_eoa_preparation_failure("race", &request("race"), failure())
                    .await
                    .map(|proof| proof.is_some())
            } else {
                journal
                    .before_broadcast("eoa", "race", REPLAY, json!({"wire":"authorized original"}))
                    .await
                    .map(|()| false)
            }
        };
        let first = tokio::spawn(operation(journal.clone(), failure_first));
        tokio::time::timeout(Duration::from_secs(5), entering)
            .await
            .unwrap()
            .unwrap();
        let second = tokio::spawn(operation(journal.clone(), !failure_first));
        assert!(
            !second.is_finished(),
            "the competing decision must wait for the committed owner"
        );
        resume.send(()).unwrap();
        let first = first.await.unwrap();
        let second = second.await.unwrap();
        if failure_first {
            assert!(first.unwrap());
            assert!(matches!(second, Err(RecoveryError::RecoveryRequired(_))));
        } else {
            assert!(!first.unwrap());
            assert!(
                !second.unwrap(),
                "authorized attempts cannot become unsigned failures"
            );
        }
        drop(journal);
        let journal = fixture.open().await;
        let record = journal.admission("eoa", "race").await.unwrap().unwrap();
        assert_eq!(
            record.state,
            if failure_first {
                AdmissionState::Terminal
            } else {
                AdmissionState::Admitted
            }
        );
        assert_eq!(record.replay_key.is_some(), !failure_first);
        assert_eq!(
            journal
                .eoa_preparation_failure("race", &request("race"))
                .await
                .unwrap()
                .is_some(),
            failure_first
        );
        let status = RecoveryJournal::status(&fixture.path).unwrap();
        assert_eq!(status.checkpoint, checkpoint + 1);
        assert_eq!(status.attempts, u64::from(!failure_first));
        drop(journal);
        fixture.cleanup(&[]).await;
    }
}

#[tokio::test]
#[ignore = "requires disposable Redis in TEST_REDIS_URL"]
async fn preparation_failure_requires_exact_unsigned_eoa_and_owned_complete_publication() {
    let fixture = Fixture::fresh();
    fixture.initialize().await;
    let journal = fixture.open().await;
    for (kind, id) in [
        ("eoa", "rejected"),
        ("eoa", "attempted"),
        ("erc4337", "foreign"),
    ] {
        admit(&journal, kind, id, request(id)).await;
    }
    assert!(matches!(
        journal
            .record_eoa_preparation_failure("foreign", &request("foreign"), failure())
            .await,
        Err(RecoveryError::Conflict)
    ));
    assert!(matches!(
        journal
            .record_eoa_preparation_failure("missing", &request("missing"), failure())
            .await,
        Err(RecoveryError::RecoveryRequired(_))
    ));
    journal
        .before_broadcast("eoa", "attempted", REPLAY, json!({"wire":"retained"}))
        .await
        .unwrap();
    // Even inconsistent legacy projection/metadata must not erase a retained
    // attempt or turn quarantine into permission to announce permanent failure.
    journal
        .db(|conn| {
            conn.execute(
                "UPDATE admissions SET state='quarantined',replay_key=NULL WHERE id='attempted'",
                [],
            )?;
            Ok(())
        })
        .await
        .unwrap();
    assert!(
        journal
            .record_eoa_preparation_failure("attempted", &request("attempted"), failure())
            .await
            .unwrap()
            .is_none()
    );
    assert!(
        journal
            .eoa_preparation_failure("attempted", &request("attempted"))
            .await
            .unwrap()
            .is_none()
    );
    let (entered, entering) = tokio::sync::oneshot::channel();
    let (resume, resuming) = tokio::sync::oneshot::channel();
    *journal.before_mirror.lock().unwrap() = Some((entered, resuming));
    let caller_journal = journal.clone();
    let caller = tokio::spawn(async move {
        caller_journal
            .record_eoa_preparation_failure("rejected", &request("rejected"), failure())
            .await
    });
    tokio::time::timeout(Duration::from_secs(5), entering)
        .await
        .unwrap()
        .unwrap();
    caller.abort();
    assert!(caller.await.unwrap_err().is_cancelled());
    assert!(
        journal.serial.try_lock().is_err(),
        "caller cancellation cannot release publication serialization"
    );
    assert!(matches!(
        owner_lock(&fixture.path, false),
        Err(RecoveryError::Locked)
    ));
    resume.send(()).unwrap();
    journal.ensure_healthy().await.unwrap();
    assert_eq!(
        journal
            .eoa_preparation_failure("rejected", &request("rejected"))
            .await
            .unwrap(),
        Some(failure())
    );
    drop(journal);
    fixture.cleanup(&[]).await;
}

#[tokio::test]
#[ignore = "requires disposable Redis in TEST_REDIS_URL"]
async fn another_terminal_or_cancelled_decision_is_not_a_preparation_failure_or_host_halt() {
    let fixture = Fixture::fresh();
    fixture.initialize().await;
    let journal = fixture.open().await;
    admit(&journal, "erc4337", "cancelled", request("cancelled")).await;
    assert!(journal.cancel_unbroadcast("cancelled").await.unwrap());
    assert!(matches!(
        journal
            .record_eoa_preparation_failure("cancelled", &request("cancelled"), failure())
            .await,
        Err(RecoveryError::Conflict)
    ));
    assert!(matches!(
        journal
            .eoa_preparation_failure("cancelled", &request("cancelled"))
            .await,
        Err(RecoveryError::Conflict)
    ));
    // Compatibility fixture for another unsigned terminal witness. Current EOA
    // chain-finality writes require attempts; future/legacy terminal evidence
    // must remain a refusal, not an unrelated whole-host storage incident.
    admit(&journal, "eoa", "other-terminal", request("other-terminal")).await;
    journal
        .db(|conn| {
            conn.execute(
                "INSERT INTO terminal_evidence(id,evidence) VALUES('other-terminal',?)",
                [json!({"kind":"other_unsigned_terminal"}).to_string()],
            )?;
            conn.execute(
                "UPDATE admissions SET state='terminal' WHERE id='other-terminal'",
                [],
            )?;
            Ok(())
        })
        .await
        .unwrap();
    assert!(
        journal
            .eoa_preparation_failure("other-terminal", &request("other-terminal"))
            .await
            .unwrap()
            .is_none()
    );
    assert!(
        journal
            .record_eoa_preparation_failure("other-terminal", &request("other-terminal"), failure())
            .await
            .unwrap()
            .is_none()
    );
    journal.ensure_healthy().await.unwrap();
    assert!(!RecoveryJournal::status(&fixture.path).unwrap().halted);
    assert!(journal.rebuild_page(None).await.unwrap().is_empty());
    assert!(matches!(
        journal
            .before_broadcast(
                "eoa",
                "other-terminal",
                REPLAY,
                json!({"wire":"must not send"})
            )
            .await,
        Err(RecoveryError::RecoveryRequired(_))
    ));
    // Malformed evidence claiming our own witness type remains fail-closed.
    journal
        .db(|conn| {
            conn.execute(
                "UPDATE terminal_evidence SET evidence=? WHERE id='other-terminal'",
                [json!({"kind":WITNESS_KIND,"error":null}).to_string()],
            )?;
            Ok(())
        })
        .await
        .unwrap();
    assert!(matches!(
        journal
            .eoa_preparation_failure("other-terminal", &request("other-terminal"))
            .await,
        Err(RecoveryError::RecoveryRequired("ledger unavailable"))
    ));
    let status = RecoveryJournal::status(&fixture.path).unwrap();
    assert!(status.halted);
    assert_eq!(status.halt_reason.as_deref(), Some("ledger unavailable"));
    drop(journal);
    fixture.cleanup(&[]).await;
}
