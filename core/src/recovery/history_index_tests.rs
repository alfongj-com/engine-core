//! Restart a populated pre-index journal through the production startup path.
use super::{
    tests::{Fixture, admit, request},
    *,
};
use alloy::primitives::B256;
use serde_json::json;
use twmq::redis::AsyncCommands;

fn replay(nonce: u64) -> String {
    format!("evm:31337:0x1111111111111111111111111111111111111111:{nonce}")
}
fn attempt(byte: u8) -> Value {
    json!({"transactionHash": B256::repeat_byte(byte), "wire": format!("fixture-{byte}")})
}
fn proof(outcome: &str, checkpoint: u64) -> Value {
    json!({"chainId":31337, "transactionHash":B256::repeat_byte(3), "outcome":outcome,
        "finality":{"blockNumber":10,"blockHash":"fixed-receipt", "checkpointNumber":checkpoint,
        "checkpointHash":format!("checkpoint-{checkpoint}"),"policy":{"mode":"finalized"}}})
}
fn remove_indexes(f: &Fixture) {
    let _owner = owner_lock(&f.path, false).unwrap();
    connection(&f.path, false).unwrap().execute_batch(
        "DROP INDEX IF EXISTS terminal_evidence_id_sequence; DROP INDEX IF EXISTS attempts_id_sequence;"
    ).unwrap();
}
fn indexes(f: &Fixture) -> Vec<String> {
    let conn = connection(&f.path, true).unwrap();
    let mut statement = conn.prepare("SELECT name FROM sqlite_schema WHERE type='index' AND name IN ('terminal_evidence_id_sequence','attempts_id_sequence') ORDER BY name").unwrap();
    let rows = statement
        .query_map([], |r| r.get(0))
        .unwrap()
        .collect::<std::result::Result<_, _>>()
        .unwrap();
    rows
}
fn snapshot(f: &Fixture, name: &str) -> Vec<u8> {
    let output = f.path.with_file_name(name);
    RecoveryJournal::export(&f.path, &output).unwrap();
    fs::read(output).unwrap()
}
fn query_plan(f: &Fixture, sql: &str) -> String {
    let conn = connection(&f.path, true).unwrap();
    let mut statement = conn.prepare(&format!("EXPLAIN QUERY PLAN {sql}")).unwrap();
    let rows = statement
        .query_map(["pending"], |r| r.get::<_, String>(3))
        .unwrap()
        .collect::<std::result::Result<Vec<_>, _>>()
        .unwrap();
    rows.join("; ")
}
async fn open_result(f: &Fixture) -> Result<Arc<RecoveryJournal>> {
    RecoveryJournal::open(&f.path, &f.url, RecoveryJournal::status(&f.path)?.namespace).await
}

#[tokio::test]
#[ignore = "requires disposable Redis in TEST_REDIS_URL"]
async fn legacy_restart_indexes_preserve_wire_history_first_proof_and_conflict_halt() {
    let f = Fixture::fresh();
    f.initialize().await;
    assert_eq!(
        indexes(&f).len(),
        2,
        "fresh initialization needs both indexes"
    );
    let j = f.open().await;
    admit(&j, "eoa", "pending", request("pending")).await;
    for byte in [1, 2] {
        j.before_broadcast("eoa", "pending", &replay(0), attempt(byte))
            .await
            .unwrap();
    }
    admit(&j, "eoa", "done", request("done")).await;
    j.before_broadcast("eoa", "done", &replay(1), attempt(3))
        .await
        .unwrap();
    let original = proof("success", 20);
    j.record_terminal("eoa", "done", original.clone())
        .await
        .unwrap();
    drop(j);
    remove_indexes(&f); // Exactly the prior schema, with real populated journal records.
    assert!(indexes(&f).is_empty());
    let before = snapshot(&f, "before.json");
    let status = RecoveryJournal::status(&f.path).unwrap();
    let mut redis = redis_connection(&f.url).await.unwrap();
    let key = marker_key(&status.namespace);
    let marker_before: String = redis.get(&key).await.unwrap();

    let j = f.open().await; // Production migration, not a direct helper test.
    assert_eq!(snapshot(&f, "after.json"), before);
    assert_eq!(redis.get::<_, String>(&key).await.unwrap(), marker_before);
    assert_eq!(indexes(&f).len(), 2);
    for (sql, index) in [
        (
            "SELECT replay_key,payload FROM attempts WHERE id=? ORDER BY sequence DESC LIMIT 1",
            "attempts_id_sequence",
        ),
        (
            "SELECT payload FROM attempts WHERE id=? ORDER BY sequence",
            "attempts_id_sequence",
        ),
        (
            "SELECT evidence FROM terminal_evidence WHERE id=? ORDER BY sequence LIMIT 1",
            "terminal_evidence_id_sequence",
        ),
    ] {
        let plan = query_plan(&f, sql);
        assert!(
            plan.contains(index) && !plan.contains("TEMP B-TREE") && !plan.contains("SCAN "),
            "{plan}"
        );
    }
    assert_eq!(
        j.latest_eoa_attempt("pending", &replay(0)).await.unwrap(),
        Some(attempt(2))
    );
    // The original, superseded wire remains an eligible recorded identity.
    for byte in [1, 2] {
        j.validate_attempt_identity(
            "eoa",
            "pending",
            &json!({"chainId":31337,"transactionHash":B256::repeat_byte(byte)}),
        )
        .await
        .unwrap();
    }
    j.record_terminal("eoa", "done", proof("success", 30))
        .await
        .unwrap();
    assert_eq!(
        RecoveryJournal::status(&f.path).unwrap().checkpoint,
        status.checkpoint
    );
    assert_eq!(
        j.terminal_evidence("eoa", "done").await.unwrap(),
        Some(original.clone())
    );
    drop(j);
    let j = f.open().await; // Repeated startup is idempotent.
    assert_eq!(snapshot(&f, "reopened.json"), before);
    assert!(
        j.record_terminal("eoa", "done", proof("reverted", 30))
            .await
            .is_err()
    );
    assert!(j.ensure_healthy().await.is_err());
    assert_eq!(
        j.terminal_evidence("eoa", "done").await.unwrap(),
        Some(original)
    );
    let conn = connection(&f.path, true).unwrap();
    let records: Vec<String> = conn
        .prepare("SELECT evidence FROM terminal_evidence WHERE id='done' ORDER BY sequence")
        .unwrap()
        .query_map([], |r| r.get(0))
        .unwrap()
        .collect::<std::result::Result<_, _>>()
        .unwrap();
    assert_eq!(
        records.len(),
        2,
        "contradictory evidence remains separate and ordered"
    );
    assert_eq!(
        serde_json::from_str::<Value>(&records[0]).unwrap()["outcome"],
        "success"
    );
    assert_eq!(
        serde_json::from_str::<Value>(&records[1]).unwrap()["outcome"],
        "reverted"
    );
    assert_eq!(
        RecoveryJournal::status(&f.path)
            .unwrap()
            .halt_reason
            .as_deref(),
        Some("terminal evidence conflict")
    );
    drop(conn);
    drop(j);
    f.cleanup(&[]).await;
}

#[tokio::test]
#[ignore = "requires disposable Redis in TEST_REDIS_URL"]
async fn legacy_readonly_inventory_and_locked_open_never_migrate() {
    let f = Fixture::fresh();
    f.initialize().await;
    remove_indexes(&f);
    let before = snapshot(&f, "readonly-before.json");
    RecoveryJournal::status(&f.path).unwrap();
    assert_eq!(snapshot(&f, "readonly-after.json"), before);
    assert!(indexes(&f).is_empty());
    let owner = owner_lock(&f.path, false).unwrap();
    assert!(matches!(open_result(&f).await, Err(RecoveryError::Locked)));
    assert!(indexes(&f).is_empty());
    drop(owner);
    f.cleanup(&[]).await;
}

#[tokio::test]
#[ignore = "requires disposable Redis in TEST_REDIS_URL"]
async fn legacy_unhealthy_projection_is_fenced_before_index_ddl() {
    let f = Fixture::fresh();
    f.initialize().await;
    remove_indexes(&f);
    let old = RecoveryJournal::status(&f.path).unwrap();
    let mut redis = redis_connection(&f.url).await.unwrap();
    let _: () = redis
        .set(marker_key(&old.namespace), "not-the-owned-checkpoint")
        .await
        .unwrap();
    assert!(matches!(
        open_result(&f).await,
        Err(RecoveryError::RecoveryRequired(_))
    ));
    assert!(
        indexes(&f).is_empty(),
        "unsafe projection must be fenced before migration"
    );
    let new = RecoveryJournal::status(&f.path).unwrap();
    assert!(new.halted);
    assert_eq!(new.checkpoint, old.checkpoint);
    f.cleanup(&[]).await;
}

#[tokio::test]
#[ignore = "requires disposable Redis in TEST_REDIS_URL"]
async fn legacy_second_index_failure_rolls_back_first_and_refuses_startup() {
    let f = Fixture::fresh();
    f.initialize().await;
    remove_indexes(&f);
    let before = snapshot(&f, "ddl-before.json");
    {
        let _owner = owner_lock(&f.path, false).unwrap();
        // Force the SECOND DDL statement to fail after the first has run.
        connection(&f.path, false)
            .unwrap()
            .execute_batch("CREATE TABLE attempts_id_sequence(dummy INTEGER)")
            .unwrap();
    }
    assert!(matches!(open_result(&f).await, Err(RecoveryError::Storage)));
    assert!(
        indexes(&f).is_empty(),
        "first CREATE INDEX must roll back too"
    );
    assert_eq!(snapshot(&f, "ddl-after.json"), before);
    {
        let _owner = owner_lock(&f.path, false).unwrap();
        connection(&f.path, false)
            .unwrap()
            .execute_batch("DROP TABLE attempts_id_sequence")
            .unwrap();
    }
    let j = f.open().await;
    assert_eq!(indexes(&f).len(), 2);
    assert_eq!(snapshot(&f, "ddl-retry.json"), before);
    drop(j);
    f.cleanup(&[]).await;
}

#[tokio::test]
#[ignore = "requires disposable Redis in TEST_REDIS_URL"]
async fn cancelled_startup_retains_owner_until_blocking_ddl_finishes() {
    let f = Fixture::fresh();
    f.initialize().await;
    let j = f.open().await;
    // Remove only physical indexes while this fixture owns the journal. The
    // helper below is the same owned startup operation that open() awaits.
    j.db(|conn| {
        conn.execute_batch(
            "DROP INDEX terminal_evidence_id_sequence; DROP INDEX attempts_id_sequence;",
        )?;
        Ok(())
    })
    .await
    .unwrap();
    let writer = connection(&f.path, false).unwrap();
    writer.execute_batch("BEGIN IMMEDIATE").unwrap();
    let (entered, entering) = tokio::sync::oneshot::channel();
    let (handle_sent, handle_received) = tokio::sync::oneshot::channel();
    let caller = tokio::spawn(async move {
        let startup = RecoveryJournal::prepare_owned_startup(j, move || {
            let _ = entered.send(());
        });
        let _ = handle_sent.send(startup.abort_handle());
        startup.await
    });
    let startup_abort = handle_received.await.unwrap();
    tokio::time::timeout(Duration::from_secs(5), entering)
        .await
        .unwrap()
        .unwrap();
    // DDL closure has entered, but its BEGIN IMMEDIATE cannot complete while
    // the separate WAL writer holds its reservation. No scheduling sleep is
    // used to infer this point.
    caller.abort();
    assert!(matches!(caller.await, Err(error) if error.is_cancelled()));
    assert!(matches!(
        owner_lock(&f.path, false),
        Err(RecoveryError::Locked)
    ));
    // Also cancel the async startup task: the blocking closure must own the OS
    // lock independently, as it does when an async runtime drops pending tasks.
    startup_abort.abort();
    tokio::time::timeout(Duration::from_secs(5), async {
        while !startup_abort.is_finished() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    assert!(matches!(
        owner_lock(&f.path, false),
        Err(RecoveryError::Locked)
    ));
    assert!(indexes(&f).is_empty());
    writer.execute_batch("ROLLBACK").unwrap();
    drop(writer);
    // Wait for an observed ownership transition, not an arbitrary sleep. The
    // deadline also bounds a regression that accidentally leaks the owner.
    let owner = tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            match owner_lock(&f.path, false) {
                Ok(owner) => break owner,
                Err(RecoveryError::Locked) => tokio::time::sleep(Duration::from_millis(5)).await,
                Err(error) => panic!("unexpected owner error: {error}"),
            }
        }
    })
    .await
    .unwrap();
    assert_eq!(indexes(&f).len(), 2);
    drop(owner);
    let j = f.open().await;
    j.ensure_healthy().await.unwrap();
    drop(j);
    f.cleanup(&[]).await;
}
