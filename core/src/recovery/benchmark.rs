//! Opt-in local journal probe. No signing keys or attempted payloads are emitted.
use super::tests::Fixture;
use super::*;
use alloy::{
    consensus::{SignableTransaction, TxEip1559, TxEnvelope},
    eips::eip2718::Encodable2718,
    primitives::{Address, B256, Bytes, TxKind, U256},
    signers::{SignerSync, local::PrivateKeySigner},
};
use serde_json::json;
use std::{io::Write, time::Instant};

struct Input {
    id: String,
    payload: Value,
    fingerprint: String,
    replay_key: String,
    attempt: Value,
    terminal: Value,
}

fn inputs(count: usize) -> Vec<Input> {
    // Public development key, generated only inside this local probe. No RPC.
    let signer: PrivateKeySigner =
        "1111111111111111111111111111111111111111111111111111111111111111"
            .parse()
            .unwrap();
    let sender = signer.address();
    (0..count).map(|nonce| {
        let id = format!("probe-{nonce}");
        let tx = TxEip1559 {
            chain_id:31337,nonce:nonce as u64,gas_limit:21000,max_fee_per_gas:1_000_000_000,
            max_priority_fee_per_gas:1_000_000,to:TxKind::Call(Address::repeat_byte(7)),
            value:U256::from(1),..Default::default()
        };
        let signature = signer.sign_hash_sync(&tx.signature_hash()).unwrap();
        let envelope: TxEnvelope = tx.into_signed(signature).into();
        let wire = Bytes::from(envelope.encoded_2718());
        let hash = alloy::primitives::keccak256(&wire);
        let payload = json!({"transactionId":id,"chainId":31337,"from":sender,
            "to":Address::repeat_byte(7),"value":"0x1","data":"0x","gasLimit":21000,
            "maxFeePerGas":1_000_000_000,"maxPriorityFeePerGas":1_000_000,
            "signingCredential":crate::credentials::SigningCredential::Environment {address:sender},
            "rpcCredentials":crate::chain::RpcCredentials::Configured,"webhookOptions":[]});
        Input {
            fingerprint:admission_fingerprint("eoa", &payload).unwrap(),
            id,payload,replay_key:format!("evm:31337:{sender:#x}:{nonce}"),
            attempt:json!({"chainId":31337,"sender":sender,"nonce":nonce,"transactionHash":hash,"signedTransaction":wire}),
            terminal:json!({"chainId":31337,"transactionHash":hash,"outcome":"success","finality":{
                "blockNumber":10,"blockHash":B256::repeat_byte(10),"checkpointNumber":12,"checkpointHash":B256::repeat_byte(12),"policy":{"mode":"finalized"}}}),
        }
    }).collect()
}

async fn phase(
    journal: Arc<RecoveryJournal>,
    inputs: Arc<Vec<Input>>,
    name: &'static str,
    concurrency: usize,
) -> Value {
    let start = Instant::now();
    let mut tasks = tokio::task::JoinSet::new();
    for worker in 0..concurrency {
        let journal = journal.clone();
        let inputs = inputs.clone();
        tasks.spawn(async move {
            let mut samples = Vec::new();
            for item in inputs.iter().skip(worker).step_by(concurrency) {
                let start = Instant::now();
                match name {
                    "admission" => {
                        let reserved = journal
                            .reserve_admission(
                                "eoa",
                                &item.id,
                                &item.fingerprint,
                                item.payload.clone(),
                            )
                            .await
                            .unwrap();
                        assert!(!reserved.terminal);
                    }
                    "attempt" => journal
                        .before_broadcast("eoa", &item.id, &item.replay_key, item.attempt.clone())
                        .await
                        .unwrap(),
                    "terminal" => journal
                        .record_terminal("eoa", &item.id, item.terminal.clone())
                        .await
                        .unwrap(),
                    _ => unreachable!(),
                }
                samples.push(start.elapsed().as_secs_f64() * 1000.0);
            }
            samples
        });
    }
    let mut samples = Vec::new();
    while let Some(result) = tasks.join_next().await {
        samples.extend(result.unwrap());
    }
    let elapsed = start.elapsed().as_secs_f64();
    assert_eq!(samples.len(), inputs.len());
    samples.sort_by(f64::total_cmp);
    let percentile =
        |p: f64| samples[((samples.len() as f64 * p).ceil() as usize).saturating_sub(1)];
    json!({"phase":name,"count":samples.len(),"elapsedSeconds":elapsed,"operationsPerSecond":samples.len() as f64/elapsed,
        "methodCompletionLatencyMs":{"min":samples[0],"p50":percentile(0.50),"p95":percentile(0.95),"p99":percentile(0.99),"max":samples[samples.len()-1]},
        "latencyIncludesQueueWait":true})
}

/// Set RECOVERY_BENCH_OUTPUT explicitly. Full correctness suites skip the probe
/// even when they select ignored tests. All chain evidence here is synthetic;
/// the measured work is real journal SQL FULL commits + local Redis checkpoints.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "opt-in local throughput probe; requires RECOVERY_BENCH_OUTPUT and TEST_REDIS_URL"]
async fn journal_throughput_probe() {
    let Ok(output) = std::env::var("RECOVERY_BENCH_OUTPUT") else {
        return;
    };
    let count: usize = std::env::var("RECOVERY_BENCH_COUNT")
        .unwrap_or_else(|_| "128".into())
        .parse()
        .unwrap();
    let concurrency: usize = std::env::var("RECOVERY_BENCH_CONCURRENCY")
        .unwrap_or_else(|_| "1".into())
        .parse()
        .unwrap();
    assert!((16..=5000).contains(&count));
    assert!((1..=MAX_CONCURRENT_ADMISSIONS).contains(&concurrency));
    let f = Fixture::fresh();
    let config = redis::Client::open(f.url.as_str())
        .unwrap()
        .get_connection_info()
        .clone();
    match config.addr {
        redis::ConnectionAddr::Tcp(ref host, _) => {
            assert!(matches!(host.as_str(), "127.0.0.1" | "localhost" | "::1"))
        }
        _ => panic!("probe requires loopback Redis TCP"),
    }
    f.initialize().await;
    let journal = f.open().await;
    let records = Arc::new(inputs(count));
    let record_bytes: usize = records
        .iter()
        .map(|r| {
            canonical(&r.payload).unwrap().len()
                + canonical(&r.attempt).unwrap().len()
                + canonical(&r.terminal).unwrap().len()
        })
        .sum();
    let mut phases = Vec::new();
    for name in ["admission", "attempt", "terminal"] {
        phases.push(phase(journal.clone(), records.clone(), name, concurrency).await);
    }
    journal.ensure_healthy().await.unwrap();
    let state = RecoveryJournal::status(&f.path).unwrap();
    assert_eq!(
        (
            state.admissions,
            state.attempts,
            state.terminal,
            state.checkpoint
        ),
        (count as u64, count as u64, count as u64, (count * 3) as i64)
    );
    assert!(!state.halted);
    let elapsed: f64 = phases
        .iter()
        .map(|p| p["elapsedSeconds"].as_f64().unwrap())
        .sum();
    let report = json!({"schema":"engine-recovery-journal-probe-v1","os":std::env::consts::OS,"arch":std::env::consts::ARCH,
        "count":count,"concurrency":concurrency,"durability":{"journalMode":"WAL","synchronous":"FULL","fullfsync":true},
        "phases":phases,"threeStageIntentsPerSecond":count as f64/elapsed,"logicalRecordBytesPerIntent":record_bytes/count,
        "reconciliation":{"admissions":state.admissions,"attempts":state.attempts,"terminal":state.terminal,"checkpoint":state.checkpoint,"healthy":true},
        "scope":"closed-loop real journal operations; excludes transaction construction, blockchain execution, provider RPC, finality assessment and Redis queue work; not Engine transaction TPS"});
    let mut file = private_new_file(Path::new(&output)).unwrap();
    serde_json::to_writer_pretty(&mut file, &report).unwrap();
    file.write_all(b"\n").unwrap();
    file.sync_all().unwrap();
    drop(journal);
    f.cleanup(&[]).await;
}
