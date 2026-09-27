use super::*;
use crate::chain::{RpcEndpointConfig, ThirdwebChain, ThirdwebChainConfig};
use alloy::{primitives::Address, rpc::types::Block};
use serde_json::{Value, json};
use std::{collections::VecDeque, sync::Arc, time::Duration};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpListener,
    sync::Mutex,
};

struct RpcFixture {
    chain: ThirdwebChain,
    remaining: Arc<Mutex<VecDeque<(Value, Value)>>>,
    task: tokio::task::JoinHandle<()>,
}

impl Drop for RpcFixture {
    fn drop(&mut self) {
        self.task.abort();
    }
}

impl RpcFixture {
    async fn new(chain_id: u64, policy: FinalityPolicy, replies: Vec<(&str, Value)>) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let endpoint = RpcEndpointConfig {
            url: format!(
                "http://{}/private-key?token=sentinel",
                listener.local_addr().unwrap()
            ),
            finality: policy,
            ..Default::default()
        };
        let chain = ThirdwebChainConfig {
            chain_id,
            secret_key: "unused",
            rpc_base_url: "rpc.invalid",
            bundler_base_url: "bundler.invalid",
            paymaster_base_url: "paymaster.invalid",
            client_id: "unused",
        }
        .to_chain_with_rpc(
            Some(&endpoint),
            Duration::from_secs(2),
            Duration::from_secs(1),
        )
        .unwrap();
        let remaining = Arc::new(Mutex::new(
            replies
                .into_iter()
                .map(|(tag, reply)| (json!([tag, false]), reply))
                .collect::<VecDeque<_>>(),
        ));
        let expected = remaining.clone();
        let task = tokio::spawn(async move {
            loop {
                let (mut stream, _) = listener.accept().await.unwrap();
                let mut bytes = Vec::new();
                let (body_start, body_length) = loop {
                    let mut buffer = [0u8; 4096];
                    let read = stream.read(&mut buffer).await.unwrap();
                    assert!(read > 0);
                    bytes.extend_from_slice(&buffer[..read]);
                    if let Some(end) = bytes.windows(4).position(|part| part == b"\r\n\r\n") {
                        let headers = String::from_utf8_lossy(&bytes[..end]);
                        let length = headers
                            .lines()
                            .find_map(|line| {
                                let (key, value) = line.split_once(':')?;
                                key.eq_ignore_ascii_case("content-length")
                                    .then(|| value.trim().parse::<usize>().unwrap())
                            })
                            .unwrap();
                        break (end + 4, length);
                    }
                };
                while bytes.len() < body_start + body_length {
                    let mut buffer = [0u8; 4096];
                    let read = stream.read(&mut buffer).await.unwrap();
                    assert!(read > 0);
                    bytes.extend_from_slice(&buffer[..read]);
                }
                let request: Value =
                    serde_json::from_slice(&bytes[body_start..body_start + body_length]).unwrap();
                assert_eq!(request["method"], "eth_getBlockByNumber");
                let (params, mut response) = expected
                    .lock()
                    .await
                    .pop_front()
                    .expect("unexpected extra RPC call");
                assert_eq!(request["params"], params);
                response["jsonrpc"] = json!("2.0");
                response["id"] = request["id"].clone();
                let body = response.to_string();
                let wire = format!(
                    "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                    body.len(),
                    body
                );
                stream.write_all(wire.as_bytes()).await.unwrap();
            }
        });
        Self {
            chain,
            remaining,
            task,
        }
    }

    async fn done(&self) {
        assert!(!self.task.is_finished(), "fixture task failed");
        assert!(
            self.remaining.lock().await.is_empty(),
            "missing expected RPC calls"
        );
    }
}

fn hash(byte: u8) -> B256 {
    B256::repeat_byte(byte)
}

fn block(number: u64, byte: u8) -> Value {
    let mut value: Block = Block::default();
    value.header.number = number;
    value.header.hash = hash(byte);
    json!({"result": value})
}

fn receipt(success: bool) -> TransactionReceipt {
    serde_json::from_value(json!({
        "transactionHash": hash(1), "transactionIndex": "0x0", "blockNumber": "0xa",
        "blockHash": hash(10), "from": Address::ZERO, "to": Address::ZERO,
        "cumulativeGasUsed": "0x5208", "gasUsed": "0x5208", "effectiveGasPrice": "0x1",
        "contractAddress": null, "logs": [], "logsBloom": format!("0x{}", "00".repeat(256)),
        "status": if success {"0x1"} else {"0x0"}, "type": "0x2"
    }))
    .unwrap()
}

fn finalized_replies() -> Vec<(&'static str, Value)> {
    vec![
        ("0xa", block(10, 10)),
        ("finalized", block(12, 12)),
        ("0xa", block(10, 10)),
        ("0xc", block(12, 12)),
    ]
}

#[tokio::test]
async fn success_and_revert_require_identical_canonical_finality_evidence() {
    for success in [true, false] {
        let fixture =
            RpcFixture::new(11155111, FinalityPolicy::Finalized, finalized_replies()).await;
        assert_eq!(
            assess_receipt_finality(&fixture.chain, hash(1), &receipt(success))
                .await
                .unwrap(),
            FinalityAssessment::Finalized(FinalityEvidence {
                block_number: 10,
                block_hash: hash(10),
                checkpoint_number: 12,
                checkpoint_hash: hash(12),
                policy: FinalityPolicy::Finalized,
            })
        );
        fixture.done().await;
    }
}

#[tokio::test]
async fn absent_or_lagging_finality_never_falls_back_to_latest() {
    for (replies, canonical) in [
        (vec![("0xa", json!({"result": null}))], false),
        (
            vec![
                ("0xa", block(10, 10)),
                ("finalized", json!({"result": null})),
            ],
            true,
        ),
        (
            vec![("0xa", block(10, 10)), ("finalized", block(9, 9))],
            true,
        ),
    ] {
        let fixture = RpcFixture::new(999999, FinalityPolicy::Finalized, replies).await;
        assert_eq!(
            assess_receipt_finality(&fixture.chain, hash(1), &receipt(true))
                .await
                .unwrap(),
            FinalityAssessment::Pending { canonical }
        );
        fixture.done().await;
    }
    let fixture = RpcFixture::new(
        999999,
        FinalityPolicy::Finalized,
        vec![
            ("0xa", block(10, 10)),
            (
                "finalized",
                json!({"error":{"code":-32602,"message":"finalized unsupported"}}),
            ),
        ],
    )
    .await;
    assert!(
        assess_receipt_finality(&fixture.chain, hash(1), &receipt(true))
            .await
            .is_err()
    );
    fixture.done().await;
}

#[tokio::test]
async fn reorgs_and_mixed_backend_checkpoints_cannot_complete() {
    for replies in [
        vec![("0xa", block(10, 99))],
        vec![
            ("0xa", block(10, 10)),
            ("finalized", block(12, 12)),
            ("0xa", block(10, 99)),
        ],
    ] {
        let fixture = RpcFixture::new(1, FinalityPolicy::Finalized, replies).await;
        assert_eq!(
            assess_receipt_finality(&fixture.chain, hash(1), &receipt(true))
                .await
                .unwrap(),
            FinalityAssessment::Orphaned
        );
        fixture.done().await;
    }
    for replies in [
        vec![("0xa", block(11, 10))],
        vec![
            ("0xa", block(10, 10)),
            ("finalized", block(12, 12)),
            ("0xa", block(10, 10)),
            ("0xc", block(12, 99)),
        ],
        vec![
            ("0xa", block(10, 10)),
            ("finalized", block(10, 99)),
            ("0xa", block(10, 10)),
            ("0xa", block(10, 99)),
        ],
    ] {
        let fixture = RpcFixture::new(1, FinalityPolicy::Finalized, replies).await;
        assert!(
            assess_receipt_finality(&fixture.chain, hash(1), &receipt(true))
                .await
                .is_err()
        );
        fixture.done().await;
    }
}

#[tokio::test]
async fn wrong_receipt_identity_and_missing_block_are_not_finality() {
    let fixture = RpcFixture::new(1, FinalityPolicy::Finalized, vec![]).await;
    assert!(
        assess_receipt_finality(&fixture.chain, hash(2), &receipt(true))
            .await
            .is_err()
    );
    let mut pending = receipt(true);
    pending.block_hash = None;
    assert_eq!(
        assess_receipt_finality(&fixture.chain, hash(1), &pending)
            .await
            .unwrap(),
        FinalityAssessment::Pending { canonical: false }
    );
    let mut legacy = serde_json::to_value(receipt(true)).unwrap();
    legacy.as_object_mut().unwrap().remove("status");
    legacy["root"] = json!(hash(3));
    let legacy: TransactionReceipt = serde_json::from_value(legacy).unwrap();
    assert!(
        assess_receipt_finality(&fixture.chain, hash(1), &legacy)
            .await
            .is_err()
    );
    fixture.done().await;
}

#[tokio::test]
async fn depth_is_explicit_bounded_and_probabilistic() {
    assert!(
        FinalityPolicy::Depth { confirmations: 0 }
            .validate(1)
            .is_err()
    );
    assert!(
        FinalityPolicy::Depth { confirmations: 0 }
            .validate(31337)
            .is_ok()
    );
    let policy = FinalityPolicy::Depth { confirmations: 2 };
    let fixture = RpcFixture::new(
        777,
        policy,
        vec![("0xa", block(10, 10)), ("latest", block(11, 11))],
    )
    .await;
    assert_eq!(
        assess_receipt_finality(&fixture.chain, hash(1), &receipt(true))
            .await
            .unwrap(),
        FinalityAssessment::Pending { canonical: true }
    );
    fixture.done().await;
    let fixture = RpcFixture::new(
        777,
        policy,
        vec![
            ("0xa", block(10, 10)),
            ("latest", block(12, 12)),
            ("0xa", block(10, 10)), // select head - depth
            ("0xa", block(10, 10)), // recheck receipt
            ("0xa", block(10, 10)), // recheck boundary
            ("0xc", block(12, 12)), // transient head consistency only
        ],
    )
    .await;
    let FinalityAssessment::Finalized(evidence) =
        assess_receipt_finality(&fixture.chain, hash(1), &receipt(true))
            .await
            .unwrap()
    else {
        panic!("policy not met")
    };
    assert_eq!(evidence.policy, policy);
    assert_eq!(evidence.checkpoint_number, 10);
    assert_eq!(evidence.checkpoint_hash, hash(10));
    assert_eq!(
        serde_json::to_value(evidence).unwrap()["policy"],
        json!({"mode":"depth", "confirmations":2})
    );
    fixture.done().await;
    let fixture = RpcFixture::new(
        777,
        FinalityPolicy::Depth {
            confirmations: u64::MAX,
        },
        vec![("0xa", block(10, 10)), ("latest", block(u64::MAX, 12))],
    )
    .await;
    assert!(
        assess_receipt_finality(&fixture.chain, hash(1), &receipt(true))
            .await
            .is_err()
    );
    fixture.done().await;
}

#[tokio::test]
async fn persisted_checkpoint_distinguishes_missing_history_from_positive_conflict() {
    let previous = FinalityEvidence {
        block_number: 10,
        block_hash: hash(10),
        checkpoint_number: 12,
        checkpoint_hash: hash(12),
        policy: FinalityPolicy::Finalized,
    };
    for (reply, expected) in [
        (block(12, 12), CheckpointContinuity::Consistent),
        (block(12, 99), CheckpointContinuity::Conflict),
        (json!({"result":null}), CheckpointContinuity::Unavailable),
    ] {
        let fixture = RpcFixture::new(1, FinalityPolicy::Finalized, vec![("0xc", reply)]).await;
        assert_eq!(
            assess_checkpoint_continuity(&fixture.chain, &previous)
                .await
                .unwrap(),
            expected
        );
        fixture.done().await;
    }
}

// Receipt at 10, latest at 13 and depth 2: persist boundary 11, not tip 13
// and not merely receipt 10. Both the boundary and observed head are re-read.
fn depth_boundary_replies() -> Vec<(&'static str, Value)> {
    vec![
        ("0xa", block(10, 10)),
        ("latest", block(13, 13)),
        ("0xb", block(11, 11)),
        ("0xa", block(10, 10)),
        ("0xb", block(11, 11)),
        ("0xd", block(13, 13)),
    ]
}

#[tokio::test]
async fn depth_success_and_revert_checkpoint_the_qualified_boundary_not_tip() {
    let policy = FinalityPolicy::Depth { confirmations: 2 };
    for success in [true, false] {
        let fixture = RpcFixture::new(777, policy, depth_boundary_replies()).await;
        assert_eq!(
            assess_receipt_finality(&fixture.chain, hash(1), &receipt(success))
                .await
                .unwrap(),
            FinalityAssessment::Finalized(FinalityEvidence {
                block_number: 10,
                block_hash: hash(10),
                checkpoint_number: 11,
                checkpoint_hash: hash(11),
                policy,
            })
        );
        fixture.done().await;
    }
}

#[tokio::test]
async fn depth_unknown_boundary_and_within_assessment_reorgs_cannot_complete() {
    let policy = FinalityPolicy::Depth { confirmations: 2 };
    // None means a positive inconsistent response must be an error. A missing
    // response is only unknown; a receipt replacement is explicitly orphaned.
    for (name, index, response, expected) in [
        (
            "missing boundary",
            2,
            json!({"result": null}),
            Some(FinalityAssessment::Pending { canonical: true }),
        ),
        ("wrong boundary number", 2, block(12, 11), None),
        (
            "orphaned receipt",
            3,
            block(10, 99),
            Some(FinalityAssessment::Orphaned),
        ),
        (
            "missing boundary recheck",
            4,
            json!({"result": null}),
            Some(FinalityAssessment::Pending { canonical: true }),
        ),
        ("boundary changed", 4, block(11, 99), None),
        (
            "missing observed head",
            5,
            json!({"result": null}),
            Some(FinalityAssessment::Pending { canonical: true }),
        ),
        ("observed tip changed", 5, block(13, 99), None),
        ("wrong observed head number", 5, block(14, 13), None),
    ] {
        let mut replies = depth_boundary_replies();
        replies[index].1 = response;
        replies.truncate(index + 1);
        let fixture = RpcFixture::new(777, policy, replies).await;
        let result = assess_receipt_finality(&fixture.chain, hash(1), &receipt(true)).await;
        match expected {
            Some(expected) => assert_eq!(result.unwrap(), expected, "{name}"),
            None => assert!(result.is_err(), "{name}"),
        }
        fixture.done().await;
    }
    // A lagging head below depth must not underflow or query an invented block.
    let fixture = RpcFixture::new(
        777,
        policy,
        vec![("0xa", block(10, 10)), ("latest", block(1, 1))],
    )
    .await;
    assert_eq!(
        assess_receipt_finality(&fixture.chain, hash(1), &receipt(true))
            .await
            .unwrap(),
        FinalityAssessment::Pending { canonical: true }
    );
    fixture.done().await;
}

#[tokio::test]
async fn local_depth_zero_keeps_tip_as_its_qualified_boundary() {
    let policy = FinalityPolicy::Depth { confirmations: 0 };
    let fixture = RpcFixture::new(
        31337,
        policy,
        vec![
            ("0xa", block(10, 10)),
            ("latest", block(12, 12)),
            ("0xa", block(10, 10)),
            ("0xc", block(12, 12)),
        ],
    )
    .await;
    assert_eq!(
        assess_receipt_finality(&fixture.chain, hash(1), &receipt(false))
            .await
            .unwrap(),
        FinalityAssessment::Finalized(FinalityEvidence {
            block_number: 10,
            block_hash: hash(10),
            checkpoint_number: 12,
            checkpoint_hash: hash(12),
            policy,
        })
    );
    fixture.done().await;
}
