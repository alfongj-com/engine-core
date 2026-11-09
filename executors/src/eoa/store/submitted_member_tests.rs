use super::SubmittedTransactionDehydrated;

#[test]
fn submitted_members_round_trip_colons_unicode_and_legacy_forms() {
    for id in ["normal", "invoice:42", "中文:🔐:"] {
        let tx = SubmittedTransactionDehydrated {
            transaction_hash: "0xhash".into(),
            transaction_id: id.into(),
            nonce: 5,
            queued_at: 12,
            submitted_at: 34,
        };
        let member = tx.to_redis_string_with_nonce();
        let decoded = SubmittedTransactionDehydrated::from_redis_strings(&[member.clone()]);
        assert_eq!(decoded.len(), 1);
        assert_eq!(decoded[0].transaction_id, id);
        assert_eq!(decoded[0].to_redis_string_with_nonce(), member);
    }
    let old = SubmittedTransactionDehydrated::from_redis_strings(&[
        ("h:id:12".into(), 5),
        ("h:id:12:34".into(), 5),
    ]);
    assert_eq!(old.len(), 2);
    assert_eq!(old[0].submitted_at, 0);
    assert_eq!(old[1].submitted_at, 34);
    for invalid in [
        "v2:h:xx:1:2",
        "v2:h:666F6F3A:1:2",
        "v2:h:666f6f:1:2",
        "v2:h:ff:1:2",
        "v2:h:3a:NaN:2",
    ] {
        assert!(
            SubmittedTransactionDehydrated::from_redis_strings(&[(invalid.into(), 5)]).is_empty()
        );
    }
}
