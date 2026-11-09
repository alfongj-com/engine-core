//! Bounded, consistent inventory for offline projection repair. No send permission.
use super::*;

/// Full payloads may contain credentials. Never log or derive Debug for this type.
pub struct RebuildRecord {
    pub admission: AdmissionRecord,
    pub attempts: Vec<Value>,
}

impl RecoveryJournal {
    /// Enumerate only admitted work. The offline caller holds this journal's
    /// exclusive owner lock and must not start workers/HTTP while repairing.
    /// A page is bounded by 16 intents, 64 attempts per intent and 32 MiB total.
    pub async fn rebuild_page(
        self: &Arc<Self>,
        after: Option<String>,
    ) -> Result<Vec<RebuildRecord>> {
        self.ensure_healthy().await?;
        let page = self.db(move |conn| {
            let tx = conn.transaction()?;
            let ids = {
                let mut statement = tx.prepare(
                    "SELECT id FROM admissions WHERE state='admitted' AND (?1 IS NULL OR id>?1) ORDER BY id LIMIT 16",
                )?;
                statement.query_map([after], |row| row.get::<_, String>(0))?
                    .collect::<std::result::Result<Vec<_>, _>>()?
            };
            let mut page = Vec::new();
            let mut bytes = 0usize;
            for id in ids {
                let admission = lookup(&tx, &id)?.ok_or(RecoveryError::Storage)?;
                if admission.fingerprint != admission_fingerprint(&admission.kind, &admission.payload)? {
                    return Err(RecoveryError::Storage);
                }
                let mut size = canonical(&admission.payload)?.len();
                let mut statement = tx.prepare(
                    "SELECT replay_key,digest,payload FROM attempts WHERE id=? ORDER BY sequence LIMIT 65",
                )?;
                let rows = statement.query_map([&id], |row| Ok((
                    row.get::<_, String>(0)?, row.get::<_, String>(1)?, row.get::<_, String>(2)?,
                )))?;
                let mut attempts = Vec::new();
                for row in rows {
                    let (key, digest, encoded) = row?;
                    if attempts.len() == 64 {
                        return Err(RecoveryError::RecoveryRequired("intent exceeds offline repair history bound; export for reconciliation"));
                    }
                    if admission.replay_key.as_deref() != Some(key.as_str())
                        || format!("{:x}", Sha256::digest(encoded.as_bytes())) != digest {
                        return Err(RecoveryError::Storage);
                    }
                    let attempt: Value = serde_json::from_str(&encoded).map_err(|_| RecoveryError::Storage)?;
                    if canonical(&attempt)? != encoded { return Err(RecoveryError::Storage); }
                    size = size.checked_add(encoded.len()).ok_or(RecoveryError::Storage)?;
                    if size > 32 * 1024 * 1024 {
                        return Err(RecoveryError::RecoveryRequired("intent exceeds offline repair byte bound; export for reconciliation"));
                    }
                    attempts.push(attempt);
                }
                if admission.replay_key.is_some() != !attempts.is_empty() {
                    return Err(RecoveryError::Storage);
                }
                if !page.is_empty() && bytes + size > 32 * 1024 * 1024 { break; }
                bytes += size;
                page.push(RebuildRecord { admission, attempts });
            }
            Ok(page)
        }).await?;
        self.ensure_healthy().await?;
        Ok(page)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::recovery::tests::{Fixture, admit, request};
    use serde_json::json;

    #[tokio::test]
    #[ignore = "requires disposable Redis in TEST_REDIS_URL"]
    async fn rebuild_inventory_pages_only_live_intents_without_changing_authority() {
        let fixture = Fixture::fresh();
        fixture.initialize().await;
        let journal = fixture.open().await;
        for n in 0..20 {
            let id = format!("item-{n:02}");
            admit(&journal, "eoa", &id, request(&id)).await;
        }
        let key = "evm:31337:0x1111111111111111111111111111111111111111:0";
        let hash = alloy::primitives::B256::repeat_byte(3);
        journal
            .before_broadcast("eoa", "item-00", key, json!({"transactionHash":hash}))
            .await
            .unwrap();
        journal
            .record_terminal(
                "eoa",
                "item-00",
                json!({"chainId":31337,"transactionHash":hash,"outcome":"success"}),
            )
            .await
            .unwrap();
        admit(
            &journal,
            "erc4337",
            "cancelled",
            json!({"credential":"private"}),
        )
        .await;
        journal.cancel_unbroadcast("cancelled").await.unwrap();
        let before = RecoveryJournal::status(&fixture.path).unwrap().checkpoint;
        let first = journal.rebuild_page(None).await.unwrap();
        assert_eq!(first.len(), 16);
        assert_eq!(first[0].admission.id, "item-01");
        let second = journal
            .rebuild_page(Some(first.last().unwrap().admission.id.clone()))
            .await
            .unwrap();
        assert_eq!(second.len(), 3);
        assert_eq!(second.last().unwrap().admission.id, "item-19");
        assert!(
            journal
                .rebuild_page(Some("item-19".into()))
                .await
                .unwrap()
                .is_empty()
        );
        assert_eq!(
            RecoveryJournal::status(&fixture.path).unwrap().checkpoint,
            before
        );
        drop(journal);
        fixture.cleanup(&[]).await;
    }

    #[tokio::test]
    #[ignore = "requires disposable Redis in TEST_REDIS_URL"]
    async fn rebuild_inventory_refuses_corrupt_or_oversized_attempt_history() {
        let fixture = Fixture::fresh();
        fixture.initialize().await;
        let journal = fixture.open().await;
        admit(&journal, "eoa", "intent", request("intent")).await;
        let key = "evm:31337:0x1111111111111111111111111111111111111111:0";
        for n in 0..65 {
            journal
                .before_broadcast(
                    "eoa",
                    "intent",
                    key,
                    json!({"transactionHash":alloy::primitives::B256::repeat_byte(n)}),
                )
                .await
                .unwrap();
        }
        assert!(matches!(
            journal.rebuild_page(None).await,
            Err(RecoveryError::RecoveryRequired(_))
        ));
        journal
            .db(|db| {
                db.execute("DELETE FROM attempts WHERE sequence>1", [])?;
                Ok(())
            })
            .await
            .unwrap();
        let page = journal.rebuild_page(None).await.unwrap();
        assert_eq!(page[0].attempts.len(), 1);
        journal
            .db(|db| {
                db.execute("UPDATE attempts SET digest='corrupt'", [])?;
                Ok(())
            })
            .await
            .unwrap();
        assert!(matches!(
            journal.rebuild_page(None).await,
            Err(RecoveryError::Storage)
        ));
        drop(journal);
        fixture.cleanup(&[]).await;
    }
}
