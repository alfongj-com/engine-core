//! Isolated connections for connection-scoped Redis transactions.
use crate::error::TwmqError;
use redis::{AsyncCommands, Client, Pipeline, aio::MultiplexedConnection};
use std::{sync::Mutex, time::Duration};

const CONNECTION_TIMEOUT: Duration = Duration::from_secs(5);
const RESPONSE_TIMEOUT: Duration = Duration::from_secs(5);
const IDLE_PROBE_TIMEOUT: Duration = Duration::from_secs(1);

/// A checked-out connection is exclusively owned by one acknowledgement. It is
/// returned only after EXEC or UNWATCH cleared connection state. Cancellation or
/// a transport error drops it, including any abandoned WATCH state.
pub(crate) struct TransactionConnections {
    client: Client,
    idle: Mutex<Vec<MultiplexedConnection>>,
}

impl TransactionConnections {
    pub(crate) fn new(client: Client) -> Self {
        Self {
            client,
            idle: Mutex::new(Vec::new()),
        }
    }

    async fn checkout(&self) -> Result<MultiplexedConnection, TwmqError> {
        let connection = self.idle.lock().expect("transaction pool poisoned").pop();
        if let Some(mut connection) = connection {
            // A restarted Redis leaves dead sockets in the idle pool. Probe
            // before WATCH, where replacing a connection cannot replay writes.
            if matches!(tokio::time::timeout(
                IDLE_PROBE_TIMEOUT,
                redis::cmd("PING").query_async::<String>(&mut connection),
            ).await, Ok(Ok(reply)) if reply == "PONG")
            {
                return Ok(connection);
            }
        }
        let config = redis::AsyncConnectionConfig::new()
            .set_connection_timeout(CONNECTION_TIMEOUT)
            .set_response_timeout(RESPONSE_TIMEOUT);
        Ok(self
            .client
            .get_multiplexed_async_connection_with_config(&config)
            .await?)
    }

    fn checkin(&self, connection: MultiplexedConnection) {
        self.idle
            .lock()
            .expect("transaction pool poisoned")
            .push(connection);
    }

    /// Return true only when the lease-protected writes committed. Redis nil is
    /// an optimistic conflict, not a successful empty transaction.
    pub(crate) async fn commit_if_leased(
        &self,
        lease_key: &str,
        pipeline: &Pipeline,
    ) -> Result<bool, TwmqError> {
        let mut connection = self.checkout().await?;
        let mut transaction = pipeline.clone();
        transaction.atomic();
        for _ in 0..10 {
            redis::cmd("WATCH")
                .arg(lease_key)
                .query_async::<()>(&mut connection)
                .await?;
            if !connection.exists::<_, bool>(lease_key).await? {
                redis::cmd("UNWATCH")
                    .query_async::<()>(&mut connection)
                    .await?;
                self.checkin(connection);
                return Ok(false);
            }
            match transaction
                .query_async::<Option<Vec<redis::Value>>>(&mut connection)
                .await?
            {
                Some(_) => {
                    self.checkin(connection);
                    return Ok(true);
                }
                None => continue,
            }
        }
        Err(TwmqError::Runtime {
            message: "Lease changed repeatedly while completing a job".into(),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    #[ignore = "requires disposable Redis in TEST_REDIS_URL"]
    async fn dead_idle_connection_is_replaced_before_completion_without_bypassing_lease() {
        let client =
            Client::open(std::env::var("TEST_REDIS_URL").expect("disposable Redis URL")).unwrap();
        let pool = TransactionConnections::new(client.clone());
        let mut observer = client.get_multiplexed_async_connection().await.unwrap();
        let key = format!("transaction-pool:{}", nanoid::nanoid!());
        let lease = format!("{key}:lease");
        let effects = format!("{key}:effects");
        let mut pipeline = redis::pipe();
        pipeline.incr(&effects, 1);

        for lease_exists in [true, false] {
            let mut idle = pool.checkout().await.unwrap();
            let id: i64 = redis::cmd("CLIENT")
                .arg("ID")
                .query_async(&mut idle)
                .await
                .unwrap();
            pool.checkin(idle);
            // Drop only the pooled socket, as Redis restart does, without
            // disrupting unrelated suites sharing the disposable test Redis.
            let killed: usize = redis::cmd("CLIENT")
                .arg("KILL")
                .arg("ID")
                .arg(id)
                .query_async(&mut observer)
                .await
                .unwrap();
            assert_eq!(killed, 1);
            if lease_exists {
                let _: () = observer.set_ex(&lease, "owned", 30).await.unwrap();
            } else {
                let _: usize = observer.del(&lease).await.unwrap();
            }
            assert_eq!(
                tokio::time::timeout(
                    Duration::from_secs(3),
                    pool.commit_if_leased(&lease, &pipeline)
                )
                .await
                .expect("stale idle connection blocked acknowledgement")
                .unwrap(),
                lease_exists
            );
            assert_eq!(
                observer.get::<_, u64>(&effects).await.unwrap(),
                1,
                "reconnecting must neither duplicate writes nor bypass an expired lease"
            );
        }
        let _: usize = observer.del(&[lease, effects]).await.unwrap();
    }
}
