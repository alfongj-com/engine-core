//! Isolated connections for connection-scoped Redis transactions.
use crate::error::TwmqError;
use redis::{AsyncCommands, Client, Pipeline, aio::MultiplexedConnection};
use std::sync::Mutex;

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
        match connection {
            Some(connection) => Ok(connection),
            None => Ok(self.client.get_multiplexed_async_connection().await?),
        }
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
