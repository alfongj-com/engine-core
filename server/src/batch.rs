//! Bound per-request work before RPC, signing or durable admission.
use engine_core::error::EngineError;
use futures::{StreamExt, stream};
use std::future::Future;

pub(crate) const MAX_ITEMS: usize = 100;
const CONCURRENCY: usize = 16;

pub(crate) fn validate_size(items: usize) -> Result<(), EngineError> {
    if items > MAX_ITEMS {
        return Err(EngineError::ValidationError {
            message: format!("A batch supports at most {MAX_ITEMS} items; split larger requests"),
        });
    }
    Ok(())
}

/// Poll at most sixteen operations and keep each result at its input index.
/// No spawned tasks survive a cancelled request.
pub(crate) async fn collect<F: Future>(items: Vec<F>) -> Vec<F::Output> {
    stream::iter(items).buffered(CONCURRENCY).collect().await
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    };
    use tokio::sync::{Semaphore, mpsc};

    #[tokio::test]
    async fn bounded_batch_keeps_order_when_later_operations_finish_first() {
        let active = Arc::new(AtomicUsize::new(0));
        let peak = Arc::new(AtomicUsize::new(0));
        let releases = Arc::new(Semaphore::new(0));
        let (started, mut starts) = mpsc::unbounded_channel();
        let work = (0..MAX_ITEMS).map(|index| {
            let (active, peak, releases, started) = (
                active.clone(),
                peak.clone(),
                releases.clone(),
                started.clone(),
            );
            async move {
                let current = active.fetch_add(1, Ordering::SeqCst) + 1;
                peak.fetch_max(current, Ordering::SeqCst);
                started.send(index).unwrap();
                if index == 0 {
                    releases.acquire().await.unwrap().forget();
                }
                active.fetch_sub(1, Ordering::SeqCst);
                index
            }
        });
        let task = tokio::spawn(collect(work.collect::<Vec<_>>()));
        for index in 0..CONCURRENCY {
            assert_eq!(
                tokio::time::timeout(std::time::Duration::from_secs(5), starts.recv())
                    .await
                    .unwrap(),
                Some(index)
            );
        }
        assert!(
            starts.try_recv().is_err(),
            "a blocked first result must bound queued completed results too"
        );
        releases.add_permits(1);
        assert_eq!(task.await.unwrap(), (0..MAX_ITEMS).collect::<Vec<_>>());
        assert_eq!(active.load(Ordering::SeqCst), 0);
        assert!(peak.load(Ordering::SeqCst) <= CONCURRENCY);
    }
}
