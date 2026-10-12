//! Bound each chain to one contender for the journal's existing FIFO mutex.
//! The permit covers only a journal operation, never RPC, signing or a whole
//! executor job. This allocates operation opportunities, not equal transaction
//! rates: different chains perform different amounts of journal work.
use crate::execution_options::solana::SolanaChainId;
use std::{
    collections::HashMap,
    future::Future,
    sync::{Arc, Mutex},
};
use tokio::sync::{OwnedSemaphorePermit, Semaphore};

/// Scheduling context only. This is never authorization or chain identity proof.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum ChainScope {
    Evm(u64),
    Solana(SolanaChainId),
    #[default]
    Unknown,
}

tokio::task_local! {
    static CHAIN: ChainScope;
}

/// Apply a chain context while polling this future. Unrelated tasks do not inherit
/// it. Journal-owned mutation tasks explicitly capture and restore this context.
///
/// Router and executor futures are large. Box before adding the task-local wrapper
/// so nested async state machines do not copy their full size onto worker stacks.
pub fn scope<F: Future>(chain: ChainScope, future: F) -> impl Future<Output = F::Output> {
    CHAIN.scope(chain, Box::pin(future))
}

pub(super) fn current() -> ChainScope {
    CHAIN.try_with(|chain| *chain).unwrap_or_default()
}

const MAX_EVM_LANES: usize = 32;

pub(super) struct JournalFairness {
    evm: Mutex<HashMap<u64, Arc<Semaphore>>>,
    solana: [Arc<Semaphore>; 3],
    // Unscoped work and additional EVM chains share a bounded fallback. Never
    // evict live lanes: that could create two permits for the same chain.
    fallback: Arc<Semaphore>,
}

impl JournalFairness {
    pub(super) fn new() -> Self {
        Self {
            evm: Mutex::new(HashMap::new()),
            solana: std::array::from_fn(|_| Arc::new(Semaphore::new(1))),
            fallback: Arc::new(Semaphore::new(1)),
        }
    }

    fn lane(&self, chain: ChainScope) -> Arc<Semaphore> {
        match chain {
            ChainScope::Evm(id) => {
                let mut lanes = self.evm.lock().unwrap_or_else(|error| error.into_inner());
                if let Some(lane) = lanes.get(&id) {
                    return lane.clone();
                }
                if lanes.len() == MAX_EVM_LANES {
                    return self.fallback.clone();
                }
                let lane = Arc::new(Semaphore::new(1));
                lanes.insert(id, lane.clone());
                lane
            }
            ChainScope::Solana(chain) => self.solana[match chain {
                SolanaChainId::SolanaMainnet => 0,
                SolanaChainId::SolanaDevnet => 1,
                SolanaChainId::SolanaLocal => 2,
            }]
            .clone(),
            ChainScope::Unknown => self.fallback.clone(),
        }
    }

    pub(super) async fn acquire(&self) -> OwnedSemaphorePermit {
        self.lane(current())
            .acquire_owned()
            .await
            .expect("journal fairness lanes are never closed")
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::{
        task::{Context, Poll, Waker},
        time::Duration,
    };
    use tokio::sync::{Mutex as AsyncMutex, mpsc, oneshot};

    #[tokio::test]
    async fn flooded_chain_cannot_fill_the_global_mutex_ahead_of_waiting_peers() {
        let fairness = Arc::new(JournalFairness::new());
        let serial = Arc::new(AsyncMutex::new(()));
        let held = serial.lock().await;
        let (entered, mut contenders) = mpsc::unbounded_channel();
        let (completed, mut completions) = mpsc::unbounded_channel();
        let mut tasks = Vec::new();
        let spawn = |chain, fairness: Arc<JournalFairness>, serial: Arc<AsyncMutex<()>>| {
            let entered = entered.clone();
            let completed = completed.clone();
            tokio::spawn(scope(chain, async move {
                let lane = fairness.acquire().await;
                entered.send(chain).unwrap();
                let guard = serial.lock().await;
                completed.send(chain).unwrap();
                // Same release order as SerialGuard: global mutex first, lane last.
                drop(guard);
                drop(lane);
            }))
        };
        let busy = ChainScope::Solana(SolanaChainId::SolanaLocal);
        tasks.push(spawn(busy, fairness.clone(), serial.clone()));
        assert_eq!(contenders.recv().await, Some(busy));
        for _ in 0..64 {
            tasks.push(spawn(busy, fairness.clone(), serial.clone()));
        }
        for chain in [ChainScope::Evm(1), ChainScope::Evm(10)] {
            tasks.push(spawn(chain, fairness.clone(), serial.clone()));
            assert_eq!(contenders.recv().await, Some(chain));
        }
        assert!(
            contenders.try_recv().is_err(),
            "one queued global contender per chain"
        );
        drop(held);
        // FIFO global mutex grants both peers before the busy chain can enter
        // again, regardless of how many of its tasks await its lane permit.
        for expected in [busy, ChainScope::Evm(1), ChainScope::Evm(10)] {
            assert_eq!(completions.recv().await, Some(expected));
        }
        for task in tasks {
            task.await.unwrap();
        }
        assert_eq!(fairness.lane(busy).available_permits(), 1);
    }

    #[tokio::test]
    async fn cancelling_waiter_and_holder_release_capacity_without_skipping_the_next_waiter() {
        let fairness = JournalFairness::new();
        let chain = ChainScope::Evm(1);
        let held = scope(chain, fairness.acquire()).await;
        let mut cancelled = Box::pin(scope(chain, fairness.acquire()));
        let mut surviving = Box::pin(scope(chain, fairness.acquire()));
        let mut context = Context::from_waker(Waker::noop());
        assert!(matches!(
            cancelled.as_mut().poll(&mut context),
            Poll::Pending
        ));
        assert!(matches!(
            surviving.as_mut().poll(&mut context),
            Poll::Pending
        ));
        // Remove a queued waiter, then release the holder. The next waiter
        // must receive the permit without a missing or extra capacity unit.
        drop(cancelled);
        drop(held);
        let surviving_permit = tokio::time::timeout(Duration::from_secs(1), surviving)
            .await
            .unwrap();
        drop(surviving_permit);
        let fairness = Arc::new(fairness);
        let (ready, started) = oneshot::channel();
        let owner = fairness.clone();
        let task = tokio::spawn(scope(chain, async move {
            let _permit = owner.acquire().await;
            ready.send(()).unwrap();
            std::future::pending::<()>().await;
        }));
        started.await.unwrap();
        task.abort();
        let _ = task.await;
        assert_eq!(fairness.lane(chain).available_permits(), 1);
    }

    #[test]
    fn overflow_chains_share_one_lane_without_evicting_existing_or_solana_lanes() {
        let fairness = JournalFairness::new();
        let first = fairness.lane(ChainScope::Evm(0));
        for id in 0..MAX_EVM_LANES as u64 {
            assert!(!Arc::ptr_eq(
                &fairness.lane(ChainScope::Evm(id)),
                &fairness.fallback
            ));
        }
        for id in MAX_EVM_LANES as u64..10_000 {
            assert!(Arc::ptr_eq(
                &fairness.lane(ChainScope::Evm(id)),
                &fairness.fallback
            ));
        }
        assert_eq!(fairness.evm.lock().unwrap().len(), MAX_EVM_LANES);
        assert!(Arc::ptr_eq(&fairness.lane(ChainScope::Evm(0)), &first));
        assert!(!Arc::ptr_eq(
            &fairness.lane(ChainScope::Solana(SolanaChainId::SolanaLocal)),
            &fairness.fallback
        ));
    }

    #[tokio::test]
    async fn scope_keeps_large_inner_futures_off_the_enclosing_state_machine() {
        let payload = [7u8; 64 * 1024];
        let operation = async move {
            tokio::task::yield_now().await;
            assert_eq!(current(), ChainScope::Evm(10));
            std::hint::black_box(payload)[0]
        };
        assert!(std::mem::size_of_val(&operation) >= 64 * 1024);
        let scoped = scope(ChainScope::Evm(10), operation);
        let small = scope(ChainScope::Unknown, std::future::ready(()));
        // A large inner future must not increase the router/worker's outer
        // state-machine size, including when another context wraps it.
        assert_eq!(
            std::mem::size_of_val(&scoped),
            std::mem::size_of_val(&small)
        );
        let nested = scope(ChainScope::Evm(1), scoped);
        assert_eq!(
            std::mem::size_of_val(&nested),
            std::mem::size_of_val(&small)
        );
        assert_eq!(nested.await, 7);
        assert_eq!(current(), ChainScope::Unknown);
    }

    #[tokio::test]
    async fn nested_scope_restores_parent_and_unrelated_tasks_do_not_inherit_it() {
        assert_eq!(current(), ChainScope::Unknown);
        scope(ChainScope::Evm(1), async {
            assert_eq!(current(), ChainScope::Evm(1));
            scope(ChainScope::Evm(10), async {
                assert_eq!(current(), ChainScope::Evm(10));
            })
            .await;
            assert_eq!(current(), ChainScope::Evm(1));
            assert_eq!(
                tokio::spawn(async { current() }).await.unwrap(),
                ChainScope::Unknown
            );
        })
        .await;
        assert_eq!(current(), ChainScope::Unknown);
    }

    #[tokio::test]
    #[ignore = "requires disposable Redis in TEST_REDIS_URL"]
    async fn owned_journal_task_keeps_scope_and_drains_after_its_caller_is_cancelled() {
        let fixture = crate::recovery::tests::Fixture::fresh();
        fixture.initialize().await;
        let journal = fixture.open().await;
        let task_journal = journal.clone();
        let (entered, arrived) = oneshot::channel();
        let (resume, resumed) = oneshot::channel();
        let (finished, completion) = oneshot::channel();
        let caller = tokio::spawn(scope(ChainScope::Evm(10), async move {
            task_journal
                .run_owned(move |journal| async move {
                    entered.send(current()).unwrap();
                    let _ = resumed.await;
                    let _guard = journal.serial_guard("fairness_test").await;
                    finished.send(current()).unwrap();
                    Ok(())
                })
                .await
        }));
        assert_eq!(arrived.await.unwrap(), ChainScope::Evm(10));
        caller.abort();
        let _ = caller.await;
        resume.send(()).unwrap();
        assert_eq!(completion.await.unwrap(), ChainScope::Evm(10));
        journal.drain_mutations().await;
        assert!(journal.fairness.evm.lock().unwrap().contains_key(&10));
        drop(journal);
        fixture.cleanup(&[]).await;
    }
}
