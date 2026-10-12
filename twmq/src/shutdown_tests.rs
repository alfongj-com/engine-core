//! Actual Redis replies stalled after pop applies. Shutdown must not wait for
//! that reply, dispatch its unseen jobs, or cancel already-running handlers.
use super::*;
use crate::lease_tests::{Gate, GatedHandler, Handler, cleanup, redis_url};
use std::sync::atomic::{AtomicBool, Ordering};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    task::{JoinHandle, JoinSet},
};

struct PausedReply {
    url: String,
    armed: Arc<AtomicBool>,
    reached: Arc<Semaphore>,
    release: Arc<Semaphore>,
    task: JoinHandle<()>,
}
impl Drop for PausedReply {
    fn drop(&mut self) {
        self.task.abort();
    }
}
impl PausedReply {
    async fn start() -> Self {
        let client = redis::Client::open(redis_url()).unwrap();
        let database = client.get_connection_info().redis.db;
        let redis::ConnectionAddr::Tcp(host, port) = client.get_connection_info().addr.clone()
        else {
            panic!("shutdown test requires local TCP Redis");
        };
        assert!(matches!(host.as_str(), "127.0.0.1" | "localhost" | "::1"));
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let proxy_port = listener.local_addr().unwrap().port();
        let armed = Arc::new(AtomicBool::new(false));
        let reached = Arc::new(Semaphore::new(0));
        let release = Arc::new(Semaphore::new(0));
        let control = (armed.clone(), reached.clone(), release.clone());
        let task = tokio::spawn(async move {
            let mut connections = JoinSet::new();
            loop {
                tokio::select! {
                    accepted = listener.accept() => {
                        let Ok((client, _)) = accepted else { break };
                        let address = (host.clone(), port);
                        let (armed, reached, release) = control.clone();
                        connections.spawn(async move {
                            let server = TcpStream::connect(address).await?;
                            let (mut client_read, mut client_write) = client.into_split();
                            let (mut server_read, mut server_write) = server.into_split();
                            let commands = tokio::io::copy(&mut client_read, &mut server_write);
                            let replies = async move {
                                let mut buffer = [0u8; 8192];
                                loop {
                                    let count = server_read.read(&mut buffer).await?;
                                    if count == 0 { return Ok::<(), std::io::Error>(()); }
                                    if armed.swap(false, Ordering::SeqCst) {
                                        reached.add_permits(1);
                                        release.acquire().await.unwrap().forget();
                                    }
                                    client_write.write_all(&buffer[..count]).await?;
                                }
                            };
                            tokio::try_join!(commands, replies)?;
                            Ok::<(), std::io::Error>(())
                        });
                    }
                    _ = connections.join_next(), if !connections.is_empty() => {},
                }
            }
        });
        Self {
            url: format!("redis://127.0.0.1:{proxy_port}/{database}"),
            armed,
            reached,
            release,
            task,
        }
    }

    async fn wait(&self) {
        tokio::time::timeout(Duration::from_secs(3), self.reached.acquire())
            .await
            .unwrap()
            .unwrap()
            .forget();
    }
}

#[tokio::test]
#[ignore = "requires disposable Redis in TEST_REDIS_URL"]
async fn shutdown_interrupts_unanswered_single_queue_pop_without_losing_its_job() {
    let proxy = PausedReply::start().await;
    let name = format!("shutdown-single:{}", nanoid::nanoid!());
    let counter = format!("twmq:{name}:hook-count");
    let queue = Arc::new(
        Queue::new(
            &proxy.url,
            &name,
            Some(QueueOptions {
                local_concurrency: 1,
                polling_interval: Duration::from_secs(3600),
                ..Default::default()
            }),
            Handler {
                counter: counter.clone(),
            },
        )
        .await
        .unwrap(),
    );
    queue
        .push(JobOptions::new(7).with_id("held"))
        .await
        .unwrap();
    // Warm the real pop script without borrowing work, so the held reply belongs
    // to an applied pop rather than Redis's first EVALSHA/NOSCRIPT negotiation.
    assert!(queue.pop_batch_jobs(0).await.unwrap().is_empty());
    proxy.armed.store(true, Ordering::SeqCst);
    let worker = queue.work();
    proxy.wait().await;
    let direct = Arc::new(
        Queue::new(
            &redis_url(),
            &name,
            None,
            Handler {
                counter: counter.clone(),
            },
        )
        .await
        .unwrap(),
    );
    let lease: String = direct
        .redis
        .clone()
        .hget(direct.job_meta_hash_name("held"), "lease_token")
        .await
        .unwrap();
    assert_eq!(direct.count(JobStatus::Active).await.unwrap(), 1);
    tokio::time::timeout(Duration::from_secs(2), worker.shutdown())
        .await
        .expect("shutdown waited for an unanswered Redis pop")
        .unwrap();
    proxy.release.add_permits(1);
    assert_eq!(
        direct
            .redis
            .clone()
            .get::<_, Option<u64>>(&counter)
            .await
            .unwrap(),
        None
    );
    // Model expiry explicitly. The original job becomes borrowable under a new
    // lease and completes exactly once; shutdown never invents an acknowledgement.
    let _: () = direct
        .redis
        .clone()
        .del(direct.lease_key_name("held", &lease))
        .await
        .unwrap();
    let resumed = direct.work();
    tokio::time::timeout(Duration::from_secs(3), async {
        while direct.count(JobStatus::Success).await.unwrap() != 1 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    resumed.shutdown().await.unwrap();
    assert_eq!(direct.count(JobStatus::Success).await.unwrap(), 1);
    assert_eq!(
        direct.redis.clone().get::<_, u64>(&counter).await.unwrap(),
        1
    );
    cleanup(&mut direct.redis.clone(), &format!("twmq:{name}")).await;
}

#[tokio::test]
#[ignore = "requires disposable Redis in TEST_REDIS_URL"]
async fn shutdown_interrupts_unanswered_multilane_pop_without_losing_its_job() {
    let proxy = PausedReply::start().await;
    let name = format!("shutdown-multilane:{}", nanoid::nanoid!());
    let counter = format!("twmq_multilane:{name}:hook-count");
    let queue = Arc::new(
        MultilaneQueue::new(
            &proxy.url,
            &name,
            Some(QueueOptions {
                local_concurrency: 1,
                polling_interval: Duration::from_secs(3600),
                ..Default::default()
            }),
            Handler {
                counter: counter.clone(),
            },
        )
        .await
        .unwrap(),
    );
    queue
        .push_to_lane("lane", JobOptions::new(7).with_id("held"))
        .await
        .unwrap();
    assert!(queue.pop_batch_jobs(0).await.unwrap().is_empty());
    proxy.armed.store(true, Ordering::SeqCst);
    let worker = queue.work();
    proxy.wait().await;
    let direct = Arc::new(
        MultilaneQueue::new(
            &redis_url(),
            &name,
            None,
            Handler {
                counter: counter.clone(),
            },
        )
        .await
        .unwrap(),
    );
    let lease: String = direct
        .redis
        .clone()
        .hget(direct.job_meta_hash_name("held"), "lease_token")
        .await
        .unwrap();
    assert_eq!(
        direct
            .redis
            .clone()
            .hlen::<_, u64>(direct.lane_active_hash_name("lane"))
            .await
            .unwrap(),
        1
    );
    tokio::time::timeout(Duration::from_secs(2), worker.shutdown())
        .await
        .expect("shutdown waited for an unanswered multilane Redis pop")
        .unwrap();
    proxy.release.add_permits(1);
    assert_eq!(
        direct
            .redis
            .clone()
            .get::<_, Option<u64>>(&counter)
            .await
            .unwrap(),
        None
    );
    let _: () = direct
        .redis
        .clone()
        .del(direct.lease_key_name("held", &lease))
        .await
        .unwrap();
    let resumed = direct.work();
    tokio::time::timeout(Duration::from_secs(3), async {
        while direct.count(JobStatus::Success, None).await.unwrap() != 1 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    resumed.shutdown().await.unwrap();
    assert_eq!(direct.count(JobStatus::Success, None).await.unwrap(), 1);
    assert_eq!(
        direct.redis.clone().get::<_, u64>(&counter).await.unwrap(),
        1
    );
    cleanup(&mut direct.redis.clone(), &format!("twmq_multilane:{name}")).await;
}

#[tokio::test]
#[ignore = "requires disposable Redis in TEST_REDIS_URL"]
async fn shutdown_still_drains_an_active_handler_before_acknowledging_it() {
    let name = format!("shutdown-active:{}", nanoid::nanoid!());
    let gate = Gate::new();
    let queue = Arc::new(
        Queue::new(
            &redis_url(),
            &name,
            Some(QueueOptions {
                local_concurrency: 1,
                ..Default::default()
            }),
            GatedHandler { gate: gate.clone() },
        )
        .await
        .unwrap(),
    );
    queue
        .push(JobOptions::new(1).with_id("active"))
        .await
        .unwrap();
    let worker = queue.work();
    gate.wait_for_started(1).await;
    let mut shutdown = Box::pin(worker.shutdown());
    std::future::poll_fn(|cx| {
        assert!(shutdown.as_mut().poll(cx).is_pending());
        std::task::Poll::Ready(())
    })
    .await;
    assert_eq!(queue.count(JobStatus::Success).await.unwrap(), 0);
    gate.releases.add_permits(1);
    tokio::time::timeout(Duration::from_secs(2), shutdown)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(queue.count(JobStatus::Success).await.unwrap(), 1);
    cleanup(&mut queue.redis.clone(), &format!("twmq:{name}")).await;
}
