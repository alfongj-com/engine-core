use super::*;
use axum::{
    body::{Body, to_bytes},
    routing::any,
};
use std::sync::atomic::{AtomicUsize, Ordering};
use tokio::sync::Notify;
use tower::ServiceExt;

fn request(method: Method) -> Request {
    Request::builder()
        .method(method)
        .uri("/work")
        .body(Body::empty())
        .unwrap()
}

fn protect(router: Router, slots: Arc<Semaphore>, gate_calls: Arc<AtomicUsize>) -> Router {
    router
        .layer(middleware::from_fn(recovery_gate))
        // An observation point immediately before the actual journal middleware.
        // If this is not entered, neither the journal nor handler can be reached.
        .layer(middleware::from_fn(move |request: Request, next: Next| {
            gate_calls.fetch_add(1, Ordering::SeqCst);
            async move { next.run(request).await }
        }))
        .layer(middleware::from_fn_with_state(slots, mutation_limit))
}

#[tokio::test]
async fn saturation_fails_before_journal_and_handler_while_get_bypasses_limit() {
    let slots = Arc::new(Semaphore::new(MAX_CONCURRENT_MUTATIONS));
    let gate_calls = Arc::new(AtomicUsize::new(0));
    let handler_calls = Arc::new(AtomicUsize::new(0));
    let handler = handler_calls.clone();
    let router = protect(
        Router::new().route(
            "/work",
            any(move || {
                handler.fetch_add(1, Ordering::SeqCst);
                async { "held response" }
            }),
        ),
        slots.clone(),
        gate_calls.clone(),
    );
    let mut held = Vec::new();
    for _ in 0..MAX_CONCURRENT_MUTATIONS {
        let response = router.clone().oneshot(request(Method::POST)).await.unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        held.push(response);
    }
    assert_eq!(
        slots.available_permits(),
        0,
        "permits remain held by undrained response bodies"
    );
    let overflow = router.clone().oneshot(request(Method::POST)).await.unwrap();
    assert_eq!(overflow.status(), StatusCode::TOO_MANY_REQUESTS);
    assert_eq!(overflow.headers()["retry-after"], "1");
    assert_eq!(gate_calls.load(Ordering::SeqCst), MAX_CONCURRENT_MUTATIONS);
    assert_eq!(
        handler_calls.load(Ordering::SeqCst),
        MAX_CONCURRENT_MUTATIONS
    );
    let health = router.clone().oneshot(request(Method::GET)).await.unwrap();
    assert_eq!(health.status(), StatusCode::OK);
    assert_eq!(slots.available_permits(), 0);
    drop(health);
    drop(held);
    assert_eq!(slots.available_permits(), MAX_CONCURRENT_MUTATIONS);
    let next = router.oneshot(request(Method::POST)).await.unwrap();
    assert_eq!(next.status(), StatusCode::OK);
    assert_eq!(
        to_bytes(next.into_body(), 1024).await.unwrap(),
        "held response"
    );
    assert_eq!(slots.available_permits(), MAX_CONCURRENT_MUTATIONS);
}

#[tokio::test]
async fn cancelling_inflight_handler_releases_http_slot_without_queueing_overflow() {
    let slots = Arc::new(Semaphore::new(1));
    let gate_calls = Arc::new(AtomicUsize::new(0));
    let entered = Arc::new(Notify::new());
    let handler = entered.clone();
    let router = protect(
        Router::new().route(
            "/work",
            any(move || {
                let handler = handler.clone();
                async move {
                    handler.notify_one();
                    std::future::pending::<()>().await;
                    StatusCode::OK
                }
            }),
        ),
        slots.clone(),
        gate_calls.clone(),
    );
    let waiting = tokio::spawn(router.clone().oneshot(request(Method::POST)));
    tokio::time::timeout(std::time::Duration::from_secs(2), entered.notified())
        .await
        .unwrap();
    assert_eq!(slots.available_permits(), 0);
    let overflow = router.oneshot(request(Method::POST)).await.unwrap();
    assert_eq!(overflow.status(), StatusCode::TOO_MANY_REQUESTS);
    assert_eq!(gate_calls.load(Ordering::SeqCst), 1);
    waiting.abort();
    assert!(waiting.await.unwrap_err().is_cancelled());
    assert_eq!(slots.available_permits(), 1);
}
