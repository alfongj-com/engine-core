use std::sync::Arc;

use axum::{
    Json, Router,
    extract::{Request, State},
    http::{Method, StatusCode},
    middleware::{self, Next},
    response::{IntoResponse, Response},
    routing::get,
};
use engine_core::{
    credentials::KmsClientCache,
    signer::{EoaSigner, SolanaSigner},
    userop::UserOpSigner,
};
use engine_executors::solana_executor::rpc_cache::SolanaRpcCache;
use http_body_util::BodyExt;
use serde_json::json;
use thirdweb_core::abi::ThirdwebAbiService;
use tokio::{
    sync::{Semaphore, watch},
    task::JoinHandle,
};
use utoipa::OpenApi;
use utoipa_axum::{router::OpenApiRouter, routes};
use utoipa_scalar::{Scalar, Servable};

use crate::{
    chains::ThirdwebChainService, execution_router::ExecutionRouter,
    http::routes::admin::eoa_diagnostics::eoa_diagnostics_router, queue::manager::QueueManager,
};
use tower_http::{
    cors::{Any, CorsLayer},
    trace::TraceLayer,
};

#[derive(Clone)]
pub struct EngineServerState {
    pub chains: Arc<ThirdwebChainService>,
    pub userop_signer: Arc<UserOpSigner>,
    pub eoa_signer: Arc<EoaSigner>,
    pub solana_signer: Arc<SolanaSigner>,
    pub solana_rpc_cache: Arc<SolanaRpcCache>,
    pub abi_service: Arc<ThirdwebAbiService>,

    pub execution_router: Arc<ExecutionRouter>,
    pub queue_manager: Arc<QueueManager>,

    pub diagnostic_access_password: Option<String>,
    pub metrics_registry: Arc<prometheus::Registry>,
    pub kms_client_cache: KmsClientCache,
}

pub struct EngineServer {
    handle: Option<JoinHandle<Result<(), std::io::Error>>>,
    shutdown_tx: Option<watch::Sender<bool>>,
    app: Router,
}

const SCALAR_HTML: &str = include_str!("../../res/scalar.html");

impl EngineServer {
    pub async fn new(state: EngineServerState) -> Self {
        #[derive(OpenApi)]
        struct ApiDoc;

        let cors = CorsLayer::new()
            .allow_origin(Any)
            .allow_methods(Any)
            .allow_headers(Any)
            .allow_credentials(false);

        let v1_router = OpenApiRouter::new()
            .routes(routes!(crate::http::routes::contract_write::write_contract,))
            .routes(routes!(
                crate::http::routes::contract_encode::encode_contract
            ))
            .routes(routes!(crate::http::routes::contract_read::read_contract,))
            .routes(routes!(
                crate::http::routes::transaction_write::write_transaction
            ))
            .routes(routes!(
                crate::http::routes::solana_transaction::send_solana_transaction
            ))
            .routes(routes!(
                crate::http::routes::sign_solana_transaction::sign_solana_transaction
            ))
            .routes(routes!(
                crate::http::routes::transaction::cancel_transaction
            ))
            .routes(routes!(crate::http::routes::sign_message::sign_message))
            .routes(routes!(
                crate::http::routes::sign_typed_data::sign_typed_data
            ))
            .routes(routes!(
                crate::http::routes::admin::queue::empty_queue_idempotency_set
            ))
            .layer(cors)
            .layer(TraceLayer::new_for_http())
            .with_state(state.clone());

        let eoa_diagnostics_router = eoa_diagnostics_router().with_state(state.clone());

        // Create metrics router
        let metrics_router = Router::new()
            .route(
                "/metrics",
                get(crate::http::routes::admin::metrics::get_metrics),
            )
            .with_state(state.metrics_registry.clone());

        let (router, api) = OpenApiRouter::with_openapi(ApiDoc::openapi())
            .nest("/v1", v1_router)
            .split_for_parts();

        // Merge the hidden diagnostic routes after OpenAPI split
        let router = router.merge(eoa_diagnostics_router).merge(metrics_router);

        let api_clone = api.clone();
        let router = router
            .merge(Scalar::with_url("/reference", api).custom_html(SCALAR_HTML))
            // health endpoint with 200 and JSON response {}
            .route("/health", get(recovery_health))
            .route("/api.json", get(|| async { Json(api_clone) }))
            .layer(middleware::from_fn(recovery_gate))
            // Outer layer rejects pressure before the journal/auth/handler can queue.
            .layer(middleware::from_fn_with_state(
                Arc::new(Semaphore::new(MAX_CONCURRENT_MUTATIONS)),
                mutation_limit,
            ));

        Self {
            handle: None,
            shutdown_tx: None,
            app: router,
        }
    }

    pub fn start(&mut self, listener: tokio::net::TcpListener) -> Result<(), std::io::Error> {
        // Create a shutdown channel
        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        let app = self.app.clone();

        // Start the HTTP server in a background task
        let handle = tokio::spawn(async move {
            tracing::info!("HTTP server starting on {}", listener.local_addr().unwrap());

            axum::serve(listener, app)
                .with_graceful_shutdown(async move {
                    let mut rx = shutdown_rx;
                    while !*rx.borrow() {
                        if rx.changed().await.is_err() {
                            break;
                        }
                    }
                    tracing::info!("HTTP server shutting down");
                })
                .await
        });

        self.handle = Some(handle);
        self.shutdown_tx = Some(shutdown_tx);

        Ok(())
    }

    pub async fn shutdown(&mut self) -> Result<(), std::io::Error> {
        if let Some(tx) = self.shutdown_tx.take() {
            if tx.send(true).is_err() {
                tracing::error!("Failed to send shutdown signal to HTTP server");
            }
        }

        if let Some(handle) = self.handle.take() {
            match handle.await {
                Ok(result) => {
                    if let Err(e) = result {
                        tracing::error!("HTTP server error during shutdown: {}", e);
                        return Err(e);
                    }
                }
                Err(e) => {
                    tracing::error!("Failed to join HTTP server task: {}", e);
                    return Err(std::io::Error::other(format!("Task join error: {e}")));
                }
            }
        }

        Ok(())
    }
}

const MAX_CONCURRENT_MUTATIONS: usize = 64;

fn is_mutation(method: &Method) -> bool {
    !matches!(*method, Method::GET | Method::HEAD | Method::OPTIONS)
}

async fn mutation_limit(
    State(slots): State<Arc<Semaphore>>,
    request: Request,
    next: Next,
) -> Response {
    if !is_mutation(request.method()) {
        return next.run(request).await;
    }
    let Ok(permit) = slots.try_acquire_owned() else {
        let mut response =
            super::error::ApiEngineError(engine_core::error::EngineError::Overloaded {
                message: "Too many concurrent mutation requests; retry the same idempotency key"
                    .into(),
            })
            .into_response();
        response.headers_mut().insert(
            axum::http::header::RETRY_AFTER,
            axum::http::HeaderValue::from_static("1"),
        );
        return response;
    };
    let response = next.run(request).await;
    response.map(|body| {
        // Keep the permit until the response body is consumed or dropped. This
        // also releases it when a client disconnects or a handler is cancelled.
        axum::body::Body::new(body.map_frame(move |frame| {
            let _keep_permit = &permit;
            frame
        }))
    })
}

async fn recovery_gate(request: Request, next: Next) -> Response {
    if is_mutation(request.method()) {
        if engine_core::recovery::ensure_healthy().await.is_err() {
            return (StatusCode::SERVICE_UNAVAILABLE, Json(json!({
                "error": "RECOVERY_REQUIRED", "message": "Transaction writes and signing are paused; inspect the recovery journal"
            }))).into_response();
        }
    }
    next.run(request).await
}

async fn recovery_health() -> Response {
    match engine_core::recovery::ensure_healthy().await {
        Ok(()) => Json(json!({"status": "ok"})).into_response(),
        Err(_) => (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(json!({"status": "recovery_required"})),
        )
            .into_response(),
    }
}

#[cfg(test)]
#[path = "server_tests.rs"]
mod tests;
