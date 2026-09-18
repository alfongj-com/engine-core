use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use engine_core::execution_options::WebhookOptions;
use hex;
use hmac::{Hmac, Mac};
use reqwest::StatusCode;
use reqwest::header::{HeaderMap, HeaderName, HeaderValue};
use serde::{Deserialize, Serialize};
use twmq::error::TwmqError;
use twmq::hooks::TransactionContext;
use twmq::job::{BorrowedJob, JobError, JobResult, RequeuePosition, ToJobResult};
use twmq::{DurableExecution, FailHookData, NackHookData, Queue, SuccessHookData, UserCancellable};

use crate::webhook::envelope::{BareWebhookNotificationEnvelope, WebhookNotificationEnvelope};

pub mod destination;
pub mod envelope;

pub use destination::WebhookDestinationPolicy;

// --- Constants ---
const SIGNATURE_HEADER_NAME: &str = "x-signature-sha256";
const TIMESTAMP_HEADER_NAME: &str = "x-request-timestamp";
const MAX_RESPONSE_BYTES: usize = 64 * 1024;

async fn read_bounded_response(mut response: reqwest::Response) -> Result<String, reqwest::Error> {
    let mut bytes = Vec::new();
    while let Some(chunk) = response.chunk().await? {
        let remaining = MAX_RESPONSE_BYTES - bytes.len();
        bytes.extend_from_slice(&chunk[..chunk.len().min(remaining)]);
        if bytes.len() == MAX_RESPONSE_BYTES {
            break;
        }
    }
    Ok(String::from_utf8_lossy(&bytes).into_owned())
}

fn error_body_preview(text: String) -> String {
    let mut characters = text.chars();
    let mut preview: String = characters.by_ref().take(512).collect();
    if characters.next().is_some() {
        preview.push_str("...");
    }
    preview
}

// --- Configuration ---
#[derive(Clone, Debug)]
pub struct WebhookRetryConfig {
    pub max_attempts: u32,
    pub initial_delay_ms: u64,
    pub max_delay_ms: u64,
    pub backoff_factor: f64,
}

impl Default for WebhookRetryConfig {
    fn default() -> Self {
        Self {
            max_attempts: 5,
            initial_delay_ms: 1000, // 1 second
            max_delay_ms: 30000,    // 30 seconds
            backoff_factor: 2.0,
        }
    }
}

// --- Execution Context ---
pub struct WebhookJobHandler {
    http_client: reqwest::Client,
    destination_policy: WebhookDestinationPolicy,
    pub retry_config: Arc<WebhookRetryConfig>,
}

impl WebhookJobHandler {
    pub fn new(
        destination_policy: WebhookDestinationPolicy,
        retry_config: Arc<WebhookRetryConfig>,
    ) -> Result<Self, WebhookError> {
        let http_client = destination::client_builder().build().map_err(|_| {
            WebhookError::RequestConstruction("failed to initialize webhook HTTP client".into())
        })?;
        Ok(Self {
            http_client,
            destination_policy,
            retry_config,
        })
    }
}

// --- Webhook Job Data Structures ---
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct WebhookJobPayload {
    pub url: String,
    pub body: String, // Assuming pre-serialized JSON or other content
    pub headers: Option<HashMap<String, String>>,
    pub hmac_secret: Option<String>, // Secret key for HMAC-SHA256
    pub http_method: Option<String>, // e.g., "POST", "PUT". Defaults to "POST"
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
pub struct WebhookJobOutput {
    pub status_code: u16,
    pub response_body: Option<String>,
}

// --- Webhook Error Enum ---
#[derive(Serialize, Deserialize, Debug, Clone, thiserror::Error)]
#[serde(
    rename_all = "SCREAMING_SNAKE_CASE",
    tag = "errorCode",
    content = "message"
)]
pub enum WebhookError {
    #[error("Webhook destination rejected: {0}")]
    DestinationRejected(String),

    #[error("Network error during webhook dispatch: {0}")]
    Network(String),

    #[error("Failed to construct webhook request: {0}")]
    RequestConstruction(String),

    #[error("HMAC signature generation failed: {0}")]
    HmacGeneration(String),

    #[error("Webhook request timed out: {0}")]
    Timeout(String),

    #[error("HTTP error from endpoint: status {status}, body: {body_preview}")]
    Http {
        status: u16,
        body_preview: String, // Store a preview of the raw response body
    },

    #[error("Failed to read or decode webhook response body: {0}")]
    ResponseReadError(String),

    #[error("Unsupported HTTP method: {0}")]
    UnsupportedHttpMethod(String),

    #[error("Internal queue error: {0}")]
    InternalQueueError(String),

    #[error("Transaction cancelled by user")]
    UserCancelled,
}

impl From<TwmqError> for WebhookError {
    fn from(error: TwmqError) -> Self {
        WebhookError::InternalQueueError(error.to_string())
    }
}

impl UserCancellable for WebhookError {
    fn user_cancelled() -> Self {
        WebhookError::UserCancelled
    }
}

// --- DurableExecution Implementation ---
impl DurableExecution for WebhookJobHandler {
    type Output = WebhookJobOutput;
    type ErrorData = WebhookError;
    type JobData = WebhookJobPayload;

    #[tracing::instrument(skip_all, fields(queue = "webhook", job_id = job.job.id))]
    async fn process(
        &self,
        job: &BorrowedJob<Self::JobData>,
    ) -> JobResult<Self::Output, Self::ErrorData> {
        let payload = &job.job.data;
        let destination = self
            .destination_policy
            .validate(&payload.url)
            .map_err(JobError::Fail)?;
        let mut request_headers = HeaderMap::new();

        // Set default Content-Type if body is present, can be overridden by payload.headers
        if !payload.body.is_empty() {
            request_headers.insert(
                reqwest::header::CONTENT_TYPE,
                HeaderValue::from_static("application/json; charset=utf-8"),
            );
        }

        // Apply custom headers
        if let Some(custom_headers) = &payload.headers {
            for (key, value) in custom_headers {
                let header_name = HeaderName::from_bytes(key.as_bytes()).map_err(|e| {
                    JobError::Fail(WebhookError::RequestConstruction(format!(
                        "Invalid header name '{key}': {e}"
                    )))
                })?;

                if matches!(
                    header_name.as_str(),
                    "host"
                        | "content-length"
                        | "transfer-encoding"
                        | "connection"
                        | "upgrade"
                        | "proxy-authorization"
                        | "proxy-connection"
                ) {
                    return Err(JobError::Fail(WebhookError::RequestConstruction(format!(
                        "webhook header '{header_name}' is not allowed"
                    ))));
                }

                let header_value = HeaderValue::from_str(value).map_err(|e| {
                    JobError::Fail(WebhookError::RequestConstruction(format!(
                        "Invalid header value for '{key}': {e}"
                    )))
                })?;

                request_headers.insert(header_name, header_value);
            }
        };

        // HMAC Signature with Timestamp
        if let Some(secret) = &payload.hmac_secret {
            if secret.is_empty() {
                return Err(JobError::Fail(WebhookError::HmacGeneration(
                    "HMAC secret cannot be empty".to_string(),
                )));
            }

            let timestamp_secs = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .map_err(|e| {
                    JobError::Fail(WebhookError::RequestConstruction(format!(
                        "Failed to get system time for timestamp: {e}"
                    )))
                })?
                .as_secs();

            let timestamp_str = timestamp_secs.to_string();

            // Canonical message to sign: "timestamp.body"
            // The dot (.) is a common separator.
            let message_to_sign = format!("{}.{}", timestamp_str, payload.body);

            type HmacSha256 = Hmac<sha2::Sha256>;
            let mut mac = HmacSha256::new_from_slice(secret.as_bytes())
                .map_err(|e| {
                    WebhookError::HmacGeneration(format!("Failed to initialize HMAC: {e}"))
                })
                .map_err_fail()?;

            mac.update(message_to_sign.as_bytes());
            let signature_bytes = mac.finalize().into_bytes();
            let signature_hex = hex::encode(signature_bytes);

            let signature_header_value = HeaderValue::from_str(&signature_hex)
                .map_err(|e| {
                    WebhookError::RequestConstruction(format!(
                        "Invalid header value for '{SIGNATURE_HEADER_NAME}': {e}"
                    ))
                })
                .map_err_fail()?;

            let timestamp_header_value = HeaderValue::from_str(&timestamp_str)
                .map_err(|e| {
                    WebhookError::RequestConstruction(format!(
                        "Invalid header value for '{TIMESTAMP_HEADER_NAME}': {e}"
                    ))
                })
                .map_err_fail()?;

            request_headers.insert(
                HeaderName::from_static(SIGNATURE_HEADER_NAME),
                signature_header_value,
            );
            request_headers.insert(
                HeaderName::from_static(TIMESTAMP_HEADER_NAME),
                timestamp_header_value,
            );
        }

        let http_method_str = payload
            .http_method
            .as_deref()
            .unwrap_or("POST")
            .to_uppercase();

        let method = reqwest::Method::from_bytes(http_method_str.as_bytes())
            .map_err(|_| WebhookError::UnsupportedHttpMethod(http_method_str.clone()))
            .map_err_fail()?;

        if !matches!(
            method,
            reqwest::Method::POST | reqwest::Method::PUT | reqwest::Method::PATCH
        ) {
            return Err(JobError::Fail(WebhookError::UnsupportedHttpMethod(
                http_method_str,
            )));
        }

        let request_builder = self
            .http_client
            .request(method, destination)
            .headers(request_headers)
            .body(payload.body.clone());

        tracing::debug!(
            job_id = job.job.id,
            method = http_method_str,
            attempt = job.job.attempts,
            "Sending webhook request"
        );

        match request_builder.send().await {
            Ok(response) => {
                let status = response.status();
                let response_body_text_result = read_bounded_response(response).await;

                let response_body_text = match response_body_text_result {
                    Ok(text) => Some(text),
                    Err(e) => {
                        let e = e.without_url();
                        if status.is_success() {
                            let err = WebhookError::ResponseReadError(format!(
                                "Failed to read response body: {e}"
                            ));
                            return Err(err).map_err_fail();
                        }
                        tracing::warn!(
                            job_id = job.job.id,
                            "Failed to read response body for error status {}: {}",
                            status,
                            e
                        );
                        None
                    }
                };

                if status.is_success() {
                    tracing::info!(job_id = job.job.id, status = ?status, "Webhook delivered successfully");
                    Ok(WebhookJobOutput {
                        status_code: status.as_u16(),
                        response_body: response_body_text,
                    })
                } else {
                    let error_body_preview = response_body_text
                        .map(error_body_preview)
                        .unwrap_or_else(|| "No body or failed to read body".to_string());

                    let webhook_error = WebhookError::Http {
                        status: status.as_u16(),
                        body_preview: error_body_preview,
                    };

                    if status.is_server_error() || status == StatusCode::TOO_MANY_REQUESTS {
                        if job.job.attempts < self.retry_config.max_attempts {
                            let delay_ms = self.retry_config.initial_delay_ms as f64
                                * self
                                    .retry_config
                                    .backoff_factor
                                    .powi(job.job.attempts as i32 - 1); // Use current attempts for backoff
                            let delay_ms =
                                (delay_ms.min(self.retry_config.max_delay_ms as f64)) as u64;
                            let delay = Duration::from_millis(delay_ms);

                            tracing::warn!(
                                job_id = job.job.id,
                                status = ?status,
                                attempt = job.job.attempts,
                                max_attempts = self.retry_config.max_attempts,
                                delay_ms = delay.as_millis(),
                                "Webhook failed with retryable status, NACKing."
                            );
                            Err(JobError::Nack {
                                error: webhook_error,
                                delay: Some(delay),
                                position: RequeuePosition::Last,
                            })
                        } else {
                            tracing::error!(
                                job_id = job.job.id,
                                status = ?status,
                                attempt = job.job.attempts,
                                "Webhook failed after max attempts, FAILING."
                            );
                            Err(JobError::Fail(webhook_error))
                        }
                    } else {
                        tracing::error!(
                            job_id = job.job.id,
                            status = ?status,
                            "Webhook failed with non-retryable client error, FAILING."
                        );
                        Err(JobError::Fail(webhook_error))
                    }
                }
            }
            Err(e) => {
                // Request URLs may carry query-string credentials.
                let e = e.without_url();
                let webhook_error = if e.is_timeout() {
                    WebhookError::Timeout(e.to_string())
                } else if e.is_connect() || e.is_request() {
                    WebhookError::Network(e.to_string())
                } else {
                    WebhookError::RequestConstruction(e.to_string())
                };

                if matches!(webhook_error, WebhookError::RequestConstruction(_))
                    && !e.is_connect()
                    && !e.is_timeout()
                {
                    tracing::error!(job_id = job.job.id, error = ?webhook_error, "Webhook construction error, FAILING.");
                    return Err(JobError::Fail(webhook_error));
                }

                if job.job.attempts < self.retry_config.max_attempts {
                    let delay_ms = self.retry_config.initial_delay_ms as f64
                        * self
                            .retry_config
                            .backoff_factor
                            .powi(job.job.attempts as i32 - 1); // Use current attempts for backoff
                    let delay_ms = (delay_ms.min(self.retry_config.max_delay_ms as f64)) as u64;
                    let delay = Duration::from_millis(delay_ms);

                    tracing::warn!(
                        job_id = job.job.id,
                        error = ?webhook_error,
                        attempt = job.job.attempts,
                        max_attempts = self.retry_config.max_attempts,
                        delay_ms = delay.as_millis(),
                        "Webhook request failed, NACKing."
                    );

                    Err(JobError::Nack {
                        error: webhook_error,
                        delay: Some(delay),
                        position: RequeuePosition::Last,
                    })
                } else {
                    tracing::error!(
                        job_id = job.job.id,
                        error = ?webhook_error,
                        attempt = job.job.attempts,
                        "Webhook request failed after max attempts, FAILING."
                    );
                    Err(JobError::Fail(webhook_error))
                }
            }
        }
    }

    #[tracing::instrument(skip_all, fields(queue = "webhook", job_id = job.job.id))]
    async fn on_success(
        &self,
        job: &BorrowedJob<Self::JobData>,
        d: SuccessHookData<'_, Self::Output>,
        _tx: &mut TransactionContext<'_>,
    ) {
        tracing::info!(
            job_id = job.job.id,
            status = d.result.status_code,
            "Webhook successfully processed (on_success hook)."
        );
    }

    #[tracing::instrument(skip_all, fields(queue = "webhook", job_id = job.job.id))]
    async fn on_nack(
        &self,
        job: &BorrowedJob<Self::JobData>,
        d: NackHookData<'_, Self::ErrorData>,
        _tx: &mut TransactionContext<'_>,
    ) {
        tracing::warn!(
            job_id = job.job.id,
            attempt = job.job.attempts,
            error = ?d.error,
            delay_ms = d.delay.map_or(0, |dur| dur.as_millis()),
            "Webhook NACKed (on_nack hook)."
        );
    }

    #[tracing::instrument(skip_all, fields(queue = "webhook", job_id = job.job.id))]
    async fn on_fail(
        &self,
        job: &BorrowedJob<Self::JobData>,
        d: FailHookData<'_, Self::ErrorData>,
        _tx: &mut TransactionContext<'_>,
    ) {
        tracing::error!(
            job_id = job.job.id,
            attempt = job.job.attempts,
            error = ?d.error,
            "Webhook FAILED permanently (on_fail hook)."
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    fn job(url: &str) -> BorrowedJob<WebhookJobPayload> {
        BorrowedJob::new(
            twmq::job::Job {
                id: "webhook-policy-test".into(),
                data: WebhookJobPayload {
                    url: url.into(),
                    body: "{}".into(),
                    headers: None,
                    hmac_secret: None,
                    http_method: None,
                },
                attempts: 1,
                created_at: 0,
                processed_at: None,
                finished_at: None,
            },
            "lease".into(),
        )
    }

    #[tokio::test]
    async fn dispatch_enforces_policy_before_request_construction() {
        let handler = WebhookJobHandler::new(
            WebhookDestinationPolicy::default(),
            Arc::new(WebhookRetryConfig::default()),
        )
        .unwrap();
        assert!(matches!(
            handler
                .process(&job("https://hooks.example.com/event"))
                .await,
            Err(JobError::Fail(WebhookError::DestinationRejected(_)))
        ));
        let handler = WebhookJobHandler::new(
            WebhookDestinationPolicy::from_csv("https://hooks.example.com").unwrap(),
            Arc::new(WebhookRetryConfig::default()),
        )
        .unwrap();
        for header in [
            "Host",
            "Content-Length",
            "Transfer-Encoding",
            "Connection",
            "Upgrade",
            "Proxy-Authorization",
        ] {
            let mut request = job("https://hooks.example.com/event");
            request.job.data.headers = Some(
                [(header.to_string(), "evil.test".to_string())]
                    .into_iter()
                    .collect(),
            );
            assert!(
                matches!(
                    handler.process(&request).await,
                    Err(JobError::Fail(WebhookError::RequestConstruction(_)))
                ),
                "accepted {header}"
            );
        }
        for method in ["CONNECT", "TRACE", "DELETE", "GET"] {
            let mut request = job("https://hooks.example.com/event");
            request.job.data.http_method = Some(method.into());
            assert!(
                matches!(
                    handler.process(&request).await,
                    Err(JobError::Fail(WebhookError::UnsupportedHttpMethod(_)))
                ),
                "accepted {method}"
            );
        }
    }

    #[test]
    fn multibyte_error_preview_never_slices_inside_a_character() {
        let input = format!("{}é🙂end", "a".repeat(511));
        assert_eq!(
            error_body_preview(input),
            format!("{}é...", "a".repeat(511))
        );
        assert_eq!(error_body_preview("🙂".repeat(4)), "🙂".repeat(4));
    }

    #[tokio::test]
    async fn response_reader_stops_at_limit_without_waiting_for_large_body_to_finish() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let (release, wait) = tokio::sync::oneshot::channel::<()>();
        let server = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.unwrap();
            let mut request = [0u8; 1024];
            assert!(
                socket.read(&mut request).await.unwrap() > 0,
                "client must begin its request before the fixture replies"
            );
            socket
                .write_all(
                    format!(
                        "HTTP/1.1 500 Error\r\nContent-Length: {}\r\n\r\n",
                        MAX_RESPONSE_BYTES * 100
                    )
                    .as_bytes(),
                )
                .await
                .unwrap();
            socket
                .write_all(&vec![b'a'; MAX_RESPONSE_BYTES + 1024])
                .await
                .unwrap();
            let _ = wait.await; // Deliberately withhold the rest of the response.
        });
        let response = destination::client_builder()
            .https_only(false)
            .build()
            .unwrap()
            .get(format!("http://{address}/"))
            .send()
            .await
            .unwrap();
        let body = tokio::time::timeout(Duration::from_secs(1), read_bounded_response(response))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(body.len(), MAX_RESPONSE_BYTES);
        assert!(body.bytes().all(|byte| byte == b'a'));
        release.send(()).unwrap();
        server.await.unwrap();
    }
}

pub fn queue_webhook_envelopes<T: Serialize + Clone>(
    envelope: BareWebhookNotificationEnvelope<T>,
    webhook_options: Vec<WebhookOptions>,
    tx: &mut TransactionContext<'_>,
    webhook_queue: Arc<Queue<WebhookJobHandler>>,
) -> Result<(), TwmqError> {
    let now = chrono::Utc::now().timestamp().max(0) as u64;
    let serialised_webhook_envelopes =
        webhook_options
            .iter()
            .map(|webhook_option| {
                let webhook_notification_envelope =
                    envelope.clone().into_webhook_notification_envelope(
                        now,
                        webhook_option.url.clone(),
                        webhook_option.user_metadata.clone(),
                    );
                let serialised_envelope = serde_json::to_string(&webhook_notification_envelope)?;
                Ok((
                    serialised_envelope,
                    webhook_notification_envelope,
                    webhook_option.clone(),
                ))
            })
            .collect::<Result<
                Vec<(String, WebhookNotificationEnvelope<T>, WebhookOptions)>,
                serde_json::Error,
            >>()?;

    let webhook_payloads = serialised_webhook_envelopes
        .into_iter()
        .map(
            |(serialised_envelope, webhook_notification_envelope, webhook_option)| {
                let payload = WebhookJobPayload {
                    url: webhook_option.url,
                    body: serialised_envelope,
                    headers: Some(
                        [
                            ("Content-Type".to_string(), "application/json".to_string()),
                            (
                                "User-Agent".to_string(),
                                format!("{}/{}", envelope.executor_name, envelope.stage_name),
                            ),
                        ]
                        .into_iter()
                        .collect(),
                    ),
                    hmac_secret: webhook_option.secret, // TODO: Add HMAC support if needed
                    http_method: Some("POST".to_string()),
                };
                (payload, webhook_notification_envelope)
            },
        )
        .collect::<Vec<_>>();

    for (payload, webhook_notification_envelope) in webhook_payloads {
        let mut webhook_job = webhook_queue.clone().job(payload);
        webhook_job.options.id = format!(
            "{}_{}_webhook",
            webhook_notification_envelope.transaction_id,
            webhook_notification_envelope.notification_id
        );

        tx.queue_job(webhook_job)?;
        tracing::info!(
            transaction_id = webhook_notification_envelope.transaction_id,
            executor = webhook_notification_envelope.executor_name,
            stage = webhook_notification_envelope.stage_name,
            event = ?webhook_notification_envelope.event_type,
            notification_id = webhook_notification_envelope.notification_id,
            "Queued webhook notification"
        );
    }

    Ok(())
}
