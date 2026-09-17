use alloy::{
    rpc::json_rpc::{RequestPacket, Response, ResponsePacket, ResponsePayload},
    transports::{
        TransportError, TransportErrorKind, TransportFut, TransportResult, http::reqwest,
    },
};
use std::{fmt, task};
use tower::Service;
use tracing::{Instrument, debug, debug_span};

/// Engine methods return bounded transaction/receipt data. Reject unexpectedly
/// large provider responses before retaining their entire body in memory.
const MAX_RPC_RESPONSE_BYTES: usize = 16 * 1024 * 1024;

/// Endpoint paths, queries, userinfo and even subdomains may contain credentials.
/// Chain IDs identify the destination in diagnostics without exposing its URL.
pub fn diagnostic_url(url: &reqwest::Url) -> String {
    format!("{}://[redacted]", url.scheme())
}

/// Shared HTTP connection pool with request-scoped headers. Credentials are never
/// installed as client defaults, so one service cannot inherit another's headers.
#[derive(Clone)]
pub struct HeaderInjectingTransport {
    client: reqwest::Client,
    url: reqwest::Url,
    custom_headers: reqwest::header::HeaderMap,
}

impl fmt::Debug for HeaderInjectingTransport {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("HeaderInjectingTransport")
            .field("endpoint", &diagnostic_url(&self.url))
            .field("headers", &"[redacted]")
            .finish()
    }
}

impl HeaderInjectingTransport {
    pub fn new(
        client: reqwest::Client,
        url: reqwest::Url,
        mut headers: reqwest::header::HeaderMap,
    ) -> Self {
        for value in headers.values_mut() {
            value.set_sensitive(true);
        }
        Self {
            client,
            url,
            custom_headers: headers,
        }
    }

    pub fn with_auth(
        client: reqwest::Client,
        url: reqwest::Url,
        client_id: &str,
        secret_key: &str,
    ) -> Result<Self, reqwest::header::InvalidHeaderValue> {
        let mut headers = reqwest::header::HeaderMap::new();
        headers.insert("x-client-id", client_id.parse()?);
        headers.insert("x-secret-key", secret_key.parse()?);
        Ok(Self::new(client, url, headers))
    }

    fn redact(&self, text: &str) -> String {
        let mut secrets = vec![self.url.as_str().to_owned()];
        // Some RPC services put an API key in a subdomain, and may echo only
        // that label in an error rather than the complete endpoint URL.
        if let Some(host) = self.url.host_str() {
            secrets.push(host.to_owned());
        }
        if let Some(domain) = self.url.domain() {
            let labels: Vec<_> = domain.split('.').collect();
            secrets.extend(
                labels[..labels.len().saturating_sub(2)]
                    .iter()
                    .map(|label| (*label).to_owned()),
            );
        }
        secrets.extend(
            self.url
                .path_segments()
                .into_iter()
                .flatten()
                .map(str::to_owned),
        );
        secrets.extend(self.url.query_pairs().map(|(_, value)| value.into_owned()));
        secrets.push(self.url.username().to_owned());
        if let Some(password) = self.url.password() {
            secrets.push(password.to_owned());
        }
        for value in self.custom_headers.values() {
            if let Ok(value) = value.to_str() {
                secrets.push(value.to_owned());
                // Protect a bearer token even if the endpoint omits the scheme when echoing it.
                if let Some((scheme, token)) = value.split_once(char::is_whitespace) {
                    if scheme.eq_ignore_ascii_case("bearer") {
                        secrets.push(token.trim().to_owned());
                    }
                }
            }
        }
        secrets.sort_by_key(|value| std::cmp::Reverse(value.len()));
        let mut result = text.to_owned();
        for secret in secrets.into_iter().filter(|value| !value.is_empty()) {
            result = result.replace(&secret, "[redacted]");
        }
        result
    }

    fn redact_response(&self, response: &mut Response) {
        if let ResponsePayload::Failure(error) = &mut response.payload {
            error.message = self.redact(&error.message).into();
            if let Some(data) = &error.data {
                // Decode before redaction so JSON escaping cannot hide a credential.
                if let Ok(mut value) = serde_json::from_str::<serde_json::Value>(data.get()) {
                    self.redact_json(&mut value);
                    error.data = serde_json::value::to_raw_value(&value).ok();
                } else {
                    error.data = None;
                }
            }
        }
    }

    fn redact_json(&self, value: &mut serde_json::Value) {
        match value {
            serde_json::Value::String(text) => *text = self.redact(text),
            serde_json::Value::Array(items) => {
                items.iter_mut().for_each(|item| self.redact_json(item))
            }
            serde_json::Value::Object(fields) => {
                // Keys may also contain echoed credentials.
                let old = std::mem::take(fields);
                for (key, mut value) in old {
                    self.redact_json(&mut value);
                    fields.insert(self.redact(&key), value);
                }
            }
            _ => {}
        }
    }

    async fn do_request(self, req: RequestPacket) -> TransportResult<ResponsePacket> {
        let mut resp = self
            .client
            .post(self.url.clone())
            .json(&req)
            .headers(self.custom_headers.clone())
            .send()
            .await
            .map_err(|error| TransportErrorKind::custom(error.without_url()))?;
        let status = resp.status();
        debug!(?status, "RPC HTTP response");
        // HTTP error pages frequently echo credential-bearing request URLs.
        if !status.is_success() {
            return Err(TransportErrorKind::http_error(
                status.as_u16(),
                "RPC response body withheld".into(),
            ));
        }
        let too_large = || {
            TransportErrorKind::custom(std::io::Error::other(
                "RPC response exceeded the 16 MiB limit; body withheld",
            ))
        };
        if resp
            .content_length()
            .is_some_and(|length| length > MAX_RPC_RESPONSE_BYTES as u64)
        {
            return Err(too_large());
        }
        let mut body = Vec::new();
        while let Some(chunk) = resp
            .chunk()
            .await
            .map_err(|error| TransportErrorKind::custom(error.without_url()))?
        {
            if chunk.len() > MAX_RPC_RESPONSE_BYTES - body.len() {
                return Err(too_large());
            }
            body.extend_from_slice(&chunk);
        }
        let mut response: ResponsePacket = serde_json::from_slice(&body).map_err(|_| {
            // serde's own diagnostic can echo a malformed string from the body.
            let error: serde_json::Error = serde::de::Error::custom("Invalid RPC response");
            TransportError::deser_err(error, "RPC response body withheld")
        })?;
        match &mut response {
            ResponsePacket::Single(item) => self.redact_response(item),
            ResponsePacket::Batch(items) => {
                items.iter_mut().for_each(|item| self.redact_response(item))
            }
        }
        Ok(response)
    }
}

impl Service<RequestPacket> for HeaderInjectingTransport {
    type Response = ResponsePacket;
    type Error = TransportError;
    type Future = TransportFut<'static>;

    fn poll_ready(&mut self, _cx: &mut task::Context<'_>) -> task::Poll<Result<(), Self::Error>> {
        task::Poll::Ready(Ok(()))
    }

    fn call(&mut self, req: RequestPacket) -> Self::Future {
        Box::pin(
            self.clone()
                .do_request(req)
                .instrument(debug_span!("RPC HTTP request")),
        )
    }
}

#[derive(Clone)]
pub struct SharedClientTransportBuilder {
    shared_client: reqwest::Client,
}

impl fmt::Debug for SharedClientTransportBuilder {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SharedClientTransportBuilder")
            .finish_non_exhaustive()
    }
}

impl SharedClientTransportBuilder {
    pub fn new(client: reqwest::Client) -> Self {
        Self {
            shared_client: client,
        }
    }

    pub fn with_headers(
        &self,
        url: reqwest::Url,
        headers: reqwest::header::HeaderMap,
    ) -> HeaderInjectingTransport {
        HeaderInjectingTransport::new(self.shared_client.clone(), url, headers)
    }

    pub fn default_transport(&self, url: reqwest::Url) -> HeaderInjectingTransport {
        self.with_headers(url, reqwest::header::HeaderMap::new())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn error_redaction_protects_bearer_tokens_and_credential_subdomains() {
        let mut headers = reqwest::header::HeaderMap::new();
        headers.insert("authorization", "bEaReR private-token".parse().unwrap());
        let transport = HeaderInjectingTransport::new(
            reqwest::Client::new(),
            "https://private-subdomain.rpc.example/keypath?token=query-secret"
                .parse()
                .unwrap(),
            headers,
        );
        let message =
            transport.redact("nonce too low: private-subdomain private-token keypath query-secret");
        assert!(message.starts_with("nonce too low:"));
        for secret in [
            "private-subdomain",
            "private-token",
            "keypath",
            "query-secret",
        ] {
            assert!(!message.contains(secret), "credential fragment leaked");
        }
        // An IP's individual octets are not secret labels. Replacing them would
        // corrupt ordinary nonce/fee diagnostics and revert data.
        let local = HeaderInjectingTransport::new(
            reqwest::Client::new(),
            "http://127.0.0.1:8788".parse().unwrap(),
            reqwest::header::HeaderMap::new(),
        );
        assert_eq!(
            local.redact("nonce too low: expected 10"),
            "nonce too low: expected 10"
        );
    }
}
