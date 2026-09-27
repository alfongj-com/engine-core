use reqwest::header::{HeaderMap, HeaderValue};
use serde::{Deserialize, Serialize};

use crate::error::ThirdwebError;

#[derive(Clone, Serialize, Deserialize)]
pub struct ThirdwebClientIdAndServiceKey {
    pub client_id: String,
    pub service_key: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ecosystem_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ecosystem_partner_id: Option<String>,
}

#[derive(Clone, Serialize, Deserialize)]
pub enum ThirdwebAuth {
    ClientIdServiceKey(ThirdwebClientIdAndServiceKey),
    SecretKey(String),
}

impl ThirdwebAuth {
    pub fn to_header_map(&self) -> Result<HeaderMap, ThirdwebError> {
        match self {
            ThirdwebAuth::ClientIdServiceKey(creds) => {
                let mut headers = HeaderMap::new();
                headers.insert(
                    "x-client-id",
                    HeaderValue::from_str(&creds.client_id)
                        .map_err(|_| ThirdwebError::header_value(creds.client_id.clone()))?,
                );
                headers.insert("x-service-api-key", secret_header(&creds.service_key)?);
                if let Some(ecosystem_id) = &creds.ecosystem_id {
                    headers.insert(
                        "x-ecosystem-id",
                        HeaderValue::from_str(ecosystem_id)
                            .map_err(|_| ThirdwebError::header_value(ecosystem_id.clone()))?,
                    );
                }
                if let Some(ecosystem_partner_id) = &creds.ecosystem_partner_id {
                    headers.insert(
                        "x-ecosystem-partner-id",
                        HeaderValue::from_str(ecosystem_partner_id).map_err(|_| {
                            ThirdwebError::header_value(ecosystem_partner_id.clone())
                        })?,
                    );
                }
                Ok(headers)
            }
            ThirdwebAuth::SecretKey(secret_key) => {
                let mut headers = HeaderMap::new();
                headers.insert("x-secret-key", secret_header(secret_key)?);
                Ok(headers)
            }
        }
    }
}

fn secret_header(value: &str) -> Result<HeaderValue, ThirdwebError> {
    let mut header = HeaderValue::from_str(value)
        .map_err(|_| ThirdwebError::header_value("invalid authentication header".into()))?;
    header.set_sensitive(true);
    Ok(header)
}

impl std::fmt::Debug for ThirdwebAuth {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::ClientIdServiceKey(creds) => {
                f.debug_tuple("ClientIdServiceKey").field(creds).finish()
            }
            Self::SecretKey(_) => f.write_str("SecretKey([REDACTED])"),
        }
    }
}

impl std::fmt::Debug for ThirdwebClientIdAndServiceKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ThirdwebClientIdAndServiceKey")
            .field("client_id", &self.client_id)
            .field("service_key", &"[REDACTED]")
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn authentication_secrets_are_not_exposed_by_debug_or_header_errors() {
        let secret = "test-secret-sentinel";
        let auth = ThirdwebAuth::SecretKey(secret.into());
        assert!(!format!("{auth:?}").contains(secret));
        let headers = auth.to_header_map().unwrap();
        assert!(headers["x-secret-key"].is_sensitive());
        assert!(!format!("{headers:?}").contains(secret));
        let bad_secret = "invalid-secret\nvalue";
        let error = ThirdwebAuth::SecretKey(bad_secret.into())
            .to_header_map()
            .unwrap_err();
        assert!(!format!("{error:?}").contains("invalid-secret"));
        let auth = ThirdwebAuth::ClientIdServiceKey(ThirdwebClientIdAndServiceKey {
            client_id: "public-client".into(),
            service_key: secret.into(),
            ecosystem_id: None,
            ecosystem_partner_id: None,
        });
        assert!(!format!("{auth:?}").contains(secret));
        assert!(auth.to_header_map().unwrap()["x-service-api-key"].is_sensitive());
    }
}
