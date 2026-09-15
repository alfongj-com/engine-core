//! Webhook destinations are operator-approved HTTPS origins, not arbitrary URLs.
//! The connector resolves once and consumes only the addresses checked here.
//! See docs/design/webhook-egress.md for the trust and network-routing boundary.

use reqwest::{
    Url,
    dns::{Addrs, Name, Resolve, Resolving},
};
use std::{
    collections::HashSet,
    io,
    net::{IpAddr, SocketAddr},
    sync::Arc,
    time::Duration,
};

use super::WebhookError;

#[derive(Clone, Debug, Default)]
pub struct WebhookDestinationPolicy {
    allowed_origins: HashSet<String>,
}

impl WebhookDestinationPolicy {
    /// Missing or empty configuration denies every destination.
    pub fn from_env() -> Result<Self, WebhookError> {
        match std::env::var("ENGINE_WEBHOOK_ALLOWED_ORIGINS") {
            Ok(value) => Self::from_csv(&value),
            Err(std::env::VarError::NotPresent) => Ok(Self::default()),
            Err(_) => Err(rejected("ENGINE_WEBHOOK_ALLOWED_ORIGINS must be UTF-8")),
        }
    }

    pub fn from_csv(value: &str) -> Result<Self, WebhookError> {
        let mut allowed_origins = HashSet::new();
        if value.trim().is_empty() {
            return Ok(Self::default());
        }
        for item in value.split(',') {
            let url = parse_destination(item.trim())?;
            if url.path() != "/" || url.query().is_some() {
                return Err(rejected(
                    "allowed entries must be HTTPS origins without a path or query",
                ));
            }
            allowed_origins.insert(url.origin().ascii_serialization());
        }
        Ok(Self { allowed_origins })
    }

    pub fn validate(&self, value: &str) -> Result<Url, WebhookError> {
        let url = parse_destination(value)?;
        if !self
            .allowed_origins
            .contains(&url.origin().ascii_serialization())
        {
            return Err(rejected(
                "webhook origin is not configured in ENGINE_WEBHOOK_ALLOWED_ORIGINS",
            ));
        }
        Ok(url)
    }
}

fn rejected(reason: &str) -> WebhookError {
    WebhookError::DestinationRejected(reason.into())
}

fn parse_destination(value: &str) -> Result<Url, WebhookError> {
    if value.trim() != value || value.chars().any(char::is_control) {
        return Err(rejected(
            "webhook URL cannot contain surrounding whitespace or control characters",
        ));
    }
    let url = Url::parse(value).map_err(|_| rejected("webhook URL is invalid"))?;
    if url.scheme() != "https" {
        return Err(rejected("webhook destinations require HTTPS"));
    }
    // Check raw authority too: the URL parser may discard empty userinfo.
    let authority = value
        .split_once("://")
        .map(|(_, rest)| rest.split(['/', '?', '#']).next().unwrap_or(""))
        .unwrap_or("");
    if authority.contains('@')
        || !url.username().is_empty()
        || url.password().is_some()
        || url.fragment().is_some()
    {
        return Err(rejected(
            "webhook URLs cannot contain userinfo or fragments",
        ));
    }
    let host = url
        .host_str()
        .ok_or_else(|| rejected("webhook URL requires a host"))?;
    if host.trim_matches(['[', ']']).parse::<IpAddr>().is_ok() || host.ends_with('.') {
        return Err(rejected(
            "webhook destinations require a DNS hostname without a trailing dot",
        ));
    }
    Ok(url)
}

/// Deliberately conservative public-address policy; special-purpose IPv6 ranges
/// inside 2000::/3 are excluded, including transition/tunnel and documentation
/// prefixes. Update together with the IANA-registry regression cases.
fn public_address(ip: IpAddr) -> bool {
    match ip {
        IpAddr::V4(ip) => {
            let [a, b, c, _] = ip.octets();
            !ip.is_private()
                && !ip.is_loopback()
                && !ip.is_link_local()
                && !ip.is_documentation()
                && a != 0
                && a < 224
                && !(a == 100 && (64..=127).contains(&b))
                && !(a == 192 && b == 0 && c == 0)
                && !(a == 192 && b == 88 && c == 99)
                && !(a == 198 && (18..=19).contains(&b))
        }
        IpAddr::V6(ip) => {
            let s = ip.segments();
            (s[0] & 0xe000) == 0x2000
                && !(s[0] == 0x2001 && s[1] < 0x200)
                && !(s[0] == 0x2001 && s[1] == 0xdb8)
                && s[0] != 0x2002
                && !(s[0] == 0x3fff && s[1] < 0x1000)
        }
    }
}

fn checked_addresses(addresses: Vec<SocketAddr>) -> Result<Addrs, io::Error> {
    if addresses.is_empty()
        || addresses
            .iter()
            .any(|address| !public_address(address.ip()))
    {
        return Err(io::Error::new(
            io::ErrorKind::PermissionDenied,
            "webhook DNS answer contains no addresses or a prohibited address",
        ));
    }
    Ok(Box::new(addresses.into_iter()))
}

struct PublicWebhookResolver;

impl Resolve for PublicWebhookResolver {
    fn resolve(&self, name: Name) -> Resolving {
        Box::pin(async move {
            let addresses = tokio::net::lookup_host((name.as_str(), 0)).await?.collect();
            // The returned SocketAddrs are the connector's inputs. There is no
            // validate-then-resolve-again window for DNS rebinding.
            Ok(checked_addresses(addresses)?)
        })
    }
}

pub(super) fn client_builder() -> reqwest::ClientBuilder {
    reqwest::Client::builder()
        .https_only(true)
        .redirect(reqwest::redirect::Policy::none())
        .no_proxy()
        .dns_resolver(Arc::new(PublicWebhookResolver))
        .pool_max_idle_per_host(50)
        .pool_idle_timeout(Duration::from_secs(30))
        .timeout(Duration::from_secs(10))
        .connect_timeout(Duration::from_secs(5))
        .tcp_keepalive(Duration::from_secs(60))
        .tcp_nodelay(true)
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    #[test]
    fn default_denies_and_allowlist_compares_exact_normalized_origins() {
        assert!(
            WebhookDestinationPolicy::default()
                .validate("https://hooks.example.com/event")
                .is_err()
        );
        let policy = WebhookDestinationPolicy::from_csv(
            "https://HOOKS.example.com:443, https://other.example.com:8443",
        )
        .unwrap();
        assert!(
            policy
                .validate("https://hooks.example.com/event?key=secret")
                .is_ok()
        );
        assert!(
            policy
                .validate("https://other.example.com:8443/event")
                .is_ok()
        );
        for url in [
            "https://hooks.example.com.evil.test/event",
            "https://sub.hooks.example.com/event",
            "https://hooks.example.com:8443/event",
            "https://other.example.com/event",
        ] {
            assert!(policy.validate(url).is_err(), "accepted {url}");
        }
    }

    #[test]
    fn rejects_ambiguous_urls_and_invalid_operator_configuration() {
        let policy = WebhookDestinationPolicy::from_csv("https://hooks.example.com").unwrap();
        for url in [
            "http://hooks.example.com",
            "file:///etc/passwd",
            "https://user:password@hooks.example.com",
            "https://@hooks.example.com",
            "https://hooks.example.com/#fragment",
            "https://hooks.example.com/#",
            "https://hooks.example.com.",
            " https://hooks.example.com",
            "https://hooks.ex\nample.com",
            "https://127.0.0.1",
            "https://2130706433",
            "https://0x7f000001",
            "https://[::1]",
            "https://[::ffff:127.0.0.1]",
        ] {
            assert!(policy.validate(url).is_err(), "accepted {url}");
        }
        for entry in [
            "https://hooks.example.com/path",
            "https://hooks.example.com?key=x",
            "http://hooks.example.com",
            "https://127.0.0.1",
            "https://hooks.example.com,",
        ] {
            assert!(
                WebhookDestinationPolicy::from_csv(entry).is_err(),
                "accepted config {entry}"
            );
        }
    }

    #[test]
    fn rejects_nonpublic_and_transition_addresses_including_mixed_dns_answers() {
        for address in [
            "0.1.2.3",
            "10.0.0.1",
            "100.64.0.1",
            "100.127.255.255",
            "127.0.0.1",
            "169.254.169.254",
            "172.16.0.1",
            "172.31.255.255",
            "192.168.1.1",
            "192.0.0.9",
            "192.0.2.1",
            "192.88.99.1",
            "198.18.0.1",
            "198.19.255.255",
            "198.51.100.1",
            "203.0.113.1",
            "224.0.0.1",
            "240.0.0.1",
            "255.255.255.255",
            "::",
            "::1",
            "::ffff:127.0.0.1",
            "64:ff9b::a9fe:a9fe",
            "100::1",
            "2001::1",
            "2001:2::1",
            "2001:db8::1",
            "2002:7f00:1::",
            "3fff::1",
            "fc00::1",
            "fe80::1",
            "ff02::1",
        ] {
            assert!(
                !public_address(address.parse().unwrap()),
                "allowed {address}"
            );
        }
        for address in [
            "1.1.1.1",
            "8.8.8.8",
            "100.63.255.255",
            "100.128.0.0",
            "172.15.255.255",
            "172.32.0.1",
            "2606:4700:4700::1111",
            "2001:4860:4860::8888",
        ] {
            assert!(
                public_address(address.parse().unwrap()),
                "blocked {address}"
            );
        }
        assert!(checked_addresses(vec![]).is_err());
        assert!(
            checked_addresses(vec![
                "1.1.1.1:0".parse().unwrap(),
                "127.0.0.1:0".parse().unwrap()
            ])
            .is_err()
        );
        let expected = vec![
            "1.1.1.1:0".parse().unwrap(),
            "[2606:4700:4700::1111]:0".parse().unwrap(),
        ];
        assert_eq!(
            checked_addresses(expected.clone())
                .unwrap()
                .collect::<Vec<_>>(),
            expected
        );
    }

    #[tokio::test]
    async fn production_client_rejects_http_before_connecting() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let result = client_builder()
            .build()
            .unwrap()
            .get(format!("http://{}/", listener.local_addr().unwrap()))
            .send()
            .await;
        assert!(result.unwrap_err().is_builder());
        assert!(
            tokio::time::timeout(Duration::from_millis(30), listener.accept())
                .await
                .is_err()
        );
    }

    #[tokio::test]
    async fn redirect_response_is_returned_without_contacting_second_destination() {
        let first = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let second = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let first_address = first.local_addr().unwrap();
        let second_address = second.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (mut socket, _) = first.accept().await.unwrap();
            let mut buffer = [0u8; 1024];
            socket.read(&mut buffer).await.unwrap();
            socket.write_all(format!("HTTP/1.1 302 Found\r\nLocation: http://{second_address}/private\r\nContent-Length: 0\r\nConnection: close\r\n\r\n").as_bytes()).await.unwrap();
        });
        // Only this test relaxes HTTPS to exercise real redirect behavior with
        // local sockets. The production redirect and proxy policy are unchanged.
        let response = client_builder()
            .https_only(false)
            .build()
            .unwrap()
            .get(format!("http://{first_address}/"))
            .send()
            .await
            .unwrap();
        assert_eq!(response.status(), reqwest::StatusCode::FOUND);
        server.await.unwrap();
        assert!(
            tokio::time::timeout(Duration::from_millis(30), second.accept())
                .await
                .is_err()
        );
    }
}
