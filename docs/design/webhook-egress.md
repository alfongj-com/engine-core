# Webhook destination policy

Status: implemented in this fork; 2026-09-15. This is a deliberate compatibility change from upstream arbitrary-URL delivery.

## Contract

Webhooks are disabled unless the operator configures exact HTTPS origins:

```sh
ENGINE_WEBHOOK_ALLOWED_ORIGINS=https://hooks.example.com,https://events.example.com:8443
```

Configuration accepts scheme/host/port only; no path, query, credentials, wildcard, or fragment. Requests may use paths and queries on those origins. Default HTTPS port and hostname casing normalize before comparison. Other ports and subdomains need separate entries. Invalid configuration fails server startup. Missing/empty configuration permits no destination. This variable is read when the webhook handler is constructed; restart after changes.

The worker validates every delivery, including previously queued jobs. Disallowed jobs fail with `DESTINATION_REJECTED`; their associated blockchain transaction may already have completed. Configure destinations before enabling workloads that require notification delivery. The local HTTP webhook listener in the upstream benchmark script no longer qualifies.

## Enforcement

- Require HTTPS, reject URL userinfo/fragments/control characters, IP literals and trailing-dot hostnames. Match the parsed origin exactly.
- Disable redirects and environment proxies. A 3xx response is a terminal delivery failure. TLS certificate/hostname verification remains enabled.
- Resolve through a custom reqwest resolver. Reject empty results or any answer containing a nonpublic address; return the validated `SocketAddr` values directly to the connector. There is no separate unchecked DNS lookup after validation. New connections repeat the check; existing pooled connections retain their already selected peer.
- Deny IPv4 private, loopback, link-local, shared, documentation, benchmarking, multicast and reserved ranges. IPv6 permits ordinary global unicast within `2000::/3`, excluding conservative blocks for special-purpose, transition and documentation ranges. IPv4-mapped IPv6 and NAT64 well-known prefixes are denied.
- Restrict methods to POST/PUT/PATCH. Reject routing/framing/proxy headers such as `Host`, `Content-Length` and `Transfer-Encoding`.
- Retain at most 64 KiB of response bytes and stop consuming the body once that limit is reached; a transport chunk can exceed the retained limit. UTF-8 decoding is lossy and may expand invalid bytes. Error previews stop at a character boundary. Logs omit destination URLs, and transport errors strip URLs to avoid exposing query credentials.

These choices follow the allowlist, redirect and DNS-rebinding defenses described by [OWASP SSRF guidance](https://cheatsheetseries.owasp.org/cheatsheets/Server_Side_Request_Forgery_Prevention_Cheat_Sheet.html). Address policy uses the [IANA IPv4](https://www.iana.org/assignments/iana-ipv4-special-registry) and [IPv6](https://www.iana.org/assignments/iana-ipv6-special-registry) registries. Sources accessed 2026-09-15; review registry changes when updating the policy.

## Trust boundary and deployment requirement

Only allow destinations controlled or explicitly trusted by the operator. An allowed service that forwards requests elsewhere remains part of that trust boundary; origin approval permits all its paths. Do not approve a customer-controlled wildcard domain or generic URL-fetch service.

The resolver closes the public-DNS-to-private-address rebinding path. Application address classification cannot identify custom routing, network-specific address translation, or a public address routed internally by the deployment. Use outbound network policy to deny metadata, private and control-plane destinations as a second boundary. Private webhook destinations and authenticated outbound proxies are intentionally unsupported by this initial policy.

## Validation

`cargo test -p engine-executors --lib webhook::` covers exact-origin/default-deny behavior; URL confusion and userinfo; encoded IP literals; IPv4/IPv6 special ranges and mixed DNS answers; rejection before dispatch; unsafe methods/headers; real HTTP redirect non-following; HTTPS rejection before a TCP connection; bounded streaming response reads; and multibyte error previews. Socket tests relax HTTPS only inside the test to use local listeners. These tests do not exercise a real TLS server or production egress firewall.
