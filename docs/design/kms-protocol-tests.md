# KMS protocol coverage

## Scope

`cargo test --locked -p engine-core --test aws_kms_protocol` exercises the real
Engine credential constructor, AWS SDK and Alloy AWS signer against a loopback
KMS protocol peer. Endpoint overrides live only in a subprocess. No AWS account,
metadata credentials or paid KMS service is used.

The peer requires signed AWS requests, the selected key ID, `MessageType=DIGEST`,
`ECDSA_SHA_256` and exactly 32 digest bytes. It returns secp256k1 SPKI public keys
and DER signatures as specified by the [KMS Sign API](https://docs.aws.amazon.com/kms/latest/APIReference/API_Sign.html).
The SDK uses its documented [service endpoint override](https://docs.aws.amazon.com/sdkref/latest/guide/feature-ss-endpoints.html).

## Checks

- Both low-S and high-S responses normalize to the independently generated native
  libsecp256k1 signature. Digest cases: zero, curve order minus one, order, order
  plus one and maximum 256-bit value.
- Personal-message signing and an EIP-155 transaction match the native backend.
- Wrong-key signatures, malformed DER and invalid SPKI fail.
- All 16 Sign and five GetPublicKey calls must reach the peer; unexpected request
  shapes fail. The child has a 30-second deadline.

These checks cover SDK/protocol and dependency compatibility. They do not certify
AWS IAM/key policies, endpoint TLS, production latency, quotas, key rotation or
an HSM's cryptographic implementation. Those need the selected AWS deployment.
