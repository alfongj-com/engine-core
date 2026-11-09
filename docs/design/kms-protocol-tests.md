# KMS protocol coverage

## Scope

`cargo test --locked -p engine-core --test aws_kms_protocol` exercises the real
Engine credential constructor, AWS SDK and Alloy AWS signer against a loopback
KMS protocol peer. Endpoint overrides live only in a subprocess. No AWS account,
metadata credentials or paid KMS service is used.

The peer checks AWS authorization headers, exact allowed key IDs, `MessageType=DIGEST`,
`ECDSA_SHA_256` and exactly 32 digest bytes. It returns secp256k1 SPKI public keys
and DER signatures as specified by the [KMS Sign API](https://docs.aws.amazon.com/kms/latest/APIReference/API_Sign.html).
The SDK uses its documented [service endpoint override](https://docs.aws.amazon.com/sdkref/latest/guide/feature-ss-endpoints.html).

## Checks

- Both low-S and high-S responses normalize to the independently generated native
  libsecp256k1 signature. Digest cases: zero, curve order minus one, order, order
  plus one and maximum 256-bit value.
- Personal-message signing and an EIP-155 transaction match the native backend.
- Wrong-key signatures, malformed DER and invalid SPKI fail.
- The actual Engine transaction-signing path preserves typed SDK and signature
  errors through Alloy's boxed signer interface. A first-call 503 for Sign or
  GetPublicKey fails as a typed service error, then the identical transaction
  succeeds when the peer recovers. Invalid DER, wrong-key signatures, signer
  address mismatch and chain-ID mismatch remain permanent validation failures.
- All 21 Sign and 13 GetPublicKey calls must reach the peer, with exact counts
  per alias. The two positive aliases use different keys, so substituting another
  valid alias fails address and signature comparisons. Unexpected request shapes
  fail. The child has a 30-second deadline.

Unsigned preparation retries SDK timeout, dispatch, response, service and unknown
signing failures within the existing retry budget. Construction failures and
cryptographic/key validation failures remain permanent. Retrying a service error
does not grant broadcast authority or create another replay identity.

These checks cover SDK/protocol and dependency compatibility. They do not certify
SigV4 verification, AWS IAM/key policies, endpoint TLS, production latency, quotas, key rotation or
an HSM's cryptographic implementation. Those need the selected AWS deployment.
