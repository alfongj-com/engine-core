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
  succeeds when the peer recovers. SDK-parsed service code and HTTP status survive
  both paths. Actual 403 AccessDenied and 400 disabled-key, invalid-key-state,
  invalid-key-usage and missing-key responses retain those fields; later valid
  credentials still sign successfully. All error responses use the same message.
  Invalid DER, wrong-key signatures, signer address mismatch and chain-ID mismatch
  remain permanent validation failures.
- All 26 Sign and 19 GetPublicKey calls must reach the peer, with exact counts
  per alias. The two positive aliases use different keys, so substituting another
  valid alias fails address and signature comparisons. Unexpected request shapes
  fail. The child has a 30-second deadline.

## Preparation policy

Known authorization, key and request rejections are permanent only when the SDK
reports both their service code and a 4xx status other than 408/429. Examples:
AccessDeniedException, NotFoundException, DisabledException, KMSInvalidStateException
and InvalidKeyUsageException. The policy follows the documented
[Sign errors](https://docs.aws.amazon.com/kms/latest/APIReference/API_Sign.html#API_Sign_Errors),
[GetPublicKey errors](https://docs.aws.amazon.com/kms/latest/APIReference/API_GetPublicKey.html#API_GetPublicKey_Errors)
and [common errors](https://docs.aws.amazon.com/kms/latest/APIReference/CommonErrors.html).

SDK timeout, dispatch, response, throttling, DependencyTimeout, KeyUnavailable,
5xx, unknown codes and legacy errors without complete metadata stay retryable.
Construction and cryptographic/key validation failures remain permanent. Message
text does not decide policy. The existing preparation budget bounds retries;
retrying does not grant broadcast authority or create another replay identity.

The Redis worker regression covers a live denied request, a missing-key failure
retained before Redis publication, and healthy work queued behind both. It checks
original failure reasons after recovery, no nonce consumed by rejected work, one
successful wire at nonce zero and no duplicate failure notifications.

## Retained error format

`EngineError::AwsKmsSignerError` nests its signer error under `error`. This preserves
both discriminators: outer `type=AWS_KMS_SIGNER_ERROR`, inner `type=SIGN` or
`GET_PUBLIC_KEY`. Service errors add optional `code` and `http_status` fields;
older service error objects without them remain readable and conservative.

The former flattened shape could overwrite the outer `type` and make a retained
KMS error unreadable. Such malformed evidence is not guessed or repaired by this
change; recovery continues to refuse it. Round-trip tests cover the corrected
Engine and worker errors and the journal-to-Redis publication path.

These checks cover SDK/protocol and dependency compatibility. They do not certify
SigV4 verification, AWS IAM/key policies, endpoint TLS, production latency, quotas, key rotation or
an HSM's cryptographic implementation. Those need the selected AWS deployment.
