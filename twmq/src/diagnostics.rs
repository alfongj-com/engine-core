//! Bounded inspection history; retry decisions use job metadata, never this list.
use std::io::{self, Write};

use redis::Pipeline;
use serde::Serialize;

use crate::{error::TwmqError, job::JobErrorRecord};

pub(crate) const MAX_JOB_ERROR_RECORDS: usize = 100;
pub(crate) const MAX_JOB_ERROR_RECORD_BYTES: usize = 16 * 1024;

/// Serialize without allocating a second unbounded copy of a handler error.
/// Small records keep their existing format. Oversized records get a distinct
/// valid-JSON omission marker; no arbitrary prefix of the original is retained.
pub(crate) fn encode_record<E: Serialize>(record: &JobErrorRecord<E>) -> Result<String, TwmqError> {
    let mut writer = LimitedWriter {
        bytes: Vec::with_capacity(1024),
        exceeded: false,
    };
    match serde_json::to_writer(&mut writer, record) {
        Ok(()) => Ok(String::from_utf8(writer.bytes).expect("serde_json emits UTF-8")),
        Err(_) if writer.exceeded => Ok(serde_json::to_string(&serde_json::json!({
            "attempt": record.attempt,
            "created_at": record.created_at,
            "details": record.details,
            "diagnosticOmitted": {
                "reason": "serialized_record_exceeds_limit",
                "maxSerializedBytes": MAX_JOB_ERROR_RECORD_BYTES,
            },
        }))?),
        Err(error) => Err(error.into()),
    }
}

/// Both commands join the same existing owner-fenced completion transaction.
/// LPUSH preserves newest-first order; LTRIM never changes key expiry.
pub(crate) fn append_record(pipeline: &mut Pipeline, key: &str, record: String) {
    pipeline
        .lpush(key, record)
        .ltrim(key, 0, (MAX_JOB_ERROR_RECORDS - 1) as isize);
}

struct LimitedWriter {
    bytes: Vec<u8>,
    exceeded: bool,
}

impl Write for LimitedWriter {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        if bytes.len() > MAX_JOB_ERROR_RECORD_BYTES.saturating_sub(self.bytes.len()) {
            self.exceeded = true;
            return Err(io::Error::other("job diagnostic size limit exceeded"));
        }
        self.bytes.extend_from_slice(bytes);
        Ok(bytes.len())
    }
    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::job::{JobErrorType, RequeuePosition};
    use std::time::Duration;

    #[test]
    fn diagnostic_bytes_include_json_escaping_and_exact_boundary() {
        let record = |error| JobErrorRecord {
            attempt: 17,
            error,
            created_at: 42,
            details: JobErrorType::nack(Some(Duration::from_millis(200)), RequeuePosition::Last),
        };
        let overhead = serde_json::to_string(&record(String::new())).unwrap().len();
        let exact = record("x".repeat(MAX_JOB_ERROR_RECORD_BYTES - overhead));
        let encoded = encode_record(&exact).unwrap();
        assert_eq!(encoded.len(), MAX_JOB_ERROR_RECORD_BYTES);
        assert_eq!(encoded, serde_json::to_string(&exact).unwrap());
        let oversized = record("\0".repeat(MAX_JOB_ERROR_RECORD_BYTES / 2));
        let marker = encode_record(&oversized).unwrap();
        assert!(marker.len() < MAX_JOB_ERROR_RECORD_BYTES);
        let marker: serde_json::Value = serde_json::from_str(&marker).unwrap();
        assert_eq!(marker["attempt"], 17);
        assert_eq!(marker["created_at"], 42);
        assert!(marker.get("error").is_none());
        assert_eq!(
            marker["details"],
            serde_json::to_value(&oversized.details).unwrap()
        );
        assert_eq!(
            marker["diagnosticOmitted"]["reason"],
            "serialized_record_exceeds_limit"
        );
    }

    #[test]
    fn diagnostic_serializer_failures_keep_existing_error_semantics() {
        struct Broken;
        impl Serialize for Broken {
            fn serialize<S: serde::Serializer>(&self, _: S) -> Result<S::Ok, S::Error> {
                Err(serde::ser::Error::custom("handler serializer failed"))
            }
        }
        let result = encode_record(&JobErrorRecord {
            attempt: 1,
            error: Broken,
            created_at: 42,
            details: JobErrorType::fail(),
        });
        assert!(matches!(result, Err(TwmqError::JsonError { .. })));
    }
}
