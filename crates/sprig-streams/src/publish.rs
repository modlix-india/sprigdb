//! The record checks and header encoding every publish path shares (SQL's
//! `INSERT INTO <stream>`, Kafka's Produce, queue broadcasts):
//!
//! - [`check_size`]: keys up to 4096 bytes and records up to
//!   `stream.max_message_bytes` (39944);
//! - [`headers_json`]: record headers as a JSON object, and
//!   [`headers_of_json`] back.

use base64::Engine as _;
use sprig_common::{Result, SprigCode, SprigError};

/// The longest key.
pub const MAX_KEY_BYTES: usize = 4096;

/// One record to publish.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Publish {
    /// The key.
    pub key: Option<Vec<u8>>,
    /// The value (`None`: a tombstone).
    pub value: Option<Vec<u8>>,
    /// Headers as a JSON object.
    pub headers: Option<String>,
    /// A partition the producer chose.
    pub partition: Option<u32>,
    /// The producer's timestamp (ms since the Unix epoch); `None`: the
    /// server's time.
    pub ts_ms: Option<i64>,
}

/// The size limits every publish path applies, the same in every tier: a
/// key of at most [`MAX_KEY_BYTES`], and a record (key, value and headers
/// together) of at most `max_message_bytes`.
///
/// # Errors
/// 39944.
pub fn check_size(key_bytes: u64, record_bytes: u64, max_message_bytes: u64) -> Result<()> {
    if key_bytes > MAX_KEY_BYTES as u64 {
        return Err(SprigError::new(
            SprigCode::StreamRecordTooLarge,
            format!(
                "ER_SPRIG_STREAM_RECORD_TOO_LARGE: a key of {key_bytes} bytes is above {MAX_KEY_BYTES}"
            ),
        ));
    }
    if record_bytes > max_message_bytes {
        return Err(SprigError::new(
            SprigCode::StreamRecordTooLarge,
            format!(
                "ER_SPRIG_STREAM_RECORD_TOO_LARGE: a record of {record_bytes} bytes is above \
                 stream.max_message_bytes ({max_message_bytes})"
            ),
        ));
    }
    Ok(())
}

/// Record headers as the outbox's `headers` JSON object: a UTF-8 value is
/// a string, a null value `null`, other bytes `{"base64": "..."}`. A key
/// given twice keeps its last value (a JSON object has one value per
/// key). `None` for no headers.
#[must_use]
pub fn headers_json(headers: &[(String, Option<Vec<u8>>)]) -> Option<String> {
    if headers.is_empty() {
        return None;
    }
    let mut m = serde_json::Map::new();
    for (k, v) in headers {
        let j = match v {
            None => serde_json::Value::Null,
            Some(b) => match std::str::from_utf8(b) {
                Ok(s) => serde_json::Value::String(s.to_owned()),
                Err(_) => serde_json::json!({
                    "base64": base64::engine::general_purpose::STANDARD.encode(b)
                }),
            },
        };
        m.insert(k.clone(), j);
    }
    Some(serde_json::Value::Object(m).to_string())
}

/// Headers back from the `headers` JSON: strings as their bytes, `null`
/// as no value, `{"base64": ...}` decoded, anything else as its JSON text
/// (what SQL publishers may store).
#[must_use]
pub fn headers_of_json(json: &str) -> Vec<(String, Option<Vec<u8>>)> {
    let Ok(serde_json::Value::Object(m)) = serde_json::from_str::<serde_json::Value>(json) else {
        return Vec::new();
    };
    m.into_iter()
        .map(|(k, v)| {
            let b = match v {
                serde_json::Value::Null => None,
                serde_json::Value::String(s) => Some(s.into_bytes()),
                other => {
                    let decoded = match &other {
                        serde_json::Value::Object(o) if o.len() == 1 => o
                            .get("base64")
                            .and_then(serde_json::Value::as_str)
                            .and_then(|b| base64::engine::general_purpose::STANDARD.decode(b).ok()),
                        _ => None,
                    };
                    Some(decoded.unwrap_or_else(|| other.to_string().into_bytes()))
                }
            };
            (k, b)
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sizes() {
        assert!(check_size(1, 10, 100).is_ok());
        assert_eq!(
            check_size(1, 101, 100).unwrap_err().code(),
            Some(SprigCode::StreamRecordTooLarge)
        );
        assert_eq!(
            check_size(4097, 4097, 1 << 20).unwrap_err().code(),
            Some(SprigCode::StreamRecordTooLarge)
        );
    }

    #[test]
    fn headers_round_trip() {
        let h = vec![
            ("source".to_owned(), Some(b"checkout".to_vec())),
            ("bin".to_owned(), Some(vec![0xff, 0x00])),
            ("none".to_owned(), None),
        ];
        let j = headers_json(&h).unwrap();
        let mut back = headers_of_json(&j);
        back.sort();
        let mut want = h.clone();
        want.sort();
        assert_eq!(back, want);
        assert_eq!(headers_json(&[]), None);
        // What a SQL publisher stores.
        let mut sql = headers_of_json(r#"{"n": 7, "s": "x"}"#);
        sql.sort();
        assert_eq!(
            sql,
            vec![
                ("n".to_owned(), Some(b"7".to_vec())),
                ("s".to_owned(), Some(b"x".to_vec()))
            ]
        );
    }
}
