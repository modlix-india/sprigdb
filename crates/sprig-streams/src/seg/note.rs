//! What the stream log puts in the engine's log: payload notes.
//!
//! ```text
//! note   := "KS" | version 1 | op
//! op     := 0x01 append | tenant u64 | stream u64 | partition u32
//!                       | flags u8 (bit 0: log-append time) | now_ms i64 | batch
//! ```
//!
//! Notes of other owners (anything not starting with the tag) are
//! skipped.

use super::part::PartKey;

const TAG: &[u8; 3] = b"KS\x01";
const APPEND: u8 = 1;
const FIXED: usize = 3 + 1 + 8 + 8 + 4 + 1 + 8;

/// A decoded note.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Op<'a> {
    /// Append one batch.
    Append {
        /// The partition.
        key: PartKey,
        /// The proposer's clock (log-append time, producer expiry).
        now_ms: i64,
        /// Stamp log-append time.
        log_append_time: bool,
        /// The batch as checked by the front end.
        batch: &'a [u8],
    },
}

/// Encodes an append.
#[must_use]
pub fn encode_append(key: PartKey, now_ms: i64, log_append_time: bool, batch: &[u8]) -> Vec<u8> {
    let mut v = Vec::with_capacity(FIXED + batch.len());
    v.extend_from_slice(TAG);
    v.push(APPEND);
    v.extend_from_slice(&key.tenant.to_be_bytes());
    v.extend_from_slice(&key.stream.to_be_bytes());
    v.extend_from_slice(&key.partition.to_be_bytes());
    v.push(u8::from(log_append_time));
    v.extend_from_slice(&now_ms.to_be_bytes());
    v.extend_from_slice(batch);
    v
}

/// Whether a payload note is a stream append (the notes a replicated
/// node's log store keeps in note blobs).
#[must_use]
pub fn is_append(n: &[u8]) -> bool {
    n.len() >= FIXED && &n[..3] == TAG && n[3] == APPEND
}

fn u64_at(b: &[u8], at: usize) -> u64 {
    let mut a = [0u8; 8];
    a.copy_from_slice(&b[at..at + 8]);
    u64::from_be_bytes(a)
}

/// Decodes a note; `None` for another owner's note or a malformed one.
#[must_use]
pub fn decode(n: &[u8]) -> Option<Op<'_>> {
    if n.len() < FIXED || &n[..3] != TAG || n[3] != APPEND {
        return None;
    }
    let mut p = [0u8; 4];
    p.copy_from_slice(&n[20..24]);
    Some(Op::Append {
        key: PartKey {
            tenant: u64_at(n, 4),
            stream: u64_at(n, 12),
            partition: u32::from_be_bytes(p),
        },
        log_append_time: n[24] & 1 != 0,
        now_ms: i64::from_be_bytes(n[25..33].try_into().ok()?),
        batch: &n[33..],
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn round_trip() {
        let k = PartKey {
            tenant: 9,
            stream: 77,
            partition: 3,
        };
        let n = encode_append(k, 1234, true, b"bytes");
        assert_eq!(
            decode(&n),
            Some(Op::Append {
                key: k,
                now_ms: 1234,
                log_append_time: true,
                batch: b"bytes"
            })
        );
        assert_eq!(decode(b"txn 9"), None);
        assert_eq!(decode(&n[..10]), None);
    }
}
