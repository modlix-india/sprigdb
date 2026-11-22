//! The fixed head of a stored record batch (magic 2), read and patched in
//! place.
//!
//! ```text
//!  0 base offset i64      8 batch length i32 (bytes after this field)
//! 12 leader epoch i32    16 magic i8          17 CRC-32C u32
//! 21 attributes i16      23 last offset delta i32
//! 27 base timestamp i64  35 max timestamp i64
//! 43 producer id i64     51 producer epoch i16  53 base sequence i32
//! 57 record count i32    61 records
//! ```
//!
//! The CRC covers the attributes to the end, so writing the base offset
//! and the leader epoch keeps it valid; only log-append time (attribute
//! bit 3 and the max timestamp) needs a new CRC.

/// Bytes before the records.
pub const HEAD: usize = 61;
/// Bytes before the length field's coverage starts.
pub const LOG_OVERHEAD: usize = 12;
const CRC_AT: usize = 17;
const CRC_FROM: usize = 21;
/// Attribute bit: timestamps are the broker's append time.
pub const ATTR_LOG_APPEND_TIME: i16 = 0x08;
/// Attribute bit: a transactional batch.
pub const ATTR_TRANSACTIONAL: i16 = 0x10;
/// Attribute bit: control records.
pub const ATTR_CONTROL: i16 = 0x20;

fn i64_at(b: &[u8], at: usize) -> i64 {
    let mut a = [0u8; 8];
    a.copy_from_slice(&b[at..at + 8]);
    i64::from_be_bytes(a)
}

fn i32_at(b: &[u8], at: usize) -> i32 {
    let mut a = [0u8; 4];
    a.copy_from_slice(&b[at..at + 4]);
    i32::from_be_bytes(a)
}

fn i16_at(b: &[u8], at: usize) -> i16 {
    i16::from_be_bytes([b[at], b[at + 1]])
}

/// The head of one batch.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Head {
    /// The first record's offset.
    pub base_offset: i64,
    /// Bytes of the whole batch, head included.
    pub total: usize,
    /// The leader epoch it was appended under.
    pub epoch: i32,
    /// Attributes.
    pub attributes: i16,
    /// The last record's offset delta.
    pub last_delta: i32,
    /// The first record's timestamp.
    pub base_ts: i64,
    /// The largest timestamp.
    pub max_ts: i64,
    /// The idempotent producer, or -1.
    pub producer_id: i64,
    /// Its epoch.
    pub producer_epoch: i16,
    /// The first record's sequence, or -1.
    pub base_sequence: i32,
    /// Records.
    pub count: i32,
}

impl Head {
    /// Reads the head at the start of `b`; `None` when `b` is shorter than
    /// a head, the magic isn't 2 or the length is impossible.
    #[must_use]
    pub fn parse(b: &[u8]) -> Option<Head> {
        if b.len() < HEAD || b[16] != 2 {
            return None;
        }
        let len = usize::try_from(i32_at(b, 8)).ok()?;
        if len < HEAD - LOG_OVERHEAD {
            return None;
        }
        Some(Head {
            base_offset: i64_at(b, 0),
            total: LOG_OVERHEAD + len,
            epoch: i32_at(b, 12),
            attributes: i16_at(b, 21),
            last_delta: i32_at(b, 23),
            base_ts: i64_at(b, 27),
            max_ts: i64_at(b, 35),
            producer_id: i64_at(b, 43),
            producer_epoch: i16_at(b, 51),
            base_sequence: i32_at(b, 53),
            count: i32_at(b, 57),
        })
    }

    /// The offset after the batch's last record.
    #[must_use]
    pub fn next_offset(&self) -> i64 {
        self.base_offset + i64::from(self.last_delta) + 1
    }

    /// The last record's offset.
    #[must_use]
    pub fn last_offset(&self) -> i64 {
        self.base_offset + i64::from(self.last_delta)
    }

    /// Whether it holds control records.
    #[must_use]
    pub fn is_control(&self) -> bool {
        self.attributes & ATTR_CONTROL != 0
    }

    /// The last record's sequence number (wrapping at 2^31 - 1).
    #[must_use]
    pub fn last_sequence(&self) -> i32 {
        wrap_seq(self.base_sequence, self.last_delta)
    }
}

/// `seq + n`, wrapping from `i32::MAX` to 0 as Kafka's sequences do.
#[must_use]
pub fn wrap_seq(seq: i32, n: i32) -> i32 {
    let s = i64::from(seq) + i64::from(n);
    let m = i64::from(i32::MAX) + 1;
    i32::try_from(s.rem_euclid(m)).unwrap_or(0)
}

/// Writes the base offset and leader epoch (the CRC stays valid).
pub fn set_offset_and_epoch(b: &mut [u8], base_offset: i64, epoch: i32) {
    b[0..8].copy_from_slice(&base_offset.to_be_bytes());
    b[12..16].copy_from_slice(&epoch.to_be_bytes());
}

/// Stamps log-append time: attribute bit 3 and the max timestamp, then a
/// new CRC.
pub fn set_log_append_time(b: &mut [u8], now_ms: i64) {
    let attrs = i16_at(b, 21) | ATTR_LOG_APPEND_TIME;
    b[21..23].copy_from_slice(&attrs.to_be_bytes());
    b[35..43].copy_from_slice(&now_ms.to_be_bytes());
    let crc = sprig_proto_kafka::crc32c::crc32c(&b[CRC_FROM..]);
    b[CRC_AT..CRC_AT + 4].copy_from_slice(&crc.to_be_bytes());
}

/// Whether the batch's CRC matches its bytes.
#[must_use]
pub fn crc_ok(b: &[u8]) -> bool {
    b.len() >= HEAD
        && sprig_proto_kafka::crc32c::crc32c(&b[CRC_FROM..])
            == u32::from_be_bytes([b[CRC_AT], b[CRC_AT + 1], b[CRC_AT + 2], b[CRC_AT + 3]])
}

#[cfg(test)]
mod tests {
    use super::*;
    use sprig_proto_kafka::records::{Record, RecordBatch};

    fn batch() -> Vec<u8> {
        RecordBatch {
            base_timestamp: 1_000,
            max_timestamp: 1_002,
            last_offset_delta: 2,
            producer_id: 7,
            producer_epoch: 1,
            base_sequence: i32::MAX - 1,
            records: (0..3)
                .map(|i| Record {
                    offset_delta: i,
                    timestamp_delta: i64::from(i),
                    key: Some(vec![b'k']),
                    value: Some(vec![1, 2, 3]),
                    ..Record::default()
                })
                .collect(),
            ..RecordBatch::default()
        }
        .to_bytes()
        .unwrap()
    }

    #[test]
    fn heads_read_and_patch_keeping_the_crc() {
        let mut b = batch();
        let h = Head::parse(&b).unwrap();
        assert_eq!(h.total, b.len());
        assert_eq!((h.count, h.last_delta, h.producer_id), (3, 2, 7));
        // i32::MAX - 1, i32::MAX, then 0.
        assert_eq!(h.last_sequence(), 0);
        set_offset_and_epoch(&mut b, 500, 4);
        assert!(crc_ok(&b));
        let h = Head::parse(&b).unwrap();
        assert_eq!((h.base_offset, h.epoch, h.next_offset()), (500, 4, 503));
        set_log_append_time(&mut b, 9_999);
        assert!(crc_ok(&b));
        let back =
            RecordBatch::decode(&mut sprig_proto_kafka::wire::Reader::new(&b), 1 << 20).unwrap();
        assert_eq!(back.max_timestamp, 9_999);
        assert_ne!(back.attributes & ATTR_LOG_APPEND_TIME, 0);
        b[70] ^= 1;
        assert!(!crc_ok(&b));
        assert!(Head::parse(&b[..20]).is_none());
    }
}
