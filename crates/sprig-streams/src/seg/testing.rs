//! Helpers for tests of the segment log (here and in `tests/`).

use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

use sprig_proto_kafka::records::{Header, Record, RecordBatch};

/// A directory under the system temp dir, removed on drop.
#[derive(Debug)]
pub struct TempDir(PathBuf);

impl TempDir {
    /// A fresh directory.
    ///
    /// # Panics
    /// The directory can't be made.
    #[must_use]
    pub fn new(tag: &str) -> Self {
        static N: AtomicU64 = AtomicU64::new(0);
        let p = std::env::temp_dir().join(format!(
            "sprig-seg-{tag}-{}-{}",
            std::process::id(),
            N.fetch_add(1, Ordering::Relaxed)
        ));
        let _ = std::fs::remove_dir_all(&p);
        std::fs::create_dir_all(&p).unwrap_or_else(|e| panic!("temp dir: {e}"));
        Self(p)
    }

    /// Its path.
    #[must_use]
    pub fn path(&self) -> &Path {
        &self.0
    }
}

impl Drop for TempDir {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

/// A plain batch of `n` records, `value_len` bytes each, keys `k0..`.
///
/// # Panics
/// Never for sane sizes.
#[must_use]
pub fn batch_bytes(n: i32, ts: i64, value_len: usize) -> Vec<u8> {
    batch_with(
        &(0..n)
            .map(|i| {
                (
                    Some(format!("k{i}").into_bytes()),
                    Some(vec![b'v'; value_len]),
                )
            })
            .collect::<Vec<_>>(),
        ts,
        None,
    )
}

/// A batch of the given (key, value) records; `producer` = (id, epoch,
/// base sequence).
///
/// # Panics
/// Never for sane sizes.
#[must_use]
pub fn batch_with(
    records: &[(Option<Vec<u8>>, Option<Vec<u8>>)],
    ts: i64,
    producer: Option<(i64, i16, i32)>,
) -> Vec<u8> {
    let n = i32::try_from(records.len()).unwrap_or(i32::MAX);
    let (pid, pe, seq) = producer.unwrap_or((-1, -1, -1));
    RecordBatch {
        base_timestamp: ts,
        max_timestamp: ts + i64::from(n - 1),
        last_offset_delta: n - 1,
        producer_id: pid,
        producer_epoch: pe,
        base_sequence: seq,
        records: records
            .iter()
            .enumerate()
            .map(|(i, (k, v))| {
                let i = i32::try_from(i).unwrap_or(0);
                Record {
                    offset_delta: i,
                    timestamp_delta: i64::from(i),
                    key: k.clone(),
                    value: v.clone(),
                    headers: vec![Header {
                        key: "h".into(),
                        value: Some(vec![1]),
                    }],
                    ..Record::default()
                }
            })
            .collect(),
        ..RecordBatch::default()
    }
    .to_bytes()
    .unwrap_or_else(|e| panic!("batch: {e}"))
}

/// Every record of stored batches: (offset, key, value).
///
/// # Panics
/// The bytes aren't whole valid batches.
#[must_use]
pub fn records_of(bytes: &[u8]) -> Vec<(i64, Option<Vec<u8>>, Option<Vec<u8>>)> {
    RecordBatch::decode_all(bytes, 64 << 20, false)
        .unwrap_or_else(|e| panic!("decode: {e}"))
        .into_iter()
        .flat_map(|b| {
            let base = b.base_offset;
            b.records
                .into_iter()
                .map(move |r| (base + i64::from(r.offset_delta), r.key, r.value))
        })
        .collect()
}
