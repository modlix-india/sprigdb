//! One partition's log: its segments, offsets, leader epochs and the
//! idempotent producers' state, changed only by the apply step (and the
//! maintenance pass, under the same lock).

use std::collections::{HashMap, VecDeque};
use std::io;
use std::path::PathBuf;

use serde::{Deserialize, Serialize};

use super::batch::{self, Head};
use super::segment::{Segment, View};

/// Kafka's `OUT_OF_ORDER_SEQUENCE_NUMBER`.
pub const OUT_OF_ORDER_SEQUENCE: i16 = 45;
/// Kafka's `INVALID_PRODUCER_EPOCH`.
pub const INVALID_PRODUCER_EPOCH: i16 = 47;
/// Kafka's `UNKNOWN_TOPIC_OR_PARTITION`.
pub const UNKNOWN_TOPIC_OR_PARTITION: i16 = 3;
/// Kafka's `CORRUPT_MESSAGE`.
pub const CORRUPT_MESSAGE: i16 = 2;
/// Kafka's `KAFKA_STORAGE_ERROR`.
pub const STORAGE_ERROR: i16 = 56;
/// Batches remembered per producer.
pub const PRODUCER_BATCHES: usize = 5;

/// A partition: tenant, stream, partition number.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
pub struct PartKey {
    /// The tenant.
    pub tenant: u64,
    /// The stream.
    pub stream: u64,
    /// The partition.
    pub partition: u32,
}

/// One batch an idempotent producer appended.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProducedBatch {
    /// The first record's sequence.
    pub first_seq: i32,
    /// The last record's sequence.
    pub last_seq: i32,
    /// Where it was appended.
    pub base_offset: i64,
}

/// An idempotent producer's state in one partition.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Producer {
    /// Its epoch.
    pub epoch: i16,
    /// The last batches, oldest first.
    pub batches: VecDeque<ProducedBatch>,
    /// When it last appended (ms; expiry).
    pub last_ms: i64,
}

/// What applying one append did.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Outcome {
    /// Appended at `base`.
    Appended {
        /// The base offset.
        base: i64,
        /// The log start then.
        log_start: i64,
        /// The append time, when the topic stamps log-append time.
        append_ms: Option<i64>,
    },
    /// A retry of a batch already appended: its base offset, or -1 when
    /// retention has removed it.
    Duplicate {
        /// The base offset.
        base: i64,
        /// The log start.
        log_start: i64,
    },
    /// Refused with a Kafka error code.
    Refused {
        /// The code.
        code: i16,
        /// Why.
        message: String,
    },
}

/// See the module docs.
#[derive(Debug)]
pub struct PartState {
    /// Its directory.
    pub dir: PathBuf,
    /// Segments in offset order; the last is the active one.
    pub segments: Vec<Segment>,
    /// The next offset (the high watermark).
    pub next_offset: i64,
    /// The first offset served.
    pub log_start: i64,
    /// (epoch, first offset) pairs, ascending.
    pub epochs: Vec<(i32, i64)>,
    /// Idempotent producers by id.
    pub producers: HashMap<i64, Producer>,
    /// Changed since the manifest was last written.
    pub dirty: bool,
    /// A segment file (or the directory) was created since the last
    /// manifest: the directories must be synced before the manifest names
    /// it.
    pub new_files: bool,
    /// The first offset not yet cleaned by compaction.
    pub cleaned_to: i64,
}

impl PartState {
    /// An empty partition in `dir`.
    #[must_use]
    pub fn new(dir: PathBuf) -> Self {
        Self {
            dir,
            segments: Vec::new(),
            next_offset: 0,
            log_start: 0,
            epochs: Vec::new(),
            producers: HashMap::new(),
            dirty: true,
            new_files: true,
            cleaned_to: 0,
        }
    }

    /// The current leader epoch of the partition (its last pair's, or
    /// `global` when it has never appended).
    #[must_use]
    pub fn epoch(&self, global: i32) -> i32 {
        self.epochs.last().map_or(global, |e| e.0).max(global)
    }

    /// The idempotence rules for a batch of `h`'s producer; `Ok(None)`
    /// appends, `Ok(Some)` is a duplicate's answer.
    fn check_producer(&self, h: &Head) -> Result<Option<Outcome>, Outcome> {
        if h.producer_id < 0 {
            return Ok(None);
        }
        let Some(p) = self.producers.get(&h.producer_id) else {
            // An unknown producer (or one whose state expired) may start
            // anywhere.
            return Ok(None);
        };
        if h.producer_epoch < p.epoch {
            return Err(Outcome::Refused {
                code: INVALID_PRODUCER_EPOCH,
                message: format!(
                    "producer {} epoch {} is older than {}",
                    h.producer_id, h.producer_epoch, p.epoch
                ),
            });
        }
        if h.producer_epoch > p.epoch {
            return if h.base_sequence == 0 {
                Ok(None)
            } else {
                Err(Outcome::Refused {
                    code: OUT_OF_ORDER_SEQUENCE,
                    message: format!(
                        "a new epoch of producer {} must start at sequence 0, not {}",
                        h.producer_id, h.base_sequence
                    ),
                })
            };
        }
        let last = h.last_sequence();
        if let Some(b) = p
            .batches
            .iter()
            .find(|b| b.first_seq == h.base_sequence && b.last_seq == last)
        {
            let base = if b.base_offset < self.log_start {
                -1
            } else {
                b.base_offset
            };
            return Ok(Some(Outcome::Duplicate {
                base,
                log_start: self.log_start,
            }));
        }
        match p.batches.back() {
            Some(b) if h.base_sequence != batch::wrap_seq(b.last_seq, 1) => Err(Outcome::Refused {
                code: OUT_OF_ORDER_SEQUENCE,
                message: format!(
                    "producer {} sent sequence {}, expected {}",
                    h.producer_id,
                    h.base_sequence,
                    batch::wrap_seq(b.last_seq, 1)
                ),
            }),
            _ => Ok(None),
        }
    }

    /// Applies one batch: idempotence, offsets, epoch, append (rolling at
    /// `segment_bytes`). `bytes` is patched in place.
    ///
    /// # Errors
    /// Writing the segment failed (the partition is then unusable until a
    /// restart replays it).
    pub fn append(
        &mut self,
        bytes: &mut [u8],
        epoch: i32,
        now_ms: i64,
        log_append_time: bool,
        segment_bytes: u64,
        interval: u64,
    ) -> io::Result<Outcome> {
        let Some(h) = Head::parse(bytes).filter(|h| h.total == bytes.len()) else {
            return Ok(Outcome::Refused {
                code: CORRUPT_MESSAGE,
                message: "stored batch is malformed".into(),
            });
        };
        match self.check_producer(&h) {
            Err(o) | Ok(Some(o)) => return Ok(o),
            Ok(None) => {}
        }
        let base = self.next_offset;
        batch::set_offset_and_epoch(bytes, base, epoch);
        let append_ms = log_append_time.then(|| {
            batch::set_log_append_time(bytes, now_ms);
            now_ms
        });
        let h = Head::parse(bytes).unwrap_or(h);
        let roll = self
            .segments
            .last()
            .is_none_or(|s| !s.is_empty() && s.len + bytes.len() as u64 > segment_bytes);
        if roll {
            std::fs::create_dir_all(&self.dir)?;
            self.new_files = true;
            self.segments
                .push(Segment::create(&self.dir, base, now_ms, interval)?);
        }
        let seg = self
            .segments
            .last_mut()
            .ok_or_else(|| io::Error::other("no segment"))?;
        seg.append(bytes, &h)?;
        self.next_offset = h.next_offset();
        if self.epochs.last().is_none_or(|e| e.0 < epoch) {
            self.epochs.push((epoch, base));
        }
        if h.producer_id >= 0 {
            let p = self.producers.entry(h.producer_id).or_insert(Producer {
                epoch: h.producer_epoch,
                batches: VecDeque::new(),
                last_ms: now_ms,
            });
            if p.epoch != h.producer_epoch {
                p.epoch = h.producer_epoch;
                p.batches.clear();
            }
            p.batches.push_back(ProducedBatch {
                first_seq: h.base_sequence,
                last_seq: h.last_sequence(),
                base_offset: base,
            });
            while p.batches.len() > PRODUCER_BATCHES {
                p.batches.pop_front();
            }
            p.last_ms = now_ms;
        }
        self.dirty = true;
        Ok(Outcome::Appended {
            base,
            log_start: self.log_start,
            append_ms,
        })
    }

    /// A view for reading from `offset`; `Ok(None)` at the high
    /// watermark.
    ///
    /// # Errors
    /// The file system's.
    pub fn view(&self, offset: i64) -> io::Result<Option<View>> {
        let i = self.segments.partition_point(|s| s.base <= offset);
        let from = i.saturating_sub(1);
        for s in &self.segments[from..] {
            if let Some(v) = s.view_at(offset.max(s.base))? {
                return Ok(Some(v));
            }
        }
        Ok(None)
    }

    /// The largest epoch at most `epoch` and the offset where its records
    /// end (Kafka's `OffsetForLeaderEpoch`); (-1, -1) below every epoch.
    #[must_use]
    pub fn epoch_end(&self, epoch: i32, current: i32) -> (i32, i64) {
        if epoch >= current || self.epochs.is_empty() {
            return (current.max(epoch.min(current)), self.next_offset);
        }
        let i = self.epochs.partition_point(|e| e.0 <= epoch);
        if i == 0 {
            return (-1, -1);
        }
        let e = self.epochs[i - 1].0;
        let end = self.epochs.get(i).map_or(self.next_offset, |n| n.1);
        (e, end)
    }

    /// Removes producers idle since `before_ms`.
    pub fn expire_producers(&mut self, before_ms: i64) {
        let n = self.producers.len();
        self.producers.retain(|_, p| p.last_ms >= before_ms);
        if self.producers.len() != n {
            self.dirty = true;
        }
    }

    /// Bytes in every segment.
    #[must_use]
    pub fn bytes(&self) -> u64 {
        self.segments.iter().map(|s| s.len).sum()
    }

    /// Drops whole segments that end at or below `offset` (never the
    /// active one) and raises the log start to `offset`; returns the
    /// files to unlink once the manifest no longer lists them.
    pub fn truncate_below(&mut self, offset: i64) -> Vec<PathBuf> {
        let offset = offset.min(self.next_offset);
        let mut gone = Vec::new();
        while self.segments.len() > 1 && self.segments.get(1).is_some_and(|s| s.base <= offset) {
            gone.push(self.segments.remove(0).path);
        }
        // An empty active segment past the cut is renamed by the next roll.
        if offset > self.log_start {
            self.log_start = offset;
            self.dirty = true;
        }
        gone
    }
}
