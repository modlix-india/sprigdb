//! Reading versions: `logical key | !commit_ts`, newest first.
//!
//! A logical key must be *prefix-free* among the keys of the store: no
//! logical key is a proper prefix of another. The memcomparable encodings
//! (fixed-width integers, terminated strings) guarantee this; raw user
//! bytes must be escaped and terminated first. Then all versions of one
//! logical key are contiguous, and the first entry at or after
//! `key | !rts` is the version visible at `rts`.
//!
//! A stored value starts with a tag byte ([`keys::value`]): a put and its
//! payload, or a delete marker alone.

use bytes::Bytes;
use sprig_common::Result;

use super::hlc::Ts;
use crate::keys::{self, KeyRange};
use crate::store::{Cf, IterOpts, ReadTier, SortedStore, StoreIter};
use crate::txn::Dir;

/// A committed version of one logical key.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Version {
    /// Its commit timestamp.
    pub ts: Ts,
    /// The value; `None` for a delete.
    pub value: Option<Bytes>,
}

/// The stored value for `v` (`None`: delete marker), appended to `out`.
pub fn encode_value_into(out: &mut Vec<u8>, v: Option<&[u8]>) {
    match v {
        None => out.push(keys::value::DELETE),
        Some(p) => {
            out.push(keys::value::PUT);
            out.extend_from_slice(p);
        }
    }
}

/// Decodes a stored value; `None` for a delete marker.
#[must_use]
pub fn decode_value(raw: &[u8]) -> Option<Bytes> {
    match raw.split_first() {
        Some((&keys::value::PUT, v)) => Some(Bytes::copy_from_slice(v)),
        _ => None,
    }
}

/// Whether a stored value is a delete marker.
#[must_use]
pub fn is_tombstone(raw: &[u8]) -> bool {
    raw.first() != Some(&keys::value::PUT)
}

/// The newest version of `logical` with `ts <= at`, from the store.
///
/// # Errors
/// Store failures.
pub fn read_at(store: &dyn SortedStore, logical: &[u8], at: Ts) -> Result<Option<Version>> {
    let mut buf = Vec::new();
    let ts = store
        .get_version(Cf::Data, logical, at, ReadTier::All, &mut buf)?
        .expect_ready()?;
    Ok(ts.map(|ts| Version {
        ts,
        value: decode_value(&buf),
    }))
}

/// [`read_at`] for several logical keys (ascending keys share the store's
/// cursor), in the order given.
///
/// # Errors
/// Store failures.
pub fn read_many_at(
    store: &dyn SortedStore,
    logicals: &[&[u8]],
    at: Ts,
) -> Result<Vec<Option<Version>>> {
    Ok(store
        .get_versions(Cf::Data, logicals, at, ReadTier::All)?
        .expect_ready()?
        .into_iter()
        .map(|v| {
            v.map(|(ts, raw)| Version {
                ts,
                value: decode_value(&raw),
            })
        })
        .collect())
}

/// The newest committed version of `logical` in the store.
///
/// # Errors
/// Store failures.
pub fn read_latest(store: &dyn SortedStore, logical: &[u8]) -> Result<Option<Version>> {
    read_at(store, logical, Ts::MAX)
}

/// Visible versions (delete markers included) over a logical key range.
///
/// The range's bounds are compared with *logical* keys, but the store holds
/// `logical | !ts`, so the store's own bounds are not exact when a bound
/// extends a stored key (a paging caller's "just after `k`" bound, `k`
/// followed by a zero byte, or a bound that cuts through `k`'s version
/// suffixes). For a stored key `k` that is a proper prefix of a bound,
/// some or all of `k`'s versions sort on the other side of the bound:
///
/// - at the start, `k < start` but versions of `k` sort at or after
///   `start`: they are skipped by comparing the logical key;
/// - at the end, `k < end` but versions of `k` sort at or after `end`, so
///   the store's range misses them: one extra lookup past `end` finds `k`
///   (prefix-free keys allow at most one such key, and it is the last key
///   of the range), and its visible version is read directly.
pub struct VersionScan<'a> {
    store: &'a dyn SortedStore,
    it: Box<dyn StoreIter + 'a>,
    dir: Dir,
    at: Ts,
    started: bool,
    start: Vec<u8>,
    end: Option<Vec<u8>>,
    /// The length of the shortest proper prefix of `end` that is still at
    /// or above `start`; `None` when no such prefix exists, so no logical
    /// key in the range can have versions past `end`.
    edge_from: Option<usize>,
    edge: Edge,
}

/// The logical key whose versions straddle the range's end.
enum Edge {
    /// Not looked up yet.
    Pending,
    /// Nothing (more) to do.
    Done,
    /// Found and already returned (reverse scans): skip its versions inside
    /// the store's range.
    Skip(Vec<u8>),
}

/// See [`VersionScan::edge_from`].
fn edge_from(start: &[u8], end: Option<&[u8]>) -> Option<usize> {
    let end = end?;
    if end <= start {
        return None;
    }
    let common = start.iter().zip(end).take_while(|(a, b)| a == b).count();
    // A prefix `end[..n]` is at or above `start` when `start` is itself a
    // prefix of it, or when it reaches past the first differing byte.
    let shortest = if common == start.len() {
        common
    } else {
        common + 1
    };
    (shortest < end.len()).then_some(shortest)
}

impl<'a> VersionScan<'a> {
    /// The versions visible at `at` of the logical keys in `range` (bounds
    /// are logical keys or prefix bounds), in `dir` order.
    #[must_use]
    pub fn new(store: &'a dyn SortedStore, range: &KeyRange, dir: Dir, at: Ts) -> Self {
        let edge_from = edge_from(&range.start, range.end.as_deref());
        Self {
            store,
            it: store.iter(Cf::Data, IterOpts::range(range)),
            dir,
            at,
            started: false,
            start: range.start.clone(),
            end: range.end.clone(),
            edge_from,
            edge: if edge_from.is_some() {
                Edge::Pending
            } else {
                Edge::Done
            },
        }
    }

    fn step(&mut self) {
        match self.dir {
            Dir::Forward => self.it.next(),
            Dir::Reverse => self.it.prev(),
        }
    }

    /// Whether `logical` is a proper prefix of the range's end that is at
    /// or above its start: the one key whose versions may lie past `end`.
    fn is_edge_key(&self, logical: &[u8]) -> bool {
        match (self.edge_from, self.end.as_deref()) {
            (Some(from), Some(end)) => {
                logical.len() >= from && logical.len() < end.len() && end.starts_with(logical)
            }
            _ => false,
        }
    }

    /// Looks past the range's end for a logical key that is a proper prefix
    /// of it, with its version visible at the scan's timestamp, if any.
    ///
    /// Every stored key at or after `end` belongs either to that one key
    /// (prefix-free keys allow only one) or to a logical key at or after
    /// `end`, and the first group sorts before the second; so the first
    /// stored key at or after `end` tells.
    fn find_edge(&self) -> Result<Option<(Vec<u8>, Option<Version>)>> {
        let (Some(from), Some(end)) = (self.edge_from, self.end.as_deref()) else {
            return Ok(None);
        };
        let found = {
            let mut it = self.store.iter(
                Cf::Data,
                IterOpts {
                    lower: Some(end.to_vec()),
                    upper: keys::prefix_end(&end[..from]),
                    ..IterOpts::default()
                },
            );
            it.seek_to_first();
            if !it.valid() {
                it.status()?.expect_ready()?;
                return Ok(None);
            }
            match keys::split_version(it.key()) {
                Some((lk, _)) if self.is_edge_key(lk) => lk.to_vec(),
                _ => return Ok(None),
            }
        };
        // Some of its versions may be inside the range and some past it:
        // read the visible one from all of them.
        let version = read_at(self.store, &found, self.at)?;
        Ok(Some((found, version)))
    }

    /// The next logical key with a version visible at the scan's timestamp.
    ///
    /// # Errors
    /// Store failures.
    pub fn next_visible(&mut self) -> Result<Option<(Bytes, Version)>> {
        if !self.started {
            self.started = true;
            match self.dir {
                Dir::Forward => self.it.seek_to_first(),
                Dir::Reverse => {
                    self.it.seek_to_last();
                    // In descending order the key straddling the end comes
                    // first.
                    if matches!(self.edge, Edge::Pending) {
                        self.edge = Edge::Done;
                        if let Some((k, v)) = self.find_edge()? {
                            self.edge = Edge::Skip(k.clone());
                            if let Some(v) = v {
                                return Ok(Some((Bytes::from(k), v)));
                            }
                        }
                    }
                }
            }
        }
        loop {
            if !self.it.valid() {
                self.it.status()?.expect_ready()?;
                // In ascending order the key straddling the end comes last.
                if self.dir == Dir::Forward && matches!(self.edge, Edge::Pending) {
                    self.edge = Edge::Done;
                    if let Some((k, Some(v))) = self.find_edge()? {
                        return Ok(Some((Bytes::from(k), v)));
                    }
                }
                return Ok(None);
            }
            let Some((lk, _)) = keys::split_version(self.it.key()) else {
                self.step();
                continue;
            };
            let logical = lk.to_vec();
            // A key below the start whose versions sort after it, or the
            // key already returned from past the end.
            let skip = logical.as_slice() < self.start.as_slice()
                || matches!(&self.edge, Edge::Skip(k) if *k == logical);
            // Walk the group of versions of `logical`, keeping the newest one
            // at or below the scan's timestamp.
            let mut best: Option<Version> = None;
            while self.it.valid() {
                match keys::split_version(self.it.key()) {
                    Some((lk2, ts)) if lk2 == logical.as_slice() => {
                        if !skip && ts <= self.at && best.as_ref().is_none_or(|b| ts > b.ts) {
                            best = Some(Version {
                                ts,
                                value: decode_value(self.it.value()),
                            });
                        }
                    }
                    _ => break,
                }
                self.step();
            }
            if let Some(b) = best {
                if self.dir == Dir::Forward && self.is_edge_key(&logical) {
                    // Its newest versions were inside the range and one of
                    // them is visible; the older ones past the end can't be
                    // newer than it.
                    self.edge = Edge::Done;
                }
                return Ok(Some((Bytes::from(logical), b)));
            }
        }
    }
}
