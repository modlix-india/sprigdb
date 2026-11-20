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

/// The newest committed version of `logical` in the store.
///
/// # Errors
/// Store failures.
pub fn read_latest(store: &dyn SortedStore, logical: &[u8]) -> Result<Option<Version>> {
    read_at(store, logical, Ts::MAX)
}

/// Visible versions (delete markers included) over a logical key range.
pub struct VersionScan<'a> {
    it: Box<dyn StoreIter + 'a>,
    dir: Dir,
    at: Ts,
    started: bool,
}

impl<'a> VersionScan<'a> {
    /// The versions visible at `at` of the logical keys in `range` (bounds
    /// are logical keys or prefix bounds), in `dir` order.
    #[must_use]
    pub fn new(store: &'a dyn SortedStore, range: &KeyRange, dir: Dir, at: Ts) -> Self {
        Self {
            it: store.iter(Cf::Data, IterOpts::range(range)),
            dir,
            at,
            started: false,
        }
    }

    fn step(&mut self) {
        match self.dir {
            Dir::Forward => self.it.next(),
            Dir::Reverse => self.it.prev(),
        }
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
                Dir::Reverse => self.it.seek_to_last(),
            }
        }
        loop {
            if !self.it.valid() {
                self.it.status()?.expect_ready()?;
                return Ok(None);
            }
            let Some((lk, _)) = keys::split_version(self.it.key()) else {
                self.step();
                continue;
            };
            let logical = lk.to_vec();
            // Walk the group of versions of `logical`, keeping the newest one
            // at or below the scan's timestamp.
            let mut best: Option<Version> = None;
            while self.it.valid() {
                match keys::split_version(self.it.key()) {
                    Some((lk2, ts)) if lk2 == logical.as_slice() => {
                        if ts <= self.at && best.as_ref().is_none_or(|b| ts > b.ts) {
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
                return Ok(Some((Bytes::from(logical), b)));
            }
        }
    }
}
