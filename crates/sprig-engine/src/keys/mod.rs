//! The key layout every model shares.
//!
//! ```text
//! logical key := tenant u64 BE | shard u32 BE | object u32 BE | kind u8 | index u16 BE | user key
//! stored key  := logical key | !commit_ts u64 BE        (newest version first)
//! ```
//!
//! The user key is already memcomparable ([`memcmp`], [`collation`]), and
//! every user-key encoding is prefix-free (fixed widths, or escaped and
//! terminated strings), so no logical key is a proper prefix of another in
//! the same index. RocksDB's prefix extractor strips the 8-byte version, so
//! Bloom filters work on the logical key for versioned point reads.
//!
//! Engine bookkeeping that is not versioned (a unit's applied index) lives
//! under the reserved tenant [`META_TENANT`].

pub mod collation;
pub mod memcmp;

use std::fmt;

/// A hybrid-logical-clock timestamp: physical milliseconds in the high 48
/// bits, a logical counter in the low 16.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Default)]
pub struct Ts(pub u64);

impl Ts {
    /// The smallest timestamp.
    pub const MIN: Ts = Ts(0);
    /// The largest timestamp: reads at `MAX` see the newest version.
    pub const MAX: Ts = Ts(u64::MAX);

    /// Builds a timestamp from its parts.
    #[must_use]
    pub const fn from_parts(physical_ms: u64, logical: u16) -> Ts {
        Ts((physical_ms << 16) | logical as u64)
    }

    /// The physical part, in milliseconds since the Unix epoch.
    #[must_use]
    pub const fn physical_ms(self) -> u64 {
        self.0 >> 16
    }

    /// The logical counter.
    #[must_use]
    pub const fn logical(self) -> u16 {
        // The mask keeps 16 bits, so the cast is exact.
        #[allow(clippy::cast_possible_truncation)]
        let l = (self.0 & 0xFFFF) as u16;
        l
    }
}

impl fmt::Display for Ts {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}.{}", self.physical_ms(), self.logical())
    }
}

/// Families inside an object. `index` numbers the indexes of a kind.
pub mod kind {
    /// Primary rows, documents, graph nodes.
    pub const PRIMARY: u8 = b'P';
    /// Secondary index entries.
    pub const SECONDARY: u8 = b'S';
    /// Unique index entries.
    pub const UNIQUE: u8 = b'U';
    /// Metadata (Redis key metadata, sequences, object state).
    pub const META: u8 = b'M';
    /// Elements of collections (hash fields, list items, set members).
    pub const ELEMENTS: u8 = b'E';
    /// Graph adjacency.
    pub const ADJACENCY: u8 = b'A';
    /// Graph label scans.
    pub const LABELS: u8 = b'L';
    /// Counters (`AUTO_INCREMENT`, sequences).
    pub const COUNTERS: u8 = b'C';
    /// Sorted-set scores.
    pub const ZSCORES: u8 = b'Z';
    /// Geo cells.
    pub const GEO: u8 = b'G';
    /// Redis expiry entries, inside the database object.
    pub const EXPIRY: u8 = b'X';
    /// A materialized copy of inherited rows: a hidden companion keyspace of
    /// the child's object, numbered by share.
    pub const COPY: u8 = b'Y';
}

/// Bytes of `tenant | shard | object | kind | index`.
pub const PREFIX_LEN: usize = 8 + 4 + 4 + 1 + 2;
/// Bytes of the version suffix.
pub const VERSION_LEN: usize = 8;
/// The reserved tenant id under which unversioned engine bookkeeping lives.
pub const META_TENANT: u64 = u64::MAX;

/// The fixed head of every logical key.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct KeyPrefix {
    /// The tenant (the routing prefix, with `shard`).
    pub tenant: u64,
    /// The shard inside the tenant; 0 for an unsharded tenant and the catalog.
    pub shard: u32,
    /// The table, collection, Redis database, graph, stream or queue.
    pub object: u32,
    /// The family inside the object ([`kind`]).
    pub kind: u8,
    /// Which index of that kind.
    pub index: u16,
}

impl KeyPrefix {
    /// Appends the 19 prefix bytes.
    pub fn encode_into(&self, out: &mut Vec<u8>) {
        out.reserve(PREFIX_LEN);
        out.extend_from_slice(&self.tenant.to_be_bytes());
        out.extend_from_slice(&self.shard.to_be_bytes());
        out.extend_from_slice(&self.object.to_be_bytes());
        out.push(self.kind);
        out.extend_from_slice(&self.index.to_be_bytes());
    }

    /// The prefix as a fresh vector.
    #[must_use]
    pub fn to_vec(&self) -> Vec<u8> {
        let mut v = Vec::with_capacity(PREFIX_LEN);
        self.encode_into(&mut v);
        v
    }

    /// Splits a logical key into its prefix and user key.
    #[must_use]
    pub fn decode(logical: &[u8]) -> Option<(KeyPrefix, &[u8])> {
        if logical.len() < PREFIX_LEN {
            return None;
        }
        let (head, rest) = logical.split_at(PREFIX_LEN);
        Some((
            KeyPrefix {
                tenant: u64::from_be_bytes(head[0..8].try_into().ok()?),
                shard: u32::from_be_bytes(head[8..12].try_into().ok()?),
                object: u32::from_be_bytes(head[12..16].try_into().ok()?),
                kind: head[16],
                index: u16::from_be_bytes(head[17..19].try_into().ok()?),
            },
            rest,
        ))
    }
}

/// Appends a logical key: the prefix, then the already-encoded user key.
pub fn logical_into(out: &mut Vec<u8>, prefix: &KeyPrefix, user_key: &[u8]) {
    out.reserve(PREFIX_LEN + user_key.len() + VERSION_LEN);
    prefix.encode_into(out);
    out.extend_from_slice(user_key);
}

/// The brief's one-call form: a logical key, plus the version suffix when
/// `version` is given.
#[must_use]
pub fn encode(
    tenant: u64,
    shard: u32,
    object: u32,
    kind: u8,
    index: u16,
    user_key: &[u8],
    version: Option<Ts>,
) -> Vec<u8> {
    let mut out = Vec::with_capacity(PREFIX_LEN + user_key.len() + VERSION_LEN);
    logical_into(
        &mut out,
        &KeyPrefix {
            tenant,
            shard,
            object,
            kind,
            index,
        },
        user_key,
    );
    if let Some(ts) = version {
        push_version(&mut out, ts);
    }
    out
}

/// Appends the version suffix `!ts`, so newer versions sort first.
pub fn push_version(out: &mut Vec<u8>, ts: Ts) {
    out.extend_from_slice(&(!ts.0).to_be_bytes());
}

/// A stored key for `logical` at `ts`.
#[must_use]
pub fn versioned(logical: &[u8], ts: Ts) -> Vec<u8> {
    let mut out = Vec::with_capacity(logical.len() + VERSION_LEN);
    out.extend_from_slice(logical);
    push_version(&mut out, ts);
    out
}

/// Splits a stored key into its logical key and version.
#[must_use]
pub fn split_version(stored: &[u8]) -> Option<(&[u8], Ts)> {
    if stored.len() < VERSION_LEN {
        return None;
    }
    let (logical, v) = stored.split_at(stored.len() - VERSION_LEN);
    let inv = u64::from_be_bytes(v.try_into().ok()?);
    Some((logical, Ts(!inv)))
}

/// The prefix extractor's transform: the key without its last 8 bytes.
/// Keys shorter than that are outside the extractor's domain.
#[must_use]
pub fn strip_version(stored: &[u8]) -> &[u8] {
    &stored[..stored.len().saturating_sub(VERSION_LEN)]
}

/// The smallest key greater than every key starting with `prefix`, or
/// `None` if `prefix` is all `0xFF` (the range is then unbounded above).
#[must_use]
pub fn prefix_end(prefix: &[u8]) -> Option<Vec<u8>> {
    let mut end = prefix.to_vec();
    while let Some(last) = end.pop() {
        if last < 0xFF {
            end.push(last + 1);
            return Some(end);
        }
    }
    None
}

/// The key range of one tenant.
#[must_use]
pub fn tenant_range(tenant: u64) -> KeyRange {
    KeyRange::prefix(&tenant.to_be_bytes())
}

/// The key range of one shard of a tenant: the span a move exports.
#[must_use]
pub fn shard_range(tenant: u64, shard: u32) -> KeyRange {
    let mut p = tenant.to_be_bytes().to_vec();
    p.extend_from_slice(&shard.to_be_bytes());
    KeyRange::prefix(&p)
}

/// The key range of one object, indexes included.
#[must_use]
pub fn object_range(tenant: u64, shard: u32, object: u32) -> KeyRange {
    let mut p = tenant.to_be_bytes().to_vec();
    p.extend_from_slice(&shard.to_be_bytes());
    p.extend_from_slice(&object.to_be_bytes());
    KeyRange::prefix(&p)
}

/// The unversioned key of one piece of a unit's engine bookkeeping, such as
/// its applied index. It sorts after every tenant's data.
#[must_use]
pub fn unit_meta_key(unit: u64, field: &[u8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(PREFIX_LEN + 8 + field.len());
    KeyPrefix {
        tenant: META_TENANT,
        shard: 0,
        object: 0,
        kind: kind::META,
        index: 0,
    }
    .encode_into(&mut out);
    out.extend_from_slice(&unit.to_be_bytes());
    out.extend_from_slice(field);
    out
}

/// A half-open key range `[start, end)`; `end = None` is unbounded.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct KeyRange {
    /// Inclusive lower bound.
    pub start: Vec<u8>,
    /// Exclusive upper bound, or unbounded.
    pub end: Option<Vec<u8>>,
}

impl KeyRange {
    /// `[start, end)`.
    #[must_use]
    pub fn new(start: impl Into<Vec<u8>>, end: impl Into<Vec<u8>>) -> Self {
        Self {
            start: start.into(),
            end: Some(end.into()),
        }
    }

    /// Every key starting with `prefix`.
    #[must_use]
    pub fn prefix(prefix: &[u8]) -> Self {
        Self {
            start: prefix.to_vec(),
            end: prefix_end(prefix),
        }
    }

    /// The whole keyspace.
    #[must_use]
    pub fn all() -> Self {
        Self::default()
    }

    /// Whether `key` is inside the range.
    #[must_use]
    pub fn contains(&self, key: &[u8]) -> bool {
        key >= self.start.as_slice() && self.end.as_deref().is_none_or(|e| key < e)
    }
}

/// The first byte of a stored value: what the version is.
pub mod value {
    /// A put: the payload is the value.
    pub const PUT: u8 = 0;
    /// A delete marker: the key is absent at this version.
    pub const DELETE: u8 = 1;
    /// A merge delta (not used yet).
    pub const MERGE: u8 = 2;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn versions_sort_newest_first() {
        let l = encode(7, 0, 3, kind::PRIMARY, 0, b"k", None);
        let old = versioned(&l, Ts(10));
        let new = versioned(&l, Ts(20));
        assert!(new < old);
        assert_eq!(split_version(&new), Some((l.as_slice(), Ts(20))));
        assert_eq!(strip_version(&old), l.as_slice());
    }

    #[test]
    fn prefix_round_trip() {
        let p = KeyPrefix {
            tenant: 0x0102_0304_0506_0708,
            shard: 9,
            object: 10,
            kind: kind::SECONDARY,
            index: 2,
        };
        let mut v = p.to_vec();
        v.extend_from_slice(b"user");
        let (q, rest) = KeyPrefix::decode(&v).unwrap();
        assert_eq!(q, p);
        assert_eq!(rest, b"user");
    }

    #[test]
    fn prefix_end_and_ranges() {
        assert_eq!(prefix_end(b"ab"), Some(b"ac".to_vec()));
        assert_eq!(prefix_end(b"a\xFF"), Some(b"b".to_vec()));
        assert_eq!(prefix_end(b"\xFF\xFF"), None);
        let r = object_range(1, 0, 5);
        assert!(r.contains(&encode(1, 0, 5, kind::PRIMARY, 0, b"x", Some(Ts(1)))));
        assert!(!r.contains(&encode(1, 0, 6, kind::PRIMARY, 0, b"x", None)));
        assert!(unit_meta_key(1, b"applied") > encode(u64::MAX - 1, 0, 0, 0, 0, b"", None));
    }

    #[test]
    fn ts_parts() {
        let t = Ts::from_parts(1_700_000_000_000, 7);
        assert_eq!(t.physical_ms(), 1_700_000_000_000);
        assert_eq!(t.logical(), 7);
    }
}
