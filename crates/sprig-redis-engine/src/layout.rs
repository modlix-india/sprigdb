//! Where a Redis database's data lives in the engine's keyspace, and how
//! a key's metadata entry is encoded.
//!
//! Every Redis key has one **metadata** entry: its type, its expiry, and
//! either its whole value (a string, or a small hash or list stored inline)
//! or, for a bigger collection, a uid and counts. A big collection's hash
//! fields and list items are **element** entries under its uid, so the
//! key's bytes aren't repeated in each, and a collection's elements are one
//! contiguous range. Uids come from a per-database counter, taken in blocks,
//! and are never reused, so a replaced collection's elements can never be
//! mistaken for the new one's. Every key also has a **scan-index** entry,
//! ordered by its routing hash, and a key with an expiry has an **expiry**
//! entry, ordered by time, which the sweeper walks.
//!
//! ```text
//! metadata    prefix(M, 0) | str(key)                          -> Meta
//! scan index  prefix(M, 1) | rk u64 | str(key)                 -> (empty)
//! dead uid    prefix(M, 3) | uid u64                           -> flags u8
//!             (a deleted big collection whose entries the sweeper removes)
//! element     prefix(E, 0) | uid u64 | crc32(field) u32 | str(field)
//!                                                -> flags u8 | value (hash)
//!             prefix(E, 0) | uid u64 | pos i64                 -> item   (list)
//!             prefix(E, 0) | uid u64 | chunk u32               -> bytes  (chunked string)
//!             prefix(E, 0) | uid u64 | crc32(member) u32 | str(member)
//!                                                -> (empty) (set) | score f64 (sorted set)
//! score index prefix(Z, 0) | uid u64 | enc(score) u64 | str(member) -> (empty)
//! field expiry prefix(E, 1) | uid u64 | at_ms u64 | str(field) -> (empty)
//! expiry      prefix(X, 0) | at_ms u64 | str(key)              -> (empty)
//! hash due    prefix(X, 1) | at_ms u64 | str(key)              -> (empty)
//!             (a hash with field lifetimes, at its earliest one)
//! counter     prefix(C, 0) | 3                                 -> uid high-water mark u64
//!             (database 0's counter serves every database of the tenant)
//! prefix(kind, index) = tenant u64 | shard u32 (0) | object u32 | kind u8 | index u16
//! ```
//!
//! All integers are big-endian; a list position has its sign bit flipped,
//! so positions sort in numeric order. `str` is the engine's memcomparable
//! byte string (`00` escaped as `00 FF`, terminated by `00 00`), so no key
//! is a proper prefix of another and the engine's versions of one key stay
//! together. `rk` is the key's routing hash: `xxh3_64` of its hash tag, the
//! same value the key-value tables keep. Hash fields are ordered by their
//! `CRC32` first, so a field scan can resume from a number alone.

use bytes::{BufMut, Bytes, BytesMut};

/// Metadata entries (index 0) and the scan index (index 1).
pub const KIND_META: u8 = b'M';
/// Collection elements.
pub const KIND_ELEM: u8 = b'E';
/// Expiry entries.
pub const KIND_EXPIRY: u8 = b'X';
/// Counters.
pub const KIND_COUNTER: u8 = b'C';
/// Sorted-set score index.
pub const KIND_ZSCORE: u8 = b'Z';

/// The scan index's index number under [`KIND_META`].
pub const SCAN_INDEX: u16 = 1;
/// The dead-uid list's index number under [`KIND_META`].
pub const DEAD_INDEX: u16 = 3;
/// A dead uid's flags: it has score-index entries.
pub const DEAD_ZSCORE: u8 = 1;
/// A dead uid's flags: it has field-expiry entries.
pub const DEAD_FEXP: u8 = 2;
/// The counter holding the uid high-water mark.
const COUNTER_UID: u8 = 3;
/// Flags byte of a hash field's value: no field expiry.
const FIELD_PLAIN: u8 = 0;
/// Flags byte of a hash field's value: its expiry follows (i64).
const FIELD_TTL: u8 = 1;
/// The field-expiry index's index number under [`KIND_ELEM`], and the
/// hash-due index's under [`KIND_EXPIRY`].
pub const TTL_INDEX: u16 = 1;

/// The object id of Redis database `db` (until the catalog assigns Redis
/// databases object ids).
pub const OBJECT_BASE: u32 = 0x5244_0000;

/// Largest inline collection: entries.
pub const INLINE_MAX_ENTRIES: usize = 128;
/// Largest inline collection: bytes per field, value or item.
pub const INLINE_MAX_BYTES: usize = 64;
/// A chunked string's chunk size.
pub const CHUNK: usize = 16 * 1024;
/// A partial write (`APPEND`, `SETRANGE`, `SETBIT`, `BITFIELD`) of a
/// string longer than this stores it in chunks.
pub const CHUNK_THRESHOLD: usize = 64 * 1024;
/// Largest inline set of integers only.
pub const INTSET_MAX_ENTRIES: usize = 512;
/// The bytes before an element's or score-index entry's uid.
pub const PREFIX_LEN: usize = 19;

/// One Redis database's part of the keyspace.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct Space {
    /// The tenant.
    pub tenant: u64,
    /// The database's object id.
    pub object: u32,
}

impl Space {
    /// Database `db` of `tenant`.
    #[must_use]
    pub const fn redis(tenant: u64, db: u32) -> Self {
        Self {
            tenant,
            object: OBJECT_BASE + db,
        }
    }

    fn put_prefix(&self, b: &mut BytesMut, kind: u8) {
        self.put_prefix_at(b, kind, 0);
    }

    fn put_prefix_at(&self, b: &mut BytesMut, kind: u8, index: u16) {
        b.put_u64(self.tenant);
        b.put_u32(0);
        b.put_u32(self.object);
        b.put_u8(kind);
        b.put_u16(index);
    }

    /// The first key of `kind` in this database.
    #[must_use]
    pub fn prefix(&self, kind: u8) -> Vec<u8> {
        let mut b = BytesMut::with_capacity(19);
        self.put_prefix(&mut b, kind);
        b.to_vec()
    }

    /// The first scan-index entry, and the key after the last.
    #[must_use]
    pub fn scan_index_range(&self) -> (Vec<u8>, Vec<u8>) {
        let mut lo = BytesMut::with_capacity(19);
        self.put_prefix_at(&mut lo, KIND_META, SCAN_INDEX);
        let mut hi = BytesMut::with_capacity(19);
        self.put_prefix_at(&mut hi, KIND_META, SCAN_INDEX + 1);
        (lo.to_vec(), hi.to_vec())
    }

    /// `key`'s scan-index entry.
    #[must_use]
    pub fn scan_entry(&self, key: &[u8]) -> Bytes {
        let mut b = BytesMut::with_capacity(29 + key.len());
        self.put_prefix_at(&mut b, KIND_META, SCAN_INDEX);
        b.put_u64(routing_hash(key));
        put_str(&mut b, key);
        b.freeze()
    }

    /// Dead uid `uid`'s entry.
    #[must_use]
    pub fn dead(&self, uid: u64) -> Bytes {
        let mut b = BytesMut::with_capacity(27);
        self.put_prefix_at(&mut b, KIND_META, DEAD_INDEX);
        b.put_u64(uid);
        b.freeze()
    }

    /// The dead-uid list: `[start, end)`.
    #[must_use]
    pub fn dead_range(&self) -> (Vec<u8>, Vec<u8>) {
        let mut lo = BytesMut::with_capacity(19);
        self.put_prefix_at(&mut lo, KIND_META, DEAD_INDEX);
        let mut hi = BytesMut::with_capacity(19);
        self.put_prefix_at(&mut hi, KIND_META, DEAD_INDEX + 1);
        (lo.to_vec(), hi.to_vec())
    }

    /// The counter holding the database's uid high-water mark.
    #[must_use]
    pub fn uid_counter(&self) -> Bytes {
        let mut b = BytesMut::with_capacity(20);
        self.put_prefix(&mut b, KIND_COUNTER);
        b.put_u8(COUNTER_UID);
        b.freeze()
    }

    /// The key after every key of `kind` in this database.
    #[must_use]
    pub fn prefix_end(&self, kind: u8) -> Vec<u8> {
        self.prefix(kind + 1)
    }

    /// `key`'s metadata entry.
    #[must_use]
    pub fn meta(&self, key: &[u8]) -> Bytes {
        let mut b = BytesMut::with_capacity(21 + key.len());
        self.put_prefix(&mut b, KIND_META);
        put_str(&mut b, key);
        b.freeze()
    }

    /// The user key of metadata entry `stored`, if it is one of ours.
    #[must_use]
    pub fn meta_user_key(&self, stored: &[u8]) -> Option<Vec<u8>> {
        let p = self.prefix(KIND_META);
        let (k, rest) = get_str(stored.strip_prefix(&p[..])?)?;
        rest.is_empty().then_some(k)
    }

    /// The start of the elements of collection `uid`.
    #[must_use]
    pub fn elems(&self, uid: u64) -> Vec<u8> {
        let mut b = BytesMut::with_capacity(27);
        self.put_prefix(&mut b, KIND_ELEM);
        b.put_u64(uid);
        b.to_vec()
    }

    /// The elements of collection `uid`: `[start, end)`.
    #[must_use]
    pub fn elems_range(&self, uid: u64) -> (Vec<u8>, Vec<u8>) {
        let start = self.elems(uid);
        let end = match uid.checked_add(1) {
            Some(next) => self.elems(next),
            None => self.prefix_end(KIND_ELEM),
        };
        (start, end)
    }

    /// Hash field `field` of collection `uid`.
    #[must_use]
    pub fn field(&self, uid: u64, field: &[u8]) -> Bytes {
        let mut b = BytesMut::with_capacity(33 + field.len());
        self.put_prefix(&mut b, KIND_ELEM);
        b.put_u64(uid);
        b.put_u32(crc32fast::hash(field));
        put_str(&mut b, field);
        b.freeze()
    }

    /// Chunk `i` of chunked string `uid`.
    #[must_use]
    pub fn chunk(&self, uid: u64, i: u32) -> Bytes {
        let mut v = self.elems(uid);
        v.extend_from_slice(&i.to_be_bytes());
        Bytes::from(v)
    }

    /// The chunk number of chunk entry `stored`.
    #[must_use]
    pub fn parse_chunk(stored: &[u8]) -> Option<u32> {
        Some(u32::from_be_bytes(
            stored
                .get(PREFIX_LEN + 8..PREFIX_LEN + 12)?
                .try_into()
                .ok()?,
        ))
    }

    /// List item at `pos` of collection `uid`.
    #[must_use]
    pub fn item(&self, uid: u64, pos: i64) -> Bytes {
        let mut v = self.elems(uid);
        v.extend_from_slice(&pos_bytes(pos));
        Bytes::from(v)
    }

    /// The start of collection `uid`'s entries of `kind` (elements or
    /// score index).
    #[must_use]
    pub fn under(&self, kind: u8, uid: u64) -> Vec<u8> {
        let mut b = BytesMut::with_capacity(27);
        self.put_prefix(&mut b, kind);
        b.put_u64(uid);
        b.to_vec()
    }

    /// Collection `uid`'s entries of `kind`: `[start, end)`.
    #[must_use]
    pub fn under_range(&self, kind: u8, uid: u64) -> (Vec<u8>, Vec<u8>) {
        let start = self.under(kind, uid);
        let end = match uid.checked_add(1) {
            Some(next) => self.under(kind, next),
            None => self.prefix_end(kind),
        };
        (start, end)
    }

    /// The elements of collection `uid` whose field hash is at least
    /// `crc`.
    #[must_use]
    pub fn elems_from_crc(&self, uid: u64, crc: u32) -> Vec<u8> {
        let mut v = self.elems(uid);
        v.extend_from_slice(&crc.to_be_bytes());
        v
    }

    /// Score-index entry of `member` at `score` in sorted set `uid`.
    #[must_use]
    pub fn zscore(&self, uid: u64, score: f64, member: &[u8]) -> Bytes {
        let mut b = BytesMut::with_capacity(37 + member.len());
        self.put_prefix(&mut b, KIND_ZSCORE);
        b.put_u64(uid);
        b.put_u64(score_bits(score));
        put_str(&mut b, member);
        b.freeze()
    }

    /// The first score-index key of sorted set `uid` at `score` or after.
    #[must_use]
    pub fn zscore_from(&self, uid: u64, score: f64) -> Vec<u8> {
        let mut v = self.under(KIND_ZSCORE, uid);
        v.extend_from_slice(&score_bits(score).to_be_bytes());
        v
    }

    /// The first score-index key of sorted set `uid` after every entry at
    /// `score`.
    #[must_use]
    pub fn zscore_after(&self, uid: u64, score: f64) -> Vec<u8> {
        let mut v = self.under(KIND_ZSCORE, uid);
        match score_bits(score).checked_add(1) {
            Some(n) => v.extend_from_slice(&n.to_be_bytes()),
            None => return self.under_range(KIND_ZSCORE, uid).1,
        }
        v
    }

    /// The score and member of score-index entry `stored`.
    #[must_use]
    pub fn parse_zscore(stored: &[u8]) -> Option<(f64, Vec<u8>)> {
        let rest = stored.get(PREFIX_LEN + 8..)?;
        let (bits, m) = rest.split_at_checked(8)?;
        let score = score_of_bits(u64::from_be_bytes(bits.try_into().ok()?));
        let (m, tail) = get_str(m)?;
        tail.is_empty().then_some((score, m))
    }

    /// The field (or member) of element entry `stored` of a hash, set or
    /// sorted set, and its hash.
    #[must_use]
    pub fn parse_field(stored: &[u8]) -> Option<(u32, Vec<u8>)> {
        let rest = stored.get(PREFIX_LEN + 8..)?;
        let (crc, f) = rest.split_at_checked(4)?;
        let crc = u32::from_be_bytes(crc.try_into().ok()?);
        let (f, tail) = get_str(f)?;
        tail.is_empty().then_some((crc, f))
    }

    /// The position of list item entry `stored`.
    #[must_use]
    pub fn parse_pos(stored: &[u8]) -> Option<i64> {
        let b: [u8; 8] = stored
            .get(PREFIX_LEN + 8..PREFIX_LEN + 16)?
            .try_into()
            .ok()?;
        Some((u64::from_be_bytes(b) ^ (1 << 63)).cast_signed())
    }

    /// Entry `stored` (an element or score-index entry of some collection
    /// of any database) moved to collection `uid` of this database.
    #[must_use]
    pub fn rekey(&self, kind: u8, uid: u64, stored: &[u8]) -> Bytes {
        let mut b = BytesMut::with_capacity(stored.len());
        self.put_prefix(&mut b, kind);
        b.put_u64(uid);
        b.put_slice(stored.get(PREFIX_LEN + 8..).unwrap_or_default());
        b.freeze()
    }

    /// Field `field` of hash `uid`'s entry in the field-expiry index, for
    /// instant `at_ms`.
    #[must_use]
    pub fn fexp(&self, uid: u64, at_ms: i64, field: &[u8]) -> Bytes {
        let mut b = BytesMut::with_capacity(37 + field.len());
        self.put_prefix_at(&mut b, KIND_ELEM, TTL_INDEX);
        b.put_u64(uid);
        b.put_u64(u64::try_from(at_ms.max(0)).unwrap_or(0));
        put_str(&mut b, field);
        b.freeze()
    }

    /// Hash `uid`'s field-expiry entries: `[start, end)`; with `upto`, only
    /// those due at that instant.
    #[must_use]
    pub fn fexp_range(&self, uid: u64, upto: Option<i64>) -> (Vec<u8>, Vec<u8>) {
        let mut lo = BytesMut::with_capacity(27);
        self.put_prefix_at(&mut lo, KIND_ELEM, TTL_INDEX);
        lo.put_u64(uid);
        let mut hi = lo.clone();
        if let Some(t) = upto {
            hi.put_u64(u64::try_from(t.max(0)).unwrap_or(0).saturating_add(1));
        } else {
            hi.truncate(hi.len() - 8);
            if let Some(n) = uid.checked_add(1) {
                hi.put_u64(n);
            } else {
                hi.clear();
                self.put_prefix_at(&mut hi, KIND_ELEM, TTL_INDEX + 1);
            }
        }
        (lo.to_vec(), hi.to_vec())
    }

    /// The instant and field of field-expiry entry `stored`.
    #[must_use]
    pub fn parse_fexp(stored: &[u8]) -> Option<(i64, Vec<u8>)> {
        let rest = stored.get(PREFIX_LEN + 8..)?;
        let (at, f) = rest.split_at_checked(8)?;
        let at = i64::try_from(u64::from_be_bytes(at.try_into().ok()?)).ok()?;
        let (f, tail) = get_str(f)?;
        tail.is_empty().then_some((at, f))
    }

    /// `key`'s entry in the hash-due index for instant `at_ms`.
    #[must_use]
    pub fn hdue(&self, at_ms: i64, key: &[u8]) -> Bytes {
        let mut b = BytesMut::with_capacity(29 + key.len());
        self.put_prefix_at(&mut b, KIND_EXPIRY, TTL_INDEX);
        b.put_u64(u64::try_from(at_ms.max(0)).unwrap_or(0));
        put_str(&mut b, key);
        b.freeze()
    }

    /// The hash-due entries due at `now_ms`: `[start, end)`.
    #[must_use]
    pub fn hdue_due(&self, now_ms: i64) -> (Vec<u8>, Vec<u8>) {
        let mut lo = BytesMut::with_capacity(27);
        self.put_prefix_at(&mut lo, KIND_EXPIRY, TTL_INDEX);
        let mut hi = lo.clone();
        hi.put_u64(u64::try_from(now_ms.max(0)).unwrap_or(0).saturating_add(1));
        (lo.to_vec(), hi.to_vec())
    }

    /// The instant and key of hash-due entry `stored`.
    #[must_use]
    pub fn parse_hdue(&self, stored: &[u8]) -> Option<(i64, Vec<u8>)> {
        let mut p = BytesMut::with_capacity(19);
        self.put_prefix_at(&mut p, KIND_EXPIRY, TTL_INDEX);
        let rest = stored.strip_prefix(&p[..])?;
        let (at, key) = rest.split_at_checked(8)?;
        let at = i64::try_from(u64::from_be_bytes(at.try_into().ok()?)).ok()?;
        let (key, tail) = get_str(key)?;
        tail.is_empty().then_some((at, key))
    }

    /// Entry `stored` (of a collection's index `index` entries under `kind`)
    /// moved to collection `uid` of this database.
    #[must_use]
    pub fn rekey_at(&self, kind: u8, index: u16, uid: u64, stored: &[u8]) -> Bytes {
        let mut b = BytesMut::with_capacity(stored.len());
        self.put_prefix_at(&mut b, kind, index);
        b.put_u64(uid);
        b.put_slice(stored.get(PREFIX_LEN + 8..).unwrap_or_default());
        b.freeze()
    }

    /// `key`'s expiry entry for instant `at_ms`.
    #[must_use]
    pub fn expiry(&self, at_ms: i64, key: &[u8]) -> Bytes {
        let mut b = BytesMut::with_capacity(29 + key.len());
        self.put_prefix(&mut b, KIND_EXPIRY);
        b.put_u64(u64::try_from(at_ms.max(0)).unwrap_or(0));
        put_str(&mut b, key);
        b.freeze()
    }

    /// The expiry entries due at `now_ms`: `[start, end)`.
    #[must_use]
    pub fn expiry_due(&self, now_ms: i64) -> (Vec<u8>, Vec<u8>) {
        let start = self.prefix(KIND_EXPIRY);
        let mut end = start.clone();
        end.extend_from_slice(
            &u64::try_from(now_ms.max(0))
                .unwrap_or(0)
                .saturating_add(1)
                .to_be_bytes(),
        );
        (start, end)
    }

    /// The instant and user key of expiry entry `stored`.
    #[must_use]
    pub fn parse_expiry(&self, stored: &[u8]) -> Option<(i64, Vec<u8>)> {
        let p = self.prefix(KIND_EXPIRY);
        let rest = stored.strip_prefix(&p[..])?;
        let (at, key) = rest.split_at_checked(8)?;
        let at = u64::from_be_bytes(at.try_into().ok()?);
        let (key, rest) = get_str(key)?;
        rest.is_empty().then_some((i64::try_from(at).ok()?, key))
    }
}

/// A key's routing hash: `xxh3_64` of its hash tag, as the key-value
/// tables' `_sprig_rk`.
#[must_use]
pub fn routing_hash(key: &[u8]) -> u64 {
    sprig_kv::layout::routing_hash_of(sprig_kv::layout::hash_tag(key))
}

/// A hash field's stored value: the flags byte, then the value.
#[must_use]
pub fn field_value(v: &[u8]) -> Bytes {
    let mut b = BytesMut::with_capacity(1 + v.len());
    b.put_u8(FIELD_PLAIN);
    b.put_slice(v);
    b.freeze()
}

/// A hash field's stored value with an expiry instant.
#[must_use]
pub fn field_value_ttl(v: &[u8], at_ms: i64) -> Bytes {
    let mut b = BytesMut::with_capacity(9 + v.len());
    b.put_u8(FIELD_TTL);
    b.put_i64(at_ms);
    b.put_slice(v);
    b.freeze()
}

/// The expiry and value of a stored hash field; `None` if it is
/// malformed.
#[must_use]
pub fn field_parts(stored: &Bytes) -> Option<(Option<i64>, Bytes)> {
    match *stored.first()? {
        FIELD_PLAIN => Some((None, stored.slice(1..))),
        FIELD_TTL => {
            let at = i64::from_be_bytes(stored.get(1..9)?.try_into().ok()?);
            Some((Some(at), stored.slice(9..)))
        }
        _ => None,
    }
}

/// The value of a stored hash field; `None` if it is malformed.
#[must_use]
pub fn field_value_of(stored: &Bytes) -> Option<Bytes> {
    field_parts(stored).map(|(_, v)| v)
}

/// Appends `s` as a memcomparable byte string: `00` escaped as `00 FF`,
/// terminated by `00 00`.
pub fn put_str(b: &mut BytesMut, s: &[u8]) {
    let mut rest = s;
    while let Some(i) = rest.iter().position(|&c| c == 0) {
        b.put_slice(&rest[..i]);
        b.put_slice(&[0, 0xff]);
        rest = &rest[i + 1..];
    }
    b.put_slice(rest);
    b.put_slice(&[0, 0]);
}

/// Reads a string [`put_str`] wrote: it and what follows.
#[must_use]
pub fn get_str(input: &[u8]) -> Option<(Vec<u8>, &[u8])> {
    let mut out = Vec::with_capacity(input.len());
    let mut at = 0;
    loop {
        let i = at + input.get(at..)?.iter().position(|&c| c == 0)?;
        out.extend_from_slice(&input[at..i]);
        match *input.get(i + 1)? {
            0 => return Some((out, &input[i + 2..])),
            0xff => {
                out.push(0);
                at = i + 2;
            }
            _ => return None,
        }
    }
}

/// The first key after `k` and after every key that starts with `k`: where
/// the next page of a scan that ended at `k` starts. (The engine stores a
/// key's versions as the key followed by a timestamp, so `k` followed by a
/// zero byte would still find `k` itself.) No stored key of this layout is
/// a proper prefix of another, so this skips nothing else.
#[must_use]
pub fn after(k: &[u8]) -> Vec<u8> {
    let mut v = k.to_vec();
    while let Some(last) = v.pop() {
        if last < 0xff {
            v.push(last + 1);
            return v;
        }
    }
    // Every byte 0xff: nothing sorts after it with this prefix length;
    // an empty range follows.
    let mut v = k.to_vec();
    v.push(0xff);
    v
}

/// A score as sortable bits: `-0` as `0`; NaN is never stored.
#[must_use]
pub fn score_bits(score: f64) -> u64 {
    let b = if score == 0.0 { 0 } else { score.to_bits() };
    if b >> 63 == 0 { b | (1 << 63) } else { !b }
}

/// The score [`score_bits`] encoded.
#[must_use]
pub fn score_of_bits(b: u64) -> f64 {
    f64::from_bits(if b >> 63 == 1 { b & !(1 << 63) } else { !b })
}

/// A list position as sortable bytes.
#[must_use]
pub fn pos_bytes(pos: i64) -> [u8; 8] {
    (pos.cast_unsigned() ^ (1 << 63)).to_be_bytes()
}

/// A key's type, as `TYPE` names it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Type {
    /// A string.
    String,
    /// A hash.
    Hash,
    /// A list.
    List,
    /// A set.
    Set,
    /// A sorted set.
    ZSet,
}

impl Type {
    /// The name `TYPE` answers.
    #[must_use]
    pub const fn name(self) -> &'static str {
        match self {
            Self::String => "string",
            Self::Hash => "hash",
            Self::List => "list",
            Self::Set => "set",
            Self::ZSet => "zset",
        }
    }
}

/// A set's members.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SetBody {
    /// In the metadata: integers only, in numeric order (`ints`), or any
    /// members in insertion order.
    Inline {
        /// Every member is an integer, and the set has never held another.
        ints: bool,
        /// The members.
        members: Vec<Bytes>,
    },
    /// As element entries.
    Split {
        /// The collection's uid.
        uid: u64,
        /// Member count.
        count: u64,
    },
}

/// A sorted set's members.
#[derive(Debug, Clone, PartialEq)]
pub enum ZBody {
    /// In the metadata, ordered by (score, member).
    Inline(Vec<(f64, Bytes)>),
    /// As member and score-index entries.
    Split {
        /// The collection's uid.
        uid: u64,
        /// Member count.
        count: u64,
    },
}

impl Eq for ZBody {}

/// A hash's fields.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum HashBody {
    /// In the metadata, in insertion order.
    Inline(Vec<(Bytes, Bytes)>),
    /// As element entries.
    Split {
        /// The collection's uid.
        uid: u64,
        /// Field count (fields whose expiry passed included, until they
        /// are removed).
        count: u64,
        /// Some field may have an expiry (and an entry in the field-expiry
        /// index).
        ttl: bool,
    },
}

/// A list's items.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ListBody {
    /// In the metadata, head first.
    Inline(std::collections::VecDeque<Bytes>),
    /// As element entries at positions `head..tail`.
    Split {
        /// The collection's uid.
        uid: u64,
        /// Position of the first item.
        head: i64,
        /// Position after the last item.
        tail: i64,
    },
}

/// A key's value.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Body {
    /// A string.
    Str(Bytes),
    /// A hash.
    Hash(HashBody),
    /// A list.
    List(ListBody),
    /// A set.
    Set(SetBody),
    /// A sorted set.
    ZSet(ZBody),
    /// A string held in chunks of [`CHUNK`] bytes (a missing chunk reads
    /// as zeros).
    Chunked {
        /// The string's uid.
        uid: u64,
        /// Its length in bytes.
        len: u64,
    },
}

/// A key's metadata entry.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Meta {
    /// When the key expires, in ms since the epoch.
    pub exp: Option<i64>,
    /// Its value.
    pub body: Body,
}

const TAG_STR: u8 = 1;
const TAG_HASH_INLINE: u8 = 2;
const TAG_HASH_SPLIT: u8 = 3;
const TAG_LIST_INLINE: u8 = 4;
const TAG_LIST_SPLIT: u8 = 5;
const TAG_SET_INTS: u8 = 6;
const TAG_SET_INLINE: u8 = 7;
const TAG_SET_SPLIT: u8 = 8;
const TAG_ZSET_INLINE: u8 = 9;
const TAG_ZSET_SPLIT: u8 = 10;
const TAG_HASH_SPLIT_TTL: u8 = 11;
const TAG_STR_CHUNKED: u8 = 12;
const FLAG_EXP: u8 = 1;

fn put_varint(b: &mut BytesMut, mut v: u64) {
    while v >= 0x80 {
        b.put_u8(u8::try_from(v & 0x7f).unwrap_or(0) | 0x80);
        v >>= 7;
    }
    b.put_u8(u8::try_from(v).unwrap_or(0));
}

fn get_varint(b: &[u8], at: &mut usize) -> Option<u64> {
    let mut v = 0u64;
    let mut shift = 0;
    loop {
        let byte = *b.get(*at)?;
        *at += 1;
        v |= u64::from(byte & 0x7f) << shift;
        if byte & 0x80 == 0 {
            return Some(v);
        }
        shift += 7;
        if shift > 63 {
            return None;
        }
    }
}

fn put_blob(b: &mut BytesMut, s: &[u8]) {
    put_varint(b, s.len() as u64);
    b.put_slice(s);
}

fn get_blob(src: &Bytes, at: &mut usize) -> Option<Bytes> {
    let n = usize::try_from(get_varint(src, at)?).ok()?;
    let end = at.checked_add(n)?;
    if end > src.len() {
        return None;
    }
    let v = src.slice(*at..end);
    *at = end;
    Some(v)
}

fn get_u64(b: &[u8], at: &mut usize) -> Option<u64> {
    let v = u64::from_be_bytes(b.get(*at..*at + 8)?.try_into().ok()?);
    *at += 8;
    Some(v)
}

impl Meta {
    /// The key's type.
    #[must_use]
    pub const fn kind(&self) -> Type {
        match self.body {
            Body::Str(_) | Body::Chunked { .. } => Type::String,
            Body::Hash(_) => Type::Hash,
            Body::List(_) => Type::List,
            Body::Set(_) => Type::Set,
            Body::ZSet(_) => Type::ZSet,
        }
    }

    /// Whether the key has expired at `now_ms`.
    #[must_use]
    pub fn expired(&self, now_ms: i64) -> bool {
        self.exp.is_some_and(|e| e <= now_ms)
    }

    /// The collection's uid, for a split collection.
    #[must_use]
    pub const fn uid(&self) -> Option<u64> {
        match self.body {
            Body::Hash(HashBody::Split { uid, .. })
            | Body::List(ListBody::Split { uid, .. })
            | Body::Set(SetBody::Split { uid, .. })
            | Body::ZSet(ZBody::Split { uid, .. })
            | Body::Chunked { uid, .. } => Some(uid),
            _ => None,
        }
    }

    /// The entry's bytes.
    #[must_use]
    pub fn encode(&self) -> Bytes {
        let mut b = BytesMut::with_capacity(
            16 + match &self.body {
                Body::Str(s) => s.len(),
                _ => 64,
            },
        );
        let tag = match &self.body {
            Body::Str(_) => TAG_STR,
            Body::Chunked { .. } => TAG_STR_CHUNKED,
            Body::Hash(HashBody::Inline(_)) => TAG_HASH_INLINE,
            Body::Hash(HashBody::Split { ttl: false, .. }) => TAG_HASH_SPLIT,
            Body::Hash(HashBody::Split { ttl: true, .. }) => TAG_HASH_SPLIT_TTL,
            Body::List(ListBody::Inline(_)) => TAG_LIST_INLINE,
            Body::List(ListBody::Split { .. }) => TAG_LIST_SPLIT,
            Body::Set(SetBody::Inline { ints: true, .. }) => TAG_SET_INTS,
            Body::Set(SetBody::Inline { ints: false, .. }) => TAG_SET_INLINE,
            Body::Set(SetBody::Split { .. }) => TAG_SET_SPLIT,
            Body::ZSet(ZBody::Inline(_)) => TAG_ZSET_INLINE,
            Body::ZSet(ZBody::Split { .. }) => TAG_ZSET_SPLIT,
        };
        b.put_u8(tag);
        b.put_u8(if self.exp.is_some() { FLAG_EXP } else { 0 });
        if let Some(e) = self.exp {
            b.put_i64(e);
        }
        match &self.body {
            Body::Str(s) => b.put_slice(s),
            Body::Chunked { uid, len } => {
                b.put_u64(*uid);
                b.put_u64(*len);
            }
            Body::Hash(HashBody::Inline(fields)) => {
                put_varint(&mut b, fields.len() as u64);
                for (f, v) in fields {
                    put_blob(&mut b, f);
                    put_blob(&mut b, v);
                }
            }
            Body::Hash(HashBody::Split { uid, count, .. })
            | Body::Set(SetBody::Split { uid, count })
            | Body::ZSet(ZBody::Split { uid, count }) => {
                b.put_u64(*uid);
                b.put_u64(*count);
            }
            Body::List(ListBody::Inline(items)) => {
                put_varint(&mut b, items.len() as u64);
                for i in items {
                    put_blob(&mut b, i);
                }
            }
            Body::List(ListBody::Split { uid, head, tail }) => {
                b.put_u64(*uid);
                b.put_i64(*head);
                b.put_i64(*tail);
            }
            Body::Set(SetBody::Inline { members, .. }) => {
                put_varint(&mut b, members.len() as u64);
                for m in members {
                    put_blob(&mut b, m);
                }
            }
            Body::ZSet(ZBody::Inline(items)) => {
                put_varint(&mut b, items.len() as u64);
                for (s, m) in items {
                    b.put_f64(*s);
                    put_blob(&mut b, m);
                }
            }
        }
        b.freeze()
    }

    /// Decodes an entry; `None` if it is malformed.
    #[must_use]
    pub fn decode(src: &Bytes) -> Option<Self> {
        let tag = *src.first()?;
        let flags = *src.get(1)?;
        let mut at = 2;
        let exp = if flags & FLAG_EXP != 0 {
            Some(get_u64(src, &mut at)?.cast_signed())
        } else {
            None
        };
        let body = match tag {
            TAG_STR => Body::Str(src.slice(at..)),
            TAG_STR_CHUNKED => Body::Chunked {
                uid: get_u64(src, &mut at)?,
                len: get_u64(src, &mut at)?,
            },
            TAG_HASH_INLINE => {
                let n = get_varint(src, &mut at)?;
                let mut fields = Vec::with_capacity(usize::try_from(n).ok()?.min(1024));
                for _ in 0..n {
                    let f = get_blob(src, &mut at)?;
                    let v = get_blob(src, &mut at)?;
                    fields.push((f, v));
                }
                Body::Hash(HashBody::Inline(fields))
            }
            TAG_HASH_SPLIT | TAG_HASH_SPLIT_TTL => Body::Hash(HashBody::Split {
                uid: get_u64(src, &mut at)?,
                count: get_u64(src, &mut at)?,
                ttl: tag == TAG_HASH_SPLIT_TTL,
            }),
            TAG_LIST_INLINE => {
                let n = get_varint(src, &mut at)?;
                let mut items =
                    std::collections::VecDeque::with_capacity(usize::try_from(n).ok()?.min(1024));
                for _ in 0..n {
                    items.push_back(get_blob(src, &mut at)?);
                }
                Body::List(ListBody::Inline(items))
            }
            TAG_LIST_SPLIT => Body::List(ListBody::Split {
                uid: get_u64(src, &mut at)?,
                head: get_u64(src, &mut at)?.cast_signed(),
                tail: get_u64(src, &mut at)?.cast_signed(),
            }),
            TAG_SET_INTS | TAG_SET_INLINE => {
                let n = get_varint(src, &mut at)?;
                let mut members = Vec::with_capacity(usize::try_from(n).ok()?.min(1024));
                for _ in 0..n {
                    members.push(get_blob(src, &mut at)?);
                }
                Body::Set(SetBody::Inline {
                    ints: tag == TAG_SET_INTS,
                    members,
                })
            }
            TAG_SET_SPLIT => Body::Set(SetBody::Split {
                uid: get_u64(src, &mut at)?,
                count: get_u64(src, &mut at)?,
            }),
            TAG_ZSET_INLINE => {
                let n = get_varint(src, &mut at)?;
                let mut items = Vec::with_capacity(usize::try_from(n).ok()?.min(1024));
                for _ in 0..n {
                    let s = f64::from_bits(get_u64(src, &mut at)?);
                    items.push((s, get_blob(src, &mut at)?));
                }
                Body::ZSet(ZBody::Inline(items))
            }
            TAG_ZSET_SPLIT => Body::ZSet(ZBody::Split {
                uid: get_u64(src, &mut at)?,
                count: get_u64(src, &mut at)?,
            }),
            _ => return None,
        };
        Some(Self { exp, body })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;

    fn arb_meta() -> impl Strategy<Value = Meta> {
        let blob = proptest::collection::vec(any::<u8>(), 0..80).prop_map(Bytes::from);
        let body = prop_oneof![
            blob.clone().prop_map(Body::Str),
            (any::<u64>(), any::<u64>()).prop_map(|(uid, len)| Body::Chunked { uid, len }),
            proptest::collection::vec((blob.clone(), blob.clone()), 0..20)
                .prop_map(|f| Body::Hash(HashBody::Inline(f))),
            (any::<u64>(), any::<u64>()).prop_map(|(uid, count)| Body::Hash(HashBody::Split {
                uid,
                count,
                ttl: uid % 2 == 0
            })),
            proptest::collection::vec(blob.clone(), 0..20)
                .prop_map(|i| Body::List(ListBody::Inline(i.into()))),
            (any::<u64>(), any::<i64>(), any::<i64>())
                .prop_map(|(uid, head, tail)| Body::List(ListBody::Split { uid, head, tail })),
            (
                any::<bool>(),
                proptest::collection::vec(blob.clone(), 0..20)
            )
                .prop_map(|(ints, members)| Body::Set(SetBody::Inline { ints, members })),
            proptest::collection::vec((-1e9f64..1e9, blob), 0..20)
                .prop_map(|z| Body::ZSet(ZBody::Inline(z))),
            (any::<u64>(), any::<u64>())
                .prop_map(|(uid, count)| Body::ZSet(ZBody::Split { uid, count })),
        ];
        (proptest::option::of(any::<i64>()), body).prop_map(|(exp, body)| Meta { exp, body })
    }

    proptest! {
        #[test]
        fn meta_round_trips(m in arb_meta()) {
            prop_assert_eq!(Meta::decode(&m.encode()), Some(m));
        }

        #[test]
        fn strings_round_trip_and_sort(a in proptest::collection::vec(any::<u8>(), 0..12),
                                       b in proptest::collection::vec(any::<u8>(), 0..12)) {
            let (mut x, mut y) = (BytesMut::new(), BytesMut::new());
            put_str(&mut x, &a);
            put_str(&mut y, &b);
            prop_assert_eq!(a.cmp(&b), x.cmp(&y));
            x.put_u8(7);
            prop_assert_eq!(get_str(&x), Some((a, &[7u8][..])));
        }

        #[test]
        fn scores_sort_numerically(a in any::<f64>(), b in any::<f64>()) {
            prop_assume!(!a.is_nan() && !b.is_nan());
            let (x, y) = (if a == 0.0 { 0.0 } else { a }, if b == 0.0 { 0.0 } else { b });
            prop_assert_eq!(x.partial_cmp(&y), Some(score_bits(a).cmp(&score_bits(b))));
            prop_assert_eq!(score_of_bits(score_bits(x)).to_bits(), x.to_bits());
        }

        #[test]
        fn positions_sort_numerically(a in any::<i64>(), b in any::<i64>()) {
            prop_assert_eq!(a.cmp(&b), pos_bytes(a).cmp(&pos_bytes(b)));
        }
    }

    #[test]
    fn kinds_and_databases_do_not_overlap() {
        let s = Space::redis(7, 0);
        let t = Space::redis(7, 1);
        let (si, si_end) = s.scan_index_range();
        assert!(s.meta(b"\xff\xff").as_ref() < si.as_slice());
        assert!(
            si.as_slice() < &s.scan_entry(b"k")[..] && &s.scan_entry(b"k")[..] < si_end.as_slice()
        );
        assert!(si_end <= s.prefix_end(KIND_META));
        assert!(s.uid_counter().as_ref() < s.prefix(KIND_ELEM).as_slice());
        assert!(s.prefix_end(KIND_ELEM) <= s.prefix(KIND_META));
        // A collection's elements sit inside its uid's range, and the
        // routing hash follows the hash tag.
        let (lo, hi) = s.elems_range(5);
        for k in [
            s.field(5, b""),
            s.field(5, b"\xff\xff"),
            s.item(5, i64::MIN),
            s.item(5, i64::MAX),
        ] {
            assert!(lo.as_slice() <= &k[..] && &k[..] < hi.as_slice());
        }
        assert!(&s.field(4, b"\xff")[..] < lo.as_slice() && hi.as_slice() <= &s.field(6, b"")[..]);
        assert_eq!(routing_hash(b"{u1}.a"), routing_hash(b"u1"));
        assert_eq!(
            field_value_of(&field_value(b"v")),
            Some(Bytes::from_static(b"v"))
        );
        assert!(s.prefix_end(KIND_META) <= s.prefix(KIND_EXPIRY));
        assert!(s.prefix_end(KIND_EXPIRY) <= t.prefix(KIND_META));
        assert_eq!(s.meta_user_key(&s.meta(b"k\0x")), Some(b"k\0x".to_vec()));
        let e = s.expiry(1234, b"key");
        assert_eq!(s.parse_expiry(&e), Some((1234, b"key".to_vec())));
        // No key's entries are a prefix of another's.
        assert!(!s.meta(b"ab").starts_with(&s.meta(b"a")));
        assert!(s.meta(b"a") < s.meta(b"a\0") && s.meta(b"a\0") < s.meta(b"ab"));
        let (lo, hi) = s.expiry_due(1234);
        assert!(lo.as_slice() <= &e[..] && &e[..] < hi.as_slice());
        let (_, hi) = s.expiry_due(1233);
        assert!(&e[..] >= hi.as_slice());
    }
}
