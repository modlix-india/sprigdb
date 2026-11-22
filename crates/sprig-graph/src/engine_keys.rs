//! Where a native graph's elements live in the storage engine's keyspace,
//! and how their values are encoded ([`crate::engine`]).
//!
//! ```text
//! node         d | P | 0 | node u64                              -> labelset u32 | record
//! label scan   d | L | 0 | labelset u32 | node u64               -> (empty)
//! out-adjacency d | A | 0 | src u64 | type u32 | sort | dst u64 | edge u64
//!                                       -> dst labelset u32 | flag u8 | [record]
//! in-adjacency d | A | 1 | dst u64 | type u32 | sort | src u64 | edge u64
//!                                       -> src labelset u32
//! edge record  d | P | 1 | edge u64      -> src u64 | type u32 | dst u64 | sort len u8 | sort | record
//! type scan    d | L | 1 | type u32 | edge u64                   -> src u64 | dst u64
//! index entry  d | S | n | key | element u64                     -> (empty)
//! unique entry g | U | n | key                                   -> element u64
//! id counters  g | C | 0 | kind u8                               -> high-water mark u64
//! names        g | M | 1 | kind u8 | str(name)                   -> id u32
//!              g | M | 2 | kind u8 | id u32                      -> name
//! counters     g | M | 0 | kind u8                               -> last id given u32
//! indexes      g | M | 3 | id u32                                -> the definition
//! schema lock  g | M | 9                                         (locked, never written)
//! ```
//!
//! `g` is the graph's own prefix (shard 0); `d` is a data shard's
//! (shard `1 + i`, `i` the shard owning the element's routing hash). Out-
//! adjacency, the edge record and the type-scan entry live with the source,
//! in-adjacency with the destination. `sort` is the sort-key slot: `00` (no
//! sort key) today, `01` and a memcomparable value once a type declares
//! one, so declaring one later rewrites keys without changing the format.
//! An index `key` is the memcomparable byte strings of the indexed values'
//! typed texts (`schema::prop_index_value`), in the index's order.
//!
//! **The property record** is a list of `(key id varint, length varint,
//! payload)` sorted by key id, so a read of a few keys skips the others
//! without decoding them. A payload is the value as compact binary JSON,
//! then its type tags (the `_types` entries under that key: temporal
//! values, byte arrays, integral floats) as `(path suffix, tag)` strings.
//! A record decodes back to exactly the `props` and `_types` it was made
//! from.
//!
//! All integers in keys are big-endian, so ids sort numerically.

use serde_json::{Map, Number, Value as Json};
use sprig_common::{Result, SprigError};
use sprig_engine::keys::{KeyPrefix, kind, memcmp};

/// The node family (`P | 0`) and edge records (`P | 1`).
pub(crate) const NODE: u16 = 0;
/// Edge records.
pub(crate) const EDGE: u16 = 1;
/// Out-adjacency (`A | 0`).
pub(crate) const OUT: u16 = 0;
/// In-adjacency (`A | 1`).
pub(crate) const IN: u16 = 1;
/// The label-scan family (`L | 0`).
pub(crate) const LABEL_SCAN: u16 = 0;
/// The type-scan family (`L | 1`).
pub(crate) const TYPE_SCAN: u16 = 1;
/// `M | 0`: the dictionary's counters.
pub(crate) const DICT_COUNTER: u16 = 0;
/// `M | 1`: name to id.
pub(crate) const DICT_BY_NAME: u16 = 1;
/// `M | 2`: id to name.
pub(crate) const DICT_BY_ID: u16 = 2;
/// `M | 3`: index and constraint definitions.
pub(crate) const INDEX_DEFS: u16 = 3;
/// `M | 9`: the schema lock (no value is ever written).
pub(crate) const SCHEMA_LOCK: u16 = 9;

/// The sort-key slot of a type without a sort key.
pub(crate) const NO_SORT: u8 = 0;

/// The engine object ids graphs take: `GRAPH_OBJECT_BASE + graph id`.
pub const GRAPH_OBJECT_BASE: u32 = 0x4752_0000;
/// Graph ids above this one don't fit the graph object range.
pub const MAX_GRAPH_ID: u64 = 0xFFFF;

/// The bytes after which an edge's properties stay out of its out-
/// adjacency entry (they are in its record).
pub(crate) const INLINE_MAX: usize = 256;

/// A graph's place in the keyspace.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct GraphSpace {
    /// The tenant.
    pub tenant: u64,
    /// The engine object.
    pub object: u32,
}

impl GraphSpace {
    /// The prefix of `kind | index` on keyspace shard `shard`.
    #[must_use]
    pub(crate) fn prefix(&self, shard: u32, kind: u8, index: u16) -> Vec<u8> {
        let mut v = Vec::with_capacity(64);
        KeyPrefix {
            tenant: self.tenant,
            shard,
            object: self.object,
            kind,
            index,
        }
        .encode_into(&mut v);
        v
    }

    /// The node key of `id` on data shard `d`.
    #[must_use]
    pub(crate) fn node(&self, d: u32, id: u64) -> Vec<u8> {
        let mut k = self.prefix(d, kind::PRIMARY, NODE);
        k.extend_from_slice(&id.to_be_bytes());
        k
    }

    /// The edge record key.
    #[must_use]
    pub(crate) fn edge(&self, d: u32, id: u64) -> Vec<u8> {
        let mut k = self.prefix(d, kind::PRIMARY, EDGE);
        k.extend_from_slice(&id.to_be_bytes());
        k
    }

    /// The label-scan entry.
    #[must_use]
    pub(crate) fn label_entry(&self, d: u32, labelset: u32, id: u64) -> Vec<u8> {
        let mut k = self.label_prefix(d, labelset);
        k.extend_from_slice(&id.to_be_bytes());
        k
    }

    /// The label-scan prefix of a label set.
    #[must_use]
    pub(crate) fn label_prefix(&self, d: u32, labelset: u32) -> Vec<u8> {
        let mut k = self.prefix(d, kind::LABELS, LABEL_SCAN);
        k.extend_from_slice(&labelset.to_be_bytes());
        k
    }

    /// The type-scan entry.
    #[must_use]
    pub(crate) fn type_entry(&self, d: u32, etype: u32, id: u64) -> Vec<u8> {
        let mut k = self.prefix(d, kind::LABELS, TYPE_SCAN);
        k.extend_from_slice(&etype.to_be_bytes());
        k.extend_from_slice(&id.to_be_bytes());
        k
    }

    /// An adjacency key: `dir` is [`OUT`] (near = source) or [`IN`].
    #[must_use]
    pub(crate) fn adj(
        &self,
        d: u32,
        dir: u16,
        near: u64,
        etype: u32,
        far: u64,
        edge: u64,
    ) -> Vec<u8> {
        let mut k = self.adj_prefix(d, dir, near, Some(etype));
        k.push(NO_SORT);
        k.extend_from_slice(&far.to_be_bytes());
        k.extend_from_slice(&edge.to_be_bytes());
        k
    }

    /// The adjacency prefix of `near`, of one type or all.
    #[must_use]
    pub(crate) fn adj_prefix(&self, d: u32, dir: u16, near: u64, etype: Option<u32>) -> Vec<u8> {
        let mut k = self.prefix(d, kind::ADJACENCY, dir);
        k.extend_from_slice(&near.to_be_bytes());
        if let Some(t) = etype {
            k.extend_from_slice(&t.to_be_bytes());
        }
        k
    }

    /// An index entry's prefix for `key` (the encoded values).
    #[must_use]
    pub(crate) fn index_prefix(&self, d: u32, index: u16, key: &[u8]) -> Vec<u8> {
        let mut k = self.prefix(d, kind::SECONDARY, index);
        k.extend_from_slice(key);
        k
    }

    /// A unique entry.
    #[must_use]
    pub(crate) fn unique(&self, index: u16, key: &[u8]) -> Vec<u8> {
        let mut k = self.prefix(0, kind::UNIQUE, index);
        k.extend_from_slice(key);
        k
    }

    /// An id counter.
    #[must_use]
    pub(crate) fn id_counter(&self, which: u8) -> Vec<u8> {
        let mut k = self.prefix(0, kind::COUNTERS, 0);
        k.push(which);
        k
    }

    /// A dictionary entry by name.
    #[must_use]
    pub(crate) fn dict_name(&self, dk: u8, name: &[u8]) -> Vec<u8> {
        let mut k = self.prefix(0, kind::META, DICT_BY_NAME);
        k.push(dk);
        memcmp::put_bytes(&mut k, name);
        k
    }

    /// A dictionary entry by id.
    #[must_use]
    pub(crate) fn dict_id(&self, dk: u8, id: u32) -> Vec<u8> {
        let mut k = self.prefix(0, kind::META, DICT_BY_ID);
        k.push(dk);
        k.extend_from_slice(&id.to_be_bytes());
        k
    }

    /// A dictionary kind's counter.
    #[must_use]
    pub(crate) fn dict_counter(&self, dk: u8) -> Vec<u8> {
        let mut k = self.prefix(0, kind::META, DICT_COUNTER);
        k.push(dk);
        k
    }

    /// An index definition.
    #[must_use]
    pub(crate) fn index_def(&self, id: u32) -> Vec<u8> {
        let mut k = self.prefix(0, kind::META, INDEX_DEFS);
        k.extend_from_slice(&id.to_be_bytes());
        k
    }
}

/// The end of a prefix range (`None`: the prefix is all `FF`).
#[must_use]
pub(crate) fn end_of(prefix: &[u8]) -> Vec<u8> {
    sprig_engine::keys::prefix_end(prefix).unwrap_or_else(|| vec![0xFF; prefix.len() + 1])
}

fn corrupt(what: &str) -> SprigError {
    SprigError::internal(format!("graph storage: a damaged {what}"))
}

/// Reads a big-endian `u64` at `at`.
pub(crate) fn be64(b: &[u8], at: usize) -> Result<u64> {
    b.get(at..at + 8)
        .and_then(|s| s.try_into().ok())
        .map(u64::from_be_bytes)
        .ok_or_else(|| corrupt("key"))
}

/// Reads a big-endian `u32` at `at`.
pub(crate) fn be32(b: &[u8], at: usize) -> Result<u32> {
    b.get(at..at + 4)
        .and_then(|s| s.try_into().ok())
        .map(u32::from_be_bytes)
        .ok_or_else(|| corrupt("key"))
}

/// An adjacency key's parts after its prefix (19 bytes + 8 near): type,
/// far node, edge id.
pub(crate) fn adj_parts(key: &[u8]) -> Result<(u64, u32, u64, u64)> {
    let p = sprig_engine::keys::PREFIX_LEN;
    let near = be64(key, p)?;
    let etype = be32(key, p + 8)?;
    // The sort slot: `00`, or `01` and a value whose end the far node
    // and edge ids (the last 16 bytes) mark.
    let n = key.len();
    if n < p + 13 + 16 {
        return Err(corrupt("adjacency key"));
    }
    let far = be64(key, n - 16)?;
    let edge = be64(key, n - 8)?;
    Ok((near, etype, far, edge))
}

// ---------------------------------------------------------------------
// Varints and the property record.
// ---------------------------------------------------------------------

pub(crate) fn put_varint(out: &mut Vec<u8>, mut v: u64) {
    while v >= 0x80 {
        // Seven bits at a time: the truncation is the point.
        #[allow(clippy::cast_possible_truncation)]
        out.push((v as u8) | 0x80);
        v >>= 7;
    }
    #[allow(clippy::cast_possible_truncation)]
    out.push(v as u8);
}

pub(crate) fn get_varint(b: &[u8], at: &mut usize) -> Result<u64> {
    let mut v: u64 = 0;
    let mut shift = 0;
    loop {
        let c = *b.get(*at).ok_or_else(|| corrupt("record"))?;
        *at += 1;
        if shift > 63 {
            return Err(corrupt("record"));
        }
        v |= u64::from(c & 0x7F) << shift;
        if c < 0x80 {
            return Ok(v);
        }
        shift += 7;
    }
}

fn len_of(b: &[u8], at: &mut usize) -> Result<usize> {
    usize::try_from(get_varint(b, at)?).map_err(|_| corrupt("record"))
}

fn put_str(out: &mut Vec<u8>, s: &str) {
    put_varint(out, s.len() as u64);
    out.extend_from_slice(s.as_bytes());
}

fn get_str<'a>(b: &'a [u8], at: &mut usize) -> Result<&'a str> {
    let n = len_of(b, at)?;
    let s = b.get(*at..*at + n).ok_or_else(|| corrupt("record"))?;
    *at += n;
    std::str::from_utf8(s).map_err(|_| corrupt("record"))
}

const J_NULL: u8 = 0;
const J_FALSE: u8 = 1;
const J_TRUE: u8 = 2;
const J_INT: u8 = 3;
const J_UINT: u8 = 4;
const J_FLOAT: u8 = 5;
const J_STR: u8 = 6;
const J_ARRAY: u8 = 7;
const J_OBJECT: u8 = 8;

/// A JSON value as compact binary.
pub(crate) fn put_json(out: &mut Vec<u8>, j: &Json) {
    match j {
        Json::Null => out.push(J_NULL),
        Json::Bool(false) => out.push(J_FALSE),
        Json::Bool(true) => out.push(J_TRUE),
        Json::Number(n) => {
            if n.is_f64() {
                out.push(J_FLOAT);
                out.extend_from_slice(&n.as_f64().unwrap_or(0.0).to_bits().to_le_bytes());
            } else if let Some(i) = n.as_i64() {
                out.push(J_INT);
                // Zigzag, so small negatives stay short.
                #[allow(clippy::cast_sign_loss)]
                put_varint(out, ((i << 1) ^ (i >> 63)) as u64);
            } else {
                out.push(J_UINT);
                put_varint(out, n.as_u64().unwrap_or(0));
            }
        }
        Json::String(s) => {
            out.push(J_STR);
            put_str(out, s);
        }
        Json::Array(a) => {
            out.push(J_ARRAY);
            put_varint(out, a.len() as u64);
            for x in a {
                put_json(out, x);
            }
        }
        Json::Object(m) => {
            out.push(J_OBJECT);
            put_varint(out, m.len() as u64);
            for (k, x) in m {
                put_str(out, k);
                put_json(out, x);
            }
        }
    }
}

/// Reads a value [`put_json`] wrote.
pub(crate) fn get_json(b: &[u8], at: &mut usize) -> Result<Json> {
    let t = *b.get(*at).ok_or_else(|| corrupt("record"))?;
    *at += 1;
    Ok(match t {
        J_NULL => Json::Null,
        J_FALSE => Json::Bool(false),
        J_TRUE => Json::Bool(true),
        J_INT => {
            let z = get_varint(b, at)?;
            #[allow(clippy::cast_possible_wrap)]
            let i = ((z >> 1) as i64) ^ -((z & 1) as i64);
            Json::from(i)
        }
        J_UINT => Json::from(get_varint(b, at)?),
        J_FLOAT => {
            let s = b.get(*at..*at + 8).ok_or_else(|| corrupt("record"))?;
            *at += 8;
            let f = f64::from_bits(u64::from_le_bytes(
                s.try_into().map_err(|_| corrupt("record"))?,
            ));
            Number::from_f64(f).map_or(Json::Null, Json::Number)
        }
        J_STR => Json::String(get_str(b, at)?.to_owned()),
        J_ARRAY => {
            let n = len_of(b, at)?;
            let mut v = Vec::with_capacity(n.min(1024));
            for _ in 0..n {
                v.push(get_json(b, at)?);
            }
            Json::Array(v)
        }
        J_OBJECT => {
            let n = len_of(b, at)?;
            let mut m = Map::new();
            for _ in 0..n {
                let k = get_str(b, at)?.to_owned();
                let v = get_json(b, at)?;
                m.insert(k, v);
            }
            Json::Object(m)
        }
        _ => return Err(corrupt("record")),
    })
}

/// The `_types` entries of top-level key `k`: `(suffix, tag)` where the
/// full path is `k` + suffix (`""` or `[i]...`).
fn types_under<'a>(types: Option<&'a Map<String, Json>>, k: &str) -> Vec<(&'a str, &'a str)> {
    let Some(t) = types else { return Vec::new() };
    t.iter()
        .filter_map(|(p, tag)| {
            let rest = p.strip_prefix(k)?;
            (rest.is_empty() || rest.starts_with('[') || rest.starts_with('.'))
                .then_some(())
                .and_then(|()| Some((rest, tag.as_str()?)))
        })
        .collect()
}

/// A property record from `props` and `_types`, with key ids from `key_id`
/// (every key of `props` must have one).
///
/// # Errors
/// A key without an id.
pub(crate) fn put_record(
    out: &mut Vec<u8>,
    props: &Json,
    types: Option<&Json>,
    key_id: &dyn Fn(&str) -> Option<u32>,
) -> Result<()> {
    let Some(obj) = props.as_object() else {
        return Ok(());
    };
    let tmap = types.and_then(Json::as_object);
    let mut ents: Vec<(u32, &str, &Json)> = Vec::with_capacity(obj.len());
    for (k, v) in obj {
        if v.is_null() {
            continue;
        }
        let id = key_id(k).ok_or_else(|| {
            SprigError::internal(format!("graph storage: property key {k} has no id"))
        })?;
        ents.push((id, k, v));
    }
    ents.sort_unstable_by_key(|e| e.0);
    let mut payload = Vec::new();
    for (id, k, v) in ents {
        payload.clear();
        put_json(&mut payload, v);
        let ts = types_under(tmap, k);
        put_varint(&mut payload, ts.len() as u64);
        for (suffix, tag) in ts {
            put_str(&mut payload, suffix);
            put_str(&mut payload, tag);
        }
        put_varint(out, u64::from(id));
        put_varint(out, payload.len() as u64);
        out.extend_from_slice(&payload);
    }
    Ok(())
}

/// Decodes a record: `props` and `_types` (`None` when empty), keeping
/// only the keys `want` accepts (by id).
///
/// # Errors
/// A damaged record, or a key id the dictionary lacks.
pub(crate) fn get_record(
    b: &[u8],
    want: &dyn Fn(u32) -> bool,
    key_name: &dyn Fn(u32) -> Option<String>,
) -> Result<(Json, Option<Json>)> {
    let mut at = 0;
    let mut obj = Map::new();
    let mut types = Map::new();
    while at < b.len() {
        let id = u32::try_from(get_varint(b, &mut at)?).map_err(|_| corrupt("record"))?;
        let n = len_of(b, &mut at)?;
        let end = at + n;
        if end > b.len() {
            return Err(corrupt("record"));
        }
        if !want(id) {
            at = end;
            continue;
        }
        let name = key_name(id).ok_or_else(|| {
            SprigError::internal(format!("graph storage: property key id {id} has no name"))
        })?;
        let v = get_json(b, &mut at)?;
        let nt = len_of(b, &mut at)?;
        for _ in 0..nt {
            let suffix = get_str(b, &mut at)?;
            let tag = get_str(b, &mut at)?;
            types.insert(format!("{name}{suffix}"), Json::from(tag));
        }
        if at != end {
            return Err(corrupt("record"));
        }
        obj.insert(name, v);
    }
    let t = if types.is_empty() {
        None
    } else {
        Some(Json::Object(types))
    };
    Ok((Json::Object(obj), t))
}

/// The key ids a record holds.
#[cfg(test)]
pub(crate) fn record_keys(b: &[u8]) -> Result<Vec<u32>> {
    let mut at = 0;
    let mut out = Vec::new();
    while at < b.len() {
        out.push(u32::try_from(get_varint(b, &mut at)?).map_err(|_| corrupt("record"))?);
        let n = len_of(b, &mut at)?;
        at += n;
    }
    if at != b.len() {
        return Err(corrupt("record"));
    }
    Ok(out)
}

// ---------------------------------------------------------------------
// Values of the families.
// ---------------------------------------------------------------------

/// A node value: its label set, then its record.
pub(crate) fn node_value(labelset: u32, record: &[u8]) -> Vec<u8> {
    let mut v = Vec::with_capacity(4 + record.len());
    v.extend_from_slice(&labelset.to_be_bytes());
    v.extend_from_slice(record);
    v
}

/// A node value's label set and record.
pub(crate) fn split_node(v: &[u8]) -> Result<(u32, &[u8])> {
    Ok((be32(v, 0)?, &v[4..]))
}

/// The out-adjacency value's flag: the record isn't inline.
pub(crate) const FLAG_OUTLINE: u8 = 1;

/// An out-adjacency value: the far node's label set, and the properties
/// when they fit.
pub(crate) fn out_value(far_labelset: u32, record: &[u8]) -> Vec<u8> {
    let inline = record.len() <= INLINE_MAX;
    let mut v = Vec::with_capacity(5 + if inline { record.len() } else { 0 });
    v.extend_from_slice(&far_labelset.to_be_bytes());
    if inline {
        v.push(0);
        v.extend_from_slice(record);
    } else {
        v.push(FLAG_OUTLINE);
    }
    v
}

/// An out-adjacency value's far label set, and its inline record (`None`:
/// read the edge record).
pub(crate) fn split_out(v: &[u8]) -> Result<(u32, Option<&[u8]>)> {
    let ls = be32(v, 0)?;
    match v.get(4) {
        Some(&FLAG_OUTLINE) => Ok((ls, None)),
        Some(_) => Ok((ls, Some(&v[5..]))),
        None => Err(corrupt("adjacency value")),
    }
}

/// An edge record value.
pub(crate) fn edge_value(src: u64, etype: u32, dst: u64, record: &[u8]) -> Vec<u8> {
    let mut v = Vec::with_capacity(22 + record.len());
    v.extend_from_slice(&src.to_be_bytes());
    v.extend_from_slice(&etype.to_be_bytes());
    v.extend_from_slice(&dst.to_be_bytes());
    // The sort-key bytes: none yet.
    v.push(0);
    v.extend_from_slice(record);
    v
}

/// An edge record's source, type, destination and record.
pub(crate) fn split_edge(v: &[u8]) -> Result<(u64, u32, u64, &[u8])> {
    let src = be64(v, 0)?;
    let etype = be32(v, 8)?;
    let dst = be64(v, 12)?;
    let sl = usize::from(*v.get(20).ok_or_else(|| corrupt("edge record"))?);
    let rec = v.get(21 + sl..).ok_or_else(|| corrupt("edge record"))?;
    Ok((src, etype, dst, rec))
}

/// An index key from the values' typed texts.
pub(crate) fn index_key(vals: &[String]) -> Vec<u8> {
    let mut k = Vec::new();
    for v in vals {
        memcmp::put_bytes(&mut k, v.as_bytes());
    }
    k
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn records_round_trip_with_types() {
        let props = serde_json::json!({
            "a": 1, "b": -7, "c": 2.0, "d": "x", "e": [1, 2], "f": true,
            "g": u64::MAX, "h": "2020-01-01", "i": [1.5, 2.0], "j": {"k": 1},
        });
        let types = serde_json::json!({"c": "float", "h": "date", "i[1]": "float"});
        let ids = ["a", "b", "c", "d", "e", "f", "g", "h", "i", "j"];
        let key_id = |k: &str| {
            ids.iter()
                .position(|x| *x == k)
                .and_then(|p| u32::try_from(p).ok())
        };
        let name = |i: u32| ids.get(usize::try_from(i).ok()?).map(|s| (*s).to_owned());
        let mut rec = Vec::new();
        put_record(&mut rec, &props, Some(&types), &key_id).unwrap();
        let (p, t) = get_record(&rec, &|_| true, &name).unwrap();
        assert_eq!(p, props);
        assert_eq!(t, Some(types));
        let (p, t) = get_record(&rec, &|i| i == 3, &name).unwrap();
        assert_eq!(p, serde_json::json!({"d": "x"}));
        assert_eq!(t, None);
        assert_eq!(record_keys(&rec).unwrap(), (0..10).collect::<Vec<u32>>());
    }

    #[test]
    fn adjacency_keys_split() {
        let g = GraphSpace {
            tenant: 7,
            object: GRAPH_OBJECT_BASE + 3,
        };
        let k = g.adj(1, OUT, 10, 4, 20, 99);
        assert_eq!(adj_parts(&k).unwrap(), (10, 4, 20, 99));
        assert!(k.starts_with(&g.adj_prefix(1, OUT, 10, Some(4))));
        assert!(k.starts_with(&g.adj_prefix(1, OUT, 10, None)));
        let v = out_value(5, &[1, 2, 3]);
        assert_eq!(split_out(&v).unwrap(), (5, Some(&[1u8, 2, 3][..])));
        let big = vec![0u8; INLINE_MAX + 1];
        assert_eq!(split_out(&out_value(5, &big)).unwrap(), (5, None));
        let e = edge_value(1, 2, 3, &[9]);
        assert_eq!(split_edge(&e).unwrap(), (1, 2, 3, &[9u8][..]));
    }
}
