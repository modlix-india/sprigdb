//! Key layout for SQL rows and secondary indexes, on the engine's shared
//! layout (`sprig_engine::keys`):
//!
//! ```text
//! tenant u64 | shard u32 (0) | object u32 | kind u8 | index u16 | user key
//!   kind P, index g: a row of generation g (0 until a change of the
//!                    table's definition copies the rows); user key = the
//!                    primary key's parts
//!   kind S, index n: non-unique secondary index n; user key = its parts,
//!                    then the primary key's parts
//!   kind U, index n: unique secondary index n; user key = its parts, and
//!                    the primary key's parts in the value (with a NULL
//!                    part the entry is keyed as a non-unique one, since
//!                    NULLs don't collide)
//!   kind M, index 0: the table's records: its indexes (see `indexes`) and
//!                    its row format
//! ```
//!
//! Parts are the engine's typed key encodings (`sprig_engine::keys::typed`):
//! memcomparable per type, strings as their collation's sort key, a prefix
//! part (`col(10)`) as the prefix's key, a `DESC` part with its bytes
//! inverted. Secondary index parts carry a NULL marker first, so NULL
//! sorts first as in InnoDB; primary key parts carry none. Every encoding
//! is prefix-free.
//!
//! The index number `n` is not the index's position in the definition: it
//! comes from the table's index record, so dropping or adding an index never
//! renumbers the others. A [`Space`] carries one table's numbers for one
//! statement.
//!
//! An entry's value names its row: the primary key's parts. A non-unique
//! entry of a table whose primary key has a fixed width leaves the value
//! empty, and the row's parts are the key's last bytes.

use std::sync::Arc;

use bytes::Bytes;
use sprig_engine::keys::typed::{self, KeyColumn};
use sprig_engine::keys::{KeyPrefix, kind, prefix_end as engine_prefix_end};
use sprig_engine::row::temporal::frac_bytes;

use super::layout::Shared;
use super::row::Datum;
use super::table::{ColType, TableDef};

/// One table's keyspace for one tenant: the prefixes of its rows and of
/// each secondary index (by position in the definition), and its row
/// format.
#[derive(Debug, Clone)]
pub(crate) struct Space {
    /// The tenant.
    pub tenant: u64,
    /// The table's object number in keys.
    pub object: u32,
    /// The key index number of each secondary index of the definition.
    pub ids: Arc<[u16]>,
    /// The table's rows are in the first row format (it had rows before
    /// the engine's row record existed).
    pub first_format: bool,
    /// How the rows are laid out: the versions of the column list they
    /// carry and the generation they live under. Every statement (and open
    /// transaction) using the table holds a copy, which a change that
    /// copies the rows waits for.
    pub layout: Shared,
}

impl Space {
    /// The keyspace of a table whose columns never changed.
    #[cfg(test)]
    pub(crate) fn plain(
        tenant: u64,
        object: u32,
        ids: Vec<u16>,
        first_format: bool,
        def: &TableDef,
    ) -> Self {
        Self {
            tenant,
            object,
            ids: Arc::from(ids),
            first_format,
            layout: Arc::new(super::layout::Layout::first(def)),
        }
    }

    /// The prefix of the rows.
    pub(crate) fn rows(&self) -> Vec<u8> {
        self.prefix(kind::PRIMARY, self.layout.row_gen)
    }

    /// The prefix of the entries under key index number `id`.
    pub(crate) fn index_of(&self, id: u16, unique: bool) -> Vec<u8> {
        self.prefix(
            if unique {
                kind::UNIQUE
            } else {
                kind::SECONDARY
            },
            id,
        )
    }

    /// The prefix of secondary index `ix` (its position in `def`).
    pub(crate) fn index(&self, def: &TableDef, ix: usize) -> Vec<u8> {
        let k = if def.indexes[ix].unique {
            kind::UNIQUE
        } else {
            kind::SECONDARY
        };
        self.prefix(k, self.ids[ix])
    }

    fn prefix(&self, kind: u8, index: u16) -> Vec<u8> {
        let mut k = Vec::with_capacity(64);
        KeyPrefix {
            tenant: self.tenant,
            shard: 0,
            object: self.object,
            kind,
            index,
        }
        .encode_into(&mut k);
        k
    }
}

fn meta_key(tenant: u64, object: u32, name: &[u8]) -> Vec<u8> {
    let mut k = Vec::with_capacity(32);
    KeyPrefix {
        tenant,
        shard: 0,
        object,
        kind: kind::META,
        index: 0,
    }
    .encode_into(&mut k);
    k.extend_from_slice(name);
    k
}

/// The key of sequence `object`'s stored state.
pub(crate) fn sequence_key(tenant: u64, object: u32) -> Vec<u8> {
    meta_key(tenant, object, b"sequence")
}

/// The prefix of the rows of generation `row_gen` of a table.
pub(crate) fn rows_prefix(tenant: u64, object: u32, row_gen: u16) -> Vec<u8> {
    let mut k = Vec::with_capacity(64);
    KeyPrefix {
        tenant,
        shard: 0,
        object,
        kind: kind::PRIMARY,
        index: row_gen,
    }
    .encode_into(&mut k);
    k
}

/// The prefix of every key of a table: its rows of every generation, its
/// index entries and its records.
pub(crate) fn object_prefix(tenant: u64, object: u32) -> Vec<u8> {
    let mut k = rows_prefix(tenant, object, 0);
    // tenant u64 | shard u32 | object u32, then the kind.
    k.truncate(16);
    k
}

/// The index number of the tenant's list of dropped tables, in the
/// tenant's records object (object 0, kind M; index 0 holds the catalog's
/// records).
const DROPS_INDEX: u16 = 1;

/// The prefix of `tenant`'s list of dropped tables whose keys are still
/// being deleted: one key per table, `prefix | object u32`, empty value.
pub(crate) fn drops_prefix(tenant: u64) -> Vec<u8> {
    let mut k = Vec::with_capacity(24);
    KeyPrefix {
        tenant,
        shard: 0,
        object: 0,
        kind: kind::META,
        index: DROPS_INDEX,
    }
    .encode_into(&mut k);
    k
}

/// The mark of dropped table `object` in [`drops_prefix`]'s list.
pub(crate) fn drop_mark(tenant: u64, object: u32) -> Vec<u8> {
    let mut k = drops_prefix(tenant);
    k.extend_from_slice(&object.to_be_bytes());
    k
}

/// The key of a table's index record.
pub(crate) fn index_record_key(tenant: u64, object: u32) -> Vec<u8> {
    meta_key(tenant, object, b"indexes")
}

/// The key of a table's layout record (`super::layout`).
pub(crate) fn layout_key(tenant: u64, object: u32) -> Vec<u8> {
    meta_key(tenant, object, b"layout")
}

/// The key of a table's row format marker: tables of the engine's first
/// prototype carry one byte, [`FORMAT_FIRST`], under it. That format is no
/// longer read: a table with the marker is refused.
pub(crate) fn format_key(tenant: u64, object: u32) -> Vec<u8> {
    meta_key(tenant, object, b"rowformat")
}

/// The prototype row format's marker byte.
pub(crate) const FORMAT_FIRST: u8 = 0;

/// The first key after every key starting with `p`.
pub(crate) fn prefix_end(p: &[u8]) -> Vec<u8> {
    // A prefix of 0xFF bytes only can't occur under a real tenant id.
    engine_prefix_end(p).unwrap_or_else(|| vec![0xFF; p.len() + 1])
}

/// Appends the key part of `v` under `kc`; `false` when `v` isn't a value
/// of the part's column (callers convert values first).
pub(crate) fn put_part(out: &mut Vec<u8>, v: &Datum, kc: &KeyColumn) -> bool {
    typed::encode_column(out, kc, v).is_ok()
}

/// The bytes a key part of this type takes, when every value takes the
/// same.
fn part_width(kc: &KeyColumn) -> Option<usize> {
    if kc.nullable {
        return None;
    }
    Some(match kc.ty {
        ColType::Int { bytes, .. } => usize::from(bytes),
        ColType::Bit { bits } => usize::from(bits).div_ceil(8),
        ColType::Float => 4,
        ColType::Double | ColType::Set { .. } => 8,
        ColType::Date => 3,
        ColType::Time { fsp } => 3 + frac_bytes(fsp),
        ColType::DateTime { fsp } => 5 + frac_bytes(fsp),
        ColType::Timestamp { fsp } => 4 + frac_bytes(fsp),
        ColType::Year => 1,
        ColType::Enum { .. } => 2,
        _ => return None,
    })
}

/// The total width of the primary key's parts, when every part has a
/// fixed width. A table in the first row format counts integer keys only,
/// as it did when its entries were written.
pub(crate) fn fixed_pk_width(def: &TableDef, first_format: bool) -> Option<usize> {
    def.pk_key(first_format).iter().try_fold(0usize, |n, kc| {
        if first_format && !matches!(kc.ty, ColType::Int { .. }) {
            return None;
        }
        Some(n + part_width(kc)?)
    })
}

/// The row key of the row whose primary key values are `pk` (key order).
#[cfg(test)]
pub(crate) fn row_key(sp: &Space, def: &TableDef, pk: &[Datum]) -> Option<Vec<u8>> {
    let mut k = sp.rows();
    for (v, kc) in pk.iter().zip(def.pk_key(sp.first_format)) {
        if !put_part(&mut k, v, kc) {
            return None;
        }
    }
    Some(k)
}

/// The row key of `row` (all columns).
pub(crate) fn row_key_of(sp: &Space, def: &TableDef, row: &[Datum]) -> Option<Vec<u8>> {
    let mut k = sp.rows();
    for (&ci, kc) in def.pk.iter().zip(def.pk_key(sp.first_format)) {
        if !put_part(&mut k, &row[ci], kc) {
            return None;
        }
    }
    Some(k)
}

/// Appends the first `vals.len()` parts of index `ix` (with NULL markers).
pub(crate) fn put_index_parts(
    out: &mut Vec<u8>,
    sp: &Space,
    def: &TableDef,
    ix: usize,
    vals: &[&Datum],
) -> bool {
    def.index_key(ix, sp.first_format)
        .iter()
        .zip(vals)
        .all(|(kc, v)| put_part(out, v, kc))
}

/// The key and value of `row`'s entry in index `ix` of `def`.
pub(crate) fn index_entry(
    sp: &Space,
    def: &TableDef,
    ix: usize,
    row: &[Datum],
) -> Option<(Vec<u8>, Bytes)> {
    let x = &def.indexes[ix];
    let mut k = sp.index(def, ix);
    let vals: Vec<&Datum> = x.cols.iter().map(|&c| &row[c]).collect();
    if !put_index_parts(&mut k, sp, def, ix, &vals) {
        return None;
    }
    let mut pk = Vec::with_capacity(16);
    for (&ci, kc) in def.pk.iter().zip(def.pk_key(sp.first_format)) {
        if !put_part(&mut pk, &row[ci], kc) {
            return None;
        }
    }
    if x.unique && !vals.iter().any(|v| v.is_null()) {
        return Some((k, Bytes::from(pk)));
    }
    k.extend_from_slice(&pk);
    let v = if fixed_pk_width(def, sp.first_format).is_some() {
        Bytes::new()
    } else {
        Bytes::from(pk)
    };
    Some((k, v))
}

/// The primary key parts an index entry points at.
pub(crate) fn entry_target<'a>(
    sp: &Space,
    def: &TableDef,
    key: &'a [u8],
    value: &'a [u8],
) -> Option<&'a [u8]> {
    if !value.is_empty() {
        return Some(value);
    }
    let w = fixed_pk_width(def, sp.first_format)?;
    key.get(key.len().checked_sub(w)?..)
}

/// Decodes the value of a one-column integer primary key from its part
/// `b` (the `AUTO_INCREMENT` counter reads the last row's key).
pub(crate) fn decode_int_pk(def: &TableDef, first_format: bool, b: &[u8]) -> Option<Datum> {
    let [kc] = def.pk_key(first_format) else {
        return None;
    };
    if !matches!(kc.ty, ColType::Int { .. }) {
        return None;
    }
    typed::decode_column(b, kc).ok()?.0
}

#[cfg(test)]
mod tests {
    use sprig_common::ids::{DbId, ObjectId};

    use super::*;
    use crate::engine::table;

    fn space_of(def: &TableDef, first_format: bool) -> Space {
        Space::plain(7, 3, vec![4u16], first_format, def)
    }

    #[test]
    fn signed_parts_sort_numerically() {
        let def = table::parse(
            ObjectId(3),
            1,
            DbId(1),
            "CREATE TABLE `t` (\n  `a` int(11) NOT NULL,\n  PRIMARY KEY (`a`)\n) ENGINE=InnoDB",
        )
        .unwrap();
        let sp = space_of(&def, false);
        let vals = [-5i64, 3, -1, 0, i64::from(i32::MIN), i64::from(i32::MAX)];
        let mut keys: Vec<Vec<u8>> = vals
            .iter()
            .map(|v| row_key(&sp, &def, &[Datum::Int(*v)]).unwrap())
            .collect();
        keys.sort();
        let p = sp.rows().len();
        let back: Vec<Datum> = keys
            .iter()
            .map(|k| decode_int_pk(&def, false, &k[p..]).unwrap())
            .collect();
        let want: Vec<Datum> = [i64::from(i32::MIN), -5, -1, 0, 3, i64::from(i32::MAX)]
            .into_iter()
            .map(Datum::Int)
            .collect();
        assert_eq!(back, want);
        // INT parts take the column's four bytes.
        assert_eq!(keys[0].len(), p + 4);
        // A value outside the column's range is no key.
        assert!(row_key(&sp, &def, &[Datum::Int(i64::MAX)]).is_none());
    }

    #[test]
    fn integer_keys_keep_the_first_formats_bytes() {
        // The bytes the first key helpers wrote: big-endian with the sign
        // bit flipped, in the column's width; an index part after its NULL
        // marker, then the primary key's part.
        let def = table::parse(
            ObjectId(3),
            1,
            DbId(1),
            "CREATE TABLE `t` (\n  `a` int(11) NOT NULL,\n  `b` bigint(20) unsigned DEFAULT NULL,\n  `s` varchar(10) DEFAULT NULL,\n  PRIMARY KEY (`a`),\n  KEY `b` (`b`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_uca1400_ai_ci",
        )
        .unwrap();
        for first_format in [true, false] {
            let sp = space_of(&def, first_format);
            let p = sp.rows().len();
            let k = row_key(&sp, &def, &[Datum::Int(-2)]).unwrap();
            assert_eq!(&k[p..], &[0x7F, 0xFF, 0xFF, 0xFE]);
            let row = vec![Datum::Int(5), Datum::UInt(258), Datum::Null];
            let (ek, ev) = index_entry(&sp, &def, 0, &row).unwrap();
            let ip = sp.index(&def, 0).len();
            assert_eq!(
                &ek[ip..],
                &[1, 0, 0, 0, 0, 0, 0, 1, 2, 0x80, 0, 0, 5],
                "first_format {first_format}"
            );
            assert!(ev.is_empty());
            assert_eq!(entry_target(&sp, &def, &ek, &ev).unwrap(), &k_of(5)[..]);
            let null_row = vec![Datum::Int(5), Datum::Null, Datum::Null];
            let (nk, _) = index_entry(&sp, &def, 0, &null_row).unwrap();
            assert_eq!(&nk[ip..], &[0, 0x80, 0, 0, 5]);
        }
    }

    fn k_of(v: u8) -> [u8; 4] {
        [0x80, 0, 0, v]
    }

    #[test]
    fn string_keys_follow_the_collation() {
        let def = table::parse(
            ObjectId(3),
            1,
            DbId(1),
            "CREATE TABLE `t` (\n  `s` varchar(10) NOT NULL,\n  `k` int(11) DEFAULT NULL,\n  PRIMARY KEY (`s`),\n  KEY `k` (`k`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_uca1400_ai_ci",
        )
        .unwrap();
        let sp = space_of(&def, false);
        let k = |s: &str| {
            row_key(
                &sp,
                &def,
                &[Datum::Str(Bytes::copy_from_slice(s.as_bytes()))],
            )
        };
        // Case and trailing spaces don't count (PAD SPACE, case-insensitive).
        assert_eq!(k("abc"), k("ABC "));
        assert!(k("abc") < k("abd"));
        // A non-unique entry of a string-keyed table names its row in the
        // value.
        let row = vec![Datum::Str(Bytes::from_static(b"x")), Datum::Int(5)];
        let (ek, ev) = index_entry(&sp, &def, 0, &row).unwrap();
        assert!(!ev.is_empty());
        let target = entry_target(&sp, &def, &ek, &ev).unwrap();
        assert_eq!(
            [sp.rows().as_slice(), target].concat(),
            row_key_of(&sp, &def, &row).unwrap()
        );
    }

    #[test]
    fn prefix_and_descending_parts() {
        let def = table::parse(
            ObjectId(3),
            1,
            DbId(1),
            "CREATE TABLE `t` (\n  `a` int(11) NOT NULL,\n  `n` varchar(40) DEFAULT NULL,\n  PRIMARY KEY (`a` DESC),\n  KEY `ix` (`n`(2))\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin",
        )
        .unwrap();
        let sp = space_of(&def, false);
        // A descending key: larger values first.
        let k = |v: i64| row_key(&sp, &def, &[Datum::Int(v)]).unwrap();
        assert!(k(5) < k(4));
        assert!(k(-1) > k(0));
        // A prefix part keys the first characters only.
        let e = |s: &str| {
            let row = vec![
                Datum::Int(1),
                Datum::Str(Bytes::copy_from_slice(s.as_bytes())),
            ];
            index_entry(&sp, &def, 0, &row).unwrap().0
        };
        assert_eq!(e("abcd"), e("abzz"));
        assert_ne!(e("abcd"), e("acbd"));
    }

    #[test]
    fn prefix_end_skips_all_suffixes() {
        assert_eq!(prefix_end(&[1, 2, 0xFF]), vec![1, 3]);
        assert_eq!(prefix_end(&[1, 2]), vec![1, 3]);
    }
}
