//! A table's row layout: which permanent id each column has, which
//! versions of the column list its rows may carry, and where its rows live.
//!
//! The row record names the column list it was written under by a version
//! number, and matches columns across versions by a permanent id
//! (`sprig_engine::row::record`). The catalog's definition has neither, so
//! the engine keeps them in the table's layout record (key kind `M`, name
//! `layout`):
//!
//! - **The current version**: per column its id, name, type, nullability and
//!   what rows written before the column existed show for it.
//! - **Older versions** that rows of the current generation may still carry.
//!   A row of an older version is read through `record::decode_as`: added
//!   columns show their missing value, dropped ones are skipped, widened
//!   ones are converted. So adding, dropping, renaming, reordering and
//!   widening columns touch no row.
//! - **The row generation**: the key index number the rows live under. A
//!   change that can't be read through (a narrowing, another type family,
//!   `NULL` to `NOT NULL`, another primary key) copies every row, converted,
//!   into the next generation; the switch to it is one small write.
//! - **A pending change**: what a definition change prepared before the
//!   catalog published it. It is applied when the engine first sees the new
//!   definition, and discarded when the next change finds it unapplied.
//!
//! A table without a layout record never had its columns changed: its
//! column ids are the positions in its definition, its rows are version 1
//! of generation 0.

use std::sync::Arc;

use bytes::{BufMut, Bytes, BytesMut};
use sprig_common::{Result, SprigError};
use sprig_engine::keys::collation::Collation;
use sprig_engine::row::record::{self, Column as RecordColumn, RowSchema};
use sprig_engine::row::{Fbt, LobSize};

use super::indexes;
use super::row::Datum;
use super::table::{ColType, Column, TableDef};

/// The version of the first column list.
pub(crate) const FIRST_VERSION: u32 = 1;

/// One table's row layout as statements use it.
#[derive(Debug, Clone)]
pub(crate) struct Layout {
    /// The column list new rows are written under.
    pub current: RowSchema,
    /// Older column lists rows may still carry.
    pub older: Vec<RowSchema>,
    /// The key index number the rows live under.
    pub row_gen: u16,
    /// The table was emptied by `TRUNCATE` under this definition: its
    /// `AUTO_INCREMENT` counter starts at 1 again, whatever start the
    /// definition's text still names.
    pub restart_counter: bool,
}

impl Layout {
    /// The layout of a table whose columns never changed.
    pub(crate) fn first(def: &TableDef) -> Self {
        Self {
            current: def.schema.clone(),
            older: Vec::new(),
            row_gen: 0,
            restart_counter: false,
        }
    }

    /// Decodes a stored row into the current column list.
    ///
    /// # Errors
    /// The row doesn't decode, or carries a version the layout doesn't know.
    pub(crate) fn decode(&self, raw: &Bytes) -> Result<Vec<Datum>> {
        if self.older.is_empty() {
            return record::decode(&self.current, raw);
        }
        let v = record::peek_header(raw)?.schema_version;
        if v == self.current.version() {
            return record::decode(&self.current, raw);
        }
        let writer = self
            .older
            .iter()
            .find(|s| s.version() == v)
            .ok_or_else(|| {
                SprigError::internal("engine: a stored row carries an unknown column list")
            })?;
        record::decode_as(writer, &self.current, raw)
    }
}

/// One column of a version.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct VerCol {
    /// The permanent id.
    pub id: u32,
    /// The name, lower case.
    pub name: String,
    /// The type.
    pub ty: ColType,
    /// `NULL` allowed.
    pub nullable: bool,
    /// What a row written before the column existed shows.
    pub missing: Datum,
    /// A fingerprint of an `ENUM`'s or `SET`'s members (0 for other types):
    /// the same count of other members is another column list.
    pub members: u64,
}

/// One version of the column list.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct Ver {
    /// The number rows written under it carry.
    pub version: u32,
    /// The columns, in order.
    pub cols: Vec<VerCol>,
}

/// A fingerprint of a column's members (FNV-1a).
pub(crate) fn members_print(c: &Column) -> u64 {
    if c.members.is_empty() {
        return 0;
    }
    let mut h = 0xcbf2_9ce4_8422_2325u64;
    for m in c.members.iter() {
        for b in m.iter().chain(&[0u8]) {
            h ^= u64::from(*b);
            h = h.wrapping_mul(0x0000_0100_0000_01b3);
        }
    }
    h.max(1)
}

impl Ver {
    /// The first version of `def`: ids by position.
    pub(crate) fn first(def: &TableDef) -> Self {
        Self {
            version: FIRST_VERSION,
            cols: def
                .columns
                .iter()
                .enumerate()
                .map(|(i, c)| VerCol {
                    id: u32::try_from(i + 1).unwrap_or(u32::MAX),
                    name: c.name.to_lowercase(),
                    ty: c.ty,
                    nullable: c.nullable,
                    missing: if c.nullable {
                        Datum::Null
                    } else {
                        c.implicit_default()
                    },
                    members: members_print(c),
                })
                .collect(),
        }
    }

    /// Whether `def`'s columns are this version's.
    pub(crate) fn matches(&self, def: &TableDef) -> bool {
        self.cols.len() == def.columns.len()
            && self.cols.iter().zip(&def.columns).all(|(v, c)| {
                v.name == c.name.to_lowercase()
                    && v.ty == c.ty
                    && v.nullable == c.nullable
                    && v.members == members_print(c)
            })
    }

    /// The row record's column list.
    ///
    /// # Errors
    /// The version doesn't make a schema (a fault of whoever built it).
    pub(crate) fn schema(&self) -> Result<RowSchema> {
        RowSchema::new(
            self.version,
            self.cols
                .iter()
                .map(|c| RecordColumn {
                    id: c.id,
                    ty: c.ty,
                    nullable: c.nullable,
                    default: c.missing.clone(),
                })
                .collect(),
        )
    }
}

/// What a definition change prepared, waiting for the catalog to publish
/// the new definition.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct Pending {
    /// The catalog version of the definition the change started from: a
    /// definition newer than it, with these columns, is the new one.
    pub base: u64,
    /// The column list of the new definition.
    pub ver: Ver,
    /// The generation the rows were copied into, when they were copied.
    pub copied: Option<u16>,
    /// The index record to install.
    pub indexes: indexes::Record,
    /// The key index numbers this change wrote entries under (their
    /// entries go if the change is discarded).
    pub built: Vec<(u16, bool)>,
    /// The key index numbers whose entries go once the change is applied.
    pub dropped: Vec<(u16, bool)>,
}

/// The layout record.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct LayoutRecord {
    /// The catalog version of the definition the current column list is
    /// of: an older definition is not the table's any more.
    pub at: u64,
    /// The catalog version of the definition under which `TRUNCATE` last
    /// emptied the table (0: never).
    pub truncated: u64,
    /// The generation the rows live under.
    pub row_gen: u16,
    /// The next generation to give.
    pub next_gen: u16,
    /// The next column id to give.
    pub next_col: u32,
    /// The current column list.
    pub current: Ver,
    /// Older lists rows of this generation may carry.
    pub older: Vec<Ver>,
    /// A prepared change.
    pub pending: Option<Pending>,
    /// Key ranges (key kind, key index number) whose entries are no longer
    /// part of the table and still have to be deleted: an older row
    /// generation, a dropped index, what a discarded change wrote.
    pub garbage: Vec<(u8, u16)>,
}

impl LayoutRecord {
    /// The record of a table whose columns never changed.
    pub(crate) fn first(def: &TableDef) -> Self {
        let current = Ver::first(def);
        Self {
            at: def.version,
            truncated: 0,
            row_gen: 0,
            next_gen: 1,
            next_col: u32::try_from(def.columns.len() + 1).unwrap_or(u32::MAX),
            current,
            older: Vec::new(),
            pending: None,
            garbage: Vec::new(),
        }
    }

    /// The layout statements holding the definition of catalog version
    /// `version` use.
    ///
    /// # Errors
    /// A version doesn't make a schema.
    pub(crate) fn layout(&self, version: u64) -> Result<Layout> {
        Ok(Layout {
            current: self.current.schema()?,
            older: self
                .older
                .iter()
                .map(Ver::schema)
                .collect::<Result<Vec<_>>>()?,
            row_gen: self.row_gen,
            restart_counter: self.truncated == version,
        })
    }

    /// Applies the pending change: its column list becomes the current
    /// one. Returns what it was.
    pub(crate) fn promote(&mut self) -> Option<Pending> {
        use sprig_engine::keys::kind;
        let p = self.pending.take()?;
        for (id, unique) in &p.dropped {
            self.garbage.push((
                if *unique {
                    kind::UNIQUE
                } else {
                    kind::SECONDARY
                },
                *id,
            ));
        }
        if let Some(g) = p.copied {
            // Every row was rewritten under the new list.
            self.garbage.push((kind::PRIMARY, self.row_gen));
            self.row_gen = g;
            self.older.clear();
        } else if self.current != p.ver {
            let old = std::mem::replace(&mut self.current, p.ver.clone());
            if old.version != p.ver.version {
                self.older.push(old);
            }
        }
        self.current = p.ver.clone();
        Some(p)
    }

    /// Drops the pending change unapplied: what it wrote becomes garbage.
    pub(crate) fn discard(&mut self) {
        use sprig_engine::keys::kind;
        let Some(p) = self.pending.take() else {
            return;
        };
        for (id, unique) in &p.built {
            self.garbage.push((
                if *unique {
                    kind::UNIQUE
                } else {
                    kind::SECONDARY
                },
                *id,
            ));
        }
        if let Some(g) = p.copied {
            self.garbage.push((kind::PRIMARY, g));
        }
    }

    /// Encodes the record.
    pub(crate) fn encode(&self) -> Bytes {
        let mut b = BytesMut::with_capacity(256);
        b.put_u8(1);
        b.put_u64_le(self.at);
        b.put_u64_le(self.truncated);
        b.put_u16_le(self.row_gen);
        b.put_u16_le(self.next_gen);
        b.put_u32_le(self.next_col);
        put_ver(&mut b, &self.current);
        b.put_u16_le(u16::try_from(self.older.len()).unwrap_or(u16::MAX));
        for v in &self.older {
            put_ver(&mut b, v);
        }
        match &self.pending {
            None => b.put_u8(0),
            Some(p) => {
                b.put_u8(1);
                b.put_u64_le(p.base);
                put_ver(&mut b, &p.ver);
                match p.copied {
                    None => b.put_u8(0),
                    Some(g) => {
                        b.put_u8(1);
                        b.put_u16_le(g);
                    }
                }
                put_bytes(&mut b, &p.indexes.encode());
                put_ids(&mut b, &p.built);
                put_ids(&mut b, &p.dropped);
            }
        }
        b.put_u16_le(u16::try_from(self.garbage.len()).unwrap_or(u16::MAX));
        for (kind, id) in &self.garbage {
            b.put_u8(*kind);
            b.put_u16_le(*id);
        }
        b.freeze()
    }

    /// Decodes a record; `None` when it doesn't decode.
    pub(crate) fn decode(raw: &[u8]) -> Option<Self> {
        let mut r = Reader(raw);
        if r.u8()? != 1 {
            return None;
        }
        let at = r.u64()?;
        let truncated = r.u64()?;
        let row_gen = r.u16()?;
        let next_gen = r.u16()?;
        let next_col = r.u32()?;
        let current = get_ver(&mut r)?;
        let n = r.u16()?;
        let mut older = Vec::with_capacity(usize::from(n));
        for _ in 0..n {
            older.push(get_ver(&mut r)?);
        }
        let pending = match r.u8()? {
            0 => None,
            1 => {
                let base = r.u64()?;
                let ver = get_ver(&mut r)?;
                let copied = match r.u8()? {
                    0 => None,
                    1 => Some(r.u16()?),
                    _ => return None,
                };
                let indexes = indexes::Record::decode(r.bytes()?)?;
                let built = get_ids(&mut r)?;
                let dropped = get_ids(&mut r)?;
                Some(Pending {
                    base,
                    ver,
                    copied,
                    indexes,
                    built,
                    dropped,
                })
            }
            _ => return None,
        };
        let n = r.u16()?;
        let mut garbage = Vec::with_capacity(usize::from(n));
        for _ in 0..n {
            garbage.push((r.u8()?, r.u16()?));
        }
        r.0.is_empty().then_some(Self {
            at,
            truncated,
            row_gen,
            next_gen,
            next_col,
            current,
            older,
            pending,
            garbage,
        })
    }
}

fn put_bytes(b: &mut BytesMut, s: &[u8]) {
    b.put_u32_le(u32::try_from(s.len()).unwrap_or(u32::MAX));
    b.put_slice(s);
}

fn put_ids(b: &mut BytesMut, ids: &[(u16, bool)]) {
    b.put_u16_le(u16::try_from(ids.len()).unwrap_or(u16::MAX));
    for (id, unique) in ids {
        b.put_u16_le(*id);
        b.put_u8(u8::from(*unique));
    }
}

fn get_ids(r: &mut Reader<'_>) -> Option<Vec<(u16, bool)>> {
    let n = r.u16()?;
    let mut v = Vec::with_capacity(usize::from(n));
    for _ in 0..n {
        v.push((r.u16()?, r.u8()? != 0));
    }
    Some(v)
}

fn put_ver(b: &mut BytesMut, v: &Ver) {
    b.put_u32_le(v.version);
    b.put_u16_le(u16::try_from(v.cols.len()).unwrap_or(u16::MAX));
    for c in &v.cols {
        b.put_u32_le(c.id);
        put_bytes(b, c.name.as_bytes());
        put_type(b, c.ty);
        b.put_u8(u8::from(c.nullable));
        b.put_u64_le(c.members);
        put_bytes(b, &value_bytes(c.ty, &c.missing));
    }
}

fn get_ver(r: &mut Reader<'_>) -> Option<Ver> {
    let version = r.u32()?;
    let n = r.u16()?;
    let mut cols = Vec::with_capacity(usize::from(n));
    for _ in 0..n {
        let id = r.u32()?;
        let name = String::from_utf8(r.bytes()?.to_vec()).ok()?;
        let ty = get_type(r)?;
        let nullable = r.u8()? != 0;
        let members = r.u64()?;
        let missing = value_of(ty, r.bytes()?)?;
        cols.push(VerCol {
            id,
            name,
            ty,
            nullable,
            missing,
            members,
        });
    }
    Some(Ver { version, cols })
}

/// A value as bytes: the row record of a one-column row.
fn value_bytes(ty: ColType, v: &Datum) -> Vec<u8> {
    let mut out = Vec::new();
    if let Ok(s) = RowSchema::new(0, vec![RecordColumn::new(1, ty)]) {
        // A value its own column can't hold is stored as NULL: it is only
        // read for rows older than the column, under a list that checked it.
        if record::encode(&s, std::slice::from_ref(v), None, &mut out).is_err() {
            out.clear();
            let _ = record::encode(&s, &[Datum::Null], None, &mut out);
        }
    }
    out
}

fn value_of(ty: ColType, raw: &[u8]) -> Option<Datum> {
    let s = RowSchema::new(0, vec![RecordColumn::new(1, ty)]).ok()?;
    let mut v = record::decode(&s, &Bytes::copy_from_slice(raw)).ok()?;
    let d = v.pop()?;
    // Detached from the buffer it was read from.
    Some(match d {
        Datum::Str(b) => Datum::Str(Bytes::copy_from_slice(&b)),
        Datum::Bytes(b) => Datum::Bytes(Bytes::copy_from_slice(&b)),
        Datum::Json(b) => Datum::Json(Bytes::copy_from_slice(&b)),
        o => o,
    })
}

fn lob(s: LobSize) -> u8 {
    match s {
        LobSize::Tiny => 0,
        LobSize::Normal => 1,
        LobSize::Medium => 2,
        LobSize::Long => 3,
    }
}

fn lob_of(b: u8) -> Option<LobSize> {
    Some(match b {
        0 => LobSize::Tiny,
        1 => LobSize::Normal,
        2 => LobSize::Medium,
        3 => LobSize::Long,
        _ => return None,
    })
}

fn put_collation(b: &mut BytesMut, c: Collation) {
    put_bytes(b, format!("{c:?}").as_bytes());
}

fn get_collation(r: &mut Reader<'_>) -> Option<Collation> {
    let name = r.bytes()?;
    Collation::all()
        .iter()
        .copied()
        .find(|c| format!("{c:?}").as_bytes() == name)
}

fn put_type(b: &mut BytesMut, ty: ColType) {
    match ty {
        ColType::Int { bytes, unsigned } => {
            b.put_u8(1);
            b.put_u8(bytes);
            b.put_u8(u8::from(unsigned));
        }
        ColType::Bit { bits } => {
            b.put_u8(2);
            b.put_u8(bits);
        }
        ColType::Decimal { precision, scale } => {
            b.put_u8(3);
            b.put_u8(precision);
            b.put_u8(scale);
        }
        ColType::Float => b.put_u8(4),
        ColType::Double => b.put_u8(5),
        ColType::Date => b.put_u8(6),
        ColType::Time { fsp } => {
            b.put_u8(7);
            b.put_u8(fsp);
        }
        ColType::DateTime { fsp } => {
            b.put_u8(8);
            b.put_u8(fsp);
        }
        ColType::Timestamp { fsp } => {
            b.put_u8(9);
            b.put_u8(fsp);
        }
        ColType::Year => b.put_u8(10),
        ColType::Char { chars, collation } => {
            b.put_u8(11);
            b.put_u16_le(chars);
            put_collation(b, collation);
        }
        ColType::VarChar { chars, collation } => {
            b.put_u8(12);
            b.put_u32_le(chars);
            put_collation(b, collation);
        }
        ColType::Text { size, collation } => {
            b.put_u8(13);
            b.put_u8(lob(size));
            put_collation(b, collation);
        }
        ColType::Binary { bytes } => {
            b.put_u8(14);
            b.put_u16_le(bytes);
        }
        ColType::VarBinary { bytes } => {
            b.put_u8(15);
            b.put_u32_le(bytes);
        }
        ColType::Blob { size } => {
            b.put_u8(16);
            b.put_u8(lob(size));
        }
        ColType::Enum { members } => {
            b.put_u8(17);
            b.put_u16_le(members);
        }
        ColType::Set { members } => {
            b.put_u8(18);
            b.put_u8(members);
        }
        ColType::Json => b.put_u8(19),
        ColType::Fbt(f) => b.put_u8(match f {
            Fbt::Uuid => 20,
            Fbt::Inet4 => 21,
            Fbt::Inet6 => 22,
        }),
    }
}

fn get_type(r: &mut Reader<'_>) -> Option<ColType> {
    Some(match r.u8()? {
        1 => ColType::Int {
            bytes: r.u8()?,
            unsigned: r.u8()? != 0,
        },
        2 => ColType::Bit { bits: r.u8()? },
        3 => ColType::Decimal {
            precision: r.u8()?,
            scale: r.u8()?,
        },
        4 => ColType::Float,
        5 => ColType::Double,
        6 => ColType::Date,
        7 => ColType::Time { fsp: r.u8()? },
        8 => ColType::DateTime { fsp: r.u8()? },
        9 => ColType::Timestamp { fsp: r.u8()? },
        10 => ColType::Year,
        11 => ColType::Char {
            chars: r.u16()?,
            collation: get_collation(r)?,
        },
        12 => ColType::VarChar {
            chars: r.u32()?,
            collation: get_collation(r)?,
        },
        13 => ColType::Text {
            size: lob_of(r.u8()?)?,
            collation: get_collation(r)?,
        },
        14 => ColType::Binary { bytes: r.u16()? },
        15 => ColType::VarBinary { bytes: r.u32()? },
        16 => ColType::Blob {
            size: lob_of(r.u8()?)?,
        },
        17 => ColType::Enum { members: r.u16()? },
        18 => ColType::Set { members: r.u8()? },
        19 => ColType::Json,
        20 => ColType::Fbt(Fbt::Uuid),
        21 => ColType::Fbt(Fbt::Inet4),
        22 => ColType::Fbt(Fbt::Inet6),
        _ => return None,
    })
}

struct Reader<'a>(&'a [u8]);

impl<'a> Reader<'a> {
    fn take(&mut self, n: usize) -> Option<&'a [u8]> {
        let (h, t) = (self.0.get(..n)?, self.0.get(n..)?);
        self.0 = t;
        Some(h)
    }
    fn u8(&mut self) -> Option<u8> {
        self.take(1).map(|b| b[0])
    }
    fn u16(&mut self) -> Option<u16> {
        self.take(2).map(|b| u16::from_le_bytes([b[0], b[1]]))
    }
    fn u32(&mut self) -> Option<u32> {
        self.take(4)
            .map(|b| u32::from_le_bytes([b[0], b[1], b[2], b[3]]))
    }
    fn u64(&mut self) -> Option<u64> {
        self.take(8)
            .and_then(|b| b.try_into().ok())
            .map(u64::from_le_bytes)
    }
    fn bytes(&mut self) -> Option<&'a [u8]> {
        let n = self.u32()? as usize;
        self.take(n)
    }
}

/// A layout as a keyspace carries it. The cache hands every thread its own
/// copy (see `Engine::cached`), so the statements of many sessions on one
/// table don't write one shared reference count.
pub(crate) type Shared = Arc<Layout>;

#[cfg(test)]
mod tests {
    use sprig_common::ids::{DbId, ObjectId};

    use super::*;
    use crate::engine::table;

    fn def(sql: &str) -> Arc<TableDef> {
        table::parse(ObjectId(3), 1, DbId(1), sql).unwrap()
    }

    #[test]
    fn every_type_and_collation_round_trips() {
        for c in Collation::all() {
            let mut b = BytesMut::new();
            put_collation(&mut b, *c);
            assert_eq!(get_collation(&mut Reader(&b)), Some(*c), "{c:?}");
        }
        let d = def(
            "CREATE TABLE `t` (\n  `a` int(11) NOT NULL,\n  `b` bigint(20) unsigned DEFAULT NULL,\n  `c` bit(7) DEFAULT NULL,\n  `d` decimal(8,3) NOT NULL DEFAULT 1.500,\n  `e` float DEFAULT NULL,\n  `f` double DEFAULT NULL,\n  `g` date DEFAULT NULL,\n  `h` time(3) DEFAULT NULL,\n  `i` datetime(6) DEFAULT NULL,\n  `j` timestamp NULL DEFAULT NULL,\n  `k` year(4) DEFAULT NULL,\n  `l` char(5) NOT NULL DEFAULT 'x',\n  `m` varchar(30) CHARACTER SET latin1 COLLATE latin1_swedish_ci DEFAULT NULL,\n  `n` mediumtext DEFAULT NULL,\n  `o` binary(4) DEFAULT NULL,\n  `p` varbinary(9) DEFAULT NULL,\n  `q` longblob DEFAULT NULL,\n  `r` enum('x','y') NOT NULL,\n  `s` set('a','b','c') DEFAULT NULL,\n  `u` longtext CHARACTER SET utf8mb4 COLLATE utf8mb4_bin DEFAULT NULL CHECK (json_valid(`u`)),\n  PRIMARY KEY (`a`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_uca1400_ai_ci",
        );
        let mut rec = LayoutRecord::first(&d);
        assert!(rec.current.matches(&d));
        rec.older.push(rec.current.clone());
        rec.pending = Some(Pending {
            base: 7,
            ver: Ver {
                version: 2,
                cols: rec.current.cols[..3].to_vec(),
            },
            copied: Some(4),
            indexes: indexes::Record::default(),
            built: vec![(3, true)],
            dropped: vec![(1, false), (2, true)],
        });
        rec.garbage = vec![(b'P', 0), (b'U', 9)];
        let back = LayoutRecord::decode(&rec.encode()).unwrap();
        assert_eq!(back, rec);
        // The schema of the first version is the definition's own.
        assert_eq!(rec.current.schema().unwrap(), d.schema);
        assert_eq!(LayoutRecord::decode(b"\x01\x00"), None);
    }

    #[test]
    fn promoting_keeps_or_drops_older_lists() {
        let d = def(
            "CREATE TABLE `t` (\n  `a` int(11) NOT NULL,\n  `b` int(11) DEFAULT NULL,\n  PRIMARY KEY (`a`)\n) ENGINE=InnoDB",
        );
        let mut rec = LayoutRecord::first(&d);
        let mut v2 = rec.current.clone();
        v2.version = 2;
        v2.cols.push(VerCol {
            id: 3,
            name: "c".into(),
            ty: ColType::Int {
                bytes: 4,
                unsigned: false,
            },
            nullable: true,
            missing: Datum::Null,
            members: 0,
        });
        let pend = |ver: &Ver, copied| Pending {
            base: 1,
            ver: ver.clone(),
            copied,
            indexes: indexes::Record::default(),
            built: Vec::new(),
            dropped: Vec::new(),
        };
        rec.pending = Some(pend(&v2, None));
        rec.promote().unwrap();
        assert_eq!(
            (rec.current.version, rec.older.len(), rec.row_gen),
            (2, 1, 0)
        );
        // A row of version 1 reads with the added column missing.
        let l = rec.layout(1).unwrap();
        let mut raw = Vec::new();
        record::encode(&d.schema, &[Datum::Int(1), Datum::Int(2)], None, &mut raw).unwrap();
        assert_eq!(
            l.decode(&Bytes::from(raw)).unwrap(),
            vec![Datum::Int(1), Datum::Int(2), Datum::Null]
        );
        // A copy starts over: one list, the next generation.
        let mut v3 = v2.clone();
        v3.version = 3;
        rec.pending = Some(pend(&v3, Some(1)));
        rec.promote().unwrap();
        assert_eq!(
            (rec.current.version, rec.older.len(), rec.row_gen),
            (3, 0, 1)
        );
        // The generation it left is garbage until its rows are deleted.
        assert_eq!(rec.garbage, vec![(b'P', 0)]);
        assert!(rec.promote().is_none());
        // A discarded change leaves what it wrote as garbage.
        let mut v4 = v3.clone();
        v4.version = 4;
        let mut p = pend(&v4, Some(2));
        p.built = vec![(5, true)];
        rec.pending = Some(p);
        rec.discard();
        assert_eq!(rec.garbage, vec![(b'P', 0), (b'U', 5), (b'P', 2)]);
        assert_eq!(rec.current.version, 3);
    }

    #[test]
    fn other_members_are_another_list() {
        let a = def(
            "CREATE TABLE `t` (\n  `a` int(11) NOT NULL,\n  `e` enum('x','y') DEFAULT NULL,\n  PRIMARY KEY (`a`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_uca1400_ai_ci",
        );
        let b = def(
            "CREATE TABLE `t` (\n  `a` int(11) NOT NULL,\n  `e` enum('y','x') DEFAULT NULL,\n  PRIMARY KEY (`a`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_uca1400_ai_ci",
        );
        let v = Ver::first(&a);
        assert!(v.matches(&a));
        assert!(!v.matches(&b));
    }
}
