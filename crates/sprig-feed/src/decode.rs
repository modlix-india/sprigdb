//! Table decoding (chapter 38, "Decoding", "Filtering" rule 4).
//!
//! A row change becomes a key, a full before and after image and, for
//! updates, the changed columns. Internal columns (`_sprig_*`, `_v`, and
//! `my_row_id` unless it is the only key) are removed first; an update
//! whose remaining images are equal is dropped, so back-fills of internal
//! columns produce no events, unless it changed chapter 42's layer
//! columns (`_sprig_state`, `_sprig_ovr`), which change what descendant
//! spaces see.
//!
//! A row of a materialized copy (chapter 42: a table with a `_sprig_live`
//! column, `__sprig_ih_<object>`) is decoded by its liveness, since a
//! tombstone is a removed record: a row becoming live is an insert, a live
//! row becoming a tombstone a delete (with the live row as its before
//! image), a live row changing an update, and a tombstone written, changed
//! or purged nothing (decision RL69).

use sprig_binlog::RowImage;
use sprig_common::names::is_internal_column;

use crate::event::{DdlKind, Image, Op};
use crate::source::{RawRow, RowKind, TableShape};

/// A decoded row change.
#[derive(Debug, Clone, PartialEq)]
pub struct Decoded {
    /// The primary key.
    pub key: Image,
    /// Insert, update or delete.
    pub op: Op,
    /// The visible row before.
    pub before: Option<Image>,
    /// The visible row after.
    pub after: Option<Image>,
    /// Changed columns of an update.
    pub delta: Option<Vec<String>>,
}

/// Whether column `i` of `shape` is visible to consumers.
fn visible(shape: &TableShape, i: usize) -> bool {
    let name = shape.columns[i].as_str();
    if name == "my_row_id" {
        return shape.key == [i];
    }
    !(is_internal_column(name) || name == "_v")
}

fn image(shape: &TableShape, img: &RowImage) -> Image {
    Image(
        (0..shape.columns.len())
            .filter(|i| visible(shape, *i))
            .filter_map(|i| img.get(i).map(|v| (shape.columns[i].clone(), v.clone())))
            .collect(),
    )
}

fn key(shape: &TableShape, img: &RowImage) -> Image {
    let cols: Vec<usize> = if shape.key.is_empty() {
        (0..shape.columns.len())
            .filter(|i| visible(shape, *i))
            .collect()
    } else {
        shape.key.clone()
    };
    Image(
        cols.into_iter()
            .filter_map(|i| Some((shape.columns.get(i)?.clone(), img.get(i)?.clone())))
            .collect(),
    )
}

/// Whether an update changed an inheritable object's layer columns
/// (`_sprig_state`, `_sprig_ovr`; chapter 42): a hide marker or an orphan
/// can change what descendants see without changing a visible value, so
/// such an update is kept (with an empty delta) for the copy worker.
fn layer_changed(row: &RawRow) -> bool {
    let (Some(b), Some(a)) = (row.before.as_ref(), row.after.as_ref()) else {
        return false;
    };
    row.table
        .columns
        .iter()
        .enumerate()
        .filter(|(_, n)| n.as_str() == "_sprig_state" || n.as_str() == "_sprig_ovr")
        .any(|(i, _)| b.get(i) != a.get(i))
}

/// A materialized copy's liveness column (chapter 42).
const LIVE: &str = "_sprig_live";

/// Whether `img`'s `_sprig_live` (column `i`) is true.
fn is_live(img: Option<&RowImage>, i: usize) -> bool {
    use sprig_binlog::Value;
    match img.and_then(|m| m.get(i)) {
        Some(Value::Int(v)) => *v != 0,
        Some(Value::UInt(v)) => *v != 0,
        _ => false,
    }
}

/// Decodes one row change; `None` for an update that changed only
/// internal columns (other than an inheritable object's layer columns),
/// and for a copy's tombstones (module docs).
#[must_use]
pub fn decode_row(row: &RawRow) -> Option<Decoded> {
    let shape = &row.table;
    let mut op = match row.kind {
        RowKind::Insert => Op::Insert,
        RowKind::Delete => Op::Delete,
        RowKind::Update => Op::Update,
    };
    let (mut before_img, mut after_img) = (row.before.as_ref(), row.after.as_ref());
    if let Some(i) = shape.columns.iter().position(|c| c == LIVE) {
        let was = before_img.is_some() && is_live(before_img, i);
        let now = after_img.is_some() && is_live(after_img, i);
        (op, before_img, after_img) = match (was, now) {
            (false, false) => return None,
            (false, true) => (Op::Insert, None, after_img),
            (true, false) => (Op::Delete, before_img, None),
            (true, true) => (Op::Update, before_img, after_img),
        };
    }
    let before = before_img.map(|i| image(shape, i));
    let after = after_img.map(|i| image(shape, i));
    let key_img = after_img.or(before_img)?;
    let delta = if op == Op::Update {
        let (b, a) = (before.as_ref()?, after.as_ref()?);
        if b == a && !layer_changed(row) {
            return None;
        }
        Some(
            a.0.iter()
                .filter(|(n, v)| b.get(n) != Some(v))
                .map(|(n, _)| n.clone())
                .collect(),
        )
    } else {
        None
    };
    Some(Decoded {
        key: key(shape, key_img),
        op,
        before,
        after,
        delta,
    })
}

/// The kind of a DDL statement (`TRUNCATE` is its own op).
#[must_use]
pub fn ddl_op(sql: &str) -> Op {
    let first = sql
        .trim_start()
        .split_ascii_whitespace()
        .next()
        .unwrap_or("")
        .to_ascii_uppercase();
    match first.as_str() {
        "TRUNCATE" => Op::Truncate,
        "CREATE" => Op::Ddl(DdlKind::Create),
        "ALTER" => Op::Ddl(DdlKind::Alter),
        "DROP" => Op::Ddl(DdlKind::Drop),
        "RENAME" => Op::Ddl(DdlKind::Rename),
        _ => Op::Ddl(DdlKind::Other),
    }
}

/// The table a DDL statement names in `schema`: the identifier after
/// `` `schema`. `` or `schema.`; for an unqualified statement run in
/// `schema`, the identifier after `TABLE`. Best effort: `None` when it
/// can't tell.
#[must_use]
pub fn ddl_table(sql: &str, schema: &str, default_db: &str) -> Option<String> {
    let ident = |s: &str| -> Option<String> {
        let s = s.trim_start();
        if let Some(r) = s.strip_prefix('`') {
            return Some(r[..r.find('`')?].to_owned());
        }
        let end = s
            .find(|c: char| !(c.is_ascii_alphanumeric() || c == '_' || c == '$'))
            .unwrap_or(s.len());
        (end > 0).then(|| s[..end].to_owned())
    };
    for q in [format!("`{schema}`."), format!("{schema}.")] {
        if let Some(i) = sql.find(&q) {
            return ident(&sql[i + q.len()..]);
        }
    }
    if default_db != schema {
        return None;
    }
    let upper = sql.to_ascii_uppercase();
    let i = upper.find("TABLE ")?;
    let mut rest = &sql[i + 6..];
    for kw in ["IF NOT EXISTS ", "IF EXISTS "] {
        if rest.trim_start().to_ascii_uppercase().starts_with(kw) {
            rest = &rest.trim_start()[kw.len()..];
        }
    }
    ident(rest)
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use sprig_binlog::Value;

    use super::*;

    fn shape() -> Arc<TableShape> {
        Arc::new(TableShape {
            schema: "t1_d1_g1_s1".into(),
            table: "a".into(),
            columns: vec!["k".into(), "v".into(), "_sprig_rk".into(), "_v".into()],
            key: vec![0],
        })
    }

    fn img(k: i64, v: &str, rk: u64) -> RowImage {
        RowImage(vec![
            Some(Value::Int(k)),
            Some(Value::String(v.as_bytes().to_vec())),
            Some(Value::UInt(rk)),
            Some(Value::Int(1)),
        ])
    }

    #[test]
    fn internal_columns_are_removed_and_noop_updates_dropped() {
        let ins = RawRow {
            table: shape(),
            kind: RowKind::Insert,
            before: None,
            after: Some(img(1, "x", 9)),
            hint: None,
        };
        let d = decode_row(&ins).unwrap();
        assert_eq!(d.op, Op::Insert);
        assert_eq!(d.key.0, vec![("k".to_owned(), Value::Int(1))]);
        assert_eq!(d.after.as_ref().unwrap().0.len(), 2);
        let backfill = RawRow {
            table: shape(),
            kind: RowKind::Update,
            before: Some(img(1, "x", 9)),
            after: Some(img(1, "x", 10)),
            hint: None,
        };
        assert_eq!(decode_row(&backfill), None);
        let upd = RawRow {
            table: shape(),
            kind: RowKind::Update,
            before: Some(img(1, "x", 9)),
            after: Some(img(1, "y", 9)),
            hint: None,
        };
        assert_eq!(decode_row(&upd).unwrap().delta, Some(vec!["v".to_owned()]));
        // Chapter 42: a hide marker over an override with the same values.
        let layer = Arc::new(TableShape {
            schema: "t1_d1_g1_s1".into(),
            table: "p".into(),
            columns: vec![
                "id".into(),
                "v".into(),
                "_sprig_space".into(),
                "_sprig_state".into(),
                "_sprig_ovr".into(),
            ],
            key: vec![0, 2],
        });
        let row = |state: &str, ovr: u64| {
            RowImage(vec![
                Some(Value::Int(5)),
                Some(Value::String(b"x".to_vec())),
                Some(Value::UInt(7)),
                Some(Value::String(state.as_bytes().to_vec())),
                Some(Value::UInt(ovr)),
            ])
        };
        let hide = RawRow {
            table: Arc::clone(&layer),
            kind: RowKind::Update,
            before: Some(row("ovr", 2)),
            after: Some(row("hide", 0)),
            hint: None,
        };
        let d = decode_row(&hide).unwrap();
        assert_eq!(d.delta, Some(Vec::new()));
        assert_eq!(d.key.0.len(), 2, "the key keeps the space");
    }

    #[test]
    fn copy_rows_decode_by_liveness() {
        let copy = Arc::new(TableShape {
            schema: "t1_d1_g1_s1".into(),
            table: "__sprig_ih_9".into(),
            columns: vec![
                "id".into(),
                "v".into(),
                "_sprig_share".into(),
                "_sprig_live".into(),
                "_sprig_src_v".into(),
            ],
            key: vec![0, 2],
        });
        let img = |v: &str, live: i64, src: u64| {
            RowImage(vec![
                Some(Value::Int(5)),
                Some(Value::String(v.as_bytes().to_vec())),
                Some(Value::UInt(0)),
                Some(Value::Int(live)),
                Some(Value::UInt(src)),
            ])
        };
        let row = |kind, before: Option<RowImage>, after: Option<RowImage>| RawRow {
            table: Arc::clone(&copy),
            kind,
            before,
            after,
            hint: None,
        };
        // A new live row, and a tombstone made live again: inserts.
        let d = decode_row(&row(RowKind::Insert, None, Some(img("x", 1, 1)))).unwrap();
        assert_eq!((d.op, d.before.is_none()), (Op::Insert, true));
        assert_eq!(d.key.0.len(), 2, "the key keeps the share");
        let d = decode_row(&row(
            RowKind::Update,
            Some(img("x", 0, 1)),
            Some(img("x", 1, 2)),
        ))
        .unwrap();
        assert_eq!((d.op, d.before, d.delta), (Op::Insert, None, None));
        // A live row becoming a tombstone: a delete with the live image.
        let d = decode_row(&row(
            RowKind::Update,
            Some(img("x", 1, 1)),
            Some(img("x", 0, 2)),
        ))
        .unwrap();
        assert_eq!(d.op, Op::Delete);
        assert!(d.after.is_none());
        assert_eq!(d.before.unwrap().0.len(), 2, "id and v");
        // A live row changing: an update; only its guard: nothing.
        let d = decode_row(&row(
            RowKind::Update,
            Some(img("x", 1, 1)),
            Some(img("y", 1, 2)),
        ))
        .unwrap();
        assert_eq!((d.op, d.delta), (Op::Update, Some(vec!["v".to_owned()])));
        assert_eq!(
            decode_row(&row(
                RowKind::Update,
                Some(img("x", 1, 1)),
                Some(img("x", 1, 2))
            )),
            None
        );
        // Tombstones written, rewritten and purged: nothing.
        assert_eq!(
            decode_row(&row(RowKind::Insert, None, Some(img("", 0, 1)))),
            None
        );
        assert_eq!(
            decode_row(&row(
                RowKind::Update,
                Some(img("x", 0, 1)),
                Some(img("x", 0, 2))
            )),
            None
        );
        assert_eq!(
            decode_row(&row(RowKind::Delete, Some(img("x", 0, 1)), None)),
            None
        );
        let d = decode_row(&row(RowKind::Delete, Some(img("x", 1, 1)), None)).unwrap();
        assert_eq!(d.op, Op::Delete);
    }

    #[test]
    fn ddl_names_and_kinds() {
        assert_eq!(ddl_op("truncate table a"), Op::Truncate);
        assert_eq!(ddl_op("ALTER TABLE a ADD b INT"), Op::Ddl(DdlKind::Alter));
        let s = "t1_d1_g1_s1";
        assert_eq!(
            ddl_table("ALTER TABLE `t1_d1_g1_s1`.`orders` COMMENT 'x'", s, "x").as_deref(),
            Some("orders")
        );
        assert_eq!(
            ddl_table("CREATE TABLE IF NOT EXISTS items (a INT)", s, s).as_deref(),
            Some("items")
        );
        assert_eq!(ddl_table("CREATE TABLE items (a INT)", s, "other"), None);
    }
}
