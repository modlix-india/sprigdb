//! `EXPLAIN` in the servers' tabular shape: one row per table access, in
//! the order the tables are read.

use std::ops::Bound;

use sprig_engine::keys::collation::Charset;
use sprig_engine::row::temporal::frac_bytes;
use sprig_engine::row::types::packed_decimal_len;
use sprig_engine::row::{ColumnType, Value};
use sprig_exec::ops::{RowSource, VecSource};
use sprig_exec::types::Derivation;
use sprig_exec::{ExprType, Session};

use crate::catalog::{IndexPart, KeyId, TableDef};
use crate::plan::Column;
use crate::range::RangeSet;

/// One row of `EXPLAIN`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExplainRow {
    /// The query block's number.
    pub id: usize,
    /// `SIMPLE`, `PRIMARY`, `DERIVED`, `SUBQUERY`, `UNION`...
    pub select_type: String,
    /// The table's alias; `None` for a row that is only a message.
    pub table: Option<String>,
    /// The access type: `const`, `eq_ref`, `ref`, `range`, `index`, `ALL`,
    /// or `hash_ALL` and the like for the build side of a hash join.
    pub access: Option<String>,
    /// The keys the conditions could use.
    pub possible_keys: Option<String>,
    /// The key read.
    pub key: Option<String>,
    /// The bytes of the key parts used.
    pub key_len: Option<String>,
    /// What the key parts are compared with.
    pub r#ref: Option<String>,
    /// The planner's estimate of rows read per execution of this step.
    pub rows: Option<u64>,
    /// The notes of the `Extra` column.
    pub extra: Vec<String>,
}

impl ExplainRow {
    pub(crate) fn new(id: usize, select_type: &str, table: &str) -> Self {
        Self {
            id,
            select_type: select_type.to_owned(),
            table: Some(table.to_owned()),
            access: None,
            possible_keys: None,
            key: None,
            key_len: None,
            r#ref: None,
            rows: None,
            extra: Vec::new(),
        }
    }

    pub(crate) fn message(id: usize, select_type: &str, message: &str) -> Self {
        Self {
            table: None,
            extra: vec![message.to_owned()],
            ..Self::new(id, select_type, "")
        }
    }

    /// The `Extra` column as the servers print it.
    #[must_use]
    pub fn extra_text(&self) -> String {
        self.extra.join("; ")
    }
}

fn mb_max(charset: Charset) -> u32 {
    match charset {
        Charset::Utf8mb4 => 4,
        _ => 1,
    }
}

/// The bytes the servers count for these key parts: each column's storage
/// length, two more for a variable-length string, one more when the column
/// can be `NULL`.
pub(crate) fn key_len(table: &TableDef, parts: &[IndexPart]) -> usize {
    parts
        .iter()
        .map(|p| {
            let col = &table.columns[p.column];
            let chars = |n: u32| p.prefix.unwrap_or(n);
            let len = match col.ty {
                ColumnType::Int { bytes, .. } => u32::from(bytes),
                ColumnType::Bit { bits } => u32::from(bits).div_ceil(8),
                ColumnType::Decimal { precision, scale } => {
                    packed_decimal_len(precision, scale) as u32
                }
                ColumnType::Float => 4,
                ColumnType::Double => 8,
                ColumnType::Date => 3,
                ColumnType::Time { fsp } => 3 + frac_bytes(fsp) as u32,
                ColumnType::DateTime { fsp } => 5 + frac_bytes(fsp) as u32,
                ColumnType::Timestamp { fsp } => 4 + frac_bytes(fsp) as u32,
                ColumnType::Year => 1,
                ColumnType::Char {
                    chars: n,
                    collation,
                } => chars(u32::from(n)) * mb_max(collation.charset()),
                ColumnType::VarChar {
                    chars: n,
                    collation,
                } => chars(n) * mb_max(collation.charset()) + 2,
                ColumnType::Text { collation, .. } => chars(0) * mb_max(collation.charset()) + 2,
                ColumnType::Binary { bytes } => chars(u32::from(bytes)),
                ColumnType::VarBinary { bytes } => chars(bytes) + 2,
                ColumnType::Blob { .. } => chars(0) + 2,
                ColumnType::Enum { .. } => 2,
                ColumnType::Set { .. } => 8,
                ColumnType::Json => 0,
            };
            len as usize + usize::from(col.nullable)
        })
        .sum()
}

/// Fills an `EXPLAIN` row for a scan of `key`.
pub(crate) fn describe_path(
    row: &mut ExplainRow,
    table: &TableDef,
    key: KeyId,
    full: bool,
    set: Option<&RangeSet>,
    covering: bool,
    rows: f64,
    ordered: bool,
) {
    row.rows = Some(rows.max(1.0).round() as u64);
    let Some(def) = table.key(key) else {
        row.access = Some("ALL".into());
        return;
    };
    // A full scan of the primary key is "ALL", unless it is read for
    // its order: then the servers call it an index scan.
    if full && key == KeyId::Primary && !ordered {
        row.access = Some("ALL".into());
        return;
    }
    row.key = Some(def.name.clone());
    if covering {
        row.extra.push("Using index".into());
    }
    let parts = table.ordering_parts(key);
    let Some(set) = set.filter(|_| !full) else {
        row.access = Some("index".into());
        row.key_len = Some(key_len(table, &def.parts).to_string());
        return;
    };
    if set.is_empty() {
        row.access = None;
        row.key = None;
        row.extra = vec!["Impossible WHERE noticed after reading const tables".into()];
        row.rows = None;
        return;
    }
    let bound = set
        .ranges
        .iter()
        .flat_map(|r| [&r.start, &r.end])
        .map(|b| match b {
            Bound::Included(v) | Bound::Excluded(v) => v.len(),
            Bound::Unbounded => 0,
        })
        .max()
        .unwrap_or(0)
        .min(parts.len());
    row.key_len = Some(key_len(table, &parts[..bound]).to_string());
    let one_point = set.ranges.len() == 1 && set.ranges[0].is_point(bound);
    if one_point {
        let whole = bound >= def.parts.len();
        // One value of every part of a unique key: at most one row
        // (a NULL is never such a value).
        let no_null = matches!(&set.ranges[0].start,
            Bound::Included(v) if v.iter().all(|x| !x.is_null()));
        if whole && def.unique && no_null && def.parts.iter().all(|p| p.prefix.is_none()) {
            row.access = Some("const".into());
            row.rows = Some(1);
        } else {
            row.access = Some("ref".into());
        }
        row.r#ref = Some(vec!["const"; bound].join(","));
    } else {
        row.access = Some("range".into());
    }
}

/// The plan as a result set with the servers' ten columns; `extended`
/// (`EXPLAIN EXTENDED`, which the MySQL port sends for its plain
/// `EXPLAIN`) adds `filtered` before `Extra`: the planner keeps no
/// estimate of what a condition filters, so it is always 100.00.
pub(crate) fn as_rows(
    rows: &[ExplainRow],
    session: &Session,
    extended: bool,
) -> (Box<dyn RowSource>, Vec<Column>) {
    let text = |n: u32| {
        ExprType::string(n, session.collation_connection, Derivation::Coercible).nullable()
    };
    let maria = session.persona.is_maria();
    // The servers' own column definitions: `id` an unsigned BIGINT of
    // three (MariaDB) or four (MySQL, not null) digits; `select_type` and
    // `Extra` never NULL; MySQL's `ref` half as long.
    let id = ExprType {
        unsigned: true,
        width: 8,
        len: if maria { 3 } else { 4 },
        nullable: maria,
        ..ExprType::bigint(true)
    };
    let not_null = |n: u32| ExprType {
        nullable: false,
        ..text(n)
    };
    let mut columns = vec![
        ("id", id),
        ("select_type", not_null(19)),
        ("table", text(64)),
        ("type", text(10)),
        ("possible_keys", text(4096)),
        ("key", text(64)),
        ("key_len", text(4096)),
        ("ref", text(if maria { 2048 } else { 1024 })),
        ("rows", text(64)),
        ("Extra", not_null(255)),
    ];
    if extended {
        let filtered = ExprType {
            dec: 2,
            len: 4,
            ..ExprType::double()
        }
        .nullable();
        columns.insert(9, ("filtered", filtered));
    }
    let opt = |s: &Option<String>| s.as_deref().map_or(Value::Null, Value::str);
    let data = rows
        .iter()
        .map(|r| {
            let mut row = vec![
                Value::Int(r.id as i64),
                Value::str(&r.select_type),
                opt(&r.table),
                opt(&r.access),
                opt(&r.possible_keys),
                opt(&r.key),
                opt(&r.key_len),
                opt(&r.r#ref),
                r.rows.map_or(Value::Null, |n| Value::str(&n.to_string())),
                // MySQL leaves an empty `Extra` NULL.
                match r.extra_text() {
                    t if t.is_empty() && !maria => Value::Null,
                    t => Value::str(&t),
                },
            ];
            if extended {
                row.insert(9, Value::Double(100.0));
            }
            row
        })
        .collect();
    let types: Vec<ExprType> = columns.iter().map(|c| c.1).collect();
    (
        Box::new(VecSource::new(data, types)),
        columns
            .into_iter()
            .map(|(name, ty)| Column {
                name: name.to_owned(),
                ty,
            })
            .collect(),
    )
}
