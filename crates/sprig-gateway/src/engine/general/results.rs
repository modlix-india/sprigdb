//! A general `SELECT`'s result as each port's server reports it: the
//! column definitions and the values of the text and binary protocols.
//!
//! - **A column of a table, selected unchanged** (the statement's
//!   outermost select list names it): its definition and values come from
//!   the table's column through the single-table path's encoders
//!   ([`crate::engine::meta`]), so they are the bytes that path sends. A
//!   column of the inner side of an outer join can be `NULL`: its
//!   `NOT NULL` flag is cleared.
//! - **Anything computed**: the definition comes from the planner's static
//!   type (`ExprType::meta`), the value's text from the expression
//!   engine's printer. No table or database name, as on the servers.

use bytes::Bytes;
use sprig_common::{Result, SprigError};
use sprig_engine::keys::collation::{Charset, Collation};
use sprig_exec::{ExprType, FieldType, Kind, Persona};
use sprig_proto_mysql::resultset::{ColumnFlags, column_type};
use sprig_proto_mysql::{ColumnDef, Value};

use super::General;
use crate::engine::assign::{self, Env};
use crate::engine::meta::{self, Results};
use crate::engine::row::Datum;
use crate::engine::temporal;

/// What one result column is.
#[derive(Debug, Clone)]
pub(crate) enum Shape {
    /// Column `column` of table `table` ([`General::tables`]), unchanged.
    Column {
        /// The table.
        table: usize,
        /// The column.
        column: usize,
    },
    /// A computed value of this type.
    Value(ExprType),
}

/// A result's columns: what each is, and its definition for the session's
/// results character set.
#[derive(Debug, Clone)]
pub(crate) struct Layout {
    /// What each column is.
    pub shapes: Vec<Shape>,
    /// The definitions sent.
    pub defs: Vec<ColumnDef>,
}

fn exec_persona(results: Results) -> Persona {
    if results.mysql {
        Persona::MySql
    } else {
        Persona::MariaDb
    }
}

fn wire_type(f: FieldType) -> u8 {
    match f {
        FieldType::Null => column_type::NULL,
        FieldType::Tiny => column_type::TINY,
        FieldType::Short => column_type::SHORT,
        FieldType::Int24 => column_type::INT24,
        FieldType::Long => column_type::LONG,
        FieldType::LongLong => column_type::LONGLONG,
        FieldType::Year => column_type::YEAR,
        FieldType::Bit => column_type::BIT,
        FieldType::NewDecimal => column_type::NEWDECIMAL,
        FieldType::Float => column_type::FLOAT,
        FieldType::Double => column_type::DOUBLE,
        FieldType::VarString => column_type::VAR_STRING,
        FieldType::String => column_type::STRING,
        FieldType::Blob => column_type::BLOB,
        FieldType::MediumBlob => column_type::MEDIUM_BLOB,
        FieldType::LongBlob => column_type::LONG_BLOB,
        FieldType::Date => column_type::DATE,
        FieldType::Time => column_type::TIME,
        FieldType::DateTime => column_type::DATETIME,
        FieldType::Timestamp => column_type::TIMESTAMP,
    }
}

/// The definition of a computed column named `name`.
fn value_def(name: &str, ty: &ExprType, results: Results) -> ColumnDef {
    let m = ty.meta(exec_persona(results));
    let text = ty.kind == Kind::Str && ty.coll != Collation::Binary;
    let (charset, length) = if text {
        // A character string is reported in the results character set.
        let own_mb = sprig_exec::types::mb_max_len(ty.coll);
        let (id, mb) = match results.cs {
            Some(r) => (r.id, r.mbmax),
            None => (meta::id_of(ty.coll.name()).unwrap_or(224), own_mb),
        };
        (
            id,
            u32::try_from(u64::from(ty.len) * u64::from(mb)).unwrap_or(u32::MAX),
        )
    } else {
        (63, m.len)
    };
    let mut flags = ColumnFlags::empty();
    if m.not_null {
        flags |= ColumnFlags::NOT_NULL;
    }
    if m.unsigned {
        flags |= ColumnFlags::UNSIGNED;
    }
    if m.binary || ty.coll.name().ends_with("_bin") {
        flags |= ColumnFlags::BINARY;
    }
    if matches!(
        m.field,
        FieldType::Blob | FieldType::MediumBlob | FieldType::LongBlob
    ) {
        flags |= ColumnFlags::BLOB;
    }
    ColumnDef {
        name: Bytes::from(name.to_owned()),
        charset,
        length,
        col_type: wire_type(m.field),
        flags: flags.bits(),
        decimals: m.dec,
        ..ColumnDef::default()
    }
}

/// The columns of `g`'s result as the planner typed them (`cols`), for
/// the session's results character set.
pub(crate) fn layout(g: &General, cols: &[sprig_plan::Column], results: Results) -> Layout {
    // Columns placed by a `*` over a merging join were worked out from the
    // text: each must carry its column's name in the planner's result, or
    // nothing is placed.
    let placed = !g.explain
        && g.origins.len() == cols.len()
        && g.origins.iter().zip(cols).all(|(o, c)| {
            o.as_ref().is_none_or(|o| {
                !o.merged
                    || g.tables[o.table].def.columns[o.column]
                        .name
                        .eq_ignore_ascii_case(&c.name)
            })
        });
    let mut shapes = Vec::with_capacity(cols.len());
    let mut defs = Vec::with_capacity(cols.len());
    for (i, c) in cols.iter().enumerate() {
        let origin = if placed { g.origins[i].as_ref() } else { None };
        if let Some(o) = origin {
            let t = &g.tables[o.table];
            let col = &t.def.columns[o.column];
            let mut d = meta::column_def(
                &Bytes::from(t.db_name.clone()),
                &Bytes::from(o.alias.clone()),
                &t.def,
                &Bytes::from(t.def.name.clone()),
                o.column,
                &Bytes::from(c.name.clone()),
                &Bytes::from(col.name.clone()),
                results,
            );
            if o.outer {
                d.flags &= !ColumnFlags::NOT_NULL.bits();
            }
            defs.push(d);
            shapes.push(Shape::Column {
                table: o.table,
                column: o.column,
            });
        } else {
            // The item's text as the client wrote it, where known.
            let name = if placed {
                g.written.get(i).and_then(Option::as_deref)
            } else {
                None
            };
            defs.push(value_def(name.unwrap_or(&c.name), &c.ty, results));
            shapes.push(Shape::Value(c.ty));
        }
    }
    Layout { shapes, defs }
}

/// A `latin1` string in the results character set: as UTF-8, unless the
/// results are `latin1` too.
fn from_latin1(s: Bytes, coll: Collation, results: Results) -> Bytes {
    if coll.charset() != Charset::Latin1 || s.is_ascii() {
        return s;
    }
    if results
        .cs
        .is_none_or(|r| matches!(r.id, 5 | 8 | 15 | 31 | 47 | 48 | 49 | 94))
    {
        return s;
    }
    Bytes::from(
        s.iter()
            .map(|b| assign::latin1_char(*b))
            .collect::<String>(),
    )
}

/// A computed value's text.
fn value_text(v: &Datum, ty: &ExprType, results: Results) -> Option<Bytes> {
    let t = sprig_exec::value_text(v, ty, exec_persona(results))?;
    Some(if ty.kind == Kind::Str {
        from_latin1(t, ty.coll, results)
    } else {
        t
    })
}

/// The text protocol's bytes of value `v` of a column of shape `shape`;
/// `None` for NULL.
pub(crate) fn text(
    g: &General,
    shape: &Shape,
    v: &Datum,
    env: &Env,
    results: Results,
) -> Option<Bytes> {
    match shape {
        Shape::Column { table, column } => {
            meta::text_in(&g.tables[*table].def.columns[*column], v, env, results)
        }
        Shape::Value(ty) => value_text(v, ty, results),
    }
}

fn mismatch(v: &Datum, col_type: u8) -> SprigError {
    SprigError::internal(format!(
        "engine: a result value ({v:?}) that isn't of its column's type ({col_type})"
    ))
}

fn wire_datetime(d: sprig_engine::row::DateTime) -> Value {
    Value::DateTime(sprig_proto_mysql::resultset::DateTime {
        year: d.date.year,
        month: d.date.month,
        day: d.date.day,
        hour: d.hour,
        minute: d.minute,
        second: d.second,
        micro: d.micros,
    })
}

/// The binary protocol's value of a computed `v` for a column sent as
/// wire type `col_type`.
fn value_binary(
    v: &Datum,
    ty: &ExprType,
    col_type: u8,
    env: &Env,
    results: Results,
) -> Result<Value> {
    if v.is_null() {
        return Ok(Value::Null);
    }
    let text = || value_text(v, ty, results).unwrap_or_default();
    Ok(match col_type {
        column_type::NULL => Value::Null,
        column_type::TINY
        | column_type::SHORT
        | column_type::INT24
        | column_type::LONG
        | column_type::LONGLONG
        | column_type::YEAR => match v {
            Datum::Int(i) => Value::Int(*i),
            Datum::UInt(u) | Datum::Bit(u) | Datum::Set(u) => Value::UInt(*u),
            Datum::Year(y) | Datum::Enum(y) => Value::UInt(u64::from(*y)),
            _ => {
                // A whole number in another form: by its text.
                let t = text();
                let s = std::str::from_utf8(&t).unwrap_or("");
                let whole = s.split('.').next().unwrap_or("");
                if let Ok(i) = whole.parse::<i64>() {
                    Value::Int(i)
                } else if let Ok(u) = whole.parse::<u64>() {
                    Value::UInt(u)
                } else {
                    return Err(mismatch(v, col_type));
                }
            }
        },
        column_type::FLOAT | column_type::DOUBLE => match v {
            Datum::Double(d) => Value::Double(*d),
            Datum::Float(f) => Value::Float(*f),
            #[allow(clippy::cast_precision_loss)]
            Datum::Int(i) => Value::Double(*i as f64),
            #[allow(clippy::cast_precision_loss)]
            Datum::UInt(u) => Value::Double(*u as f64),
            _ => {
                let t = text();
                match std::str::from_utf8(&t)
                    .ok()
                    .and_then(|s| s.parse::<f64>().ok())
                {
                    Some(d) => Value::Double(d),
                    None => return Err(mismatch(v, col_type)),
                }
            }
        },
        column_type::DATE | column_type::DATETIME | column_type::TIMESTAMP => match v {
            Datum::Date(d) => wire_datetime(sprig_engine::row::DateTime::new(*d, 0, 0, 0, 0)),
            Datum::DateTime(d) => wire_datetime(*d),
            Datum::Timestamp(t) => wire_datetime(temporal::from_timestamp(*t, env.tz.unwrap_or(0))),
            _ => return Err(mismatch(v, col_type)),
        },
        column_type::TIME => match v {
            Datum::Time(t) => {
                let abs = t.micros().unsigned_abs();
                let secs = abs / 1_000_000;
                // A TIME is below 839 hours: every part fits its field.
                #[allow(clippy::cast_possible_truncation)]
                let w = sprig_proto_mysql::resultset::Time {
                    negative: t.micros() < 0,
                    days: (secs / 86_400) as u32,
                    hour: (secs / 3600 % 24) as u8,
                    minute: (secs / 60 % 60) as u8,
                    second: (secs % 60) as u8,
                    micro: (abs % 1_000_000) as u32,
                };
                Value::Time(w)
            }
            _ => return Err(mismatch(v, col_type)),
        },
        column_type::BIT => match v {
            Datum::Bit(u) | Datum::UInt(u) => {
                let bytes = u.to_be_bytes();
                let skip = bytes.iter().take_while(|b| **b == 0).count().min(7);
                Value::Bytes(Bytes::copy_from_slice(&bytes[skip..]))
            }
            _ => Value::Bytes(text()),
        },
        // Every length-encoded type: strings, DECIMAL text.
        _ => Value::Bytes(text()),
    })
}

/// The binary protocol's value of `v` for the column of shape `shape`
/// whose definition is `def`.
///
/// # Errors
/// An internal error when a computed value isn't of the type its column
/// was announced with (nothing wrong is sent).
pub(crate) fn binary(
    g: &General,
    shape: &Shape,
    def: &ColumnDef,
    v: &Datum,
    env: &Env,
    results: Results,
) -> Result<Value> {
    match shape {
        Shape::Column { table, column } => Ok(meta::binary(
            &g.tables[*table].def.columns[*column],
            v,
            env,
            results,
        )),
        Shape::Value(ty) => value_binary(v, ty, def.col_type, env, results),
    }
}
