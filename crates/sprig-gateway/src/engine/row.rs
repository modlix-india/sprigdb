//! Values and the stored row.
//!
//! A value is the engine's typed value (`sprig_engine::row::Value`), one
//! variant per stored type; expressions use the same variants for their
//! constants (`Int`, `UInt`, `Decimal`, `Double`, `Str`, `Bytes`).
//!
//! A row is stored as the engine's row record (`sprig_engine::row::record`:
//! format byte, schema version, null bitmap, fixed columns, variable
//! columns), for every table (the prototype's first row format is no
//! longer read). A row record names the version of the column list it was written
//! under; the table's layout (`super::layout`) reads every version the
//! table's rows may carry.
//!
//! Decoding is zero copy: string values are slices of the stored value.

use bytes::Bytes;
use sprig_common::Result;
use sprig_engine::row::record;

use super::keys::Space;
use super::table::TableDef;

/// One value as the executor holds it.
pub(crate) use sprig_engine::row::Value as Datum;

/// Truth value in MySQL's sense: `None` for NULL.
pub(crate) fn truth(d: &Datum) -> Option<bool> {
    Some(match d {
        Datum::Null => return None,
        Datum::Int(v) => *v != 0,
        Datum::UInt(v) | Datum::Bit(v) | Datum::Set(v) => *v != 0,
        Datum::Enum(v) | Datum::Year(v) => *v != 0,
        Datum::Decimal(v) => !v.is_zero(),
        Datum::Float(v) => *v != 0.0,
        Datum::Double(v) => *v != 0.0,
        Datum::Str(s) | Datum::Bytes(s) | Datum::Json(s) => super::expr::str_to_f64(s).0 != 0.0,
        Datum::Date(v) => v.year != 0 || v.month != 0 || v.day != 0,
        Datum::Time(v) => v.micros() != 0,
        Datum::DateTime(v) => *v != sprig_engine::row::DateTime::default(),
        Datum::Timestamp(v) => !v.is_zero(),
    })
}

/// Encodes `row` (one value per column of `def`, each already in its
/// column's stored form) as the table's rows are laid out (`sp`).
///
/// # Errors
/// A value its column can't hold: a fault of the caller, which converts
/// values first.
pub(crate) fn encode(def: &TableDef, sp: &Space, row: &[Datum]) -> Result<Bytes> {
    let mut cap = 8 + def.columns.len().div_ceil(8);
    for d in row {
        cap += match d {
            Datum::Str(s) | Datum::Bytes(s) | Datum::Json(s) => 2 + s.len(),
            Datum::Null => 0,
            Datum::Decimal(_) => 30,
            _ => 8,
        };
    }
    let mut out = Vec::with_capacity(cap);
    record::encode(&sp.layout.current, row, None, &mut out)?;
    Ok(Bytes::from(out))
}

/// Decodes a stored row of `def`, whatever version of the column list it
/// was written under.
///
/// # Errors
/// The row doesn't decode.
pub(crate) fn decode(_def: &TableDef, sp: &Space, raw: &Bytes) -> Result<Vec<Datum>> {
    sp.layout.decode(raw)
}

/// [`decode`] into a reused vector.
pub(crate) fn decode_into(
    _def: &TableDef,
    sp: &Space,
    raw: &Bytes,
    out: &mut Vec<Datum>,
) -> Result<()> {
    *out = sp.layout.decode(raw)?;
    Ok(())
}


