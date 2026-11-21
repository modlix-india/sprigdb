//! The row record: every column type round-trips, one column reads without
//! the others, and a row written under an older schema version reads under a
//! newer one.

#![allow(
    clippy::unwrap_used,
    clippy::missing_panics_doc,
    clippy::too_many_lines,
    clippy::many_single_char_names,
    clippy::similar_names
)]

mod types_common;

use bytes::Bytes;
use proptest::prelude::*;
use sprig_engine::row::record::{
    self, Column, RowSchema, decode, decode_as, decode_column, decode_column_as, encode,
    peek_header, reads_as,
};
use sprig_engine::row::{ColumnType, Value};
use types_common::{all_types, nullable_value_of, value_of};

/// Equality of stored values: bit for bit, so `-0.0` isn't `0.0`.
fn identical(a: &Value, b: &Value) -> bool {
    match (a, b) {
        (Value::Float(x), Value::Float(y)) => x.to_bits() == y.to_bits(),
        (Value::Double(x), Value::Double(y)) => x.to_bits() == y.to_bits(),
        _ => a == b,
    }
}

fn row_types() -> Vec<ColumnType> {
    let mut t = all_types();
    t.push(ColumnType::Json);
    t
}

fn schema_and_rows() -> impl Strategy<Value = (RowSchema, Vec<Value>, Option<u64>)> {
    proptest::collection::vec(
        (proptest::sample::select(row_types()), any::<bool>()),
        0..12,
    )
    .prop_flat_map(|spec| {
        let defaults: Vec<BoxedStrategy<Value>> =
            spec.iter().map(|&(ty, _)| value_of(ty)).collect();
        let vals: Vec<BoxedStrategy<Value>> = spec
            .iter()
            .map(|&(ty, nullable)| {
                if nullable {
                    nullable_value_of(ty)
                } else {
                    value_of(ty)
                }
            })
            .collect();
        (
            Just(spec),
            defaults,
            vals,
            proptest::option::of(any::<u64>()),
            any::<u32>(),
        )
    })
    .prop_map(|(spec, defaults, vals, expiry, version)| {
        let cols = spec
            .iter()
            .zip(defaults)
            .enumerate()
            .map(|(i, (&(ty, nullable), d))| {
                let c = Column::new(u32::try_from(i).unwrap() * 3 + 1, ty);
                if nullable { c } else { c.not_null(d) }
            })
            .collect();
        (RowSchema::new(version, cols).unwrap(), vals, expiry)
    })
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(4000))]

    #[test]
    fn rows_round_trip((schema, vals, expiry) in schema_and_rows()) {
        let mut out = vec![0xAA];
        encode(&schema, &vals, expiry, &mut out).unwrap();
        let row = Bytes::from(out).slice(1..);
        let header = peek_header(&row).unwrap();
        prop_assert_eq!(header.schema_version, schema.version());
        prop_assert_eq!(header.expires_at, expiry);
        let got = decode(&schema, &row).unwrap();
        prop_assert_eq!(got.len(), vals.len());
        for (i, (g, v)) in got.iter().zip(&vals).enumerate() {
            prop_assert!(identical(g, v), "column {}: {:?} came back as {:?}", i, v, g);
            let one = decode_column(&schema, &row, i).unwrap();
            prop_assert!(identical(&one, v), "column {} alone: {:?} came back as {:?}", i, v, one);
        }
        // Every proper prefix of a row is refused, never misread.
        for cut in 0..row.len() {
            prop_assert!(decode(&schema, &row.slice(..cut)).is_err(), "cut at {}", cut);
        }
    }

    #[test]
    fn damaged_rows_never_panic((schema, vals, expiry) in schema_and_rows(), at in any::<prop::sample::Index>(), bit in 0u8..8) {
        let mut out = Vec::new();
        encode(&schema, &vals, expiry, &mut out).unwrap();
        let i = at.index(out.len());
        out[i] ^= 1 << bit;
        let row = Bytes::from(out);
        let _ = decode(&schema, &row);
        for c in 0..schema.columns().len() {
            let _ = decode_column(&schema, &row, c);
        }
        let _ = peek_header(&row);
    }
}

/// A type `ty` widens to, if the test knows one.
fn wider(ty: ColumnType) -> Option<ColumnType> {
    use ColumnType as T;
    Some(match ty {
        T::Int { bytes, unsigned } if bytes < 8 => T::Int {
            bytes: if bytes == 4 { 8 } else { bytes + 1 },
            unsigned,
        },
        T::Bit { bits } if bits < 64 => T::Bit { bits: bits + 1 },
        T::Decimal { precision, scale } if precision < 64 && scale < 38 => T::Decimal {
            precision: precision + 1,
            scale: scale + 1,
        },
        T::Float => T::Double,
        T::Time { fsp } if fsp < 6 => T::Time { fsp: fsp + 1 },
        T::DateTime { fsp } if fsp < 6 => T::DateTime { fsp: 6 },
        T::Timestamp { fsp } if fsp < 6 => T::Timestamp { fsp: fsp + 1 },
        T::Char { chars, collation } => T::VarChar {
            chars: u32::from(chars) + 1,
            collation,
        },
        T::VarChar { collation, .. } => T::Text {
            size: sprig_engine::row::LobSize::Medium,
            collation,
        },
        T::Binary { bytes } => T::VarBinary {
            bytes: u32::from(bytes) + 3,
        },
        T::VarBinary { .. } => T::Blob {
            size: sprig_engine::row::LobSize::Normal,
        },
        T::Enum { members } if members < 65_535 => T::Enum {
            members: members + 1,
        },
        T::Set { members } if members < 64 => T::Set {
            members: members + 1,
        },
        _ => return None,
    })
}

/// What a stored value is after its column widened, worked out from the
/// value's text rather than by the code under test.
fn widened(v: &Value, to: ColumnType) -> Value {
    match (v, to) {
        (
            Value::UInt(u),
            ColumnType::Int {
                unsigned: false, ..
            },
        ) => Value::Int(i64::try_from(*u).unwrap()),
        (Value::Decimal(d), ColumnType::Decimal { scale, .. }) => {
            let mut text = d.to_string();
            if d.scale() == 0 {
                text.push('.');
            }
            text.extend(std::iter::repeat_n('0', usize::from(scale - d.scale())));
            Value::Decimal(sprig_engine::row::Decimal::parse(&text).unwrap())
        }
        (Value::Float(f), ColumnType::Double) => {
            Value::Double(f.to_string().parse::<f32>().map(f64::from).unwrap())
        }
        _ => v.clone(),
    }
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(3000))]

    #[test]
    fn older_versions_read((schema, vals, expiry) in schema_and_rows(), plan in proptest::collection::vec(0u8..4, 12), added in proptest::collection::vec(proptest::sample::select(row_types()).prop_flat_map(|t| (Just(t), value_of(t), any::<bool>())), 0..4), rotate in 0usize..12) {
        prop_assume!(schema.version() < u32::MAX);
        let mut out = Vec::new();
        encode(&schema, &vals, expiry, &mut out).unwrap();
        let row = Bytes::from(out);
        // The newer version: per old column keep (0, 1), widen (2) or drop
        // (3); then new columns; then a rotation, so positions move.
        let mut cols = Vec::new();
        let mut want = Vec::new();
        for ((c, v), step) in schema.columns().iter().zip(&vals).zip(&plan) {
            match (step, wider(c.ty)) {
                (3, _) => {}
                (2, Some(ty)) => {
                    prop_assert!(reads_as(&c.ty, &ty));
                    let mut n = c.clone();
                    n.ty = ty;
                    n.default = widened(&c.default, ty);
                    cols.push(n);
                    want.push(widened(v, ty));
                }
                _ => {
                    cols.push(c.clone());
                    want.push(v.clone());
                }
            }
        }
        for (i, (ty, default, nullable)) in added.into_iter().enumerate() {
            let c = Column::new(1000 + u32::try_from(i).unwrap(), ty);
            if nullable {
                cols.push(c);
                want.push(Value::Null);
            } else {
                want.push(default.clone());
                cols.push(c.not_null(default));
            }
        }
        if !cols.is_empty() {
            let r = rotate % cols.len();
            cols.rotate_left(r);
            want.rotate_left(r);
        }
        let newer = RowSchema::new(schema.version() + 1, cols).unwrap();
        let got = decode_as(&schema, &newer, &row).unwrap();
        prop_assert_eq!(got.len(), want.len());
        for (i, (g, w)) in got.iter().zip(&want).enumerate() {
            prop_assert!(identical(g, w), "column {}: wanted {:?}, got {:?}", i, w, g);
            let one = decode_column_as(&schema, &newer, &row, i).unwrap();
            prop_assert!(identical(&one, w), "column {} alone: wanted {:?}, got {:?}", i, w, one);
        }
        // The widened values are storable in the newer schema as they are.
        let mut again = Vec::new();
        encode(&newer, &got, expiry, &mut again).unwrap();
        prop_assert_eq!(decode(&newer, &Bytes::from(again)).unwrap().len(), want.len());
    }
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(5000))]

    /// Bytes that are no row are refused, never a panic.
    #[test]
    fn arbitrary_bytes_never_panic((schema, _vals, _expiry) in schema_and_rows(), mut bytes in proptest::collection::vec(any::<u8>(), 0..60), version_ok in any::<bool>()) {
        if version_ok && bytes.len() > 1 && schema.version() < 128 {
            // Get past the header checks more often.
            bytes[0] = record::FORMAT;
            bytes[1] = u8::try_from(schema.version()).unwrap();
        }
        let row = Bytes::from(bytes);
        let _ = decode(&schema, &row);
        for c in 0..schema.columns().len() {
            let _ = decode_column(&schema, &row, c);
        }
        let _ = peek_header(&row);
    }
}

#[test]
fn format_constant() {
    assert_eq!(record::FORMAT, 1);
}
