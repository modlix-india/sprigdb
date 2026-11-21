//! Key encodings of typed values: for every column type, ascending and
//! descending, byte order is value order, equal values have equal bytes, and
//! what can be decoded decodes to what was encoded.

#![allow(
    clippy::unwrap_used,
    clippy::missing_panics_doc,
    clippy::too_many_lines,
    clippy::many_single_char_names,
    clippy::similar_names
)]

mod types_common;

use std::cmp::Ordering;

use proptest::prelude::*;
use sprig_engine::keys::collation::Collation;
use sprig_engine::keys::typed::{KeyColumn, decode_column, decode_key, encode_column, encode_key};
use sprig_engine::row::{ColumnType, Value};
use types_common::{key_types, nullable_value_of, type_and_pair, value_of};

fn key(col: &KeyColumn, v: &Value) -> Vec<u8> {
    let mut out = Vec::new();
    encode_column(&mut out, col, v).unwrap();
    out
}

/// Whether a key of this type decodes back to its value.
fn reversible(ty: ColumnType) -> bool {
    ty.collation().is_none_or(|c| c == Collation::Binary)
}

/// Equality that treats `-0.0` and `0.0` as one value, as the key does.
fn same(ty: ColumnType, a: &Value, b: &Value) -> bool {
    ty.compare(a, b) == Ordering::Equal
}

#[test]
fn every_type_is_covered() {
    let types = key_types();
    for t in &types {
        t.validate().unwrap();
        assert!(t.is_key_type());
    }
    assert!(types.len() > 60);
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(6000))]

    #[test]
    fn key_order_is_value_order((ty, a, b) in type_and_pair(key_types()), desc in any::<bool>()) {
        let mut col = KeyColumn::new(ty).nullable();
        col.desc = desc;
        let want = ty.compare(&a, &b);
        let want = if desc { want.reverse() } else { want };
        prop_assert_eq!(key(&col, &a).cmp(&key(&col, &b)), want, "{:?} {:?} {:?}", ty, a, b);
    }

    #[test]
    fn keys_decode((ty, a, _b) in type_and_pair(key_types()), desc in any::<bool>(), nullable in any::<bool>()) {
        prop_assume!(nullable || !a.is_null());
        let mut col = KeyColumn::new(ty);
        col.desc = desc;
        col.nullable = nullable;
        let mut k = key(&col, &a);
        let len = k.len();
        k.extend_from_slice(&[0x00, 0xFF, 0x40]);
        let (got, rest) = decode_column(&k, &col).unwrap();
        prop_assert_eq!(rest.len(), 3, "{:?} {:?} used {} of {}", ty, a, k.len() - rest.len(), len);
        match got {
            Some(v) => {
                prop_assert!(reversible(ty) || a.is_null());
                prop_assert!(same(ty, &v, &a), "{:?}: {:?} came back as {:?}", ty, a, v);
                // Re-encoding the decoded value gives the same key.
                prop_assert_eq!(&key(&col, &v)[..], &k[..len]);
            }
            None => prop_assert!(!reversible(ty)),
        }
    }

    #[test]
    fn not_null_columns_have_no_marker((ty, a) in proptest::sample::select(key_types()).prop_flat_map(|t| (Just(t), value_of(t)))) {
        let plain = key(&KeyColumn::new(ty), &a);
        let marked = key(&KeyColumn::new(ty).nullable(), &a);
        prop_assert_eq!(&marked[1..], &plain[..]);
        prop_assert_eq!(marked[0], 1);
    }
}

fn composite() -> impl Strategy<Value = (Vec<KeyColumn>, Vec<Value>, Vec<Value>)> {
    proptest::collection::vec(
        (
            proptest::sample::select(key_types()),
            any::<bool>(),
            any::<bool>(),
        ),
        1..5,
    )
    .prop_flat_map(|spec| {
        let cols: Vec<KeyColumn> = spec
            .iter()
            .map(|&(ty, desc, nullable)| {
                let mut c = KeyColumn::new(ty);
                c.desc = desc;
                c.nullable = nullable;
                c
            })
            .collect();
        let row = || -> Vec<BoxedStrategy<Value>> {
            cols.iter()
                .map(|c| {
                    if c.nullable {
                        nullable_value_of(c.ty)
                    } else {
                        value_of(c.ty)
                    }
                })
                .collect()
        };
        (Just(cols.clone()), row(), row())
    })
}

/// A second row that shares a prefix of the first's columns, so later
/// columns decide the order.
fn share_prefix(a: &[Value], b: &[Value], n: usize) -> Vec<Value> {
    a.iter().take(n).chain(b.iter().skip(n)).cloned().collect()
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(3000))]

    #[test]
    fn composite_keys((cols, a, b) in composite(), shared in 0usize..5) {
        let b = share_prefix(&a, &b, shared.min(cols.len()));
        let (mut ka, mut kb) = (Vec::new(), Vec::new());
        encode_key(&mut ka, &cols, &a).unwrap();
        encode_key(&mut kb, &cols, &b).unwrap();
        let mut want = Ordering::Equal;
        for ((c, x), y) in cols.iter().zip(&a).zip(&b) {
            let o = c.ty.compare(x, y);
            want = if c.desc { o.reverse() } else { o };
            if want.is_ne() {
                break;
            }
        }
        prop_assert_eq!(ka.cmp(&kb), want, "{:?} {:?} {:?}", cols, a, b);
        // No key is a proper prefix of another: what follows a key (the
        // primary key in an index entry, the version) can't change the order.
        if want.is_ne() {
            prop_assert!(!ka.starts_with(&kb) && !kb.starts_with(&ka));
        }
        let (got, rest) = decode_key(&ka, &cols).unwrap();
        prop_assert!(rest.is_empty());
        for ((c, v), g) in cols.iter().zip(&a).zip(&got) {
            match g {
                Some(g) => prop_assert!(same(c.ty, g, v)),
                None => prop_assert!(!reversible(c.ty)),
            }
        }
    }

    #[test]
    fn index_prefixes((ty, a) in proptest::sample::select(key_types())
            .prop_filter("strings", |t| matches!(t, ColumnType::VarChar { .. } | ColumnType::Text { .. } | ColumnType::VarBinary { .. } | ColumnType::Blob { .. }))
            .prop_flat_map(|t| (Just(t), value_of(t))),
        n in 0u32..8,
    ) {
        let whole = KeyColumn::new(ty);
        let cut = match &a {
            Value::Str(s) => {
                let cs = ty.collation().unwrap().charset();
                Value::Str(s.slice(..cs.char_prefix(s, n as usize).len()))
            }
            Value::Bytes(b) => Value::Bytes(b.slice(..b.len().min(n as usize))),
            other => other.clone(),
        };
        prop_assert_eq!(key(&whole.prefix(n), &a), key(&whole, &cut));
    }
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(20_000))]

    /// Bytes that are no key are refused or read as some value, never a
    /// panic; and what does decode is a storable value of the column.
    #[test]
    fn arbitrary_bytes_never_panic(
        ty in proptest::sample::select(key_types()),
        desc in any::<bool>(),
        nullable in any::<bool>(),
        bytes in proptest::collection::vec(any::<u8>(), 0..40),
    ) {
        let mut col = KeyColumn::new(ty);
        col.desc = desc;
        col.nullable = nullable;
        if let Ok((Some(v), rest)) = decode_column(&bytes, &col) {
            prop_assert!(rest.len() <= bytes.len());
            if !matches!(
                ty,
                ColumnType::Char { .. }
                    | ColumnType::VarChar { .. }
                    | ColumnType::Text { .. }
                    | ColumnType::Binary { .. }
                    | ColumnType::VarBinary { .. }
                    | ColumnType::Blob { .. }
            ) {
                prop_assert!(ty.check(&v).is_ok(), "{:?} decoded to {:?}", ty, v);
            }
        }
        let _ = decode_key(&bytes, &[col, col]);
    }
}
