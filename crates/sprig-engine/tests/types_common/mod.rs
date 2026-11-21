//! Shared by the typed-value tests: a list of column types that covers
//! every type and its parameter edges, and generators of storable values.

#![allow(
    dead_code,
    unreachable_pub,
    clippy::unwrap_used,
    clippy::missing_panics_doc,
    clippy::too_many_lines,
    clippy::cast_precision_loss,
    clippy::cast_possible_truncation,
    clippy::cast_sign_loss
)]

use bytes::Bytes;
use proptest::prelude::*;
use sprig_engine::keys::collation::{Charset, Collation};
use sprig_engine::row::temporal::Time;
use sprig_engine::row::{ColumnType, Date, DateTime, Decimal, LobSize, Timestamp, Value};

/// The collations string columns are tested under.
pub fn collations() -> Vec<Collation> {
    Collation::all().to_vec()
}

/// Every type, with the parameter values at which its encoding changes
/// shape. Character types appear once per collation.
pub fn all_types() -> Vec<ColumnType> {
    let mut t = Vec::new();
    for bytes in [1u8, 2, 3, 4, 8] {
        for unsigned in [false, true] {
            t.push(ColumnType::Int { bytes, unsigned });
        }
    }
    for bits in [1u8, 7, 8, 9, 33, 63, 64] {
        t.push(ColumnType::Bit { bits });
    }
    for (precision, scale) in [
        (1u8, 0u8),
        (1, 1),
        (5, 2),
        (10, 0),
        (18, 9),
        (38, 38),
        (65, 0),
        (65, 30),
        (65, 38),
    ] {
        t.push(ColumnType::Decimal { precision, scale });
    }
    t.extend([
        ColumnType::Float,
        ColumnType::Double,
        ColumnType::Date,
        ColumnType::Year,
    ]);
    for fsp in 0u8..=6 {
        t.push(ColumnType::Time { fsp });
        t.push(ColumnType::DateTime { fsp });
        t.push(ColumnType::Timestamp { fsp });
    }
    for collation in collations() {
        t.push(ColumnType::Char {
            chars: 6,
            collation,
        });
        t.push(ColumnType::VarChar {
            chars: 12,
            collation,
        });
        t.push(ColumnType::Text {
            size: LobSize::Normal,
            collation,
        });
    }
    t.extend([
        ColumnType::Binary { bytes: 0 },
        ColumnType::Binary { bytes: 4 },
        ColumnType::VarBinary { bytes: 9 },
        ColumnType::Blob {
            size: LobSize::Tiny,
        },
        ColumnType::Blob {
            size: LobSize::Long,
        },
        ColumnType::Enum { members: 3 },
        ColumnType::Enum { members: 65_535 },
        ColumnType::Set { members: 1 },
        ColumnType::Set { members: 64 },
    ]);
    t
}

/// The types that can be key columns (all of [`all_types`]; `JSON` isn't in
/// it).
pub fn key_types() -> Vec<ColumnType> {
    all_types()
}

fn round_micros(micros: u32, fsp: u8) -> u32 {
    let unit = 10u32.pow(6 - u32::from(fsp));
    micros / unit * unit
}

fn micros() -> impl Strategy<Value = u32> {
    prop_oneof![Just(0u32), Just(999_999), Just(500_000), 0u32..1_000_000]
}

/// Characters that exercise the collations: case and accent pairs,
/// expansions, contractions, ignorables, characters below and equal to the
/// space, CJK, and a supplementary character.
const UNICODE_ALPHABET: &[char] = &[
    ' ',
    ' ',
    '\t',
    '\u{1}',
    '\0',
    'a',
    'A',
    'b',
    'B',
    'z',
    '0',
    '9',
    '_',
    '-',
    '!',
    '~',
    '\u{e9}',
    '\u{c9}',
    '\u{e0}',
    '\u{df}',
    's',
    'S',
    '\u{e6}',
    '\u{f1}',
    '\u{fc}',
    '\u{a0}',
    '\u{ad}',
    '\u{200b}',
    '\u{3000}',
    '\u{0301}',
    '\u{0306}',
    '\u{0418}',
    '\u{0438}',
    '\u{0419}',
    '\u{3b1}',
    '\u{391}',
    '\u{4e00}',
    '\u{4e01}',
    '\u{ff21}',
    '\u{1F600}',
    '\u{1F4A9}',
    'L',
    'l',
    '\u{b7}',
    '\u{131}',
    '\u{130}',
    'i',
    'I',
    '\u{fb01}',
    '\u{2126}',
    '\u{1e9e}',
    '\u{ffff}',
];

/// A string of at most `max` characters in the collation's character set.
pub fn string_for(collation: Collation, max: usize) -> BoxedStrategy<Vec<u8>> {
    match collation.charset() {
        Charset::Utf8mb4 => {
            proptest::collection::vec(proptest::sample::select(UNICODE_ALPHABET), 0..=max)
                .prop_map(|v| v.into_iter().collect::<String>().into_bytes())
                .boxed()
        }
        Charset::Ascii => proptest::collection::vec(
            prop_oneof![Just(b' '), Just(b'\t'), Just(b'a'), Just(b'A'), 0u8..0x80],
            0..=max,
        )
        .boxed(),
        Charset::Latin1 | Charset::Binary => proptest::collection::vec(
            prop_oneof![
                Just(b' '),
                Just(b'\t'),
                Just(b'a'),
                Just(b'A'),
                Just(0xE9u8),
                Just(0xC9u8),
                Just(0xDFu8),
                Just(0u8),
                Just(0xFFu8),
                any::<u8>()
            ],
            0..=max,
        )
        .boxed(),
    }
}

fn strip_spaces(mut s: Vec<u8>) -> Vec<u8> {
    while s.last() == Some(&b' ') {
        s.pop();
    }
    s
}

fn decimal_of(precision: u8, scale: u8) -> impl Strategy<Value = Decimal> {
    let int = usize::from(precision - scale);
    let digit = || prop_oneof![Just(0u8), Just(9u8), 0u8..10];
    (
        any::<bool>(),
        proptest::collection::vec(digit(), 0..=int),
        proptest::collection::vec(digit(), usize::from(scale)),
    )
        .prop_map(|(neg, i, f)| Decimal::from_parts(neg, &i, &f).unwrap())
}

/// A storable, non-`NULL` value of `ty`, biased towards the edges of its
/// domain.
pub fn value_of(ty: ColumnType) -> BoxedStrategy<Value> {
    match ty {
        ColumnType::Int {
            bytes,
            unsigned: false,
        } => {
            let bits = u32::from(bytes) * 8;
            let (lo, hi) = if bits == 64 {
                (i64::MIN, i64::MAX)
            } else {
                (-(1i64 << (bits - 1)), (1i64 << (bits - 1)) - 1)
            };
            prop_oneof![
                Just(lo),
                Just(hi),
                Just(0i64),
                Just(-1i64),
                Just(1i64),
                lo..=hi
            ]
            .prop_map(Value::Int)
            .boxed()
        }
        ColumnType::Int {
            bytes,
            unsigned: true,
        } => {
            let hi = if bytes == 8 {
                u64::MAX
            } else {
                (1u64 << (u32::from(bytes) * 8)) - 1
            };
            prop_oneof![Just(0u64), Just(hi), Just(1u64), 0..=hi]
                .prop_map(Value::UInt)
                .boxed()
        }
        ColumnType::Bit { bits } => {
            let hi = if bits == 64 {
                u64::MAX
            } else {
                (1u64 << bits) - 1
            };
            prop_oneof![Just(0u64), Just(hi), 0..=hi]
                .prop_map(Value::Bit)
                .boxed()
        }
        ColumnType::Decimal { precision, scale } => decimal_of(precision, scale)
            .prop_map(Value::Decimal)
            .boxed(),
        ColumnType::Float => prop_oneof![
            Just(0.0f32),
            Just(-0.0f32),
            Just(f32::MAX),
            Just(f32::MIN),
            Just(f32::MIN_POSITIVE),
            Just(1e-45f32),
            Just(-1e-45f32),
            any::<f32>().prop_filter("finite", |f| f.is_finite()),
            (-1000i32..1000).prop_map(|i| i as f32 / 8.0),
        ]
        .prop_map(Value::Float)
        .boxed(),
        ColumnType::Double => prop_oneof![
            Just(0.0f64),
            Just(-0.0f64),
            Just(f64::MAX),
            Just(f64::MIN),
            Just(f64::MIN_POSITIVE),
            Just(5e-324f64),
            Just(-5e-324f64),
            any::<f64>().prop_filter("finite", |f| f.is_finite()),
            (-1000i32..1000).prop_map(|i| f64::from(i) / 8.0),
        ]
        .prop_map(Value::Double)
        .boxed(),
        ColumnType::Date => date().prop_map(Value::Date).boxed(),
        ColumnType::Time { fsp } => (prop_oneof![
            Just(-Time::MAX_MICROS),
            Just(Time::MAX_MICROS),
            Just(0i64),
            -Time::MAX_MICROS..=Time::MAX_MICROS,
            -5_000_000i64..5_000_000
        ])
        .prop_map(move |m| {
            let unit = 10i64.pow(6 - u32::from(fsp));
            Value::Time(Time::from_micros(m / unit * unit).unwrap())
        })
        .boxed(),
        ColumnType::DateTime { fsp } => (date(), 0u8..24, 0u8..60, 0u8..60, micros())
            .prop_map(move |(d, h, mi, s, us)| {
                Value::DateTime(DateTime::new(d, h, mi, s, round_micros(us, fsp)))
            })
            .boxed(),
        ColumnType::Timestamp { fsp } => (
            prop_oneof![
                Just(0u32),
                Just(1u32),
                Just(u32::MAX),
                Just(i32::MAX as u32),
                any::<u32>()
            ],
            micros(),
        )
            .prop_map(move |(s, us)| Value::Timestamp(Timestamp::new(s, round_micros(us, fsp))))
            .boxed(),
        ColumnType::Year => prop_oneof![Just(0u16), Just(1901u16), Just(2155u16), 1901u16..=2155]
            .prop_map(Value::Year)
            .boxed(),
        ColumnType::Char { chars, collation } => string_for(collation, usize::from(chars))
            .prop_map(|s| Value::Str(Bytes::from(strip_spaces(s))))
            .boxed(),
        ColumnType::VarChar { chars, collation } => string_for(collation, chars as usize)
            .prop_map(|s| Value::Str(Bytes::from(s)))
            .boxed(),
        ColumnType::Text { collation, .. } => string_for(collation, 20)
            .prop_map(|s| Value::Str(Bytes::from(s)))
            .boxed(),
        ColumnType::Binary { bytes } => string_for(Collation::Binary, usize::from(bytes))
            .prop_map(move |mut s| {
                s.resize(usize::from(bytes), 0);
                Value::Bytes(Bytes::from(s))
            })
            .boxed(),
        ColumnType::VarBinary { bytes } => string_for(Collation::Binary, bytes as usize)
            .prop_map(|s| Value::Bytes(Bytes::from(s)))
            .boxed(),
        ColumnType::Blob { .. } => string_for(Collation::Binary, 20)
            .prop_map(|s| Value::Bytes(Bytes::from(s)))
            .boxed(),
        ColumnType::Enum { members } => prop_oneof![Just(0u16), Just(members), 0..=members]
            .prop_map(Value::Enum)
            .boxed(),
        ColumnType::Set { members } => {
            let hi = if members == 64 {
                u64::MAX
            } else {
                (1u64 << members) - 1
            };
            prop_oneof![Just(0u64), Just(hi), 0..=hi]
                .prop_map(Value::Set)
                .boxed()
        }
        ColumnType::Json => proptest::collection::vec(any::<u8>(), 0..40)
            .prop_map(|b| Value::Json(Bytes::from(b)))
            .boxed(),
    }
}

fn date() -> impl Strategy<Value = Date> {
    prop_oneof![
        Just(Date::new(0, 0, 0)),
        Just(Date::new(9999, 12, 31)),
        (0u16..=9999, 0u8..=12, 0u8..=31).prop_map(|(y, m, d)| Date::new(y, m, d)),
        (1990u16..=2030, 1u8..=12, 1u8..=28).prop_map(|(y, m, d)| Date::new(y, m, d)),
    ]
}

/// A value of `ty`, or `NULL` one time in six.
pub fn nullable_value_of(ty: ColumnType) -> BoxedStrategy<Value> {
    prop_oneof![1 => Just(Value::Null), 5 => value_of(ty)].boxed()
}

/// A type from `types`, with two values of it.
pub fn type_and_pair(types: Vec<ColumnType>) -> impl Strategy<Value = (ColumnType, Value, Value)> {
    proptest::sample::select(types)
        .prop_flat_map(|ty| (Just(ty), nullable_value_of(ty), nullable_value_of(ty)))
}
