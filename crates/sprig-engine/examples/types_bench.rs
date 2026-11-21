//! Rough speed of the typed encoders: nanoseconds per call for key
//! encoding by type and collation, and for the row record.
//!
//! `cargo run --release -p sprig-engine --example types_bench`

#![allow(clippy::unwrap_used, clippy::cast_precision_loss)]

use std::hint::black_box;
use std::time::Instant;

use bytes::Bytes;
use sprig_engine::keys::collation::Collation;
use sprig_engine::keys::typed::{KeyColumn, decode_column, encode_column};
use sprig_engine::row::record::{self, Column, RowSchema};
use sprig_engine::row::{ColumnType, Date, DateTime, Decimal, Value};

fn time(name: &str, iterations: u32, mut f: impl FnMut()) {
    // Warm up: tables load on first use.
    for _ in 0..1000 {
        f();
    }
    let start = Instant::now();
    for _ in 0..iterations {
        f();
    }
    let ns = start.elapsed().as_nanos() as f64 / f64::from(iterations);
    println!("{name:<44} {ns:>9.1} ns");
}

fn key_case(name: &str, ty: ColumnType, v: &Value) {
    let col = KeyColumn::new(ty);
    let mut out = Vec::with_capacity(256);
    time(&format!("key encode  {name}"), 2_000_000, || {
        out.clear();
        encode_column(&mut out, &col, black_box(v)).unwrap();
        black_box(&out);
    });
    let key = out.clone();
    time(&format!("key decode  {name}"), 2_000_000, || {
        black_box(decode_column(black_box(&key), &col).unwrap());
    });
}

fn main() {
    key_case("INT", ColumnType::int(false), &Value::Int(-123_456));
    key_case(
        "BIGINT UNSIGNED",
        ColumnType::bigint(true),
        &Value::UInt(u64::MAX - 5),
    );
    key_case(
        "DECIMAL(18,4)",
        ColumnType::Decimal {
            precision: 18,
            scale: 4,
        },
        &Value::Decimal(Decimal::parse("-12345678901.2300").unwrap()),
    );
    key_case("DOUBLE", ColumnType::Double, &Value::Double(-1234.5e-7));
    key_case(
        "DATETIME(6)",
        ColumnType::DateTime { fsp: 6 },
        &Value::DateTime(DateTime::new(Date::new(2026, 11, 21), 0, 45, 59, 123_456)),
    );
    let ascii = Value::str("Order 4711 shipped");
    let mixed = Value::str("Stra\u{df}e 12, K\u{f6}ln \u{4e2d}\u{6587}");
    for c in [
        Collation::Binary,
        Collation::Utf8mb4Bin,
        Collation::Utf8mb4GeneralCi,
        Collation::Utf8mb4Uca1400AiCi,
        Collation::Utf8mb4Uca1400AsCs,
        Collation::Utf8mb4UnicodeCi,
        Collation::Utf8mb4Mysql0900AiCi,
        Collation::Utf8mb4Mysql0900AsCs,
        Collation::Latin1SwedishCi,
    ] {
        let ty = ColumnType::VarChar {
            chars: 64,
            collation: c,
        };
        key_case(&format!("VARCHAR 18 ASCII  {}", c.name()), ty, &ascii);
        if c.charset() == sprig_engine::keys::collation::Charset::Utf8mb4 {
            key_case(&format!("VARCHAR 19 mixed  {}", c.name()), ty, &mixed);
        }
    }

    // A row shaped like sysbench's: two integers and two strings.
    let text = |chars| ColumnType::Char {
        chars,
        collation: Collation::Utf8mb4Uca1400AiCi,
    };
    let schema = RowSchema::new(
        1,
        vec![
            Column::new(1, ColumnType::int(false)).not_null(Value::Int(0)),
            Column::new(2, ColumnType::int(false)).not_null(Value::Int(0)),
            Column::new(3, text(120)).not_null(Value::str("")),
            Column::new(4, text(60)).not_null(Value::str("")),
        ],
    )
    .unwrap();
    let values = [
        Value::Int(424_242),
        Value::Int(500_000),
        Value::Str(Bytes::from(vec![b'7'; 119])),
        Value::Str(Bytes::from(vec![b'3'; 59])),
    ];
    let mut out = Vec::with_capacity(256);
    time("row encode  4 columns, 190 bytes", 2_000_000, || {
        out.clear();
        record::encode(&schema, black_box(&values), None, &mut out).unwrap();
        black_box(&out);
    });
    let row = Bytes::from(out.clone());
    time("row decode  4 columns", 2_000_000, || {
        black_box(record::decode(&schema, black_box(&row)).unwrap());
    });
    time("row decode  one column (the last)", 2_000_000, || {
        black_box(record::decode_column(&schema, black_box(&row), 3).unwrap());
    });
}
