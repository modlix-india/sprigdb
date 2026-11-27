//! Result metadata of engine tables, as each persona's server reports it
//! (observed from MariaDB 11.8 and MySQL 8.4 as black boxes).
//!
//! **Character set id of a string column:** the collation of the results
//! as the session set it (the handshake's collation, or `SET NAMES cs
//! [COLLATE c]`); with `character_set_results = NULL`, the column's own
//! collation. MariaDB 11.8 maps a handshake naming its character set's
//! compiled default (`utf8mb4_general_ci`, 45) through
//! `character_set_collations` to `utf8mb4_uca1400_ai_ci`, and reports
//! the `uca1400` collations, whose ids don't fit one byte, as the
//! `unicode_ci` collation of the same character set (`utf8mb4`: 224,
//! `utf8mb3`: 192). MySQL 8.4 reports the handshake's id as it came.
//!
//! **Type, length, decimals** per column type (the same on both servers
//! unless said):
//!
//! | Column | Wire type | Length | Decimals |
//! |---|---|---|---|
//! | integers | `TINY` … `LONGLONG` | the display width (`int(11)`: 11) | 0 |
//! | `BIT(n)` | `BIT` | n | 0 |
//! | `DECIMAL(m,d)` | `NEWDECIMAL` | m, +1 with decimals, +1 when signed | d |
//! | `FLOAT`, `DOUBLE` | `FLOAT`, `DOUBLE` | 12, 22 (`(M,D)`: M) | 31 (`(M,D)`: D) |
//! | `DATE`, `TIME(f)`, `DATETIME(f)`, `TIMESTAMP(f)`, `YEAR` | their own | 10; 10, 19, 19 (+ f + 1 with a fraction); 4 | f |
//! | `CHAR(n)`, `VARCHAR(n)` | `STRING`, `VAR_STRING` | n × the results character set's maximum bytes per character | 0 |
//! | `TEXT` family | `BLOB` | the type's byte limit × the same, at most 2^32 − 1 | 0 |
//! | `BINARY(n)`, `VARBINARY(n)`, `BLOB` family | `STRING`, `VAR_STRING`, `BLOB` | n; the byte limit | 0 |
//! | `ENUM`, `SET` | `STRING` | the longest member (`SET`: all members and commas) × the same | 0 |
//! | `JSON` | MariaDB: `BLOB` with the extended type `json`; MySQL: `JSON`, binary | 2^32 − 1 | 0 |
//!
//! **Flags:** the base column's (`table::parse` computes them, as MariaDB
//! reports them; MySQL leaves `UNSIGNED` off a `TIMESTAMP`).
//!
//! **Values** ([`text`], [`binary`]): the text protocol's bytes and the
//! binary protocol's value of each stored value.

use std::collections::HashMap;
use std::sync::OnceLock;

use bytes::Bytes;
use sprig_engine::keys::collation::{Charset, Collation};
use sprig_proto_mysql::resultset::{ColumnFlags, column_type};
use sprig_proto_mysql::{ColumnDef, Persona, Value};

use super::assign::{self, Env};
use super::expr;
use super::row::Datum;
use super::table::{ColType, Column, TableDef};
use super::temporal;
use crate::backend::charset;

/// The results' collation as reported, and its character set's maximum
/// bytes per character.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct ResultsCs {
    /// The id reported.
    pub id: u16,
    /// Maximum bytes per character.
    pub mbmax: u32,
}

/// What a session's results are encoded in, and which server's metadata
/// its port reports.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct Results {
    /// The results' collation; `None`: `character_set_results` is NULL
    /// (each column reports its own).
    pub cs: Option<ResultsCs>,
    /// The MySQL port.
    pub mysql: bool,
}

fn mbmax_of(cs: &str) -> u32 {
    match cs.to_ascii_lowercase().as_str() {
        "utf8mb4" | "utf16" | "utf32" | "utf16le" | "gb18030" => 4,
        "utf8mb3" | "utf8" | "ujis" | "eucjpms" => 3,
        "big5" | "sjis" | "cp932" | "gbk" | "gb2312" | "euckr" | "ucs2" => 2,
        _ => 1,
    }
}

/// The handshake id of collation `name`, if a client can name it.
pub(crate) fn id_of(name: &str) -> Option<u16> {
    // Asked once per statement for the session's results collation, so
    // the table is indexed by name once (lowest id first, as a scan of
    // the ids in order finds it).
    static BY_NAME: OnceLock<HashMap<String, u16>> = OnceLock::new();
    let by_name = BY_NAME.get_or_init(|| {
        let mut m = HashMap::new();
        for i in 1..=255u16 {
            if let Some((n, _)) = charset::by_id(i) {
                m.entry(n.to_ascii_lowercase()).or_insert(i);
            }
        }
        m
    });
    if name.bytes().any(|b| b.is_ascii_uppercase()) {
        by_name.get(&name.to_ascii_lowercase()).copied()
    } else {
        by_name.get(name).copied()
    }
}

/// What MariaDB reports for a collation of more than one byte's id.
fn mariadb_reported(name: &str, id: Option<u16>) -> Option<u16> {
    if name.contains("uca1400") {
        return match charset::charset_of(name) {
            "utf8mb4" => Some(224),
            "utf8mb3" | "utf8" => Some(192),
            _ => None,
        };
    }
    id
}

/// The id MySQL 8.4 reports for collation `name` as the backend names it:
/// its own id, with MariaDB's `utf8mb4_uca1400_ai_ci` standing for MySQL's
/// default `utf8mb4_0900_ai_ci` (255), as on the MySQL port everywhere.
fn mysql_reported(name: &str) -> Option<u16> {
    if name.eq_ignore_ascii_case("utf8mb4_uca1400_ai_ci") {
        return Some(255);
    }
    id_of(name)
}

/// The session's results collation: from `character_set_results` and
/// `collation_connection` (as the backend session state tracks them), or
/// from the handshake id when they aren't known. `results_set`: the
/// session set its results character set since login (`SET NAMES`, `SET
/// CHARACTER SET`, `SET character_set_results`).
///
/// What each server reports for a string column (checked against both as
/// black boxes, handshake collation 45):
///
/// | Session | MariaDB 11.8 | MySQL 8.4 |
/// |---|---|---|
/// | the handshake alone | its collation, a compiled default mapped (224) | the handshake's id (45) |
/// | `SET NAMES cs` | `cs`'s default (224 for `utf8mb4`) | `cs`'s default (255 for `utf8mb4`, 33, 8) |
/// | `SET NAMES cs COLLATE c` | `c` | `c` |
/// | `SET character_set_results = cs` | by the connection's collation | `cs`'s default (255 for `utf8mb4`) |
/// | `SET collation_connection = c` | `c` | no change |
///
/// On the MySQL port the tracked state after a login with 45 and after
/// `SET NAMES utf8mb4` is the same (the pool maps both to MySQL's utf8mb4
/// default, `utf8mb4_uca1400_ai_ci` in the backend's names), so the
/// handshake's id stands until the session sets its results character
/// set (`results_set`).
pub(crate) fn session_results(
    persona: Persona,
    handshake: u16,
    results_cs: Option<&str>,
    collation_connection: Option<&str>,
    results_set: bool,
    login_unsettled: bool,
) -> Results {
    Results {
        cs: results_collation(
            persona,
            handshake,
            results_cs,
            collation_connection,
            results_set,
            login_unsettled,
        ),
        mysql: persona == Persona::MySql84,
    }
}

fn results_collation(
    persona: Persona,
    handshake: u16,
    results_cs: Option<&str>,
    collation_connection: Option<&str>,
    results_set: bool,
    login_unsettled: bool,
) -> Option<ResultsCs> {
    if results_cs.is_some_and(|r| r.is_empty() || r.eq_ignore_ascii_case("NULL")) {
        return None;
    }
    let from_handshake = || {
        let (name, primary) = charset::by_id(handshake).unwrap_or(("utf8mb4_general_ci", true));
        let cs = charset::charset_of(name);
        let id = match persona {
            Persona::MariaDb if primary => {
                let mapped = charset::mapped(charset::DEFAULT_CHARSET_COLLATIONS, cs);
                mapped
                    .and_then(|m| mariadb_reported(m, id_of(m)))
                    .unwrap_or(handshake)
            }
            Persona::MySql84 | Persona::MariaDb => handshake,
        };
        Some(ResultsCs {
            id,
            mbmax: mbmax_of(cs),
        })
    };
    let (Some(rcs), Some(cc)) = (results_cs, collation_connection) else {
        return from_handshake();
    };
    if login_unsettled {
        return from_handshake();
    }
    if persona == Persona::MySql84 {
        if !results_set {
            // Nothing set the results character set since login.
            return from_handshake();
        }
        let id = if charset::charset_of(cc).eq_ignore_ascii_case(rcs) {
            mysql_reported(cc)
        } else if rcs.eq_ignore_ascii_case("utf8mb4") {
            // MySQL 8.4's default collation of utf8mb4.
            Some(255)
        } else {
            charset::compiled_default(rcs).and_then(id_of)
        };
        return match id {
            Some(id) => Some(ResultsCs {
                id,
                mbmax: mbmax_of(rcs),
            }),
            None => from_handshake(),
        };
    }
    let ccs = charset::charset_of(cc);
    let name = if ccs.eq_ignore_ascii_case(rcs) {
        cc.to_owned()
    } else {
        match charset::compiled_default(rcs) {
            Some(d) => d.to_owned(),
            None => return from_handshake(),
        }
    };
    match mariadb_reported(&name, id_of(&name)) {
        Some(id) => Some(ResultsCs {
            id,
            mbmax: mbmax_of(rcs),
        }),
        None => from_handshake(),
    }
}

/// What a column reports with `character_set_results = NULL`: its own
/// collation.
fn own_cs(c: &Column) -> ResultsCs {
    let Some(coll) = c.collation else {
        return ResultsCs { id: 63, mbmax: 1 };
    };
    let mbmax = match coll.charset() {
        Charset::Utf8mb4 if c.mb3 => 3,
        Charset::Utf8mb4 => 4,
        _ => 1,
    };
    let name = coll.name();
    let name = if c.mb3 {
        name.replacen("utf8mb4", "utf8mb3", 1)
    } else {
        name.to_owned()
    };
    let id = mariadb_reported(&name, id_of(&name)).unwrap_or(if mbmax == 1 { 8 } else { 224 });
    ResultsCs { id, mbmax }
}

/// The wire type code of an integer of `bytes` bytes.
pub(crate) fn int_type(bytes: u8) -> u8 {
    match bytes {
        1 => column_type::TINY,
        2 => column_type::SHORT,
        3 => column_type::INT24,
        4 => column_type::LONG,
        _ => column_type::LONGLONG,
    }
}

/// Seven fraction digits and their point, or nothing without a fraction.
fn frac_len(fsp: u8) -> u32 {
    if fsp > 0 { u32::from(fsp) + 1 } else { 0 }
}

/// The column definition of column `ci` of `def`, selected from table
/// `alias` of logical database `db`, named `name` in the result.
#[allow(clippy::too_many_arguments, clippy::too_many_lines)] // the names a column definition carries; one arm per type
pub(crate) fn column_def(
    db: &Bytes,
    alias: &Bytes,
    def: &TableDef,
    org_table: &Bytes,
    ci: usize,
    name: &Bytes,
    org_name: &Bytes,
    results: Results,
) -> ColumnDef {
    let c = &def.columns[ci];
    // A string's character set as reported, and how many bytes one of its
    // characters may take there.
    let string = || results.cs.unwrap_or_else(|| own_cs(c));
    let chars = |n: u64| -> (u16, u32) {
        let r = string();
        (
            r.id,
            u32::try_from(n.saturating_mul(u64::from(r.mbmax))).unwrap_or(u32::MAX),
        )
    };
    let member_chars =
        |m: &Bytes| c.collation.map_or(m.len(), |x| x.charset().char_count(m)) as u64;
    let mut flags = c.flags;
    let mut ext_type = None;
    let (charset, length, col_type, decimals) = match c.ty {
        ColType::Int { bytes, .. } => (63, c.width, int_type(bytes), 0),
        ColType::Bit { bits } => (63, u32::from(bits), column_type::BIT, 0),
        ColType::Decimal { precision, scale } => (
            63,
            u32::from(precision) + u32::from(scale > 0) + u32::from(!c.unsigned),
            column_type::NEWDECIMAL,
            scale,
        ),
        ColType::Float => match c.float_md {
            Some((m, d)) => (63, u32::from(m), column_type::FLOAT, d),
            None => (63, 12, column_type::FLOAT, 31),
        },
        ColType::Double => match c.float_md {
            Some((m, d)) => (63, u32::from(m), column_type::DOUBLE, d),
            None => (63, 22, column_type::DOUBLE, 31),
        },
        ColType::Date => (63, 10, column_type::DATE, 0),
        ColType::Time { fsp } => (63, 10 + frac_len(fsp), column_type::TIME, fsp),
        ColType::DateTime { fsp } => {
            // MySQL reports ON UPDATE only on a TIMESTAMP.
            if results.mysql {
                flags &= !ColumnFlags::ON_UPDATE_NOW.bits();
            }
            (63, 19 + frac_len(fsp), column_type::DATETIME, fsp)
        }
        ColType::Timestamp { fsp } => {
            if results.mysql {
                flags &= !ColumnFlags::UNSIGNED.bits();
            }
            (63, 19 + frac_len(fsp), column_type::TIMESTAMP, fsp)
        }
        ColType::Year => (63, 4, column_type::YEAR, 0),
        ColType::Char { chars: n, .. } => {
            let (cs, len) = chars(u64::from(n));
            (cs, len, column_type::STRING, 0)
        }
        ColType::VarChar { chars: n, .. } => {
            let (cs, len) = chars(u64::from(n));
            (cs, len, column_type::VAR_STRING, 0)
        }
        ColType::Text { size, .. } => {
            let (cs, len) = chars(size.max_bytes());
            (cs, len, column_type::BLOB, 0)
        }
        ColType::Binary { bytes } => (63, u32::from(bytes), column_type::STRING, 0),
        ColType::VarBinary { bytes } => (63, bytes, column_type::VAR_STRING, 0),
        ColType::Blob { size } => (
            63,
            u32::try_from(size.max_bytes()).unwrap_or(u32::MAX),
            column_type::BLOB,
            0,
        ),
        ColType::Enum { .. } => {
            let longest = c.members.iter().map(member_chars).max().unwrap_or(0);
            let (cs, len) = chars(longest);
            (cs, len, column_type::STRING, 0)
        }
        ColType::Set { .. } => {
            let all: u64 = c.members.iter().map(member_chars).sum::<u64>()
                + (c.members.len() as u64).saturating_sub(1);
            let (cs, len) = chars(all);
            (cs, len, column_type::STRING, 0)
        }
        ColType::Json => {
            if results.mysql {
                (63, u32::MAX, column_type::JSON, 0)
            } else {
                ext_type = Some(Bytes::from_static(b"json"));
                (string().id, u32::MAX, column_type::BLOB, 0)
            }
        }
        // MariaDB's UUID, INET4 and INET6: their text, as a fixed string.
        ColType::Fbt(f) => {
            ext_type = Some(Bytes::from_static(f.name().as_bytes()));
            let (cs, len) = chars(u64::from(f.text_len()));
            (cs, len, column_type::STRING, 0)
        }
    };
    ColumnDef {
        schema: db.clone(),
        table: alias.clone(),
        org_table: org_table.clone(),
        name: name.clone(),
        org_name: org_name.clone(),
        ext_type,
        charset,
        length,
        col_type,
        flags,
        decimals,
    }
}

/// A `PREPARE_OK` parameter definition, as each server sends it: MariaDB
/// reports every parameter as type `NULL`; MySQL 8.4 reports the type the
/// parameter takes from its context (an integer column: `LONGLONG`, length
/// 21; a string column or none: `VAR_STRING` in the results character set).
pub(crate) fn param_def(persona: Persona, ty: Option<ColType>, results: Results) -> ColumnDef {
    let base = ColumnDef {
        name: Bytes::from_static(b"?"),
        charset: 63,
        length: 0,
        col_type: column_type::NULL,
        flags: ColumnFlags::BINARY.bits(),
        ..ColumnDef::default()
    };
    if persona == Persona::MariaDb {
        return base;
    }
    match ty {
        Some(ColType::Int { unsigned, .. }) => ColumnDef {
            length: 21,
            col_type: column_type::LONGLONG,
            flags: if unsigned {
                (ColumnFlags::BINARY | ColumnFlags::UNSIGNED).bits()
            } else {
                ColumnFlags::BINARY.bits()
            },
            ..base
        },
        Some(ColType::Decimal { .. }) => ColumnDef {
            length: 67,
            col_type: column_type::NEWDECIMAL,
            decimals: 30,
            ..base
        },
        // Dates and times: their own type, as text in the results
        // character set would be long.
        Some(ColType::Date) => ColumnDef {
            charset: results.cs.map_or(255, |r| r.id),
            length: 10 * results.cs.map_or(4, |r| r.mbmax),
            col_type: column_type::DATE,
            flags: 0,
            ..base
        },
        Some(ColType::Time { .. }) => ColumnDef {
            charset: results.cs.map_or(255, |r| r.id),
            length: 17 * results.cs.map_or(4, |r| r.mbmax),
            col_type: column_type::TIME,
            flags: 0,
            decimals: 6,
            ..base
        },
        Some(ColType::DateTime { .. } | ColType::Timestamp { .. }) => ColumnDef {
            charset: results.cs.map_or(255, |r| r.id),
            length: 26 * results.cs.map_or(4, |r| r.mbmax),
            col_type: column_type::DATETIME,
            flags: 0,
            decimals: 6,
            ..base
        },
        Some(ColType::Year) => ColumnDef {
            length: 4,
            col_type: column_type::YEAR,
            flags: (ColumnFlags::BINARY | ColumnFlags::UNSIGNED).bits(),
            ..base
        },
        Some(ColType::Binary { .. } | ColType::VarBinary { .. } | ColType::Blob { .. }) => {
            ColumnDef {
                length: 65_535,
                col_type: column_type::VAR_STRING,
                decimals: 31,
                ..base
            }
        }
        Some(ColType::Bit { .. }) => ColumnDef {
            length: 64,
            col_type: column_type::BIT,
            flags: ColumnFlags::UNSIGNED.bits(),
            ..base
        },
        Some(ColType::Float | ColType::Double) => ColumnDef {
            length: 23,
            col_type: column_type::DOUBLE,
            decimals: 31,
            ..base
        },
        _ => {
            let r = results.cs.unwrap_or(ResultsCs { id: 255, mbmax: 4 });
            // A string as long as the column's character set lets a
            // VARCHAR be, in the results character set; a column that
            // sorts by code keeps the BINARY flag.
            let coll = ty.and_then(|t| t.collation());
            let chars = match coll.map(Collation::charset) {
                Some(Charset::Latin1 | Charset::Ascii | Charset::Binary) => 65_535,
                _ => 16_383,
            };
            let binary = coll.is_some_and(|c| c == Collation::Binary || c.name().ends_with("_bin"));
            ColumnDef {
                charset: r.id,
                length: chars * r.mbmax,
                col_type: column_type::VAR_STRING,
                flags: if binary {
                    ColumnFlags::BINARY.bits()
                } else {
                    0
                },
                decimals: 31,
                ..base
            }
        }
    }
}

/// Whether collation id `id` is one of `latin1`.
fn is_latin1(id: u16) -> bool {
    matches!(id, 5 | 8 | 15 | 31 | 47 | 48 | 49 | 94)
}

/// A stored string in the results' character set: a `latin1` column's
/// bytes as UTF-8, unless the results are `latin1` too.
fn in_results(c: &Column, s: &Bytes, results: Results) -> Bytes {
    // The column's character set first: only a latin1 column's bytes are
    // ever looked at.
    if c.collation.map(Collation::charset) != Some(Charset::Latin1) || s.is_ascii() {
        return s.clone();
    }
    if results.cs.is_none_or(|r| is_latin1(r.id)) {
        return s.clone();
    }
    let t: String = s.iter().map(|b| assign::latin1_char(*b)).collect();
    Bytes::from(t)
}

/// A `FLOAT` or `DOUBLE` value's text: `plain` for a column without
/// decimals, its `D` decimals for a `(M,D)` column. MySQL prints a
/// negative zero as `-0`; MariaDB prints `0`.
fn float_text(c: &Column, v: f64, results: Results, plain: String) -> Bytes {
    Bytes::from(match c.float_md {
        Some((_, d)) => {
            let t = format!("{:.*}", usize::from(d), v);
            // A value that rounds to zero prints without a sign.
            match t.strip_prefix('-') {
                Some(rest) if rest.bytes().all(|b| b == b'0' || b == b'.') => rest.to_owned(),
                _ => t,
            }
        }
        None if v == 0.0 && v.is_sign_negative() && results.mysql => "-0".to_owned(),
        None => plain,
    })
}

/// A `BIT(bits)` value's bytes: big-endian, in the column's width.
fn bit_bytes(v: u64, bits: u8) -> Bytes {
    let n = usize::from(bits).div_ceil(8).clamp(1, 8);
    Bytes::copy_from_slice(&v.to_be_bytes()[8 - n..])
}

/// The text of value `d` of column `c`, for messages (in UTC, as stored).
pub(crate) fn text(c: &Column, d: &Datum) -> Option<Bytes> {
    let r = Results {
        cs: None,
        mysql: false,
    };
    text_in(c, d, &Env::STRICT, r)
}

/// The text protocol's bytes of value `d` of column `c`; `None` for NULL.
pub(crate) fn text_in(c: &Column, d: &Datum, env: &Env, results: Results) -> Option<Bytes> {
    Some(match d {
        Datum::Null => return None,
        Datum::Str(s) => in_results(c, s, results),
        Datum::Int(v) => Bytes::from(v.to_string()),
        Datum::UInt(v) => Bytes::from(v.to_string()),
        // A UUID or address column's stored bytes: its text.
        Datum::Bytes(s) if matches!(c.ty, ColType::Fbt(_)) => match c.ty {
            ColType::Fbt(f) => f.text(s).map_or_else(|| s.clone(), Bytes::from),
            _ => s.clone(),
        },
        Datum::Bytes(s) | Datum::Json(s) => s.clone(),
        Datum::Decimal(v) => Bytes::from(v.to_string()),
        Datum::Float(v) => float_text(c, f64::from(*v), results, expr::float_text(*v)),
        Datum::Double(v) => float_text(c, *v, results, expr::double_text(*v)),
        Datum::Bit(v) => match c.ty {
            ColType::Bit { bits } => bit_bytes(*v, bits),
            _ => Bytes::from(v.to_string()),
        },
        Datum::Year(v) => Bytes::from(format!("{v:04}")),
        Datum::Enum(v) => in_results(c, &expr::enum_text(&c.members, *v), results),
        Datum::Set(v) => in_results(c, &expr::set_text(&c.members, *v), results),
        Datum::Date(v) => Bytes::from(v.to_string()),
        Datum::Time(v) => Bytes::from(temporal::time_text(*v, fsp_of(c))),
        Datum::DateTime(v) => Bytes::from(temporal::datetime_text(*v, fsp_of(c))),
        Datum::Timestamp(v) => Bytes::from(temporal::datetime_text(
            temporal::from_timestamp(*v, env.tz.unwrap_or(0)),
            fsp_of(c),
        )),
    })
}

/// The fractional-seconds precision of a temporal column (six for a
/// value shown outside one).
fn fsp_of(c: &Column) -> u8 {
    match c.ty {
        ColType::Time { fsp } | ColType::DateTime { fsp } | ColType::Timestamp { fsp } => fsp,
        _ => 6,
    }
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

/// The binary protocol's value of `d` of column `c`.
pub(crate) fn binary(c: &Column, d: &Datum, env: &Env, results: Results) -> Value {
    match d {
        Datum::Null => Value::Null,
        Datum::Int(v) => Value::Int(*v),
        Datum::UInt(v) => Value::UInt(*v),
        Datum::Str(s) => Value::Bytes(in_results(c, s, results)),
        Datum::Float(v) => Value::Float(*v),
        Datum::Double(v) => Value::Double(*v),
        Datum::Year(v) => Value::UInt(u64::from(*v)),
        Datum::Date(v) => wire_datetime(sprig_engine::row::DateTime::new(*v, 0, 0, 0, 0)),
        Datum::DateTime(v) => wire_datetime(*v),
        Datum::Timestamp(v) => wire_datetime(temporal::from_timestamp(*v, env.tz.unwrap_or(0))),
        Datum::Time(v) => {
            let abs = v.micros().unsigned_abs();
            let secs = abs / 1_000_000;
            // A TIME is below 839 hours: every part fits its field.
            #[allow(clippy::cast_possible_truncation)]
            let t = sprig_proto_mysql::resultset::Time {
                negative: v.micros() < 0,
                days: (secs / 86_400) as u32,
                hour: (secs / 3600 % 24) as u8,
                minute: (secs / 60 % 60) as u8,
                second: (secs % 60) as u8,
                micro: (abs % 1_000_000) as u32,
            };
            Value::Time(t)
        }
        _ => match text_in(c, d, env, results) {
            Some(t) => Value::Bytes(t),
            None => Value::Null,
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn handshake_ids_report_as_each_server_does() {
        // MariaDB 11.8: 45 is utf8mb4's compiled default, mapped to
        // uca1400_ai_ci, reported as 224.
        let sr = |p, h, rcs, cc, set, unsettled| session_results(p, h, rcs, cc, set, unsettled).cs;
        let m = sr(Persona::MariaDb, 45, None, None, false, true).unwrap();
        assert_eq!((m.id, m.mbmax), (224, 4));
        // MySQL 8.4 reports the handshake's id.
        let y = sr(Persona::MySql84, 45, None, None, false, true).unwrap();
        assert_eq!((y.id, y.mbmax), (45, 4));
        let y = sr(Persona::MySql84, 255, None, None, false, true).unwrap();
        assert_eq!(y.id, 255);
        let y = sr(Persona::MySql84, 33, None, None, false, true).unwrap();
        assert_eq!((y.id, y.mbmax), (33, 3));
        // SET NAMES utf8mb4 COLLATE utf8mb4_bin: 46 on both.
        let bin = "utf8mb4_bin";
        for p in [Persona::MariaDb, Persona::MySql84] {
            let r = sr(p, 45, Some("utf8mb4"), Some(bin), true, false).unwrap();
            assert_eq!(r.id, 46);
        }
        // SET NAMES latin1.
        let l1 = "latin1_swedish_ci";
        for p in [Persona::MariaDb, Persona::MySql84] {
            let l = sr(p, 45, Some("latin1"), Some(l1), true, false).unwrap();
            assert_eq!((l.id, l.mbmax), (8, 1));
        }
        // character_set_results = NULL.
        assert!(sr(Persona::MariaDb, 45, Some(""), Some("x"), true, false).is_none());
    }

    #[test]
    fn mysql_reports_the_results_character_sets_own_collation() {
        let my = |handshake: u16, rcs: &str, cc: &str, results_set: bool| {
            session_results(
                Persona::MySql84,
                handshake,
                Some(rcs),
                Some(cc),
                results_set,
                false,
            )
            .cs
            .unwrap()
            .id
        };
        // The backend's connection collation after a login on this port
        // is the pool's (MySQL's utf8mb4 default under MariaDB's name).
        let uca = "utf8mb4_uca1400_ai_ci";
        let general = "utf8mb4_general_ci";
        // The handshake alone: its id, whatever the pool mapped it to;
        // SET collation_connection alone changes nothing.
        assert_eq!(my(45, "utf8mb4", uca, false), 45);
        assert_eq!(my(255, "utf8mb4", uca, false), 255);
        assert_eq!(my(46, "utf8mb4", uca, false), 46);
        assert_eq!(my(45, "utf8mb4", "utf8mb4_bin", false), 45);
        // SET NAMES utf8mb4 (or SET character_set_results = utf8mb4)
        // after a handshake with 45: MySQL's default collation of
        // utf8mb4, not the handshake's.
        assert_eq!(my(45, "utf8mb4", uca, true), 255);
        // SET NAMES utf8mb4 COLLATE utf8mb4_general_ci: that collation.
        assert_eq!(my(255, "utf8mb4", general, true), 45);
        // SET NAMES utf8mb3: 33.
        assert_eq!(my(45, "utf8mb3", "utf8mb3_general_ci", true), 33);
        // SET character_set_results to another character set than the
        // connection's: its default.
        assert_eq!(my(8, "utf8mb4", "latin1_swedish_ci", true), 255);
        assert_eq!(my(45, "latin1", uca, true), 8);
        // MariaDB's port goes by the connection's collation: 224.
        let m = session_results(
            Persona::MariaDb,
            45,
            Some("utf8mb4"),
            Some(uca),
            true,
            false,
        )
        .cs
        .unwrap();
        assert_eq!(m.id, 224);
    }
}
