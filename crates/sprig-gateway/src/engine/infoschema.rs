//! `information_schema` reads about tables, answered from the engine's
//! definitions.
//!
//! The name layer turns a client's `information_schema` query into one over
//! derived tables that read MariaDB's own views restricted to the tenant's
//! physical schemas. No table exists on MariaDB, so the engine answers that
//! query instead: it builds MariaDB 11.8's `TABLES`, `COLUMNS`,
//! `STATISTICS`, `TABLE_CONSTRAINTS`, `KEY_COLUMN_USAGE`,
//! `CHECK_CONSTRAINTS` and `REFERENTIAL_CONSTRAINTS` in memory (the rows of
//! the physical schemas the query names, with MariaDB's values), and runs
//! the whole query on the planner over them. The answer has MariaDB's
//! column definitions for columns read straight from those tables; a query
//! with any other result column, or one the planner can't run, is left to
//! the backend as before.

use std::collections::HashMap;
use std::sync::Arc;

use bytes::Bytes;
use sprig_common::ids::DbId;
use sprig_plan::catalog::{ColumnDef, TableDef};
use sprig_plan::mem::MemDb;

use super::catalog::EngineCatalog;
use super::define::{self, Def, Default, KeyKind};
use super::row::Datum;
use super::show::{Answer, coldef};
use super::table::{ColType, collation_named};

/// MariaDB 11.8's columns of the tables built here: (table, column,
/// character set, length, wire type, flags) as its result metadata shows
/// them.
const COLS: &[(&str, &str, u16, u32, u8, u16)] = &[
    ("TABLES", "TABLE_CATALOG", 224, 2048, 0xfd, 0x1001),
    ("TABLES", "TABLE_SCHEMA", 224, 256, 0xfd, 0x1001),
    ("TABLES", "TABLE_NAME", 224, 256, 0xfd, 0x1001),
    ("TABLES", "TABLE_TYPE", 224, 256, 0xfd, 0x1001),
    ("TABLES", "ENGINE", 224, 256, 0xfd, 0x1000),
    ("TABLES", "VERSION", 63, 21, 0x08, 0x1020),
    ("TABLES", "ROW_FORMAT", 224, 40, 0xfd, 0x1000),
    ("TABLES", "TABLE_ROWS", 63, 21, 0x08, 0x1020),
    ("TABLES", "AVG_ROW_LENGTH", 63, 21, 0x08, 0x1020),
    ("TABLES", "DATA_LENGTH", 63, 21, 0x08, 0x1020),
    ("TABLES", "MAX_DATA_LENGTH", 63, 21, 0x08, 0x1020),
    ("TABLES", "INDEX_LENGTH", 63, 21, 0x08, 0x1020),
    ("TABLES", "DATA_FREE", 63, 21, 0x08, 0x1020),
    ("TABLES", "AUTO_INCREMENT", 63, 21, 0x08, 0x1020),
    ("TABLES", "CREATE_TIME", 63, 19, 0x0c, 0x1080),
    ("TABLES", "UPDATE_TIME", 63, 19, 0x0c, 0x1080),
    ("TABLES", "CHECK_TIME", 63, 19, 0x0c, 0x1080),
    ("TABLES", "TABLE_COLLATION", 224, 256, 0xfd, 0x1000),
    ("TABLES", "CHECKSUM", 63, 21, 0x08, 0x1020),
    ("TABLES", "CREATE_OPTIONS", 224, 8192, 0xfd, 0x1000),
    ("TABLES", "TABLE_COMMENT", 224, 8192, 0xfd, 0x1001),
    ("TABLES", "MAX_INDEX_LENGTH", 63, 21, 0x08, 0x1020),
    ("TABLES", "TEMPORARY", 224, 4, 0xfd, 0x1000),
    ("COLUMNS", "TABLE_CATALOG", 224, 2048, 0xfd, 0x1001),
    ("COLUMNS", "TABLE_SCHEMA", 224, 256, 0xfd, 0x1001),
    ("COLUMNS", "TABLE_NAME", 224, 256, 0xfd, 0x1001),
    ("COLUMNS", "COLUMN_NAME", 224, 256, 0xfd, 0x1001),
    ("COLUMNS", "ORDINAL_POSITION", 63, 21, 0x08, 0x1021),
    ("COLUMNS", "COLUMN_DEFAULT", 224, 786_384, 0xfc, 0x1010),
    ("COLUMNS", "IS_NULLABLE", 224, 12, 0xfd, 0x1001),
    ("COLUMNS", "DATA_TYPE", 224, 256, 0xfd, 0x1001),
    ("COLUMNS", "CHARACTER_MAXIMUM_LENGTH", 63, 21, 0x08, 0x1020),
    ("COLUMNS", "CHARACTER_OCTET_LENGTH", 63, 21, 0x08, 0x1020),
    ("COLUMNS", "NUMERIC_PRECISION", 63, 21, 0x08, 0x1020),
    ("COLUMNS", "NUMERIC_SCALE", 63, 21, 0x08, 0x1020),
    ("COLUMNS", "DATETIME_PRECISION", 63, 21, 0x08, 0x1020),
    ("COLUMNS", "CHARACTER_SET_NAME", 224, 128, 0xfd, 0x1000),
    ("COLUMNS", "COLLATION_NAME", 224, 256, 0xfd, 0x1000),
    ("COLUMNS", "COLUMN_TYPE", 224, 786_420, 0xfc, 0x1011),
    ("COLUMNS", "COLUMN_KEY", 224, 12, 0xfd, 0x1001),
    ("COLUMNS", "EXTRA", 224, 320, 0xfd, 0x1001),
    ("COLUMNS", "PRIVILEGES", 224, 320, 0xfd, 0x1001),
    ("COLUMNS", "COLUMN_COMMENT", 224, 4096, 0xfd, 0x1001),
    ("COLUMNS", "IS_GENERATED", 224, 24, 0xfd, 0x1001),
    ("COLUMNS", "GENERATION_EXPRESSION", 224, 786_384, 0xfc, 0x1010),
    ("COLUMNS", "IS_SYSTEM_TIME_PERIOD_START", 224, 12, 0xfd, 0x1001),
    ("COLUMNS", "IS_SYSTEM_TIME_PERIOD_END", 224, 12, 0xfd, 0x1001),
    ("STATISTICS", "TABLE_CATALOG", 224, 2048, 0xfd, 0x1001),
    ("STATISTICS", "TABLE_SCHEMA", 224, 256, 0xfd, 0x1001),
    ("STATISTICS", "TABLE_NAME", 224, 256, 0xfd, 0x1001),
    ("STATISTICS", "NON_UNIQUE", 63, 1, 0x08, 0x1001),
    ("STATISTICS", "INDEX_SCHEMA", 224, 256, 0xfd, 0x1001),
    ("STATISTICS", "INDEX_NAME", 224, 256, 0xfd, 0x1001),
    ("STATISTICS", "SEQ_IN_INDEX", 63, 2, 0x03, 0x1021),
    ("STATISTICS", "COLUMN_NAME", 224, 256, 0xfd, 0x1001),
    ("STATISTICS", "COLLATION", 224, 4, 0xfd, 0x1000),
    ("STATISTICS", "CARDINALITY", 63, 21, 0x08, 0x1000),
    ("STATISTICS", "SUB_PART", 63, 3, 0x08, 0x1000),
    ("STATISTICS", "PACKED", 224, 40, 0xfd, 0x1000),
    ("STATISTICS", "NULLABLE", 224, 12, 0xfd, 0x1001),
    ("STATISTICS", "INDEX_TYPE", 224, 64, 0xfd, 0x1001),
    ("STATISTICS", "COMMENT", 224, 64, 0xfd, 0x1001),
    ("STATISTICS", "INDEX_COMMENT", 224, 4096, 0xfd, 0x1001),
    ("STATISTICS", "IGNORED", 224, 12, 0xfd, 0x1001),
    ("TABLE_CONSTRAINTS", "CONSTRAINT_CATALOG", 224, 2048, 0xfd, 0x1001),
    ("TABLE_CONSTRAINTS", "CONSTRAINT_SCHEMA", 224, 256, 0xfd, 0x1001),
    ("TABLE_CONSTRAINTS", "CONSTRAINT_NAME", 224, 256, 0xfd, 0x1001),
    ("TABLE_CONSTRAINTS", "TABLE_SCHEMA", 224, 256, 0xfd, 0x1001),
    ("TABLE_CONSTRAINTS", "TABLE_NAME", 224, 256, 0xfd, 0x1001),
    ("TABLE_CONSTRAINTS", "CONSTRAINT_TYPE", 224, 256, 0xfd, 0x1001),
    ("KEY_COLUMN_USAGE", "CONSTRAINT_CATALOG", 224, 2048, 0xfd, 0x1001),
    ("KEY_COLUMN_USAGE", "CONSTRAINT_SCHEMA", 224, 256, 0xfd, 0x1001),
    ("KEY_COLUMN_USAGE", "CONSTRAINT_NAME", 224, 256, 0xfd, 0x1001),
    ("KEY_COLUMN_USAGE", "TABLE_CATALOG", 224, 2048, 0xfd, 0x1001),
    ("KEY_COLUMN_USAGE", "TABLE_SCHEMA", 224, 256, 0xfd, 0x1001),
    ("KEY_COLUMN_USAGE", "TABLE_NAME", 224, 256, 0xfd, 0x1001),
    ("KEY_COLUMN_USAGE", "COLUMN_NAME", 224, 256, 0xfd, 0x1001),
    ("KEY_COLUMN_USAGE", "ORDINAL_POSITION", 63, 10, 0x08, 0x1001),
    ("KEY_COLUMN_USAGE", "POSITION_IN_UNIQUE_CONSTRAINT", 63, 10, 0x08, 0x1000),
    ("KEY_COLUMN_USAGE", "REFERENCED_TABLE_SCHEMA", 224, 256, 0xfd, 0x1000),
    ("KEY_COLUMN_USAGE", "REFERENCED_TABLE_NAME", 224, 256, 0xfd, 0x1000),
    ("KEY_COLUMN_USAGE", "REFERENCED_COLUMN_NAME", 224, 256, 0xfd, 0x1000),
    ("CHECK_CONSTRAINTS", "CONSTRAINT_CATALOG", 224, 2048, 0xfd, 0x1001),
    ("CHECK_CONSTRAINTS", "CONSTRAINT_SCHEMA", 224, 256, 0xfd, 0x1001),
    ("CHECK_CONSTRAINTS", "TABLE_NAME", 224, 256, 0xfd, 0x1001),
    ("CHECK_CONSTRAINTS", "CONSTRAINT_NAME", 224, 256, 0xfd, 0x1001),
    ("CHECK_CONSTRAINTS", "LEVEL", 224, 24, 0xfd, 0x1001),
    ("CHECK_CONSTRAINTS", "CHECK_CLAUSE", 224, 786_384, 0xfc, 0x1011),
    ("REFERENTIAL_CONSTRAINTS", "CONSTRAINT_CATALOG", 224, 2048, 0xfd, 0x1001),
    ("REFERENTIAL_CONSTRAINTS", "CONSTRAINT_SCHEMA", 224, 256, 0xfd, 0x1001),
    ("REFERENTIAL_CONSTRAINTS", "CONSTRAINT_NAME", 224, 256, 0xfd, 0x1001),
    ("REFERENTIAL_CONSTRAINTS", "UNIQUE_CONSTRAINT_CATALOG", 224, 2048, 0xfd, 0x1001),
    ("REFERENTIAL_CONSTRAINTS", "UNIQUE_CONSTRAINT_SCHEMA", 224, 256, 0xfd, 0x1001),
    ("REFERENTIAL_CONSTRAINTS", "UNIQUE_CONSTRAINT_NAME", 224, 256, 0xfd, 0x1000),
    ("REFERENTIAL_CONSTRAINTS", "MATCH_OPTION", 224, 256, 0xfd, 0x1001),
    ("REFERENTIAL_CONSTRAINTS", "UPDATE_RULE", 224, 256, 0xfd, 0x1001),
    ("REFERENTIAL_CONSTRAINTS", "DELETE_RULE", 224, 256, 0xfd, 0x1001),
    ("REFERENTIAL_CONSTRAINTS", "TABLE_NAME", 224, 256, 0xfd, 0x1001),
    ("REFERENTIAL_CONSTRAINTS", "REFERENCED_TABLE_NAME", 224, 256, 0xfd, 0x1000),
];

/// The tables built here.
pub(crate) const TABLES: [&str; 7] = [
    "TABLES",
    "COLUMNS",
    "STATISTICS",
    "TABLE_CONSTRAINTS",
    "KEY_COLUMN_USAGE",
    "CHECK_CONSTRAINTS",
    "REFERENTIAL_CONSTRAINTS",
];

fn cols_of(t: &str) -> impl Iterator<Item = &'static (&'static str, &'static str, u16, u32, u8, u16)> + '_ {
    COLS.iter().filter(move |c| c.0 == t)
}

/// The planner's definition of table `t`.
fn plan_def(t: &str) -> TableDef {
    let ci = collation_named("utf8mb4_general_ci");
    let columns = cols_of(t)
        .map(|&(_, name, _, len, ty, flags)| {
            let cty = match ty {
                0x08 => ColType::Int { bytes: 8, unsigned: flags & 0x20 != 0 },
                0x03 => ColType::Int { bytes: 4, unsigned: flags & 0x20 != 0 },
                0x0c => ColType::DateTime { fsp: 0 },
                _ => match ci {
                    Some(collation) => ColType::VarChar { chars: (len / 4).max(1), collation },
                    None => ColType::VarBinary { bytes: len.min(65_535) },
                },
            };
            let c = ColumnDef::new(name, cty);
            if flags & 1 != 0 { c.not_null() } else { c }
        })
        .collect();
    TableDef { schema: "information_schema".into(), name: t.into(), columns, primary: None, indexes: Vec::new() }
}

/// A value of a row being built.
enum V {
    S(String),
    U(u64),
    N,
}

fn s(x: impl Into<String>) -> V {
    V::S(x.into())
}

fn row_of(t: &str, vals: &HashMap<&str, V>) -> Vec<Datum> {
    cols_of(t)
        .map(|c| match vals.get(c.1) {
            Some(V::S(x)) => Datum::Str(Bytes::from(x.clone().into_bytes())),
            Some(V::U(n)) => Datum::UInt(*n),
            Some(V::N) | None => Datum::Null,
        })
        .collect()
}

/// The `COLUMN_DEFAULT` of `information_schema.COLUMNS`: string constants
/// quoted, a nullable column without a default `NULL` (the word), a `NOT
/// NULL` one SQL NULL.
fn is_default(c: &define::Col) -> V {
    if c.generated.is_some() {
        return if c.not_null { V::N } else { s("NULL") };
    }
    match &c.default {
        None | Some(Default::Null) if !c.not_null && !c.auto_inc => s("NULL"),
        None | Some(Default::Null) => V::N,
        Some(Default::Lit(l)) => s(l.clone()),
        Some(Default::Raw(e) | Default::Expr(e)) => s(define::expr::print(e)),
        Some(Default::Now(p)) => s(if *p == 0 { "current_timestamp()".to_owned() } else { format!("current_timestamp({p})") }),
    }
}

/// Precision, scale and datetime precision of a column type, as `COLUMNS`
/// shows them.
fn numeric(c: &define::Col) -> (V, V, V) {
    let b = c.ty.base.as_str();
    let args: Vec<u64> = c.ty.args.trim_matches(|ch| ch == '(' || ch == ')').split(',').filter_map(|x| x.trim().parse().ok()).collect();
    let u = c.ty.unsigned;
    match b {
        "tinyint" => (V::U(3), V::U(0), V::N),
        "smallint" => (V::U(5), V::U(0), V::N),
        "mediumint" => (V::U(if u { 8 } else { 7 }), V::U(0), V::N),
        "int" => (V::U(10), V::U(0), V::N),
        "bigint" => (V::U(if u { 20 } else { 19 }), V::U(0), V::N),
        "decimal" => (V::U(args.first().copied().unwrap_or(10)), V::U(args.get(1).copied().unwrap_or(0)), V::N),
        "float" => match args.as_slice() {
            [m, d] => (V::U(*m), V::U(*d), V::N),
            _ => (V::U(12), V::N, V::N),
        },
        "double" => match args.as_slice() {
            [m, d] => (V::U(*m), V::U(*d), V::N),
            _ => (V::U(22), V::N, V::N),
        },
        "bit" => (V::U(args.first().copied().unwrap_or(1)), V::N, V::N),
        "datetime" | "timestamp" | "time" => (V::N, V::N, V::U(args.first().copied().unwrap_or(0))),
        _ => (V::N, V::N, V::N),
    }
}

/// Character and octet lengths.
fn lengths(c: &define::Col) -> (V, V) {
    let mb = u64::from(c.ty.mbmax());
    let n = u64::from(c.ty.len);
    match c.ty.base.as_str() {
        "char" | "varchar" => (V::U(n), V::U(n * mb)),
        "binary" | "varbinary" => (V::U(n), V::U(n)),
        "tinytext" => (V::U(255 / mb), V::U(255)),
        "text" => (V::U(65_535 / mb), V::U(65_535)),
        "mediumtext" => (V::U(16_777_215 / mb), V::U(16_777_215)),
        "longtext" => (V::U(4_294_967_295 / mb), V::U(4_294_967_295)),
        "tinyblob" => (V::U(255), V::U(255)),
        "blob" => (V::U(65_535), V::U(65_535)),
        "mediumblob" => (V::U(16_777_215), V::U(16_777_215)),
        "longblob" => (V::U(4_294_967_295), V::U(4_294_967_295)),
        "enum" | "set" => {
            let longest = c.ty.members.iter().map(|m| m.chars().count()).max().unwrap_or(0) as u64;
            let n = if c.ty.base == "set" {
                c.ty.members.iter().map(|m| m.chars().count() as u64).sum::<u64>() + c.ty.members.len().saturating_sub(1) as u64
            } else {
                longest
            };
            (V::U(n), V::U(n * mb))
        }
        _ => (V::N, V::N),
    }
}

fn key_of(def: &Def, col: &str) -> &'static str {
    let first = |k: &define::Key| k.parts.first().is_some_and(|p| p.col.eq_ignore_ascii_case(col));
    if def.keys.iter().any(|k| k.kind == KeyKind::Primary && k.parts.iter().any(|p| p.col.eq_ignore_ascii_case(col))) {
        "PRI"
    } else if def.keys.iter().any(|k| k.kind == KeyKind::Unique && k.parts.len() == 1 && first(k)) {
        "UNI"
    } else if def.keys.iter().any(first) {
        "MUL"
    } else {
        ""
    }
}

fn extra_of(c: &define::Col) -> String {
    let mut parts: Vec<String> = Vec::new();
    if c.auto_inc {
        parts.push("auto_increment".into());
    }
    if let Some(p) = c.on_update {
        parts.push(if p == 0 { "on update current_timestamp()".into() } else { format!("on update current_timestamp({p})") });
    }
    if let Some((_, stored)) = &c.generated {
        parts.push(if *stored { "STORED GENERATED".into() } else { "VIRTUAL GENERATED".into() });
    }
    if c.invisible {
        parts.push("INVISIBLE".into());
    }
    parts.join(" ")
}

/// Adds every row one table contributes to the in-memory tables.
fn add_table(db: &mut MemDb, schema: &str, def: &Def, next: Option<u64>) {
    let mut put = |t: &str, vals: HashMap<&str, V>| {
        let _ = db.insert(t, row_of(t, &vals));
    };
    let base = |vals: &mut HashMap<&str, V>| {
        vals.insert("TABLE_CATALOG", s("def"));
        vals.insert("TABLE_SCHEMA", s(schema));
        vals.insert("TABLE_NAME", s(def.name.clone()));
    };
    let mut v = HashMap::new();
    base(&mut v);
    v.insert("TABLE_TYPE", s("BASE TABLE"));
    v.insert("ENGINE", s("InnoDB"));
    v.insert("VERSION", V::U(10));
    v.insert("ROW_FORMAT", s("Dynamic"));
    v.insert("TABLE_ROWS", V::U(0));
    v.insert("AVG_ROW_LENGTH", V::U(0));
    v.insert("DATA_LENGTH", V::U(16384));
    v.insert("MAX_DATA_LENGTH", V::U(0));
    v.insert("INDEX_LENGTH", V::U(16384 * def.keys.iter().filter(|k| k.kind != KeyKind::Primary).count() as u64));
    v.insert("DATA_FREE", V::U(0));
    v.insert("AUTO_INCREMENT", next.map_or(V::N, V::U));
    v.insert("TABLE_COLLATION", s(def.cs.1.clone()));
    v.insert("CREATE_OPTIONS", s(""));
    v.insert("TABLE_COMMENT", s(def.comment.clone().unwrap_or_default()));
    v.insert("MAX_INDEX_LENGTH", V::U(0));
    v.insert("TEMPORARY", s("N"));
    put("TABLES", v);
    for (i, c) in def.cols.iter().enumerate() {
        let mut v = HashMap::new();
        base(&mut v);
        v.insert("COLUMN_NAME", s(c.name.clone()));
        v.insert("ORDINAL_POSITION", V::U(i as u64 + 1));
        v.insert("COLUMN_DEFAULT", is_default(c));
        v.insert("IS_NULLABLE", s(if c.not_null { "NO" } else { "YES" }));
        v.insert("DATA_TYPE", s(c.ty.base.clone()));
        let (cl, ol) = lengths(c);
        v.insert("CHARACTER_MAXIMUM_LENGTH", cl);
        v.insert("CHARACTER_OCTET_LENGTH", ol);
        let (p, sc, dp) = numeric(c);
        v.insert("NUMERIC_PRECISION", p);
        v.insert("NUMERIC_SCALE", sc);
        v.insert("DATETIME_PRECISION", dp);
        if let Some((cs, co)) = &c.ty.cs {
            v.insert("CHARACTER_SET_NAME", s(cs.clone()));
            v.insert("COLLATION_NAME", s(co.clone()));
        }
        v.insert("COLUMN_TYPE", s(c.ty.print()));
        v.insert("COLUMN_KEY", s(key_of(def, &c.name)));
        v.insert("EXTRA", s(extra_of(c)));
        v.insert("PRIVILEGES", s("select,insert,update,references"));
        v.insert("COLUMN_COMMENT", s(c.comment.clone().unwrap_or_default()));
        v.insert("IS_GENERATED", s(if c.generated.is_some() { "ALWAYS" } else { "NEVER" }));
        v.insert("GENERATION_EXPRESSION", c.generated.as_ref().map_or(V::N, |(g, _)| s(define::expr::print(g))));
        v.insert("IS_SYSTEM_TIME_PERIOD_START", s("NO"));
        v.insert("IS_SYSTEM_TIME_PERIOD_END", s("NO"));
        put("COLUMNS", v);
    }
    for k in &def.keys {
        for (i, p) in k.parts.iter().enumerate() {
            let mut v = HashMap::new();
            base(&mut v);
            v.insert("NON_UNIQUE", V::U(u64::from(!matches!(k.kind, KeyKind::Primary | KeyKind::Unique))));
            v.insert("INDEX_SCHEMA", s(schema));
            v.insert("INDEX_NAME", s(k.name.clone()));
            v.insert("SEQ_IN_INDEX", V::U(i as u64 + 1));
            v.insert("COLUMN_NAME", s(p.col.clone()));
            v.insert("COLLATION", s(if p.desc { "D" } else { "A" }));
            v.insert("CARDINALITY", V::U(0));
            v.insert("SUB_PART", p.len.map_or(V::N, |l| V::U(u64::from(l))));
            let nullable = def.col(&p.col).is_some_and(|ci| !def.cols[ci].not_null);
            v.insert("NULLABLE", s(if nullable { "YES" } else { "" }));
            v.insert("INDEX_TYPE", s(k.using.clone().unwrap_or_else(|| "BTREE".into())));
            v.insert("COMMENT", s(""));
            v.insert("INDEX_COMMENT", s(k.comment.clone().unwrap_or_default()));
            v.insert("IGNORED", s(if k.ignored { "YES" } else { "NO" }));
            put("STATISTICS", v);
        }
    }
    let constraint = |name: &str, ty: &str| {
        let mut v = HashMap::new();
        v.insert("CONSTRAINT_CATALOG", s("def"));
        v.insert("CONSTRAINT_SCHEMA", s(schema));
        v.insert("CONSTRAINT_NAME", s(name));
        v.insert("TABLE_SCHEMA", s(schema));
        v.insert("TABLE_NAME", s(def.name.clone()));
        v.insert("CONSTRAINT_TYPE", s(ty));
        v
    };
    for k in def.keys.iter().filter(|k| matches!(k.kind, KeyKind::Primary | KeyKind::Unique)) {
        put("TABLE_CONSTRAINTS", constraint(&k.name, if k.kind == KeyKind::Primary { "PRIMARY KEY" } else { "UNIQUE" }));
        for (i, p) in k.parts.iter().enumerate() {
            let mut v = HashMap::new();
            v.insert("CONSTRAINT_CATALOG", s("def"));
            v.insert("CONSTRAINT_SCHEMA", s(schema));
            v.insert("CONSTRAINT_NAME", s(k.name.clone()));
            base(&mut v);
            v.insert("COLUMN_NAME", s(p.col.clone()));
            v.insert("ORDINAL_POSITION", V::U(i as u64 + 1));
            put("KEY_COLUMN_USAGE", v);
        }
    }
    for f in &def.fks {
        put("TABLE_CONSTRAINTS", constraint(&f.name, "FOREIGN KEY"));
        for (i, c) in f.cols.iter().enumerate() {
            let mut v = HashMap::new();
            v.insert("CONSTRAINT_CATALOG", s("def"));
            v.insert("CONSTRAINT_SCHEMA", s(schema));
            v.insert("CONSTRAINT_NAME", s(f.name.clone()));
            base(&mut v);
            v.insert("COLUMN_NAME", s(c.clone()));
            v.insert("ORDINAL_POSITION", V::U(i as u64 + 1));
            v.insert("POSITION_IN_UNIQUE_CONSTRAINT", V::U(i as u64 + 1));
            v.insert("REFERENCED_TABLE_SCHEMA", s(f.ref_db.clone().unwrap_or_else(|| schema.to_owned())));
            v.insert("REFERENCED_TABLE_NAME", s(f.ref_table.clone()));
            v.insert("REFERENCED_COLUMN_NAME", s(f.ref_cols.get(i).cloned().unwrap_or_default()));
            put("KEY_COLUMN_USAGE", v);
        }
        let mut v = HashMap::new();
        v.insert("CONSTRAINT_CATALOG", s("def"));
        v.insert("CONSTRAINT_SCHEMA", s(schema));
        v.insert("CONSTRAINT_NAME", s(f.name.clone()));
        v.insert("UNIQUE_CONSTRAINT_CATALOG", s("def"));
        v.insert("UNIQUE_CONSTRAINT_SCHEMA", s(f.ref_db.clone().unwrap_or_else(|| schema.to_owned())));
        v.insert("UNIQUE_CONSTRAINT_NAME", s("PRIMARY"));
        v.insert("MATCH_OPTION", s("NONE"));
        v.insert("UPDATE_RULE", s(f.on_update.clone().unwrap_or_else(|| "RESTRICT".into())));
        v.insert("DELETE_RULE", s(f.on_delete.clone().unwrap_or_else(|| "RESTRICT".into())));
        v.insert("TABLE_NAME", s(def.name.clone()));
        v.insert("REFERENCED_TABLE_NAME", s(f.ref_table.clone()));
        put("REFERENTIAL_CONSTRAINTS", v);
    }
    // Checks: a column's own check is named after its column, at level
    // `Column`; table checks at level `Table`.
    let mut checks: Vec<(String, &'static str, String)> = def
        .cols
        .iter()
        .filter_map(|c| c.check.as_ref().map(|e| (c.name.clone(), "Column", define::expr::print(e))))
        .collect();
    checks.extend(def.checks.iter().map(|c| (c.name.clone(), "Table", define::expr::print(&c.expr))));
    for (name, level, clause) in checks {
        put("TABLE_CONSTRAINTS", constraint(&name, "CHECK"));
        let mut v = HashMap::new();
        v.insert("CONSTRAINT_CATALOG", s("def"));
        v.insert("CONSTRAINT_SCHEMA", s(schema));
        v.insert("TABLE_NAME", s(def.name.clone()));
        v.insert("CONSTRAINT_NAME", s(name));
        v.insert("LEVEL", s(level));
        v.insert("CHECK_CLAUSE", s(clause));
        put("CHECK_CONSTRAINTS", v);
    }
}

/// The physical schemas of tenant `tenant` that `sql` names, with their
/// databases.
fn schemas_named(sql: &str, tenant: u64) -> Vec<(String, DbId)> {
    let prefix = format!("t{tenant}_d");
    let mut out: Vec<(String, DbId)> = Vec::new();
    let b = sql.as_bytes();
    let mut i = 0;
    while let Some(k) = sql[i..].find(&prefix) {
        let start = i + k;
        let mut j = start;
        while j < b.len() && (b[j].is_ascii_alphanumeric() || b[j] == b'_') {
            j += 1;
        }
        let name = &sql[start..j];
        if let Ok(sn) = name.parse::<sprig_common::names::SchemaName>()
            && let Some(db) = sn.db()
            && !out.iter().any(|(n, _)| n == name)
        {
            out.push((name.to_owned(), db));
        }
        i = j.max(start + 1);
    }
    out
}

/// The name layer's statement with two of its forms put the planner's way:
/// `CONVERT('x' USING cs) COLLATE c` (a logical name as the view's
/// character set) is the string itself, and the conjuncts that hide keys
/// on hidden columns, `AND (a, b) NOT IN (SELECT ...)`, go (the engine's
/// tables have no hidden columns).
fn simplify(sql: &str) -> String {
    // Functional key parts (the MySQL port's hidden `_sprig_fx_` columns)
    // don't exist on the engine: their join and the columns it feeds go.
    let mut owned = sql.replace("IF(`_sprig_fc` IS NULL, `COLUMN_NAME`, NULL)", "`COLUMN_NAME`").replace(
        "REPLACE(REPLACE(`_sprig_fe`, 'lcase(', 'lower('), 'ucase(', 'upper(')",
        "NULL",
    );
    if let Some(a) = owned.find(" LEFT JOIN (SELECT `TABLE_NAME` AS `_sprig_ft`") {
        let tail_marker = "AND `_sprig_fc` = `COLUMN_NAME`";
        if let Some(b) = owned[a..].find(tail_marker) {
            owned.replace_range(a..a + b + tail_marker.len(), "");
        }
    }
    let sql = owned.as_str();
    let mut out = String::with_capacity(sql.len());
    let b = sql.as_bytes();
    let mut i = 0;
    while i < b.len() {
        let rest = &sql[i..];
        if rest.len() >= 9 && rest[..9].eq_ignore_ascii_case("CONVERT('") {
            // The string literal, then ` USING cs)`, then ` COLLATE c`.
            let mut j = i + 9;
            while j < b.len() {
                if b[j] == b'\\' {
                    j += 2;
                    continue;
                }
                if b[j] == b'\'' {
                    if b.get(j + 1) == Some(&b'\'') {
                        j += 2;
                        continue;
                    }
                    break;
                }
                j += 1;
            }
            let lit = &sql[i + 8..=j.min(b.len() - 1)];
            let after = &sql[(j + 1).min(b.len())..];
            if let Some(using) = after.strip_prefix(" USING ")
                && let Some(close) = using.find(')')
            {
                let mut k = j + 1 + 7 + close + 1;
                let tail = &sql[k..];
                if tail.len() > 9 && tail[..9].eq_ignore_ascii_case(" COLLATE ") {
                    let name_len = tail[9..].find(|c: char| !(c.is_ascii_alphanumeric() || c == '_')).unwrap_or(tail.len() - 9);
                    k += 9 + name_len;
                }
                out.push_str(lit);
                i = k;
                continue;
            }
        }
        if rest.starts_with(" AND (`") && let Some(p) = rest.find(") NOT IN (SELECT ") {
            let head = &rest[6..p];
            if head.starts_with('`') && !head.contains('(') {
                // Skip to the parenthesis that closes the subquery.
                let start = i + p + ") NOT IN (".len();
                let mut depth = 1;
                let mut k = start;
                let mut in_str = false;
                while k < b.len() && depth > 0 {
                    match b[k] {
                        b'\'' => in_str = !in_str,
                        b'(' if !in_str => depth += 1,
                        b')' if !in_str => depth -= 1,
                        _ => {}
                    }
                    k += 1;
                }
                i = k;
                continue;
            }
        }
        let ch = rest.chars().next().unwrap_or(' ');
        out.push(ch);
        i += ch.len_utf8();
    }
    out
}

/// Whether `sql` reads one of the tables built here.
pub(crate) fn reads_tables(sql: &str) -> bool {
    let l = sql.to_ascii_lowercase();
    l.contains("information_schema")
        && TABLES.iter().any(|t| {
            let t = t.to_ascii_lowercase();
            l.contains(&format!("information_schema.{t}")) || l.contains(&format!("information_schema`.`{t}`"))
        })
}

/// What the engine needs to answer: the tables' definitions per database
/// and their counters.
pub(crate) struct Source<'a> {
    pub eng: &'a EngineCatalog,
    pub tenant: u64,
    /// The next `AUTO_INCREMENT` value per table object.
    pub counters: &'a HashMap<u64, u64>,
}

/// The answer to `sql`, or `None` when the engine leaves it to the
/// backend.
pub(crate) fn answer(sql: &str, src: &Source<'_>, mysql: bool) -> Option<Answer> {
    match try_answer(sql, src, mysql) {
        Ok(a) => Some(a),
        Err(why) => {
            tracing::info!(why, sql, "information_schema: left to the backend");
            None
        }
    }
}

fn try_answer(sql: &str, src: &Source<'_>, mysql: bool) -> Result<Answer, String> {
    use sqlparser::dialect::MySqlDialect;
    use sqlparser::parser::Parser;
    let simplified = simplify(sql);
    let sql = simplified.as_str();
    let stmts = Parser::parse_sql(&MySqlDialect {}, sql).map_err(|e| format!("parse: {e}"))?;
    let [stmt] = stmts.as_slice() else { return Err("not one statement".into()) };
    let mut db = MemDb::new("information_schema");
    for t in TABLES {
        db.create_table(plan_def(t));
    }
    for (schema, dbid) in schemas_named(sql, src.tenant) {
        if !src.eng.dbs.contains(&dbid) {
            continue;
        }
        let mut objs: Vec<(&String, &sprig_common::ids::ObjectId)> =
            src.eng.by_name.iter().filter(|((d, _), _)| *d == dbid).map(|((_, n), o)| (n, o)).collect();
        objs.sort();
        for (name, o) in objs {
            if name.starts_with("__sprig_") {
                continue;
            }
            let Some(text) = src.eng.texts.get(o) else { continue };
            let Ok(def) = define::parse_stored(text) else { continue };
            add_table(&mut db, &schema, &def, src.counters.get(&o.0).copied());
        }
    }
    let persona = if mysql { sprig_exec::Persona::MySql } else { sprig_exec::Persona::MariaDb };
    let session = sprig_exec::Session::new(persona);
    let storage: Arc<dyn sprig_plan::access::Storage> = Arc::new(db.clone());
    let planner = sprig_plan::Planner::new(&db, storage, &session);
    let mut plan = planner.plan(stmt, &[]).map_err(|e| format!("plan: {}", e.message))?;
    let mut ctx = sprig_exec::Ctx::with_params(&session, &[]);
    let mut rows = Vec::new();
    while let Some(r) = plan.source.next(&mut ctx).map_err(|e| format!("run: {}", e.message))? {
        rows.push(r);
    }
    // Every result column must be one of the tables' own columns.
    let l = sql.to_ascii_lowercase();
    // The table the outer query reads: the name layer names its derived
    // table after it.
    let table = TABLES
        .iter()
        .find(|t| l.contains(&format!(") as `{}`", t.to_ascii_lowercase())))
        .or_else(|| {
            TABLES
                .iter()
                .find(|t| l.contains(&format!("information_schema.{}", t.to_ascii_lowercase())))
        })
        .ok_or("no table")?;
    let mut defs = Vec::new();
    for c in &plan.columns {
        match cols_of(table).find(|x| x.1.eq_ignore_ascii_case(&c.name)) {
            Some(&(_, org, cs, len, ty, flags)) => {
                defs.push(coldef("information_schema", table, &c.name, org, cs, len, ty, flags, 0));
            }
            // A column the name layer computes (the MySQL port's
            // `IS_VISIBLE`, `EXPRESSION`): a string of the derived table.
            None => defs.push(coldef("", table, &c.name, "", 224, 256, 0xfd, 0x0000, 0)),
        }
    }
    let mut out = Vec::new();
    for r in rows {
        let vals: Vec<Option<Bytes>> = r
            .iter()
            .map(|v| match v {
                Datum::Null => None,
                Datum::Str(b) | Datum::Bytes(b) => Some(b.clone()),
                Datum::UInt(n) => Some(Bytes::from(n.to_string())),
                Datum::Int(n) => Some(Bytes::from(n.to_string())),
                other => Some(Bytes::from(format!("{other:?}"))),
            })
            .collect();
        out.push(sprig_proto_mysql::resultset::encode_text_row(vals.iter().map(|v| v.as_deref())));
    }
    Ok(Answer { defs, rows: out, no_index: true })
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::Arc;

    use sprig_common::ids::{DbId, ObjectId};

    use super::{Source, answer};
    use crate::engine::catalog::EngineCatalog;

    #[test]
    fn a_rewritten_query_runs_over_the_tables() {
        let text = "CREATE TABLE `k2` (\n  `id` int(11) NOT NULL,\n  `a` int(11) DEFAULT NULL,\n  PRIMARY KEY (`id`),\n  UNIQUE KEY `ua` (`a`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_uca1400_ai_ci";
        let mut eng = EngineCatalog::default();
        eng.dbs.insert(DbId(3));
        eng.by_name.insert((DbId(3), "k2".into()), ObjectId(20));
        eng.texts.insert(ObjectId(20), Arc::from(text));
        let counters = HashMap::new();
        let src = Source { eng: &eng, tenant: 7, counters: &counters };
        let sql = "SELECT CONSTRAINT_NAME, CONSTRAINT_TYPE FROM (SELECT `CONSTRAINT_CATALOG`, 'cc_ddl' AS `CONSTRAINT_SCHEMA`, `CONSTRAINT_NAME`, 'cc_ddl' AS `TABLE_SCHEMA`, `TABLE_NAME`, `CONSTRAINT_TYPE` FROM information_schema.TABLE_CONSTRAINTS WHERE `TABLE_SCHEMA` = 't7_d3_g1_s1' AND `TABLE_NAME` = 'k2' AND LEFT(`TABLE_NAME`, 8) <> '__sprig_') AS `TABLE_CONSTRAINTS` WHERE table_schema = 'cc_ddl' AND table_name = 'k2' ORDER BY CONSTRAINT_NAME";
        let a = answer(sql, &src, false).expect("answered");
        assert_eq!(a.rows.len(), 2);
    }

    #[test]
    fn the_statistics_view_of_both_ports_runs() {
        let text = "CREATE TABLE `v1` (\n  `id` int(11) NOT NULL,\n  `a` int(11) DEFAULT NULL,\n  PRIMARY KEY (`id`),\n  KEY `ka` (`a`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_uca1400_ai_ci";
        let mut eng = EngineCatalog::default();
        eng.dbs.insert(DbId(37));
        eng.by_name.insert((DbId(37), "v1".into()), ObjectId(20));
        eng.texts.insert(ObjectId(20), Arc::from(text));
        let counters = HashMap::new();
        let src = Source { eng: &eng, tenant: 61_306_000_000, counters: &counters };
        if let Ok(sql) = std::fs::read_to_string("../../target/agent-c-catalog/obs/stat.sql") {
            let a = super::try_answer(&sql, &src, true).unwrap_or_else(|e| panic!("{e}"));
            assert_eq!(a.rows.len(), 2);
        }
        let maria = "SELECT INDEX_NAME, SEQ_IN_INDEX, COLUMN_NAME, NON_UNIQUE FROM (SELECT `TABLE_CATALOG`, CONVERT('cc_ddl' USING utf8mb3) COLLATE utf8mb3_general_ci AS `TABLE_SCHEMA`, `TABLE_NAME`, `NON_UNIQUE`, `INDEX_NAME`, `SEQ_IN_INDEX`, `COLUMN_NAME` FROM information_schema.STATISTICS WHERE `TABLE_SCHEMA` = 't61306000000_d37_g37_s37' AND LEFT(`TABLE_NAME`, 8) <> '__sprig_' AND (`TABLE_NAME`, `INDEX_NAME`) NOT IN (SELECT `TABLE_NAME`, `INDEX_NAME` FROM information_schema.STATISTICS WHERE `TABLE_SCHEMA` = 't61306000000_d37_g37_s37' AND LEFT(`COLUMN_NAME`, 7) = '_sprig_') AND `TABLE_NAME` = 'v1') AS `STATISTICS` WHERE table_schema = 'cc_ddl' AND table_name = 'v1' ORDER BY INDEX_NAME, SEQ_IN_INDEX";
        let a = super::try_answer(maria, &src, false).unwrap_or_else(|e| panic!("{e}"));
        assert_eq!(a.rows.len(), 2);
    }

    #[test]
    fn the_name_layers_forms_are_simplified() {
        assert_eq!(
            super::simplify("SELECT CONVERT('cc_ddl' USING utf8mb3) COLLATE utf8mb3_general_ci AS `TABLE_SCHEMA` FROM x WHERE a = 1 AND (`TABLE_NAME`, `INDEX_NAME`) NOT IN (SELECT `TABLE_NAME`, `INDEX_NAME` FROM y WHERE c IN ('a)', 'b')) ORDER BY 1"),
            "SELECT 'cc_ddl' AS `TABLE_SCHEMA` FROM x WHERE a = 1 ORDER BY 1"
        );
    }
}
