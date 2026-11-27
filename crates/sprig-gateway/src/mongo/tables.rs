//! SQL tables read from the MongoDB port: `find`, `aggregate`, `count` and
//! `distinct` on a name that is no collection but a table of the SQL
//! database with the MongoDB database's name, and `$lookup` stages whose
//! `from` is such a table.
//!
//! ```text
//! the table's rows      SELECT * FROM db.t [WHERE <implied predicate>]
//!                       on the connection's own SQL session, as the
//!                       client's statement: the user's privileges and
//!                       row-level security apply
//! as documents          one field per column, in column order, typed by
//!                       the type map (below); no _id unless a column is so
//!                       named
//! the command           run by the store over those documents
//!                       ({aggregate: 1, pipeline: [{$documents: ...}, ...]}),
//!                       so every filter, stage and projection keeps
//!                       MongoDB's rules
//! ```
//!
//! - **Pushed predicates only narrow.** A filter's top-level equalities,
//!   `$in` lists and numeric ranges on number and text columns are sent to
//!   SQL as a condition every matching row meets (SQL may return more: a
//!   collation that ignores case, `'5'` equal to `5`), and the store's
//!   matcher then applies the whole filter with MongoDB's rules.
//! - **`$lookup` into a table** runs the pipeline up to that stage on the
//!   store, reads the table's rows whose `foreignField` is one of the
//!   input's `localField` values (`IN (...)`, `IS NULL` for null and
//!   missing; batches of 500 values), joins them with MongoDB's equality,
//!   runs the stage's own `pipeline` over each document's matches (its
//!   `let` variables bound to that document), and runs the rest of the
//!   pipeline over the joined documents.
//! - **In a MongoDB transaction** the table is read in the transaction's
//!   own engine transaction, lent to the SQL session for each statement:
//!   one snapshot with the transaction's documents, and the table rows it
//!   wrote ([`super::tablewrite`]). A failed command ends the transaction;
//!   a conflict of its SQL statement is a transient error. The result
//!   comes in one batch.
//! - **Writes** to a table are [`super::tablewrite`]'s.
//!
//! | SQL column | BSON |
//! |---|---|
//! | `TINYINT(1)` | bool |
//! | other integers, `YEAR`, `BIT` | int32 when it fits, else int64 (an unsigned value past int64: Decimal128) |
//! | `DECIMAL` | Decimal128 |
//! | `FLOAT`, `DOUBLE` | double |
//! | `DATE`, `DATETIME`, `TIMESTAMP` | date (read as UTC) |
//! | `JSON` | document, array or scalar |
//! | `UUID` (MariaDB) | binary subtype 4 |
//! | binary strings, `GEOMETRY` | binary subtype 0 |
//! | `CHAR`, `VARCHAR`, `TEXT`, `ENUM`, `SET`, `TIME` | string |
//! | `NULL` | null |

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use bson::spec::BinarySubtype;
use bson::{Binary, Bson, Document, doc};
use sprig_doc::agg::AggLimits;
use sprig_doc::docstore::cmd::{Reply, TxnLoan};
use sprig_doc::docstore::{DocStore, Name};
use sprig_proto_mongo::errors::{MongoError, code};
use sprig_proto_mysql::ColumnDef;
use sprig_proto_mysql::resultset::column_type as ct;

use super::commands::owner;
use super::conn::Conn;

/// `$lookup` values per SQL read.
const LOOKUP_BATCH: usize = 500;

/// Memory per stage, as the store's own.
const STAGE_BYTES: usize = 100 * 1024 * 1024;

/// The binary character set.
pub(super) const BINARY: u16 = 63;

/// A table's column definitions and rows.
pub(super) struct TableRows {
    pub(super) defs: Vec<ColumnDef>,
    pub(super) rows: sprig_doc::engine::Rows,
    /// The `JSON` columns (MariaDB's port shows them as `LONGTEXT` with a
    /// `json_valid` check).
    pub(super) json: HashSet<String>,
}

pub(super) fn q(name: &str) -> String {
    format!("`{}`", name.replace('`', "``"))
}

pub(super) fn sql_failed(e: &sprig_doc::engine::SqlError) -> MongoError {
    if let Some(m) = &e.mongo {
        return m.clone();
    }
    let c = match e.errno {
        Some(1142 | 1143 | 1044 | 1045) => code::UNAUTHORIZED,
        // A deadlock, a snapshot conflict, a lock wait that ran out.
        Some(1213 | 1020 | 1205) => code::WRITE_CONFLICT,
        _ => code::BAD_VALUE,
    };
    MongoError::new(c, format!("reading an SQL table: {}", e.message))
}

/// Whether `e` says the table (or database) doesn't exist.
fn missing(e: &sprig_doc::engine::SqlError) -> bool {
    matches!(e.errno, Some(1146 | 1049))
}

/// A `TINYINT(1)` column: `BOOLEAN`.
pub(super) fn is_bool(d: &ColumnDef) -> bool {
    d.col_type == ct::TINY && d.length == 1
}

pub(super) fn is_json(d: &ColumnDef) -> bool {
    d.col_type == ct::JSON
        || d.ext_type_name() == Some(&b"json"[..])
        || d.ext_format_name() == Some(&b"json"[..])
}

fn is_text(d: &ColumnDef) -> bool {
    matches!(
        d.col_type,
        ct::VARCHAR
            | ct::VAR_STRING
            | ct::STRING
            | ct::TINY_BLOB
            | ct::MEDIUM_BLOB
            | ct::LONG_BLOB
            | ct::BLOB
            | ct::ENUM
            | ct::SET
    ) && d.charset != BINARY
        && !is_json(d)
        && d.ext_type_name().is_none()
}

fn is_number(d: &ColumnDef) -> bool {
    matches!(
        d.col_type,
        ct::TINY
            | ct::SHORT
            | ct::LONG
            | ct::INT24
            | ct::LONGLONG
            | ct::DECIMAL
            | ct::NEWDECIMAL
            | ct::FLOAT
            | ct::DOUBLE
            | ct::YEAR
    ) && !is_bool(d)
}

/// An integer's BSON value: int32 when it fits.
fn int(s: &str) -> Option<Bson> {
    if let Ok(i) = s.parse::<i64>() {
        return Some(i32::try_from(i).map_or(Bson::Int64(i), Bson::Int32));
    }
    s.parse::<bson::Decimal128>().ok().map(Bson::Decimal128)
}

/// A JSON value as BSON: integers as int32/int64, other numbers as
/// doubles.
fn json_bson(v: serde_json::Value) -> Bson {
    match v {
        serde_json::Value::Null => Bson::Null,
        serde_json::Value::Bool(b) => Bson::Boolean(b),
        serde_json::Value::Number(n) => {
            if let Some(i) = n.as_i64() {
                i32::try_from(i).map_or(Bson::Int64(i), Bson::Int32)
            } else {
                Bson::Double(n.as_f64().unwrap_or(f64::NAN))
            }
        }
        serde_json::Value::String(s) => Bson::String(s),
        serde_json::Value::Array(a) => Bson::Array(a.into_iter().map(json_bson).collect()),
        serde_json::Value::Object(o) => {
            let mut d = Document::new();
            for (k, v) in o {
                d.insert(k, json_bson(v));
            }
            Bson::Document(d)
        }
    }
}

/// `YYYY-MM-DD[ hh:mm:ss[.f]]`, read as UTC, as a BSON date.
fn date(s: &str) -> Option<Bson> {
    let (d, t) = s.split_once(' ').unwrap_or((s, "00:00:00"));
    let (hms, frac) = t.split_once('.').unwrap_or((t, ""));
    let ms: String = frac.chars().chain(std::iter::repeat('0')).take(3).collect();
    let iso = format!("{d}T{hms}.{ms}Z");
    sprig_doc::json::ms_from_iso(&iso).map(|m| Bson::DateTime(bson::DateTime::from_millis(m)))
}

/// A UUID's canonical text as binary subtype 4.
fn uuid(s: &str) -> Option<Bson> {
    let hex: String = s.chars().filter(|c| *c != '-').collect();
    if hex.len() != 32 {
        return None;
    }
    let bytes: Option<Vec<u8>> = (0..16)
        .map(|i| u8::from_str_radix(&hex[2 * i..2 * i + 2], 16).ok())
        .collect();
    Some(Bson::Binary(Binary {
        subtype: BinarySubtype::Uuid,
        bytes: bytes?,
    }))
}

/// One value of column `d` (text protocol bytes) as BSON.
fn value(d: &ColumnDef, json: bool, v: Option<&[u8]>) -> Bson {
    let Some(v) = v else {
        return Bson::Null;
    };
    let s = || String::from_utf8_lossy(v).into_owned();
    let as_string = || Bson::String(s());
    if json || is_json(d) {
        return serde_json::from_slice::<serde_json::Value>(v)
            .map_or_else(|_| as_string(), json_bson);
    }
    if d.ext_type_name() == Some(&b"uuid"[..]) {
        return uuid(&s()).unwrap_or_else(as_string);
    }
    match d.col_type {
        ct::TINY if is_bool(d) => Bson::Boolean(s() != "0"),
        ct::TINY | ct::SHORT | ct::LONG | ct::INT24 | ct::LONGLONG | ct::YEAR => {
            int(&s()).unwrap_or_else(as_string)
        }
        ct::BIT => {
            let mut n: u64 = 0;
            for b in v.iter().take(8) {
                n = (n << 8) | u64::from(*b);
            }
            int(&n.to_string()).unwrap_or(Bson::Null)
        }
        ct::DECIMAL | ct::NEWDECIMAL => s()
            .parse::<bson::Decimal128>()
            .map_or_else(|_| as_string(), Bson::Decimal128),
        ct::FLOAT | ct::DOUBLE => s()
            .parse::<f64>()
            .map_or_else(|_| as_string(), Bson::Double),
        ct::DATE | ct::NEWDATE | ct::DATETIME | ct::DATETIME2 | ct::TIMESTAMP | ct::TIMESTAMP2 => {
            date(&s()).unwrap_or_else(as_string)
        }
        ct::GEOMETRY => Bson::Binary(Binary {
            subtype: BinarySubtype::Generic,
            bytes: v.to_vec(),
        }),
        _ if d.charset == BINARY => Bson::Binary(Binary {
            subtype: BinarySubtype::Generic,
            bytes: v.to_vec(),
        }),
        _ => as_string(),
    }
}

/// The rows as documents: one field per column.
pub(super) fn documents(t: &TableRows) -> Vec<Document> {
    let names: Vec<String> = t
        .defs
        .iter()
        .map(|d| String::from_utf8_lossy(&d.name).into_owned())
        .collect();
    t.rows
        .iter()
        .map(|r| {
            let mut d = Document::new();
            for (i, def) in t.defs.iter().enumerate() {
                let v = r.get(i).and_then(Option::as_deref);
                let json = t.json.contains(&names[i]);
                d.insert(names[i].clone(), value(def, json, v));
            }
            d
        })
        .collect()
}

/// An SQL literal every value MongoDB equates with `v` also equals in SQL,
/// for a column of definition `d`; `None` when there is no such literal
/// (the read then doesn't narrow by it).
fn literal(d: &ColumnDef, v: &Bson) -> Option<String> {
    match v {
        Bson::Int32(i) if is_number(d) => Some(i.to_string()),
        Bson::Int64(i) if is_number(d) => Some(i.to_string()),
        Bson::Double(f) if is_number(d) && f.is_finite() => Some(format!("{f:?}")),
        Bson::Decimal128(x) if is_number(d) => {
            let t = x.to_string();
            t.bytes()
                .all(|c| c.is_ascii_digit() || matches!(c, b'.' | b'-' | b'+' | b'E' | b'e'))
                .then_some(t)
        }
        Bson::String(s) if is_text(d) && !s.contains('\\') && !s.contains('\0') => {
            Some(format!("'{}'", s.replace('\'', "''")))
        }
        Bson::Boolean(b) if is_bool(d) => Some(if *b { "1" } else { "0" }.to_owned()),
        _ => None,
    }
}

/// `col IN (values)` (with `OR col IS NULL` for a null), or `None` when a
/// value has no literal.
fn in_list(d: &ColumnDef, col: &str, vals: &[Bson]) -> Option<String> {
    let mut lits = Vec::new();
    let mut null = false;
    for v in vals {
        match v {
            Bson::Null => null = true,
            _ => lits.push(literal(d, v)?),
        }
    }
    let c = q(col);
    let mut parts = Vec::new();
    if !lits.is_empty() {
        parts.push(format!("{c} IN ({})", lits.join(", ")));
    }
    if null {
        parts.push(format!("{c} IS NULL"));
    }
    match parts.len() {
        0 => Some("FALSE".to_owned()),
        1 => parts.pop(),
        _ => Some(format!("({})", parts.join(" OR "))),
    }
}

/// The conditions a filter implies on the table's columns (`defs`: the
/// columns that may be narrowed, `JSON` ones left out).
fn implied(filter: &Document, defs: &HashMap<String, ColumnDef>, out: &mut Vec<String>) {
    for (k, v) in filter {
        if k == "$and" {
            if let Bson::Array(a) = v {
                for x in a {
                    if let Bson::Document(d) = x {
                        implied(d, defs, out);
                    }
                }
            }
            continue;
        }
        if k.starts_with('$') || k.contains('.') {
            continue;
        }
        let Some(d) = defs.get(k) else {
            continue;
        };
        let ops = match v {
            Bson::Document(o) if o.keys().next().is_some_and(|x| x.starts_with('$')) => o.clone(),
            Bson::Document(_) | Bson::Array(_) | Bson::RegularExpression(_) => continue,
            other => doc! {"$eq": other.clone()},
        };
        for (op, arg) in &ops {
            let c = q(k);
            let cond = match (op.as_str(), arg) {
                ("$eq", Bson::Null) => Some(format!("{c} IS NULL")),
                ("$eq", a) => literal(d, a).map(|l| format!("{c} = {l}")),
                ("$in", Bson::Array(a)) => in_list(d, k, a),
                ("$gt" | "$gte" | "$lt" | "$lte", a)
                    if is_number(d)
                        && matches!(
                            a,
                            Bson::Int32(_) | Bson::Int64(_) | Bson::Double(_) | Bson::Decimal128(_)
                        ) =>
                {
                    let sym = match op.as_str() {
                        "$gt" => ">",
                        "$gte" => ">=",
                        "$lt" => "<",
                        _ => "<=",
                    };
                    literal(d, a).map(|l| format!("{c} {sym} {l}"))
                }
                _ => None,
            };
            if let Some(cond) = cond {
                out.push(cond);
            }
        }
    }
}

/// The rows of table `db.table` (narrowed by `filter` when given) through
/// the connection's SQL session; `None` when there is no such table.
async fn read(
    c: &mut Conn,
    db: &str,
    table: &str,
    filter: Option<&Document>,
    tx: &mut Loan,
) -> Result<Option<TableRows>, MongoError> {
    lend(c, tx)?;
    let r = read_rows(c, db, table, filter, false).await;
    let back = give_back(c, tx);
    let rows = r?;
    back?;
    Ok(rows)
}

/// [`read`]'s statements, on the connection's SQL session as it is;
/// `lock`: the rows read are locked for a write (`FOR UPDATE`).
pub(super) async fn read_rows(
    c: &mut Conn,
    db: &str,
    table: &str,
    filter: Option<&Document>,
    lock: bool,
) -> Result<Option<TableRows>, MongoError> {
    let from = format!("{}.{}", q(db), q(table));
    let sql = c.sql()?;
    if !sql.engine_database(db) {
        return Ok(None);
    }
    let (defs, _) = match sql
        .query_typed(&format!("SELECT * FROM {from} LIMIT 0"))
        .await
    {
        Ok(x) => x,
        Err(e) if missing(&e) => return Ok(None),
        Err(e) => return Err(sql_failed(&e)),
    };
    let json = json_columns(sql, &from).await?;
    let mut conds = Vec::new();
    if let Some(f) = filter {
        let by_name: HashMap<String, ColumnDef> = defs
            .iter()
            .map(|d| (String::from_utf8_lossy(&d.name).into_owned(), d.clone()))
            .filter(|(n, _)| !json.contains(n))
            .collect();
        implied(f, &by_name, &mut conds);
    }
    let mut text = if conds.is_empty() {
        format!("SELECT * FROM {from}")
    } else {
        format!("SELECT * FROM {from} WHERE {}", conds.join(" AND "))
    };
    if lock {
        text.push_str(" FOR UPDATE");
    }
    let (defs, rows) = sql.query_typed(&text).await.map_err(|e| sql_failed(&e))?;
    Ok(Some(TableRows { defs, rows, json }))
}

/// The `JSON` columns of table `from` (quoted `db`.`t`), read off its
/// `SHOW CREATE TABLE`: the columns with a `json_valid` check.
pub(super) async fn json_columns(
    sql: &mut super::sql::SqlSession,
    from: &str,
) -> Result<HashSet<String>, MongoError> {
    let rows = match sql.query_typed(&format!("SHOW CREATE TABLE {from}")).await {
        Ok((_, rows)) => rows,
        // A source that reads as a table without a definition of its own
        // (a stream, a queue, a Redis database): no JSON columns.
        Err(e) if missing(&e) => return Ok(HashSet::new()),
        Err(e) => return Err(sql_failed(&e)),
    };
    let text = rows
        .first()
        .and_then(|r| r.get(1))
        .and_then(Option::as_deref)
        .map(|b| String::from_utf8_lossy(b).into_owned())
        .unwrap_or_default();
    let mut out = HashSet::new();
    let mut rest = text.as_str();
    while let Some(at) = rest.find("json_valid(`") {
        rest = &rest[at + "json_valid(`".len()..];
        let mut name = String::new();
        let mut chars = rest.char_indices().peekable();
        let mut end = rest.len();
        while let Some((i, c)) = chars.next() {
            if c == '`' {
                if chars.peek().is_some_and(|(_, n)| *n == '`') {
                    name.push('`');
                    chars.next();
                    continue;
                }
                end = i + 1;
                break;
            }
            name.push(c);
        }
        rest = &rest[end..];
        out.insert(name);
    }
    Ok(out)
}

/// Whether `db.name` is a collection (or view) of the store.
fn is_collection(s: &DocStore, tenant: u64, db: &str, name: &str) -> Result<bool, MongoError> {
    Ok(s.coll(tenant, &Name::new(db, name))?.is_some())
}

/// Whether `db.name` is an SQL table (and no collection).
pub(super) async fn is_table(
    c: &mut Conn,
    s: &DocStore,
    tenant: u64,
    db: &str,
    name: &str,
) -> Result<bool, MongoError> {
    if name.is_empty() || is_collection(s, tenant, db, name)? {
        return Ok(false);
    }
    let sql = c.sql()?;
    if !sql.engine_database(db) {
        return Ok(false);
    }
    let probe = format!("SELECT * FROM {}.{} LIMIT 0", q(db), q(name));
    match sql.query_typed(&probe).await {
        // The probe reads a collection too (SQL over collections): one
        // that a concurrent command made meanwhile isn't a table.
        Ok(_) => Ok(!is_collection(s, tenant, db, name)?),
        Err(e) if missing(&e) => Ok(false),
        Err(e) => Err(sql_failed(&e)),
    }
}

/// The MongoDB transaction a command runs in (`None`: none), held while it
/// runs: its engine transaction is lent to the connection's SQL session for
/// each SQL statement ([`lend`], [`give_back`]) and is the store's for the
/// store's own reads.
pub(super) type Loan = Option<TxnLoan>;

fn lost_transaction() -> MongoError {
    MongoError::new(
        code::NO_SUCH_TRANSACTION,
        "the transaction was rolled back by a conflict of its SQL statement",
    )
    .label("TransientTransactionError")
}

/// Lends the transaction's engine transaction to the SQL session; nothing
/// outside a transaction.
pub(super) fn lend(c: &mut Conn, tx: &mut Loan) -> Result<(), MongoError> {
    let Some(l) = tx.as_mut() else {
        return Ok(());
    };
    let dt = l.txn().ok_or_else(lost_transaction)?;
    let engine_down = |e: sprig_common::SprigError| {
        MongoError::new(
            code::INTERNAL_ERROR,
            format!("storage engine: {}", e.message),
        )
    };
    let stand_in = crate::node_engine::engine()
        .map_err(engine_down)?
        .begin(sprig_engine::txn::TxnOptions::default())
        .map_err(engine_down)?;
    let real = dt.swap_engine_txn(stand_in);
    let refused = match c.sql() {
        Ok(sql) => sql.lend_engine_txn(Some(real)),
        Err(e) => {
            dt.swap_engine_txn(real).rollback();
            return Err(e);
        }
    };
    match refused {
        None => Ok(()),
        Some(back) => {
            dt.swap_engine_txn(back).rollback();
            Err(MongoError::new(
                code::OPERATION_NOT_SUPPORTED_IN_TRANSACTION,
                "the SQL session can't join this MongoDB transaction (it is in a transaction of its own)",
            ))
        }
    }
}

/// Takes the engine transaction [`lend`] lent back from the SQL session.
///
/// # Errors
/// The engine rolled it back meanwhile (a deadlock, a snapshot conflict):
/// the MongoDB transaction can't go on (251, transient).
pub(super) fn give_back(c: &mut Conn, tx: &mut Loan) -> Result<(), MongoError> {
    let Some(l) = tx.as_mut() else {
        return Ok(());
    };
    let back = c.sql()?.lend_engine_txn(None);
    let dt = l.txn().ok_or_else(lost_transaction)?;
    match back {
        Some(t) => {
            dt.swap_engine_txn(t).rollback();
            Ok(())
        }
        None => Err(lost_transaction()),
    }
}

/// The end of a command run in transaction `tx`: a failed command ends the
/// transaction, as the store's own commands do, and a conflict is
/// transient.
pub(super) async fn finish(
    s: &DocStore,
    tx: Loan,
    r: Result<Reply, MongoError>,
) -> Result<Reply, MongoError> {
    let Some(loan) = tx else {
        return r;
    };
    match r {
        Ok(reply) => Ok(reply),
        Err(e) => {
            s.loan_failed(loan).await;
            if e.code == code::WRITE_CONFLICT || e.code == code::NO_SUCH_TRANSACTION {
                Err(e.label("TransientTransactionError"))
            } else {
                Err(e)
            }
        }
    }
}

/// The transaction `body` runs in, held for the command.
pub(super) async fn loan_of(
    c: &mut Conn,
    s: &DocStore,
    body: &Document,
) -> Result<Loan, MongoError> {
    let who = owner(c);
    s.txn_loan(who.tenant, &who.user, body).await
}

/// Answers `name` when it reads an SQL table (or `$lookup`s into one);
/// `None` leaves the command to the store.
pub(super) async fn command(
    c: &mut Conn,
    s: &Arc<DocStore>,
    name: &str,
    body: &Document,
    db: &str,
) -> Option<Result<Reply, MongoError>> {
    let tenant = owner(c).tenant;
    match name {
        "find" | "count" | "distinct" => {
            let target = body.get_str(name).ok()?.to_owned();
            match is_table(c, s, tenant, db, &target).await {
                Ok(true) => {}
                Ok(false) => return None,
                Err(e) => return Some(Err(e)),
            }
            let mut tx = match loan_of(c, s, body).await {
                Ok(t) => t,
                Err(e) => return Some(Err(e)),
            };
            let r = read_command(c, s, name, body, db, &target, &mut tx).await;
            Some(finish(s, tx, r).await)
        }
        "aggregate" => {
            let target = body.get_str("aggregate").unwrap_or_default().to_owned();
            let pipeline: Vec<Document> = body
                .get_array("pipeline")
                .ok()?
                .iter()
                .filter_map(|x| x.as_document().cloned())
                .collect();
            let on_table = match is_table(c, s, tenant, db, &target).await {
                Ok(t) => t,
                Err(e) => return Some(Err(e)),
            };
            let mut lookups = Vec::new();
            for (i, st) in pipeline.iter().enumerate() {
                let Some(from) = st
                    .get_document("$lookup")
                    .ok()
                    .and_then(|l| l.get_str("from").ok())
                else {
                    continue;
                };
                match is_table(c, s, tenant, db, from).await {
                    Ok(true) => lookups.push(i),
                    Ok(false) => {}
                    Err(e) => return Some(Err(e)),
                }
            }
            if !on_table && lookups.is_empty() {
                return None;
            }
            let mut tx = match loan_of(c, s, body).await {
                Ok(t) => t,
                Err(e) => return Some(Err(e)),
            };
            let r = aggregate(
                c, s, body, db, &target, on_table, &pipeline, &lookups, &mut tx,
            )
            .await;
            Some(finish(s, tx, r).await)
        }
        "insert" | "update" | "delete" | "findAndModify" | "findandmodify" => {
            let target = body.get_str(name).ok()?.to_owned();
            match is_table(c, s, tenant, db, &target).await {
                Ok(true) => Some(super::tablewrite::command(c, s, name, body, db, &target).await),
                Ok(false) => None,
                Err(e) => Some(Err(e)),
            }
        }
        _ => None,
    }
}

/// `{aggregate: 1, pipeline: [{$documents: docs}, ...stages]}` on the store,
/// its cursor named `ns`.
#[allow(clippy::too_many_arguments)]
async fn over_documents(
    c: &mut Conn,
    s: &DocStore,
    db: &str,
    ns: &str,
    docs: Vec<Document>,
    stages: &[Document],
    cursor: Document,
    tx: &mut Loan,
) -> Result<Reply, MongoError> {
    let who = owner(c);
    if let Some(l) = tx.as_mut() {
        // In a transaction: the stages read its snapshot, and the result
        // comes in one batch.
        let dt = l.txn().ok_or_else(lost_transaction)?;
        let mut p = vec![doc! {
            "$documents": docs.into_iter().map(Bson::Document).collect::<Vec<_>>()
        }];
        p.extend(stages.iter().cloned());
        let first = s.aggregate(who.tenant, db, "", &p, Some(dt)).await?;
        return Ok(Reply::Cursor {
            ns: ns.to_owned(),
            first,
            rest: None,
        });
    }
    let mut p: Vec<Bson> = vec![Bson::Document(doc! {
        "$documents": docs.into_iter().map(Bson::Document).collect::<Vec<_>>()
    })];
    p.extend(stages.iter().cloned().map(Bson::Document));
    let cmd = doc! {"aggregate": 1, "pipeline": p, "cursor": cursor};
    let reply = s
        .command(who.tenant, &who.user, db, "aggregate", &cmd)
        .await?;
    Ok(match reply {
        Reply::Cursor { first, rest, .. } => Reply::Cursor {
            ns: ns.to_owned(),
            first,
            rest,
        },
        d @ Reply::Doc(_) => d,
    })
}

/// `find`, `count` or `distinct` on table `target`.
async fn read_command(
    c: &mut Conn,
    s: &DocStore,
    name: &str,
    body: &Document,
    db: &str,
    target: &str,
    tx: &mut Loan,
) -> Result<Reply, MongoError> {
    let filter_key = if name == "find" { "filter" } else { "query" };
    let filter = body
        .get_document(filter_key)
        .ok()
        .cloned()
        .unwrap_or_default();
    let rows = read(c, db, target, Some(&filter), tx)
        .await?
        .ok_or_else(|| MongoError::new(code::NAMESPACE_NOT_FOUND, "table not found"))?;
    let docs = documents(&rows);
    let mut stages = vec![doc! {"$match": filter}];
    let num = |k: &str| body.get(k).and_then(sprig_doc::filter::as_i64);
    let ns = format!("{db}.{target}");
    match name {
        "find" => {
            if let Ok(sort) = body.get_document("sort")
                && !sort.is_empty()
            {
                stages.push(doc! {"$sort": sort.clone()});
            }
            if let Some(n) = num("skip").filter(|n| *n > 0) {
                stages.push(doc! {"$skip": n});
            }
            let mut single = body.get_bool("singleBatch").unwrap_or(false);
            if let Some(n) = num("limit").filter(|n| *n != 0) {
                if n < 0 {
                    single = true;
                }
                stages.push(doc! {"$limit": n.abs()});
            }
            if let Ok(p) = body.get_document("projection")
                && !p.is_empty()
            {
                stages.push(doc! {"$project": p.clone()});
            }
            let mut cursor = Document::new();
            if let Some(b) = num("batchSize") {
                cursor.insert("batchSize", b);
            }
            let reply = over_documents(c, s, db, &ns, docs, &stages, cursor, tx).await?;
            Ok(match reply {
                Reply::Cursor { ns, first, .. } if single => Reply::Cursor {
                    ns,
                    first,
                    rest: None,
                },
                r => r,
            })
        }
        "count" => {
            if let Some(n) = num("skip").filter(|n| *n > 0) {
                stages.push(doc! {"$skip": n});
            }
            if let Some(n) = num("limit").filter(|n| *n != 0) {
                stages.push(doc! {"$limit": n.abs()});
            }
            stages.push(doc! {"$count": "n"});
            let reply = over_documents(c, s, db, &ns, docs, &stages, Document::new(), tx).await?;
            let n = match reply {
                Reply::Cursor { first, .. } => first
                    .first()
                    .and_then(|d| d.get("n"))
                    .and_then(sprig_doc::filter::as_i64)
                    .unwrap_or(0),
                Reply::Doc(_) => 0,
            };
            Ok(Reply::Doc(
                doc! {"n": i32::try_from(n).map_or(Bson::Int64(n), Bson::Int32), "ok": 1.0},
            ))
        }
        _ => {
            let key = body
                .get_str("key")
                .map_err(|_| MongoError::new(code::FAILED_TO_PARSE, "distinct needs a key"))?
                .to_owned();
            let tenant = owner(c).tenant;
            let matched = run_stages(s, tenant, db, docs, &stages, tx).await?;
            let values = sprig_doc::docstore::pipeline::distinct_values(&matched, &key);
            Ok(Reply::Doc(doc! {"values": values, "ok": 1.0}))
        }
    }
}

fn limits(s: &DocStore) -> AggLimits {
    AggLimits {
        stage_bytes: STAGE_BYTES,
        now: s.now(),
    }
}

/// An aggregation over a table, or with `$lookup`s into tables (at the
/// positions `lookups`).
#[allow(clippy::too_many_arguments)]
async fn aggregate(
    c: &mut Conn,
    s: &DocStore,
    body: &Document,
    db: &str,
    target: &str,
    on_table: bool,
    pipeline: &[Document],
    lookups: &[usize],
    tx: &mut Loan,
) -> Result<Reply, MongoError> {
    let tenant = owner(c).tenant;
    let ns = if target.is_empty() {
        format!("{db}.$cmd.aggregate")
    } else {
        format!("{db}.{target}")
    };
    // The input: the table's rows, or the collection read by the first
    // stretch of the pipeline.
    let mut docs: Option<Vec<Document>> = if on_table {
        let f = pipeline
            .first()
            .and_then(|st| st.get_document("$match").ok())
            .cloned();
        let rows = read(c, db, target, f.as_ref(), tx)
            .await?
            .ok_or_else(|| MongoError::new(code::NAMESPACE_NOT_FOUND, "table not found"))?;
        Some(documents(&rows))
    } else {
        None
    };
    let lim = limits(s);
    let mut from = 0usize;
    for &at in lookups {
        let stretch = &pipeline[from..at];
        let input = match docs.take() {
            Some(d) => run_stages(s, tenant, db, d, stretch, tx).await?,
            None => {
                s.aggregate(tenant, db, target, stretch, txn_of(tx)?)
                    .await?
            }
        };
        let spec = pipeline[at]
            .get_document("$lookup")
            .cloned()
            .unwrap_or_default();
        docs = Some(lookup(c, s, tenant, db, input, &spec, &lim, tx).await?);
        from = at + 1;
    }
    let input = match docs {
        Some(d) => d,
        None => s.aggregate(tenant, db, target, &[], txn_of(tx)?).await?,
    };
    let cursor = body.get_document("cursor").cloned().unwrap_or_default();
    over_documents(c, s, db, &ns, input, &pipeline[from..], cursor, tx).await
}

/// `stages` over `docs` on the store (a `$lookup` among them reads the
/// tenant's collections): every result document.
async fn run_stages(
    s: &DocStore,
    tenant: u64,
    db: &str,
    docs: Vec<Document>,
    stages: &[Document],
    tx: &mut Loan,
) -> Result<Vec<Document>, MongoError> {
    let mut p = vec![doc! {
        "$documents": docs.into_iter().map(Bson::Document).collect::<Vec<_>>()
    }];
    p.extend(stages.iter().cloned());
    s.aggregate(tenant, db, "", &p, txn_of(tx)?).await
}

/// The store's side of transaction `tx`, for its reads.
fn txn_of(tx: &mut Loan) -> Result<Option<&mut sprig_doc::docstore::DocTxn>, MongoError> {
    match tx.as_mut() {
        None => Ok(None),
        Some(l) => l.txn().map(Some).ok_or_else(lost_transaction),
    }
}

/// A `$lookup` stage whose `from` is an SQL table, over `docs`.
#[allow(clippy::too_many_lines, clippy::too_many_arguments)] // candidates, the join, then the pipeline per document
async fn lookup(
    c: &mut Conn,
    s: &DocStore,
    tenant: u64,
    db: &str,
    docs: Vec<Document>,
    spec: &Document,
    lim: &AggLimits,
    tx: &mut Loan,
) -> Result<Vec<Document>, MongoError> {
    let from = spec.get_str("from").unwrap_or_default().to_owned();
    let as_f = spec
        .get_str("as")
        .map_err(|_| {
            MongoError::new(
                code::FAILED_TO_PARSE,
                "must specify 'as' field for a $lookup",
            )
        })?
        .to_owned();
    let lets = spec
        .get_document("let")
        .ok()
        .filter(|l| !l.is_empty())
        .cloned();
    let local = spec.get_str("localField").ok().map(str::to_owned);
    let foreign = spec.get_str("foreignField").ok().map(str::to_owned);
    let pipe: Vec<Document> = spec
        .get_array("pipeline")
        .map(|a| a.iter().filter_map(|x| x.as_document().cloned()).collect())
        .unwrap_or_default();
    let fields = local.as_deref().zip(foreign.as_deref());
    let mut out = Vec::with_capacity(docs.len());
    let mut bytes = 0usize;
    // Without `localField`, every row is a candidate: read once.
    let all_rows = if fields.is_none() {
        Some(
            read(c, db, &from, None, tx)
                .await?
                .map(|r| documents(&r))
                .unwrap_or_default(),
        )
    } else {
        None
    };
    if lets.is_none()
        && let Some(rows) = &all_rows
    {
        // An uncorrelated pipeline: the rows through it once.
        let fd = run_stages(s, tenant, db, rows.clone(), &pipe, tx).await?;
        return sprig_doc::engine::join_chunk(&docs, &fd, None, &as_f, lim, &mut bytes);
    }
    for chunk in docs.chunks(LOOKUP_BATCH) {
        let joined = match (fields, &all_rows) {
            (Some((l, f)), _) => {
                // The batch's values of `localField`: each array element,
                // null for a missing field (MongoDB's rule).
                let mut keys: Vec<Bson> = Vec::new();
                let mut seen = HashSet::new();
                for d in chunk {
                    let vs = sprig_doc::filter::values_at(d, l);
                    if vs.vals.iter().all(|v| matches!(v, Bson::Array(_)))
                        && seen.insert(sprig_doc::keyenc::eq_key(&Bson::Null))
                    {
                        keys.push(Bson::Null);
                    }
                    for v in vs.vals {
                        if !matches!(v, Bson::Array(_)) && seen.insert(sprig_doc::keyenc::eq_key(v))
                        {
                            keys.push(v.clone());
                        }
                    }
                }
                let mut filter = Document::new();
                filter.insert(f, doc! {"$in": keys});
                let rows = read(c, db, &from, Some(&filter), tx)
                    .await?
                    .map(|r| documents(&r))
                    .unwrap_or_default();
                // The equality first (MongoDB's rules; it drops what SQL
                // found in excess).
                sprig_doc::engine::join_chunk(chunk, &rows, Some((l, f)), &as_f, lim, &mut bytes)?
            }
            (None, Some(rows)) => {
                sprig_doc::engine::join_chunk(chunk, rows, None, &as_f, lim, &mut bytes)?
            }
            (None, None) => Vec::new(),
        };
        if pipe.is_empty() {
            out.extend(joined);
            continue;
        }
        // Then the `$lookup`'s own pipeline over each document's matches,
        // with its `let` variables bound to that document.
        for mut d in joined {
            let matched: Vec<Document> = d
                .get_array(&as_f)
                .map(|a| a.iter().filter_map(|x| x.as_document().cloned()).collect())
                .unwrap_or_default();
            let p = match &lets {
                Some(l) => {
                    // The variables read the input document as it came.
                    let mut root = d.clone();
                    root.remove(&as_f);
                    let vars = sprig_doc::agg::Vars::new(&root, lim.now);
                    let mut bound = Document::new();
                    for (k, e) in l {
                        bound.insert(k.clone(), sprig_doc::agg::eval(e, &vars)?);
                    }
                    sprig_doc::agg::bind_pipeline(&pipe, &bound)
                }
                None => pipe.clone(),
            };
            let through = run_stages(s, tenant, db, matched, &p, tx).await?;
            d.insert(
                as_f.clone(),
                through.into_iter().map(Bson::Document).collect::<Vec<_>>(),
            );
            out.push(d);
        }
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn def(name: &str, col_type: u8, length: u32, charset: u16) -> ColumnDef {
        ColumnDef {
            name: bytes::Bytes::from(name.to_owned()),
            col_type,
            length,
            charset,
            ..ColumnDef::default()
        }
    }

    #[test]
    fn rows_read_as_documents_by_the_type_map() {
        let t = TableRows {
            defs: vec![
                def("id", ct::LONG, 11, BINARY),
                def("ok", ct::TINY, 1, BINARY),
                def("big", ct::LONGLONG, 20, BINARY),
                def("price", ct::NEWDECIMAL, 10, BINARY),
                def("name", ct::VAR_STRING, 80, 45),
                def("at", ct::DATETIME, 19, BINARY),
                def("raw", ct::BLOB, 65535, BINARY),
                def("j", ct::JSON, 0, 45),
            ],
            rows: vec![vec![
                Some(b"7".to_vec()),
                Some(b"1".to_vec()),
                Some(b"9007199254740993".to_vec()),
                Some(b"12.50".to_vec()),
                Some(b"ann".to_vec()),
                Some(b"2024-02-03 04:05:06".to_vec()),
                Some(vec![0, 1]),
                Some(br#"{"a":[1,2.5,"x"]}"#.to_vec()),
            ]],
            json: HashSet::new(),
        };
        let d = &documents(&t)[0];
        assert_eq!(d.get("id"), Some(&Bson::Int32(7)));
        assert_eq!(d.get("ok"), Some(&Bson::Boolean(true)));
        assert_eq!(d.get("big"), Some(&Bson::Int64(9_007_199_254_740_993)));
        assert_eq!(
            d.get("price"),
            Some(&Bson::Decimal128("12.50".parse().unwrap()))
        );
        assert_eq!(d.get("name"), Some(&Bson::String("ann".into())));
        assert!(matches!(d.get("at"), Some(Bson::DateTime(_))));
        assert!(matches!(d.get("raw"), Some(Bson::Binary(_))));
        let j = d.get_document("j").unwrap();
        assert_eq!(
            j.get_array("a").unwrap(),
            &vec![Bson::Int32(1), Bson::Double(2.5), Bson::String("x".into())]
        );
    }

    #[test]
    fn filters_imply_only_safe_conditions() {
        let defs: HashMap<String, ColumnDef> = [
            ("id".to_owned(), def("id", ct::LONG, 11, BINARY)),
            ("name".to_owned(), def("name", ct::VAR_STRING, 80, 45)),
            ("j".to_owned(), def("j", ct::JSON, 0, 45)),
        ]
        .into_iter()
        .collect();
        let mut out = Vec::new();
        implied(
            &doc! {"id": {"$gte": 3, "$in": [1, 2, null]}, "name": "o'k", "j": "x", "other": 1, "a.b": 2},
            &defs,
            &mut out,
        );
        assert_eq!(
            out,
            vec![
                "`id` >= 3".to_owned(),
                "(`id` IN (1, 2) OR `id` IS NULL)".to_owned(),
                "`name` = 'o''k'".to_owned(),
            ]
        );
        // A string never narrows a number column, nor a number a text one.
        let mut out = Vec::new();
        implied(&doc! {"id": "5", "name": 5}, &defs, &mut out);
        assert!(out.is_empty());
    }
}
