//! `SPRIG.QUERY "<sql>"`: SprigDB's query command on the Redis port.
//!
//! Redis has no query language, so the Redis port takes one SQL statement
//! and answers it in RESP. The statement runs on the login's own SQL
//! session as a client statement: the same catalog, privileges and
//! row-level security as on the SQL ports, with every model SQL reads
//! (tables, MongoDB collections, graphs, streams).
//!
//! ```text
//! SPRIG.QUERY <sql>
//!   one statement, read-only:   SELECT, WITH, VALUES, TABLE, SHOW,
//!                               EXPLAIN, DESCRIBE; no INSERT, UPDATE,
//!                               DELETE, REPLACE, INTO, CREATE, DROP,
//!                               ALTER, TRUNCATE, GRANT, REVOKE, LOAD,
//!                               LOCK word outside quotes and comments
//!   runs with SET SESSION TRANSACTION READ ONLY, then READ WRITE again
//!   (a write that slips past the words, in a function, fails with 1792)
//! reply: a two-element array
//!   1. the column names (bulk strings)
//!   2. the rows, each an array of values typed by its column:
//!        NULL                   null
//!        integer columns        integer (RESP3 big number past i64)
//!        FLOAT, DOUBLE          RESP3 double, RESP2 bulk string
//!        everything else        bulk string, the SQL text protocol's bytes
//! ```
//!
//! Names are as on the SQL ports: unqualified table names need a current
//! database, which the Redis session doesn't have, so names are written
//! `db.table`. Not allowed inside `MULTI` or scripts.

use bytes::Bytes;
use sprig_kv::sql::Table;
use sprig_proto_redis::Frame;

/// Words that make a statement more than a read, wherever they stand
/// outside quotes and comments (a call of a function of the same name,
/// `REPLACE(...)` or `INSERT(...)`, aside).
const WRITES: [&str; 13] = [
    "INSERT", "UPDATE", "DELETE", "REPLACE", "INTO", "CREATE", "DROP", "ALTER", "TRUNCATE",
    "GRANT", "REVOKE", "LOAD", "LOCK",
];

/// The words a read-only statement may start with.
const STARTS: [&str; 8] = [
    "SELECT", "WITH", "VALUES", "TABLE", "SHOW", "EXPLAIN", "DESCRIBE", "DESC",
];

/// One token of the statement's text, as far as the check needs.
#[derive(Debug, PartialEq, Eq)]
enum Tok<'a> {
    Word(&'a str),
    Open,
    Semi,
    Other,
}

/// The tokens of `sql` outside quotes and comments.
///
/// # Errors
/// An unterminated quote or comment, or an executable comment (`/*!`).
fn tokens(sql: &str) -> Result<Vec<Tok<'_>>, &'static str> {
    let b = sql.as_bytes();
    let mut out = Vec::new();
    let mut i = 0;
    while i < b.len() {
        let c = b[i];
        match c {
            b' ' | b'\t' | b'\r' | b'\n' => i += 1,
            b'/' if b.get(i + 1) == Some(&b'*') => {
                if b.get(i + 2) == Some(&b'!') {
                    return Err("an executable comment (/*! ... */)");
                }
                let end = sql[i + 2..].find("*/").ok_or("an unterminated comment")?;
                i += end + 4;
            }
            b'#' => i = sql[i..].find('\n').map_or(b.len(), |e| i + e + 1),
            b'-' if b.get(i + 1) == Some(&b'-')
                && b.get(i + 2).is_none_or(u8::is_ascii_whitespace) =>
            {
                i = sql[i..].find('\n').map_or(b.len(), |e| i + e + 1);
            }
            b'\'' | b'"' | b'`' => {
                let mut j = i + 1;
                loop {
                    match b.get(j) {
                        None => return Err("an unterminated quote"),
                        Some(&x) if x == b'\\' && c != b'`' => j += 2,
                        Some(&x) if x == c => {
                            if b.get(j + 1) == Some(&c) {
                                j += 2;
                            } else {
                                break;
                            }
                        }
                        Some(_) => j += 1,
                    }
                }
                out.push(Tok::Other);
                i = j + 1;
            }
            b'(' => {
                out.push(Tok::Open);
                i += 1;
            }
            b';' => {
                out.push(Tok::Semi);
                i += 1;
            }
            _ if c.is_ascii_alphabetic() || c == b'_' || c >= 0x80 => {
                let start = i;
                while i < b.len()
                    && (b[i].is_ascii_alphanumeric()
                        || b[i] == b'_'
                        || b[i] == b'$'
                        || b[i] >= 0x80)
                {
                    i += 1;
                }
                out.push(Tok::Word(&sql[start..i]));
            }
            _ => {
                out.push(Tok::Other);
                i += 1;
            }
        }
    }
    Ok(out)
}

/// Whether `sql` is one read-only statement.
///
/// # Errors
/// Why it isn't.
pub fn read_only(sql: &str) -> Result<(), String> {
    let toks = tokens(sql).map_err(str::to_owned)?;
    // One statement: a `;` may only end it.
    let mut toks: &[Tok<'_>] = &toks;
    while let [rest @ .., Tok::Semi] = toks {
        toks = rest;
    }
    if toks.contains(&Tok::Semi) {
        return Err("more than one statement".into());
    }
    let first = toks.iter().find(|t| **t != Tok::Open);
    match first {
        Some(Tok::Word(w)) if STARTS.iter().any(|s| w.eq_ignore_ascii_case(s)) => {}
        Some(Tok::Word(w)) => return Err(format!("a statement starting with {w}")),
        _ => return Err("not a query".into()),
    }
    for (i, t) in toks.iter().enumerate() {
        if let Tok::Word(w) = t
            && let Some(bad) = WRITES.iter().find(|x| w.eq_ignore_ascii_case(x))
            && toks.get(i + 1) != Some(&Tok::Open)
        {
            return Err(format!("{bad} in the statement"));
        }
    }
    Ok(())
}

/// The protocol's type codes of integer columns: TINY, SHORT, LONG,
/// LONGLONG, INT24, YEAR.
fn is_integer(ty: u8) -> bool {
    matches!(ty, 1 | 2 | 3 | 8 | 9 | 13)
}

/// FLOAT, DOUBLE.
fn is_double(ty: u8) -> bool {
    matches!(ty, 4 | 5)
}

/// The reply to a statement's result.
#[must_use]
pub fn reply(t: &Table) -> Frame {
    let header = t
        .columns
        .iter()
        .map(|c| Frame::bulk(Bytes::from(c.name.clone())))
        .collect();
    let rows = t
        .rows
        .iter()
        .map(|r| {
            Frame::Array(
                r.iter()
                    .enumerate()
                    .map(|(i, v)| value(t.columns.get(i).map(|c| c.ty), v.as_deref()))
                    .collect(),
            )
        })
        .collect();
    Frame::Array(vec![Frame::Array(header), Frame::Array(rows)])
}

fn value(ty: Option<u8>, v: Option<&[u8]>) -> Frame {
    let Some(v) = v else {
        return Frame::null();
    };
    match ty {
        Some(t) if is_integer(t) => {
            match std::str::from_utf8(v)
                .ok()
                .and_then(|s| s.parse::<i64>().ok())
            {
                Some(n) => Frame::Integer(n),
                None => Frame::BigNumber(Bytes::copy_from_slice(v)),
            }
        }
        Some(t) if is_double(t) => Frame::DoubleText(Bytes::copy_from_slice(v)),
        _ => Frame::bulk_from(v),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use sprig_kv::sql::Column;

    #[test]
    fn only_one_read_only_statement_passes() {
        for ok in [
            "SELECT 1",
            "  select * from shop.orders where note = 'drop table x; delete'",
            "(SELECT a FROM t) UNION (SELECT b FROM u);",
            "WITH c AS (SELECT 1 AS x) SELECT x FROM c",
            "SELECT REPLACE(name, 'a', 'b'), INSERT(name, 1, 2, 'zz') FROM shop.t",
            "SELECT `update`, \"delete\" FROM t -- drop it\n",
            "/* note */ SHOW TABLES FROM shop",
            "EXPLAIN SELECT * FROM shop.t",
            "SELECT updated_at, into_x FROM t",
        ] {
            assert_eq!(read_only(ok), Ok(()), "{ok}");
        }
        for bad in [
            "DELETE FROM t",
            "SELECT 1; DROP TABLE t",
            "SELECT * FROM t FOR UPDATE",
            "SELECT a INTO @v FROM t",
            "WITH c AS (SELECT 1) UPDATE t SET a = 1",
            "CALL p()",
            "SET @a = 1",
            "/*!50000 DELETE FROM t */ SELECT 1",
            "SELECT 'unterminated",
            "",
            "SELECT * FROM t LOCK IN SHARE MODE",
        ] {
            assert!(read_only(bad).is_err(), "{bad}");
        }
    }

    #[test]
    fn values_are_typed_by_their_column() {
        let col = |name: &str, ty: u8| Column {
            name: name.into(),
            ty,
            unsigned: false,
        };
        let t = Table {
            columns: vec![
                col("id", 8),
                col("price", 246),
                col("ratio", 5),
                col("name", 253),
            ],
            rows: vec![
                vec![
                    Some(b"7".to_vec()),
                    Some(b"12.50".to_vec()),
                    Some(b"0.5".to_vec()),
                    Some(b"Asha".to_vec()),
                ],
                vec![Some(b"18446744073709551615".to_vec()), None, None, None],
            ],
        };
        assert_eq!(
            reply(&t),
            Frame::Array(vec![
                Frame::Array(vec![
                    Frame::bulk_from(b"id"),
                    Frame::bulk_from(b"price"),
                    Frame::bulk_from(b"ratio"),
                    Frame::bulk_from(b"name"),
                ]),
                Frame::Array(vec![
                    Frame::Array(vec![
                        Frame::Integer(7),
                        Frame::bulk_from(b"12.50"),
                        Frame::DoubleText(Bytes::from_static(b"0.5")),
                        Frame::bulk_from(b"Asha"),
                    ]),
                    Frame::Array(vec![
                        Frame::BigNumber(Bytes::from_static(b"18446744073709551615")),
                        Frame::null(),
                        Frame::null(),
                        Frame::null(),
                    ]),
                ]),
            ])
        );
    }
}
