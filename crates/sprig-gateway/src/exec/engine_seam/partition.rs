//! Partitioned engine tables in statements: `PARTITION (p, ...)` after a
//! table name, and the row effects of partition DDL.
//!
//! ```text
//! SELECT ... t PARTITION (p) [a]      t read as (SELECT * FROM db.t WHERE <p's rows>) a
//! DELETE / UPDATE t PARTITION (p) ... the clause goes, <p's rows> joins the WHERE
//!                                     (an UPDATE's new row versions must stay in p: 1748)
//! INSERT / REPLACE t PARTITION (p)    the clause goes; each row must go to p (1748)
//! errors: 1747 (the table isn't partitioned), 1735 (no such partition)
//!
//! ALTER TABLE t DROP | TRUNCATE PARTITION p   p's rows deleted, then the change
//! ALTER TABLE t ... (a new layout)            every row must fit it (1526), then the change
//! ```
//!
//! `<p's rows>` is the partitioning's condition (`engine::partition::condition`);
//! `KEY` partitioning (the servers' own hash) can't name its rows: such a
//! clause is refused.

use std::sync::Arc;

use bytes::Bytes;
use sprig_catalog::Freshness;
use sprig_common::{Result, SprigError};
use sprig_proto_mysql::StatusFlags;
use sprig_sql::names::{DdlKind, DdlTarget};

use crate::engine::define::partition::{Change, Method, Spec};
use crate::engine::table::TableDef;
use crate::exec::PassSession;
use crate::exec::QueryAttrs;
use crate::exec::tenant::Step;
use crate::pipeline::{ExecCtx, ResultSink, RowFormat};

tokio::task_local! {
    /// Set while a rewritten statement runs (it comes back through here).
    static REWRITTEN: ();
}

/// One `name PARTITION (p, ...)` in a statement's text.
#[derive(Debug, Clone, PartialEq, Eq)]
struct Clause {
    /// Where the table's name starts.
    name_at: usize,
    db: Option<String>,
    table: String,
    /// Where `PARTITION` starts, and the end of its `)`.
    at: usize,
    end: usize,
    parts: Vec<String>,
}

/// Bytes of `t` from `i` that are a name (back-quoted or bare): the name
/// and where it ends.
fn name_at(t: &[u8], i: usize) -> Option<(String, usize)> {
    if t.get(i) == Some(&b'`') {
        let mut j = i + 1;
        let mut s = Vec::new();
        while j < t.len() {
            if t[j] == b'`' {
                if t.get(j + 1) == Some(&b'`') {
                    s.push(b'`');
                    j += 2;
                    continue;
                }
                return Some((String::from_utf8_lossy(&s).into_owned(), j + 1));
            }
            s.push(t[j]);
            j += 1;
        }
        return None;
    }
    let n = t[i..]
        .iter()
        .take_while(|b| b.is_ascii_alphanumeric() || **b == b'_' || **b == b'$' || **b >= 0x80)
        .count();
    (n > 0).then(|| (String::from_utf8_lossy(&t[i..i + n]).into_owned(), i + n))
}

/// The name ending just before `end` (back over a back-quoted name or a
/// bare word): where it starts and the name.
fn name_before(t: &[u8], end: usize) -> Option<(usize, String)> {
    if end == 0 {
        return None;
    }
    if t[end - 1] == b'`' {
        let mut j = end - 1;
        loop {
            if j == 0 {
                return None;
            }
            j -= 1;
            if t[j] == b'`' {
                if j > 0 && t[j - 1] == b'`' {
                    j -= 1;
                    continue;
                }
                let (n, e) = name_at(t, j)?;
                return (e == end).then_some((j, n));
            }
        }
    }
    let mut j = end;
    while j > 0 && (t[j - 1].is_ascii_alphanumeric() || t[j - 1] == b'_' || t[j - 1] == b'$') {
        j -= 1;
    }
    (j < end).then(|| (j, String::from_utf8_lossy(&t[j..end]).into_owned()))
}

/// The `PARTITION (...)` clauses of `text`, outside quotes and comments.
#[allow(clippy::many_single_char_names)] // a byte scanner's cursors
fn clauses(text: &str) -> Vec<Clause> {
    let t = text.as_bytes();
    let mut out = Vec::new();
    let mut i = 0;
    while i < t.len() {
        match t[i] {
            q @ (b'\'' | b'"' | b'`') => {
                i += 1;
                while i < t.len() {
                    if t[i] == b'\\' && q != b'`' {
                        i += 2;
                        continue;
                    }
                    if t[i] == q {
                        if t.get(i + 1) == Some(&q) {
                            i += 2;
                            continue;
                        }
                        break;
                    }
                    i += 1;
                }
                i += 1;
            }
            b'/' if t.get(i + 1) == Some(&b'*') => {
                i += 2;
                while i + 1 < t.len() && !(t[i] == b'*' && t[i + 1] == b'/') {
                    i += 1;
                }
                i += 2;
            }
            b'p' | b'P'
                if t.len() >= i + 9
                    && t[i..i + 9].eq_ignore_ascii_case(b"PARTITION")
                    && (i == 0 || !(t[i - 1].is_ascii_alphanumeric() || t[i - 1] == b'_'))
                    && t.get(i + 9)
                        .is_none_or(|b| !(b.is_ascii_alphanumeric() || *b == b'_')) =>
            {
                let at = i;
                let mut j = i + 9;
                while j < t.len() && t[j].is_ascii_whitespace() {
                    j += 1;
                }
                if t.get(j) != Some(&b'(') {
                    i += 9;
                    continue;
                }
                j += 1;
                let mut parts = Vec::new();
                loop {
                    while j < t.len() && t[j].is_ascii_whitespace() {
                        j += 1;
                    }
                    let Some((n, e)) = name_at(t, j) else { break };
                    parts.push(n);
                    j = e;
                    while j < t.len() && t[j].is_ascii_whitespace() {
                        j += 1;
                    }
                    if t.get(j) == Some(&b',') {
                        j += 1;
                        continue;
                    }
                    break;
                }
                if t.get(j) != Some(&b')') || parts.is_empty() {
                    i += 9;
                    continue;
                }
                let end = j + 1;
                // The table's name before it: `t` or `db`.`t`.
                let mut k = at;
                while k > 0 && t[k - 1].is_ascii_whitespace() {
                    k -= 1;
                }
                let Some((ts, table)) = name_before(t, k) else {
                    i = end;
                    continue;
                };
                let (name_at_, db) = if ts > 0 && t[ts - 1] == b'.' {
                    match name_before(t, ts - 1) {
                        Some((ds, d)) => (ds, Some(d)),
                        None => (ts, None),
                    }
                } else {
                    (ts, None)
                };
                out.push(Clause {
                    name_at: name_at_,
                    db,
                    table,
                    at,
                    end,
                    parts,
                });
                i = end;
            }
            _ => i += 1,
        }
    }
    out
}

/// Words that may follow a table reference but aren't an alias.
fn alias_follows(rest: &str) -> bool {
    let r = rest.trim_start();
    let Some(c) = r.chars().next() else {
        return false;
    };
    if c == '`' {
        return true;
    }
    if !(c.is_ascii_alphabetic() || c == '_') {
        return false;
    }
    let w: String = r
        .chars()
        .take_while(|c| c.is_ascii_alphanumeric() || *c == '_')
        .collect::<String>()
        .to_ascii_uppercase();
    !matches!(
        w.as_str(),
        "WHERE"
            | "JOIN"
            | "INNER"
            | "LEFT"
            | "RIGHT"
            | "CROSS"
            | "NATURAL"
            | "STRAIGHT_JOIN"
            | "ON"
            | "USING"
            | "GROUP"
            | "ORDER"
            | "LIMIT"
            | "HAVING"
            | "WINDOW"
            | "UNION"
            | "EXCEPT"
            | "INTERSECT"
            | "FOR"
            | "LOCK"
            | "INTO"
            | "SET"
            | "VALUES"
            | "VALUE"
            | "SELECT"
            | "USE"
            | "IGNORE"
            | "FORCE"
            | "OFFSET"
            | "FETCH"
    )
}

fn q(n: &str) -> String {
    format!("`{}`", n.replace('`', "``"))
}

fn refused(what: &str) -> SprigError {
    SprigError::standard(
        1235,
        *b"42000",
        format!("This version of SprigDB doesn't yet support '{what}'"),
    )
}

/// The first word of `text`, upper case.
fn first_word(text: &str) -> String {
    text.trim_start()
        .trim_start_matches('(')
        .trim_start()
        .chars()
        .take_while(char::is_ascii_alphabetic)
        .collect::<String>()
        .to_ascii_uppercase()
}

impl PassSession {
    /// The engine definition of `db`.`table` (`None`: not an engine table).
    async fn partition_table(&mut self, db: &str, table: &str) -> Option<Arc<TableDef>> {
        if !self.engine_database_named(db) {
            return None;
        }
        let ts = self.tenant.as_deref()?;
        let svc = Arc::clone(ts.services());
        let cat = svc.cache.get(ts.tenant, Freshness::Latest).await.ok()?;
        crate::autoindex::ddl::table_def(&svc, &cat, db, table)
    }

    /// The partitions `names` of `def` (1747, 1735).
    fn partition_numbers(
        def: &TableDef,
        names: &[String],
    ) -> Result<(Arc<crate::engine::partition::Rule>, Vec<usize>)> {
        let Some(rule) = def.partition.clone() else {
            return Err(SprigError::standard(
                1747,
                *b"HY000",
                "PARTITION () clause on non partitioned table",
            ));
        };
        let mut v = Vec::new();
        for n in names {
            let i = rule.spec.part(n).ok_or_else(|| {
                SprigError::standard(
                    1735,
                    *b"HY000",
                    format!("Unknown partition '{n}' in table '{}'", def.name),
                )
            })?;
            if !v.contains(&i) {
                v.push(i);
            }
        }
        Ok((rule, v))
    }

    /// A statement with `PARTITION (...)` after an engine table's name (see
    /// the module docs); `None` for any other statement.
    #[allow(clippy::too_many_lines)] // one branch per statement kind
    pub(in crate::exec) async fn engine_partition_dml(
        &mut self,
        cx: &mut ExecCtx<'_>,
        text: &Bytes,
        attrs: &QueryAttrs,
        more: bool,
        format: RowFormat,
        out: &mut dyn ResultSink,
    ) -> Result<Option<Step>> {
        if REWRITTEN.try_with(|()| ()).is_ok() {
            return Ok(None);
        }
        let Ok(sql) = std::str::from_utf8(text) else {
            return Ok(None);
        };
        let kind = first_word(sql);
        if !matches!(
            kind.as_str(),
            "SELECT" | "WITH" | "INSERT" | "REPLACE" | "UPDATE" | "DELETE"
        ) {
            return Ok(None);
        }
        let found = clauses(sql);
        if found.is_empty() {
            return Ok(None);
        }
        let current = self.current_logical();
        let mut resolved = Vec::new();
        for c in &found {
            let Some(db) = c.db.clone().or_else(|| current.clone()) else {
                return Ok(None);
            };
            let Some(def) = self.partition_table(&db, &c.table).await else {
                return Ok(None);
            };
            let (rule, parts) = match Self::partition_numbers(&def, &c.parts) {
                Ok(x) => x,
                Err(e) => return Ok(Some(self.fail(out, &e).await?)),
            };
            if rule.spec.method == Method::Key && !rule.spec.columns {
                let e = refused("PARTITION (...) of a table partitioned by KEY");
                return Ok(Some(self.fail(out, &e).await?));
            }
            resolved.push((c.clone(), db, def, rule, parts));
        }
        let mut selected: Option<(u32, Vec<usize>)> = None;
        let rewritten = match kind.as_str() {
            "SELECT" | "WITH"
                if resolved.len() == 1 && {
                    let (c, _, _, rule, parts) = &resolved[0];
                    let mut t = sql.to_owned();
                    t.replace_range(c.at..c.end, "");
                    crate::engine::partition::condition(&rule.spec, parts)
                        .and_then(|cond| select_splice(&t, &cond))
                        .is_some()
                } =>
            {
                // One table read: its rows' condition joins the WHERE, so the
                // result's columns keep their table.
                let (c, _, _, rule, parts) = &resolved[0];
                let mut t = sql.to_owned();
                t.replace_range(c.at..c.end, "");
                let cond =
                    crate::engine::partition::condition(&rule.spec, parts).unwrap_or_default();
                select_splice(&t, &cond).unwrap_or(t)
            }
            "SELECT" | "WITH" => {
                let mut s = sql.to_owned();
                for (c, db, _, rule, parts) in resolved.iter().rev() {
                    let Some(cond) = crate::engine::partition::condition(&rule.spec, parts) else {
                        return Ok(None);
                    };
                    let alias = if alias_follows(&sql[c.end..]) {
                        String::new()
                    } else {
                        format!(" AS {}", q(&c.table))
                    };
                    s.replace_range(
                        c.name_at..c.end,
                        &format!(
                            "(SELECT * FROM {}.{} WHERE {cond}){alias}",
                            q(db),
                            q(&c.table)
                        ),
                    );
                }
                s
            }
            "INSERT" | "REPLACE" => {
                let [(c, _, def, _, parts)] = resolved.as_slice() else {
                    return Ok(None);
                };
                selected = Some((
                    u32::try_from(def.object.0).unwrap_or(u32::MAX),
                    parts.clone(),
                ));
                let mut s = sql.to_owned();
                s.replace_range(c.at..c.end, "");
                s
            }
            _ => {
                let [(c, _, def, rule, parts)] = resolved.as_slice() else {
                    let e = refused("PARTITION (...) in an UPDATE or DELETE of several tables");
                    return Ok(Some(self.fail(out, &e).await?));
                };
                let Some(cond) = crate::engine::partition::condition(&rule.spec, parts) else {
                    return Ok(None);
                };
                if kind == "UPDATE" {
                    selected = Some((
                        u32::try_from(def.object.0).unwrap_or(u32::MAX),
                        parts.clone(),
                    ));
                }
                let mut s = sql.to_owned();
                s.replace_range(c.at..c.end, "");
                let Some(x) = where_splice(&s, &cond) else {
                    let e = refused("PARTITION (...) in this UPDATE or DELETE");
                    return Ok(Some(self.fail(out, &e).await?));
                };
                x
            }
        };
        let text = Bytes::from(rewritten);
        let run = Box::pin(self.tenant_text(cx, &text, attrs, more, format, out));
        let step = match selected {
            Some(sel) => {
                REWRITTEN
                    .scope((), crate::engine::partition::SELECTED.scope(sel, run))
                    .await?
            }
            None => REWRITTEN.scope((), run).await?,
        };
        Ok(Some(step))
    }

    /// Partition DDL on an engine table: the rows of dropped and truncated
    /// partitions deleted, a new layout checked against every row, before
    /// the change itself runs as usual (see the module docs). `None`: not
    /// such a statement (or it fails as usual).
    pub(in crate::exec) async fn engine_partition_ddl(
        &mut self,
        cx: &mut ExecCtx<'_>,
        target: &DdlTarget,
        text: &[u8],
        status: StatusFlags,
        out: &mut dyn ResultSink,
    ) -> Option<Result<Step>> {
        if REWRITTEN.try_with(|()| ()).is_ok() {
            return None;
        }
        let DdlTarget::AlterObject {
            kind: DdlKind::Table,
            name,
            ..
        } = target
        else {
            return None;
        };
        let sql = std::str::from_utf8(text).ok()?;
        if !sql.to_ascii_uppercase().contains("PARTITION") {
            return None;
        }
        let db = name.db.clone().or_else(|| self.current_logical())?;
        let def = self.partition_table(&db, &name.name).await?;
        let mysql = self.persona == sprig_proto_mysql::packets::Persona::MySql84;
        // The change on the definition: its own errors answer as usual.
        let ts = self.tenant.as_deref()?;
        let svc = Arc::clone(ts.services());
        let cat = svc.cache.get(ts.tenant, Freshness::Latest).await.ok()?;
        let eng = crate::engine::catalog::of(&cat)?;
        let stored = eng.texts.get(&def.object)?;
        let old = crate::engine::define::parse_stored(stored).ok()?;
        let changes = partition_changes(sql, &old)?;
        let cx_def = crate::engine::define::Ctx {
            mysql,
            db_cs: old.cs.clone(),
        };
        let new = crate::engine::define::alter(&old, sql, &cx_def).ok()?;
        let r: Result<Option<u64>> = async {
            let mut deleted = None;
            for ch in &changes {
                let Some(spec) = old.partition.as_ref() else {
                    continue;
                };
                let names: Option<Vec<usize>> = match ch {
                    Change::Drop(n) | Change::Truncate(Some(n)) => {
                        Some(n.iter().filter_map(|x| spec.part(x)).collect())
                    }
                    Change::Truncate(None) => Some((0..spec.parts.len()).collect()),
                    _ => None,
                };
                if let Some(which) = names {
                    let Some(cond) = crate::engine::partition::condition(spec, &which) else {
                        return Err(refused(
                            "dropping or truncating a partition of a table partitioned by KEY",
                        ));
                    };
                    let del = format!("DELETE FROM {}.{} WHERE {cond}", q(&db), q(&name.name));
                    let c = REWRITTEN
                        .scope((), self.run_internal(cx, &del, true))
                        .await?;
                    if let Some(e) = c.error {
                        return Err(e);
                    }
                    deleted = Some(deleted.unwrap_or(0) + c.affected.unwrap_or(0));
                }
            }
            // A new layout: every row must fit it.
            if let Some(spec) = &new.partition
                && Some(spec) != old.partition.as_ref()
                && matches!(spec.method, Method::Range | Method::List)
            {
                check_rows(self, cx, &db, &name.name, spec).await?;
            }
            Ok(deleted)
        }
        .await;
        if let Err(e) = r {
            return Some(self.fail(out, &e).await);
        }
        // `TRUNCATE PARTITION` changes no definition: answered here.
        if changes
            .iter()
            .all(|c| matches!(c, Change::Truncate(_) | Change::Maintain(_)))
        {
            let ok = sprig_proto_mysql::packets::OkPacket::new(status);
            return Some(out.ok(ok).await.map(|()| Step {
                errored: false,
                status: Some(status),
            }));
        }
        let text = Bytes::copy_from_slice(text);
        let attrs = QueryAttrs::default();
        let run = Box::pin(self.tenant_text(
            cx,
            &text,
            &attrs,
            status.contains(StatusFlags::MORE_RESULTS_EXISTS),
            RowFormat::Text,
            out,
        ));
        Some(REWRITTEN.scope((), run).await)
    }
}

/// The partitioning changes of `ALTER TABLE` text `sql` (`None`: none, or
/// it doesn't read: the usual path answers).
fn partition_changes(sql: &str, def: &crate::engine::define::Def) -> Option<Vec<Change>> {
    let t = crate::engine::define::lex::lex(sql).ok()?;
    let mut p = crate::engine::define::P { t: &t, i: 0 };
    let mut out = Vec::new();
    while p.i < t.len() {
        let before = p.i;
        if let Ok(Some(c)) = crate::engine::define::partition::read_change(&mut p, &def.cs.0) {
            out.push(c);
        }
        if p.i == before {
            p.i += 1;
        }
    }
    (!out.is_empty()).then_some(out)
}

/// 1526 for the first row of `db`.`table` no partition of `spec` takes.
async fn check_rows(
    s: &mut PassSession,
    cx: &mut ExecCtx<'_>,
    db: &str,
    table: &str,
    spec: &Spec,
) -> Result<()> {
    let all: Vec<usize> = (0..spec.parts.len()).collect();
    let Some(cond) = crate::engine::partition::condition(spec, &all) else {
        return Ok(());
    };
    let shown = if spec.columns {
        spec.cols
            .iter()
            .map(|c| q(c))
            .collect::<Vec<_>>()
            .join(", ")
    } else {
        spec.expr
            .as_ref()
            .map_or_else(|| "NULL".to_owned(), crate::engine::define::expr::print)
    };
    let sql = format!(
        "SELECT {shown} FROM {}.{} WHERE NOT ({cond}) OR ({cond}) IS NULL LIMIT 1",
        q(db),
        q(table)
    );
    let c = REWRITTEN.scope((), s.run_internal(cx, &sql, true)).await?;
    if let Some(e) = c.error {
        return Err(e);
    }
    if let Some(row) = c.rows.first() {
        let v = row
            .iter()
            .map(|x| {
                x.as_ref().map_or_else(
                    || "NULL".to_owned(),
                    |b| String::from_utf8_lossy(b).into_owned(),
                )
            })
            .collect::<Vec<_>>()
            .join(",");
        return Err(crate::engine::partition::nowhere(&v));
    }
    Ok(())
}

/// `sql`, a `SELECT` of one table (no join, no set operation), with
/// `cond` joining its `WHERE`, or as one after the table; `None` for any
/// other query.
fn select_splice(sql: &str, cond: &str) -> Option<String> {
    use sqlparser::ast::{SetExpr, Spanned, Statement};
    let stmts = sqlparser::parser::Parser::parse_sql(&sprig_plan::dialect::SprigMySql, sql).ok()?;
    let [Statement::Query(q)] = stmts.as_slice() else {
        return None;
    };
    if q.with.is_some() {
        return None;
    }
    let SetExpr::Select(sel) = &*q.body else {
        return None;
    };
    let [from] = sel.from.as_slice() else {
        return None;
    };
    if !from.joins.is_empty() || !matches!(from.relation, sqlparser::ast::TableFactor::Table { .. })
    {
        return None;
    }
    let off = |l: sqlparser::tokenizer::Location| {
        crate::engine::routine::subst::offset(sql, l.line, l.column)
    };
    if let Some(w) = &sel.selection {
        let sp = w.span();
        let (a, b) = (off(sp.start)?, off(sp.end)?);
        (b > a).then(|| format!("{}({}) AND {cond}{}", &sql[..a], &sql[a..b], &sql[b..]))
    } else {
        let b = off(from.relation.span().end)?;
        Some(format!("{} WHERE {cond}{}", &sql[..b], &sql[b..]))
    }
}

/// `sql` (an `UPDATE` or `DELETE` of one table) with `cond` joining its
/// `WHERE`, or as one when it has none (`None`: it has an `ORDER BY` or
/// `LIMIT` and no `WHERE`).
fn where_splice(sql: &str, cond: &str) -> Option<String> {
    use sqlparser::ast::{Spanned, Statement};
    let stmts = sqlparser::parser::Parser::parse_sql(&sprig_plan::dialect::SprigMySql, sql).ok()?;
    let [stmt] = stmts.as_slice() else {
        return None;
    };
    let (sel, tail) = match stmt {
        Statement::Update(u) => (
            u.selection.as_ref(),
            !u.order_by.is_empty() || u.limit.is_some(),
        ),
        Statement::Delete(d) => (
            d.selection.as_ref(),
            !d.order_by.is_empty() || d.limit.is_some(),
        ),
        _ => return None,
    };
    match sel {
        Some(w) => {
            let sp = w.span();
            let a = crate::engine::routine::subst::offset(sql, sp.start.line, sp.start.column)?;
            let b = crate::engine::routine::subst::offset(sql, sp.end.line, sp.end.column)?;
            (b > a).then(|| format!("{}({}) AND {cond}{}", &sql[..a], &sql[a..b], &sql[b..]))
        }
        None if !tail => {
            let body = sql.trim_end().trim_end_matches(';');
            Some(format!("{body} WHERE {cond}"))
        }
        None => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn partition_clauses_are_found() {
        let c = clauses("SELECT * FROM `db`.t PARTITION (p0, `p 1`) x WHERE 'PARTITION (q)' = a");
        assert_eq!(c.len(), 1);
        assert_eq!(c[0].db.as_deref(), Some("db"));
        assert_eq!(c[0].table, "t");
        assert_eq!(c[0].parts, ["p0", "p 1"]);
        assert_eq!(c[0].name_at, 14);
        assert!(alias_follows(" x WHERE"));
        assert!(!alias_follows(" WHERE a"));
        assert!(clauses("CREATE TABLE t (a INT) PARTITION BY HASH (a)").is_empty());
        assert_eq!(
            where_splice("DELETE FROM t WHERE a = 1 OR b = 2", "(y < 3)").as_deref(),
            Some("DELETE FROM t WHERE (a = 1 OR b = 2) AND (y < 3)")
        );
        assert_eq!(
            select_splice("SELECT * FROM t  ORDER BY id", "(y < 3)").as_deref(),
            Some("SELECT * FROM t WHERE (y < 3)  ORDER BY id")
        );
        assert_eq!(
            select_splice(
                "SELECT id FROM t  AS x WHERE x.id > 0 ORDER BY id",
                "(y < 3)"
            )
            .as_deref(),
            Some("SELECT id FROM t  AS x WHERE (x.id > 0) AND (y < 3) ORDER BY id")
        );
        assert_eq!(select_splice("SELECT * FROM t JOIN u ON 1", "1"), None);
        assert_eq!(
            where_splice("UPDATE t SET a = 1;", "(y < 3)").as_deref(),
            Some("UPDATE t SET a = 1 WHERE (y < 3)")
        );
    }
}
