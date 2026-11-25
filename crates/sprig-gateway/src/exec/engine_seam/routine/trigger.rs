//! SQL triggers firing (`crate::engine::routine::trigger`). An `INSERT`,
//! `UPDATE` or `DELETE` on an engine table with triggers for its event
//! runs row by row: the rows are found first, then for each one its
//! `BEFORE` triggers (which may change `NEW`), the one-row change, and its
//! `AFTER` triggers (which see the row as stored). The whole statement
//! runs inside a transaction or savepoint of its own, so an error in any
//! row or trigger undoes all of it.
//!
//! A table with validations (`super::validation`) for the statement's
//! event takes the same path, triggers or not: each row's validations run
//! after its `BEFORE` triggers, on the final `NEW`, before the change.

use sqlparser::ast::{
    AssignmentTarget, Expr, FromTable, ObjectName, ObjectNamePart, SetExpr, Spanned, Statement,
    TableFactor, TableObject,
};

use std::fmt::Write as _;

use super::validation;
use super::{
    Arc, Bytes, ColumnDef, Datum, ExecCtx, Frame, ObjectId, OkPacket, PassSession, Ran, Result,
    ResultSink, STACK, SprigError, StatusFlags, Step, catalog, database_key, literal,
    not_on_engine, store, subst, wire_datum, with_stack,
};
use crate::engine::routine::parse::Event;
use crate::engine::routine::trigger::{Trigger, firing};
use crate::engine::table::{ColDefault, TableDef};

tokio::task_local! {
    /// The (table, definer) pairs whose TRIGGER right the running
    /// statement has checked.
    static RIGHTS: std::cell::RefCell<Vec<(ObjectId, String, String)>>;
}

/// The marker a statement's own one-row changes carry on the routine
/// stack: they fire no trigger.
const RAW: &str = "\u{1}raw";

/// The marker of a table whose triggers are running.
fn busy(table: ObjectId) -> String {
    format!("\u{1}t{}", table.0)
}

fn on_stack(k: &str) -> bool {
    STACK
        .try_with(|s| s.borrow().iter().any(|x| x == k))
        .unwrap_or(false)
}

fn top_is_raw() -> bool {
    STACK
        .try_with(|s| s.borrow().last().is_some_and(|x| x == RAW))
        .unwrap_or(false)
}

/// A node's printed form; its original text when printing would change a
/// string's backslashes (the parser kept them unescaped).
fn src(text: &str, node: &impl Spanned, printed: String) -> String {
    if !printed.contains('\\') {
        return printed;
    }
    let sp = node.span();
    match (
        subst::offset(text, sp.start.line, sp.start.column),
        subst::offset(text, sp.end.line, sp.end.column),
    ) {
        (Some(a), Some(b)) if b > a => text[a..b].to_owned(),
        _ => printed,
    }
}

fn last_part(n: &ObjectName) -> Option<String> {
    match n.0.last()? {
        ObjectNamePart::Identifier(i) => Some(i.value.clone()),
        ObjectNamePart::Function(_) => None,
    }
}

fn db_part(n: &ObjectName) -> Option<String> {
    if n.0.len() < 2 {
        return None;
    }
    match &n.0[n.0.len() - 2] {
        ObjectNamePart::Identifier(i) => Some(i.value.clone()),
        ObjectNamePart::Function(_) => None,
    }
}

/// A statement that fires triggers, as read.
/// How an `INSERT` treats a row whose key is taken.
#[derive(Clone, Copy, PartialEq, Eq)]
enum Mode {
    /// An error.
    Plain,
    /// `INSERT IGNORE`: the row is skipped with a warning.
    Ignore,
    /// `REPLACE`: the old rows go (their `DELETE` triggers fire).
    Replace,
}

enum Dml {
    /// `INSERT`: target columns (`None`: all), rows of expressions (or a
    /// query).
    Insert {
        cols: Option<Vec<String>>,
        /// `REPLACE` / `INSERT IGNORE`.
        mode: Mode,
        rows: Vec<Vec<Option<String>>>,
        query: Option<String>,
        /// `ON DUPLICATE KEY UPDATE`: (column, expression) pairs.
        upsert: Vec<(String, String)>,
        /// MySQL's row alias (`VALUES (...) AS new`): `upsert` is read
        /// again once the table is known.
        aliased: bool,
    },
    /// `UPDATE`: (column, expression) pairs; the `FROM ... WHERE ...
    /// ORDER BY ... LIMIT` tail.
    Update {
        sets: Vec<(String, String)>,
        tail: String,
    },
    /// `DELETE`: the tail.
    Delete { tail: String },
    /// A multi-table `UPDATE` or `DELETE`: the rows' `SELECT` and targets,
    /// each target's `BEFORE` and `AFTER` triggers and its database name.
    Multi {
        rs: Box<crate::engine::multiwrite::RowsSelect>,
        trigs: Vec<[Vec<Arc<Trigger>>; 2]>,
        dbs: Vec<String>,
    },
}

impl Dml {
    fn event(&self) -> Event {
        match self {
            Dml::Insert { .. } => Event::Insert,
            Dml::Update { .. } => Event::Update,
            Dml::Delete { .. } => Event::Delete,
            Dml::Multi { rs, .. } => {
                if rs.update {
                    Event::Update
                } else {
                    Event::Delete
                }
            }
        }
    }
}

/// The `WHERE ... ORDER BY ... LIMIT` tail of an `UPDATE` / `DELETE`.
fn tail(
    text: &str,
    from: &str,
    sel: Option<&Expr>,
    order: &[sqlparser::ast::OrderByExpr],
    limit: Option<&Expr>,
) -> String {
    let mut t = format!(" FROM {from}");
    if let Some(e) = sel {
        let _ = write!(t, " WHERE {}", src(text, e, e.to_string()));
    }
    if !order.is_empty() {
        let o: Vec<String> = order.iter().map(ToString::to_string).collect();
        let _ = write!(t, " ORDER BY {}", o.join(", "));
    }
    if let Some(l) = limit {
        let _ = write!(t, " LIMIT {l}");
    }
    t
}

/// Reads `text` (one statement) as a statement that may fire triggers:
/// (table name, the statement) or why the engine can't fire them for it.
#[allow(clippy::too_many_lines)] // one arm per statement
fn read(text: &str) -> Option<std::result::Result<(ObjectName, Dml), String>> {
    let first = text.trim_start().get(..6)?.to_ascii_uppercase();
    if !matches!(first.as_str(), "INSERT" | "UPDATE" | "DELETE" | "REPLAC") {
        return None;
    }
    let mut stmts =
        sqlparser::parser::Parser::parse_sql(&sprig_plan::dialect::SprigMySql, text).ok()?;
    if stmts.len() != 1 {
        return None;
    }
    let refuse = |why: &str| Some(Err(why.to_owned()));
    match stmts.pop()? {
        Statement::Insert(ins) => {
            let TableObject::TableName(name) = &ins.table else {
                return None;
            };
            let mut upsert = Vec::new();
            match &ins.on {
                None => {}
                Some(sqlparser::ast::OnInsert::DuplicateKeyUpdate(sets)) => {
                    for a in sets {
                        let AssignmentTarget::ColumnName(n) = &a.target else {
                            return refuse("ON DUPLICATE KEY UPDATE with a column tuple");
                        };
                        upsert.push((last_part(n)?, src(text, &a.value, a.value.to_string())));
                    }
                }
                Some(_) => {
                    return refuse("this INSERT form on a table with triggers or validations");
                }
            }
            let mode = if ins.replace_into {
                Mode::Replace
            } else if ins.ignore {
                Mode::Ignore
            } else {
                Mode::Plain
            };
            let mut cols: Option<Vec<String>> = if ins.columns.is_empty() {
                None
            } else {
                Some(ins.columns.iter().filter_map(last_part).collect())
            };
            let mut rows = Vec::new();
            let mut query = None;
            if !ins.assignments.is_empty() {
                let mut names = Vec::new();
                let mut row = Vec::new();
                for a in &ins.assignments {
                    let AssignmentTarget::ColumnName(n) = &a.target else {
                        return refuse("INSERT ... SET with a column tuple");
                    };
                    names.push(last_part(n)?);
                    row.push(value_text(text, &a.value));
                }
                cols = Some(names);
                rows.push(row);
            } else if let Some(q) = &ins.source {
                match q.body.as_ref() {
                    SetExpr::Values(v) => {
                        for r in &v.rows {
                            rows.push(r.iter().map(|e| value_text(text, e)).collect());
                        }
                    }
                    _ => query = Some(src(text, q.as_ref(), q.to_string())),
                }
            }
            Some(Ok((
                name.clone(),
                Dml::Insert {
                    cols,
                    mode,
                    rows,
                    query,
                    upsert,
                    aliased: ins.insert_alias.is_some(),
                },
            )))
        }
        Statement::Update(u) => {
            if !u.table.joins.is_empty() || u.from.is_some() {
                // Multi-table: `trg_multi_hook`'s, or the usual path's error.
                return None;
            }
            let TableFactor::Table { name, .. } = &u.table.relation else {
                return None;
            };
            let mut sets = Vec::new();
            for a in &u.assignments {
                let AssignmentTarget::ColumnName(n) = &a.target else {
                    return refuse("UPDATE ... SET with a column tuple");
                };
                sets.push((last_part(n)?, src(text, &a.value, a.value.to_string())));
            }
            let from = src(text, &u.table, u.table.to_string());
            let t = tail(
                text,
                &from,
                u.selection.as_ref(),
                &u.order_by,
                u.limit.as_ref(),
            );
            Some(Ok((name.clone(), Dml::Update { sets, tail: t })))
        }
        Statement::Delete(d) => {
            let froms = match &d.from {
                FromTable::WithFromKeyword(f) | FromTable::WithoutKeyword(f) => f,
            };
            if !d.tables.is_empty() || d.using.is_some() || froms.len() != 1 {
                return None;
            }
            let f = &froms[0];
            if !f.joins.is_empty() {
                return None;
            }
            let TableFactor::Table { name, .. } = &f.relation else {
                return None;
            };
            let from = src(text, f, f.to_string());
            let t = tail(
                text,
                &from,
                d.selection.as_ref(),
                &d.order_by,
                d.limit.as_ref(),
            );
            Some(Ok((name.clone(), Dml::Delete { tail: t })))
        }
        _ => None,
    }
}

/// `ON DUPLICATE KEY UPDATE`'s (column, expression) pairs of an `INSERT`
/// with MySQL's row alias, each reference to the new row made `VALUES(c)`
/// (`None`: `text` has no row alias).
fn alias_upsert(
    text: &str,
    def: &TableDef,
    cols: Option<&[String]>,
) -> Result<Option<Vec<(String, String)>>> {
    let Ok(mut stmts) =
        sqlparser::parser::Parser::parse_sql(&sprig_plan::dialect::SprigMySql, text)
    else {
        return Ok(None);
    };
    let Some(Statement::Insert(ins)) = stmts.pop() else {
        return Ok(None);
    };
    if ins.insert_alias.is_none() {
        return Ok(None);
    }
    let idx: Vec<usize> = match cols {
        Some(names) => names.iter().filter_map(|n| def.column(n)).collect(),
        None => (0..def.columns.len())
            .filter(|&i| !def.columns[i].invisible)
            .collect(),
    };
    let new_row = |ci: usize| {
        let t = format!("VALUES({})", q(&def.columns[ci].name));
        sqlparser::parser::Parser::new(&sprig_plan::dialect::SprigMySql)
            .try_with_sql(&t)
            .and_then(|mut p| p.parse_expr())
            .unwrap_or(Expr::Value(sqlparser::ast::Value::Null.into()))
    };
    let Some(sets) = crate::engine::plan::row_alias_sets(def, &ins, &idx, &new_row)? else {
        return Ok(Some(Vec::new()));
    };
    let mut out = Vec::with_capacity(sets.len());
    for a in &sets {
        let AssignmentTarget::ColumnName(n) = &a.target else {
            return Err(not_on_engine("ON DUPLICATE KEY UPDATE with a column tuple"));
        };
        let Some(c) = last_part(n) else {
            return Err(not_on_engine("an assignment target"));
        };
        out.push((c, a.value.to_string()));
    }
    Ok(Some(out))
}

/// A `VALUES` item's text; `None` for `DEFAULT`.
fn value_text(text: &str, e: &Expr) -> Option<String> {
    let t = src(text, e, e.to_string());
    if t.trim().eq_ignore_ascii_case("DEFAULT") {
        None
    } else {
        Some(t)
    }
}

fn q(n: &str) -> String {
    format!("`{}`", n.replace('`', "``"))
}

/// `ON DUPLICATE KEY UPDATE`'s (column, expression) pairs.
type Upsert = [(String, String)];

/// An `ON DUPLICATE KEY UPDATE` expression with each `VALUES(col)` (the
/// value the row would have been inserted with) replaced by that value.
fn values_subst(e: &str, def: &TableDef, new: &[Datum]) -> String {
    let b = e.as_bytes();
    let mut out = String::with_capacity(e.len());
    let mut i = 0;
    let mut quote: Option<u8> = None;
    while i < b.len() {
        let ch = b[i];
        if let Some(qc) = quote {
            if ch == qc {
                quote = None;
            }
            out.push(char::from(ch));
            i += 1;
            continue;
        }
        if ch == b'\'' || ch == b'"' {
            quote = Some(ch);
            out.push(char::from(ch));
            i += 1;
            continue;
        }
        let word_start = i == 0 || !(b[i - 1].is_ascii_alphanumeric() || b[i - 1] == b'_');
        // `VALUES(c)`, or MariaDB's `VALUE(c)`.
        let word = if e
            .get(i..i + 6)
            .is_some_and(|w| w.eq_ignore_ascii_case("VALUES"))
        {
            6
        } else if e
            .get(i..i + 5)
            .is_some_and(|w| w.eq_ignore_ascii_case("VALUE"))
        {
            5
        } else {
            0
        };
        if word_start && word > 0 {
            let rest = &e[i + word..];
            let open = rest.len() - rest.trim_start().len();
            if rest.trim_start().starts_with('(')
                && let Some(close) = rest.find(')')
            {
                let name = rest[open + 1..close].trim().trim_matches('`');
                if let Some(k) = def
                    .columns
                    .iter()
                    .position(|c| c.name.eq_ignore_ascii_case(name))
                {
                    out.push_str(&literal(&new[k], Some(&def.columns[k])));
                    i += word + close + 1;
                    continue;
                }
            }
        }
        let c = e[i..].chars().next().unwrap_or(' ');
        out.push(c);
        i += c.len_utf8();
    }
    out
}

/// `e` with each column reference (`c`, `` `c` ``, `t.c`) that `repl`
/// gives a value for replaced by that value; and whether any was.
/// Function names, user variables and quoted strings stay.
fn cols_subst(e: &str, repl: &dyn Fn(&str) -> Option<String>) -> (String, bool) {
    let raw = e.as_bytes();
    let mut out = String::with_capacity(e.len());
    let mut hit = false;
    let mut i = 0;
    // Where the identifier chain being read started in `out`.
    let mut chain: Option<usize> = None;
    let word = |chr: u8| chr.is_ascii_alphanumeric() || chr == b'_' || chr == b'$' || chr >= 0x80;
    while i < raw.len() {
        let ch = raw[i];
        if ch == b'\'' || ch == b'"' {
            let mut at = i + 1;
            while at < raw.len() {
                if raw[at] == b'\\' {
                    at += 2;
                    continue;
                }
                if raw[at] == ch {
                    if raw.get(at + 1) == Some(&ch) {
                        at += 2;
                        continue;
                    }
                    break;
                }
                at += 1;
            }
            let end = (at + 1).min(raw.len());
            out.push_str(&e[i..end]);
            i = end;
            chain = None;
            continue;
        }
        let prev = if i == 0 { b' ' } else { raw[i - 1] };
        let starts = ch == b'`' || (word(ch) && !word(prev) && !ch.is_ascii_digit());
        if !starts || prev == b'@' {
            if ch == b'.' && chain.is_some() {
                out.push('.');
                i += 1;
                continue;
            }
            if !word(ch) {
                chain = None;
            }
            let chr = e[i..].chars().next().unwrap_or(' ');
            out.push(chr);
            i += chr.len_utf8();
            continue;
        }
        // One identifier.
        let (name, end) = if ch == b'`' {
            let mut at = i + 1;
            let mut ident = String::new();
            while at < raw.len() {
                if raw[at] == b'`' {
                    if raw.get(at + 1) == Some(&b'`') {
                        ident.push('`');
                        at += 2;
                        continue;
                    }
                    break;
                }
                let chr = e[at..].chars().next().unwrap_or(' ');
                ident.push(chr);
                at += chr.len_utf8();
            }
            (ident, (at + 1).min(raw.len()))
        } else {
            let mut at = i;
            while at < raw.len() && word(raw[at]) {
                at += 1;
            }
            (e[i..at].to_owned(), at)
        };
        let qualified = prev == b'.' && chain.is_some();
        let start = if qualified { chain } else { Some(out.len()) };
        let after = e[end..].trim_start();
        let is_call = after.starts_with('(');
        let continues = e[end..].starts_with('.');
        if !is_call
            && !continues
            && let Some(v) = repl(&name)
        {
            out.truncate(start.unwrap_or(out.len()));
            out.push_str(&v);
            hit = true;
            chain = None;
        } else {
            out.push_str(&e[i..end]);
            chain = start;
        }
        i = end;
    }
    (out, hit)
}

/// For each assignment, whether it reads a column assigned before it in
/// the same list (the servers read the new value then).
fn reads_earlier(def: &TableDef, targets: &[usize], exprs: &[&str]) -> Vec<bool> {
    exprs
        .iter()
        .enumerate()
        .map(|(k, e)| {
            let earlier = &targets[..k];
            cols_subst(e, &|n| {
                earlier
                    .iter()
                    .any(|&i| def.columns[i].name.eq_ignore_ascii_case(n))
                    .then(String::new)
            })
            .1
        })
        .collect()
}

/// The table an `INSERT | REPLACE | UPDATE | DELETE` names first.
fn table_of_text(text: &str) -> Option<String> {
    let toks = crate::engine::define::lex::lex(text).ok()?;
    let mut i = 1;
    while let Some(t) = toks.get(i) {
        if t.is("INTO") || t.is("FROM") || t.is("IGNORE") || t.is("LOW_PRIORITY") {
            i += 1;
            continue;
        }
        break;
    }
    let (crate::engine::define::lex::T::Word(w) | crate::engine::define::lex::T::Quoted(w)) =
        &toks.get(i)?.t
    else {
        return None;
    };
    if toks.get(i + 1).is_some_and(|t| t.op(".")) {
        let (crate::engine::define::lex::T::Word(n) | crate::engine::define::lex::T::Quoted(n)) =
            &toks.get(i + 2)?.t
        else {
            return None;
        };
        return Some(format!("{w}.{n}"));
    }
    Some(w.clone())
}

/// What one statement did.
#[derive(Default)]
struct Done {
    affected: u64,
    matched: u64,
    rows: u64,
    first_id: u64,
    /// Rows `INSERT IGNORE` skipped or `REPLACE` replaced.
    dups: u64,
}

impl PassSession {
    /// An `INSERT`, `UPDATE` or `DELETE` on an engine table with triggers
    /// for it: run here, row by row (`None`: no trigger fires, the
    /// statement goes on as usual).
    #[allow(clippy::too_many_lines)] // the forms, the table, then the firing lists
    pub(in crate::exec) async fn engine_sql_triggers(
        &mut self,
        cx: &mut ExecCtx<'_>,
        cat: &Arc<sprig_catalog::TenantCatalog>,
        logical: Option<&[u8]>,
        sql: &Bytes,
        more: bool,
        out: &mut dyn ResultSink,
    ) -> Result<Option<Step>> {
        let Some(eng) = catalog::of(cat) else {
            return Ok(None);
        };
        if top_is_raw() {
            return Ok(None);
        }
        let text = match logical.or(self.eng.entry.as_deref()) {
            Some(t) => String::from_utf8_lossy(t).into_owned(),
            None => String::from_utf8_lossy(sql).into_owned(),
        };
        let text = text.as_str();
        // Validations: a write checks them (and one running writes nothing).
        let vals = if validation::write_verb(text) {
            if let Some(name) = validation::running() {
                let e = validation::writes_refused(&name, text);
                return Ok(Some(self.fail(out, &e).await?));
            }
            match self.validations().await {
                Ok(v) => v,
                Err(e) => return Ok(Some(self.fail(out, &e).await?)),
            }
        } else {
            Arc::default()
        };
        if eng.triggers.is_empty() && vals.is_empty() {
            return Ok(None);
        }
        // `UPDATE a JOIN b ...`, `DELETE a, b FROM ...`: every target's
        // triggers.
        if let Some(step) =
            Box::pin(self.trg_multi_hook(cx, cat, eng, &vals, text, more, out)).await?
        {
            return Ok(Some(step));
        }
        let Some(read) = read(text) else {
            return Ok(None);
        };
        let lctn = self
            .tenant
            .as_deref()
            .is_some_and(crate::tenant::session::TenantSession::lower_case_table_names);
        let (name, dml) = match read {
            Ok(x) => x,
            Err(why) => {
                // Refused only when the table has triggers.
                let Some(name) = table_of_text(text) else {
                    return Ok(None);
                };
                // (Validations don't check deletes.)
                let v = (!validation::deletes(text)).then_some(&*vals);
                if !self.trg_has(eng, v, &name, lctn) {
                    return Ok(None);
                }
                let e = not_on_engine(&why);
                return Ok(Some(self.fail(out, &e).await?));
            }
        };
        let Some(db_name) = db_part(&name).or_else(|| self.current_logical()) else {
            return Ok(None);
        };
        let Some(row) = cat.database(&database_key(&db_name, lctn)) else {
            return Ok(None);
        };
        let Some(tname) = last_part(&name) else {
            return Ok(None);
        };
        let Some(def) = eng
            .table(row.db_id, &tname, lctn)
            .and_then(|p| p.as_ref().as_ref().ok().cloned())
        else {
            return Ok(None);
        };
        if on_stack(&busy(def.object)) {
            let e = SprigError::standard(
                1442,
                *b"HY000",
                format!(
                    "Can't update table '{}' in stored function/trigger because it is already used by statement which invoked this stored function/trigger",
                    def.name
                ),
            );
            return Ok(Some(self.fail(out, &e).await?));
        }
        let mut dml = dml;
        let mysql = self.persona == sprig_proto_mysql::Persona::MySql84;
        if let Dml::Insert {
            aliased,
            cols,
            upsert,
            ..
        } = &mut dml
        {
            // MySQL's row alias, read off the client's own text (the
            // MySQL port hands it on already written as `VALUE(c)`).
            // MariaDB has none: its syntax error comes from the usual path.
            if *aliased && !mysql {
                return Ok(None);
            }
            let client = if *aliased {
                Some(text.to_owned())
            } else if mysql && !upsert.is_empty() {
                self.eng
                    .entry
                    .as_deref()
                    .map(|t| String::from_utf8_lossy(t).into_owned())
            } else {
                None
            };
            if let Some(t) = client {
                match alias_upsert(&t, &def, cols.as_deref()) {
                    Ok(Some(u)) => *upsert = u,
                    Ok(None) => {}
                    Err(e) => return Ok(Some(self.fail(out, &e).await?)),
                }
            }
        }
        let ev = dml.event();
        let before = firing(eng.triggers.values(), def.object, true, ev);
        let after = firing(eng.triggers.values(), def.object, false, ev);
        // `REPLACE` also fires the `DELETE` triggers of the rows it
        // removes; `ON DUPLICATE KEY UPDATE` the `UPDATE` triggers of the
        // rows it changes.
        let del = match &dml {
            Dml::Insert {
                mode: Mode::Replace,
                ..
            } => (
                firing(eng.triggers.values(), def.object, true, Event::Delete),
                firing(eng.triggers.values(), def.object, false, Event::Delete),
            ),
            Dml::Insert { upsert, .. } if !upsert.is_empty() => (
                firing(eng.triggers.values(), def.object, true, Event::Update),
                firing(eng.triggers.values(), def.object, false, Event::Update),
            ),
            _ => (Vec::new(), Vec::new()),
        };
        // Validations: an insert's (an upsert's update too), an update's.
        let checks = match &dml {
            Dml::Insert { upsert, .. } => {
                vals.fires(def.object, false)
                    || (!upsert.is_empty() && vals.fires(def.object, true))
            }
            Dml::Update { .. } => vals.fires(def.object, true),
            Dml::Delete { .. } | Dml::Multi { .. } => false,
        };
        if before.is_empty() && after.is_empty() && del.0.is_empty() && del.1.is_empty() && !checks
        {
            return Ok(None);
        }
        let db_name = row.name.clone();
        let fut = self.trg_statement(
            cx,
            def,
            db_name,
            dml,
            [before, after, del.0, del.1],
            more,
            out,
        );
        let fut = RIGHTS.scope(std::cell::RefCell::new(Vec::new()), fut);
        Ok(Some(
            Box::pin(validation::with(vals, with_stack(fut))).await?,
        ))
    }

    /// `text` changes rows of an engine table that has triggers or
    /// validations: a prepared statement the engine can't run yet.
    pub(in crate::exec) async fn engine_prepared_fires(&self, text: &[u8]) -> bool {
        let text = String::from_utf8_lossy(text);
        let first = text.trim_start().get(..6).map(str::to_ascii_uppercase);
        if !matches!(
            first.as_deref(),
            Some("INSERT" | "UPDATE" | "DELETE" | "REPLAC")
        ) {
            return false;
        }
        let Some(cat) = self.engine_cat() else {
            return false;
        };
        let Some(eng) = catalog::of(&cat) else {
            return false;
        };
        // A table with validations (read again if they're stale; a failed
        // read sends the write the text way, which reports it).
        let vals = self.validations().await.ok();
        if eng.triggers.is_empty() && vals.as_ref().is_some_and(|v| v.is_empty()) {
            return false;
        }
        let lctn = self
            .tenant
            .as_deref()
            .is_some_and(crate::tenant::session::TenantSession::lower_case_table_names);
        let vals = if validation::deletes(&text) {
            Some(Arc::default())
        } else {
            vals
        };
        match (table_of_text(&text), &vals) {
            (Some(n), Some(v)) => self.trg_has(eng, Some(&**v), &n, lctn),
            (Some(_), None) => true,
            (None, _) => false,
        }
    }

    /// Whether table `name` has triggers, or validations in `vals`.
    fn trg_has(
        &self,
        eng: &catalog::EngineCatalog,
        vals: Option<&validation::Validations>,
        name: &str,
        lctn: bool,
    ) -> bool {
        let (db, t) = match name.split_once('.') {
            Some((d, t)) => (Some(d.to_owned()), t),
            None => (self.current_logical(), name),
        };
        let Some(db) = db else { return false };
        let Some(cat) = self.engine_cat() else {
            return false;
        };
        let Some(row) = cat.database(&database_key(&db, lctn)) else {
            return false;
        };
        let Some(Ok(def)) = eng.table(row.db_id, t, lctn).map(|p| p.as_ref().as_ref()) else {
            return false;
        };
        eng.triggers.values().any(|x| x.table == def.object)
            || vals.is_some_and(|v| v.has(def.object))
    }

    /// A one-row change or read of the statement's own: fires nothing.
    async fn trg_raw(&mut self, cx: &mut ExecCtx<'_>, sql: &str) -> Result<Ran> {
        let _ = STACK.try_with(|s| s.borrow_mut().push(RAW.to_owned()));
        let r = self.sp_statement(cx, sql, None, true).await;
        let _ = STACK.try_with(|s| s.borrow_mut().pop());
        r.map_err(|e| e.to_error())
    }

    /// Runs the statement in a transaction or savepoint of its own.
    #[allow(clippy::too_many_arguments, clippy::too_many_lines)]
    async fn trg_statement(
        &mut self,
        cx: &mut ExecCtx<'_>,
        def: Arc<TableDef>,
        db: String,
        dml: Dml,
        trigs: [Vec<Arc<Trigger>>; 4],
        more: bool,
        out: &mut dyn ResultSink,
    ) -> Result<Step> {
        let st = self.status();
        let nested = st.contains(StatusFlags::IN_TRANS) || !st.contains(StatusFlags::AUTOCOMMIT);
        let depth = STACK.try_with(|s| s.borrow().len()).unwrap_or(0);
        let sp = format!("`_sprig_trigger_{depth}`");
        let (begin, commit, undo) = if nested {
            (
                format!("SAVEPOINT {sp}"),
                format!("RELEASE SAVEPOINT {sp}"),
                format!("ROLLBACK TO SAVEPOINT {sp}"),
            )
        } else {
            (
                "START TRANSACTION".to_owned(),
                "COMMIT".to_owned(),
                "ROLLBACK".to_owned(),
            )
        };
        if let Err(e) = self.trg_raw(cx, &begin).await {
            return self.fail(out, &e).await;
        }
        let [before, after, del_before, del_after] = trigs;
        let mark = busy(def.object);
        let _ = STACK.try_with(|s| s.borrow_mut().push(mark));
        let r = match &dml {
            Dml::Insert {
                cols,
                mode,
                rows,
                query,
                upsert,
                ..
            } => {
                Box::pin(self.trg_insert(
                    cx,
                    &def,
                    &db,
                    (cols.as_deref(), *mode, upsert),
                    rows,
                    query.as_deref(),
                    [&before, &after, &del_before, &del_after],
                ))
                .await
            }
            Dml::Update { sets, tail } => {
                Box::pin(self.trg_update(cx, &def, &db, sets, tail, &before, &after)).await
            }
            Dml::Delete { tail } => {
                Box::pin(self.trg_delete(cx, &def, &db, tail, &before, &after)).await
            }
            Dml::Multi { rs, trigs, dbs } => Box::pin(self.trg_multi(cx, rs, trigs, dbs)).await,
        };
        let _ = STACK.try_with(|s| s.borrow_mut().pop());
        match r {
            Ok(done) => {
                if let Err(e) = self.trg_raw(cx, &commit).await {
                    return self.fail(out, &e).await;
                }
                if done.first_id != 0 {
                    self.eng.last_insert_id = done.first_id;
                }
                let mut status = self.status();
                if more {
                    status |= StatusFlags::MORE_RESULTS_EXISTS;
                }
                let mut ok = OkPacket::new(status);
                ok.affected_rows = done.affected;
                ok.last_insert_id = done.first_id;
                let info = match dml {
                    Dml::Multi { ref rs, .. } if rs.update => Some(format!(
                        "Rows matched: {}  Changed: {}  Warnings: 0",
                        done.matched, done.affected
                    )),
                    Dml::Update { .. } => Some(format!(
                        "Rows matched: {}  Changed: {}  Warnings: 0",
                        done.matched, done.affected
                    )),
                    Dml::Insert {
                        ref rows,
                        ref query,
                        mode,
                        ..
                    } if rows.len() > 1 || query.is_some() => Some(format!(
                        "Records: {}  Duplicates: {}  Warnings: {}",
                        done.rows,
                        done.dups,
                        if mode == Mode::Ignore { done.dups } else { 0 }
                    )),
                    _ => None,
                };
                if let Some(i) = info {
                    ok.info = Bytes::from(i);
                }
                out.ok(ok).await?;
                Ok(Step {
                    errored: false,
                    status: Some(status),
                })
            }
            Err(e) => {
                let _ = self.trg_raw(cx, &undo).await;
                if nested {
                    let _ = self.trg_raw(cx, &commit).await;
                }
                self.fail(out, &e).await
            }
        }
    }

    /// 1142 unless the definer of `t` has the TRIGGER right on table
    /// `def` of database `db`: checked when the trigger first fires in a
    /// statement, as both servers do (a statement that changes no row
    /// checks nothing).
    fn trg_rights(&mut self, t: &Trigger, db: &str, def: &TableDef) -> Result<()> {
        let who = &t.cx.definer;
        let key = (def.object, who.0.clone(), who.1.clone());
        if RIGHTS
            .try_with(|r| r.borrow().contains(&key))
            .unwrap_or(false)
        {
            return Ok(());
        }
        let saved = self.sp_become(Some(who))?;
        let ok = match (self.tenant.as_deref(), self.engine_cat()) {
            (Some(ts), Some(cat)) => {
                let lctn = ts.lower_case_table_names();
                let grants = crate::tenant::authz::privileges(ts, &cat);
                let db_key = database_key(db, lctn);
                let name_key = sprig_catalog::ddl::object_key(
                    sprig_catalog::store::ObjectKind::Table,
                    &def.name,
                    lctn,
                );
                let on = |target| grants.allows(sprig_auth::authz::Action::Trigger, target);
                grants.unrestricted()
                    || on(sprig_auth::authz::Target::Object {
                        db_key: &db_key,
                        db: def.db,
                        object: def.object,
                    })
                    || on(sprig_auth::authz::Target::Named {
                        db_key: &db_key,
                        name_key: &name_key,
                    })
                    || on(sprig_auth::authz::Target::Database { db_key: &db_key })
            }
            _ => true,
        };
        if let (Some(w), Some(ts)) = (saved, self.tenant.as_deref_mut()) {
            ts.principal.user_id = w.0;
            ts.principal.user = w.1;
            ts.principal.host = w.2;
            ts.authz = w.3;
        }
        if !ok {
            let shown = if self.persona == sprig_proto_mysql::Persona::MySql84 {
                format!("'{}'", def.name)
            } else {
                format!("`{db}`.`{}`", def.name)
            };
            return Err(SprigError::standard(
                1142,
                *b"42000",
                format!(
                    "TRIGGER command denied to user '{}'@'{}' for table {shown}",
                    who.0, who.1
                ),
            ));
        }
        let _ = RIGHTS.try_with(|r| r.borrow_mut().push(key));
        Ok(())
    }

    /// Runs `trigs` for one row: `new` / `old` are the slots' values
    /// (`new` is written back: a `BEFORE` trigger may change it).
    async fn trg_fire(
        &mut self,
        cx: &mut ExecCtx<'_>,
        def: &TableDef,
        db: &str,
        trigs: &[Arc<Trigger>],
        new: &mut [Datum],
        old: &[Datum],
    ) -> Result<()> {
        let names: Vec<String> = def.columns.iter().map(|c| c.name.clone()).collect();
        let n = def.columns.len();
        for t in trigs {
            self.trg_rights(t, db, def)?;
            let prog = t.program(&names)?;
            let has_new = t.when().is_some_and(|(_, e)| e != Event::Delete);
            let has_old = t.when().is_some_and(|(_, e)| e != Event::Insert);
            let mut vals = Vec::with_capacity(prog.vars.len());
            let mut cols = Vec::with_capacity(prog.vars.len());
            if has_new {
                vals.extend(new.iter().cloned());
                cols.extend(def.columns.iter().map(|c| Some(c.clone())));
            }
            if has_old {
                vals.extend(old.iter().cloned());
                cols.extend(def.columns.iter().map(|c| Some(c.clone())));
            }
            vals.resize(prog.vars.len(), Datum::Null);
            cols.resize(prog.vars.len(), None);
            let mut frame = Frame {
                cursors: vec![None; prog.cursors.len()],
                prog,
                vals,
                cols,
                function: true,
                trigger: true,
                db: db.to_owned(),
                name: t.name.clone(),
                affected: 0,
                last_affected: 0,
                definer: Some(t.cx.definer.clone()),
            };
            self.sp_run(cx, &mut frame, None).await?;
            if has_new {
                new.clone_from_slice(&frame.vals[..n]);
            }
        }
        Ok(())
    }

    /// The typed values of a row read back as text.
    fn trg_row(
        &mut self,
        cx: &ExecCtx<'_>,
        def: &TableDef,
        cols: &[ColumnDef],
        row: &[Option<Bytes>],
    ) -> Result<Vec<Datum>> {
        let env = self.engine_vars(cx).env;
        let mut conds = Vec::new();
        def.columns
            .iter()
            .enumerate()
            .map(|(i, c)| {
                let d = match cols.get(i) {
                    Some(cd) => wire_datum(row.get(i).and_then(|v| v.as_deref()), cd),
                    None => Datum::Null,
                };
                store(Some(c), d, env, &mut conds)
            })
            .collect()
    }

    /// `WHERE` that finds one row by its key (all columns for a table
    /// without one).
    fn trg_match(def: &TableDef, vals: &[Datum]) -> String {
        let cols: Vec<usize> = if def.hidden {
            (0..def.columns.len()).collect()
        } else {
            def.pk.clone()
        };
        let parts: Vec<String> = cols
            .iter()
            .filter_map(|&i| {
                let c = def.columns.get(i)?;
                Some(format!("{} <=> {}", q(&c.name), literal(&vals[i], Some(c))))
            })
            .collect();
        format!(" WHERE {} LIMIT 1", parts.join(" AND "))
    }

    fn trg_select_list(def: &TableDef) -> String {
        def.columns
            .iter()
            .map(|c| q(&c.name))
            .collect::<Vec<_>>()
            .join(", ")
    }

    /// The row with key values `vals`, as stored (`None`: not found).
    async fn trg_reread(
        &mut self,
        cx: &mut ExecCtx<'_>,
        def: &TableDef,
        db: &str,
        vals: &[Datum],
    ) -> Result<Option<Vec<Datum>>> {
        let sql = format!(
            "SELECT {} FROM {}.{}{}",
            Self::trg_select_list(def),
            q(db),
            q(&def.name),
            Self::trg_match(def, vals)
        );
        let ran = self.trg_raw(cx, &sql).await?;
        match ran.rows.first() {
            Some(r) => Ok(Some(self.trg_row(cx, def, &ran.columns, r)?)),
            None => Ok(None),
        }
    }

    /// An assignment's value read against the stored row `old`, with the
    /// columns assigned before it (`earlier`) at their new values in `new`.
    #[allow(clippy::too_many_arguments)]
    async fn trg_eval_after(
        &mut self,
        cx: &mut ExecCtx<'_>,
        def: &TableDef,
        db: &str,
        earlier: &[usize],
        e: &str,
        new: &[Datum],
        old: &[Datum],
    ) -> Result<Datum> {
        let (e, _) = cols_subst(e, &|n| {
            earlier
                .iter()
                .find(|&&i| def.columns[i].name.eq_ignore_ascii_case(n))
                .map(|&i| literal(&new[i], Some(&def.columns[i])))
        });
        let sql = format!(
            "SELECT {e} FROM {}.{}{}",
            q(db),
            q(&def.name),
            Self::trg_match(def, old)
        );
        let ran = self.trg_raw(cx, &sql).await?;
        Ok(match (ran.columns.first(), ran.rows.first()) {
            (Some(cd), Some(r)) => wire_datum(r.first().and_then(|v| v.as_deref()), cd),
            _ => Datum::Null,
        })
    }

    /// The rows holding one of `new`'s unique keys, locked.
    async fn trg_conflicts(
        &mut self,
        cx: &mut ExecCtx<'_>,
        def: &TableDef,
        db: &str,
        new: &[Datum],
    ) -> Result<Option<Ran>> {
        let mut keys: Vec<&[usize]> = def
            .indexes
            .iter()
            .filter(|x| x.unique)
            .map(|x| x.cols.as_slice())
            .collect();
        if !def.hidden {
            keys.push(&def.pk);
        }
        let ors: Vec<String> = keys
            .iter()
            .filter(|k| k.iter().all(|&i| !new[i].is_null()))
            .map(|k| {
                let ands: Vec<String> = k
                    .iter()
                    .map(|&i| {
                        let c = &def.columns[i];
                        format!("{} = {}", q(&c.name), literal(&new[i], Some(c)))
                    })
                    .collect();
                format!("({})", ands.join(" AND "))
            })
            .collect();
        if ors.is_empty() {
            return Ok(None);
        }
        let sql = format!(
            "SELECT {} FROM {}.{} WHERE {} FOR UPDATE",
            Self::trg_select_list(def),
            q(db),
            q(&def.name),
            ors.join(" OR ")
        );
        Ok(Some(self.trg_raw(cx, &sql).await?))
    }

    /// `ON DUPLICATE KEY UPDATE` of the row `old` that holds a key of
    /// `new`: its `UPDATE` triggers around the change. The affected-row
    /// count it adds (2 changed, 0 unchanged).
    #[allow(clippy::too_many_arguments)]
    async fn trg_upsert(
        &mut self,
        cx: &mut ExecCtx<'_>,
        def: &TableDef,
        db: &str,
        upsert: &[(String, String)],
        new: &[Datum],
        old: &[Datum],
        [bu, au]: [&[Arc<Trigger>]; 2],
    ) -> Result<u64> {
        let env = self.engine_vars(cx).env;
        let mut idx = Vec::with_capacity(upsert.len());
        let mut exprs = Vec::with_capacity(upsert.len());
        for (c, e) in upsert {
            let Some(i) = def
                .columns
                .iter()
                .position(|x| x.name.eq_ignore_ascii_case(c))
            else {
                return Err(SprigError::standard(
                    1054,
                    *b"42S22",
                    format!("Unknown column '{c}' in 'field list'"),
                ));
            };
            idx.push(i);
            exprs.push(values_subst(e, def, new));
        }
        let refs: Vec<&str> = exprs.iter().map(String::as_str).collect();
        let deps = reads_earlier(def, &idx, &refs);
        let list: Vec<&str> = refs
            .iter()
            .zip(&deps)
            .map(|(e, dep)| if *dep { "NULL" } else { e })
            .collect();
        let sql = format!(
            "SELECT {} FROM {}.{}{}",
            list.join(", "),
            q(db),
            q(&def.name),
            Self::trg_match(def, old)
        );
        let ran = self.trg_raw(cx, &sql).await?;
        let one = ran.rows.into_iter().next().unwrap_or_default();
        let mut upd = old.to_vec();
        let mut conds = Vec::new();
        for (k, &i) in idx.iter().enumerate() {
            let v = if deps[k] {
                self.trg_eval_after(cx, def, db, &idx[..k], refs[k], &upd, old)
                    .await?
            } else {
                ran.columns.get(k).map_or(Datum::Null, |cd| {
                    wire_datum(one.get(k).and_then(|v| v.as_deref()), cd)
                })
            };
            upd[i] = store(Some(&def.columns[i]), v, env, &mut conds)?;
        }
        let (bu, au) = (of_assigned(bu, def, &idx), of_assigned(au, def, &idx));
        let (bu, au) = (bu.as_slice(), au.as_slice());
        self.trg_fire(cx, def, db, bu, &mut upd, old).await?;
        self.val_check(cx, def, db, &upd, Some(old)).await?;
        let sets: Vec<String> = def
            .columns
            .iter()
            .enumerate()
            .filter(|(i, c)| {
                c.generated.is_none() && literal(&upd[*i], Some(c)) != literal(&old[*i], Some(c))
            })
            .map(|(i, c)| format!("{} = {}", q(&c.name), literal(&upd[i], Some(c))))
            .collect();
        let mut n = 0;
        if !sets.is_empty() {
            let sql = format!(
                "UPDATE {}.{} SET {}{}",
                q(db),
                q(&def.name),
                sets.join(", "),
                Self::trg_match(def, old)
            );
            self.trg_raw(cx, &sql).await?;
            n = 2;
        }
        let mut stored = self.trg_reread(cx, def, db, &upd).await?.unwrap_or(upd);
        self.trg_fire(cx, def, db, au, &mut stored, old).await?;
        Ok(n)
    }

    #[allow(clippy::too_many_arguments, clippy::too_many_lines)]
    async fn trg_insert(
        &mut self,
        cx: &mut ExecCtx<'_>,
        def: &TableDef,
        db: &str,
        (cols, mode, upsert): (Option<&[String]>, Mode, &Upsert),
        rows: &[Vec<Option<String>>],
        query: Option<&str>,
        [before, after, del_before, del_after]: [&[Arc<Trigger>]; 4],
    ) -> Result<Done> {
        // Target columns, by position.
        let targets: Vec<usize> = match cols {
            Some(names) => {
                let mut v = Vec::new();
                for n in names {
                    let Some(i) = def
                        .columns
                        .iter()
                        .position(|c| c.name.eq_ignore_ascii_case(n))
                    else {
                        return Err(SprigError::standard(
                            1054,
                            *b"42S22",
                            format!("Unknown column '{n}' in 'field list'"),
                        ));
                    };
                    v.push(i);
                }
                v
            }
            None => (0..def.columns.len())
                .filter(|&i| !def.columns[i].invisible)
                .collect(),
        };
        // The values of each row: given ones (`None`: the column's
        // default).
        let mut given: Vec<Vec<Option<Datum>>> = Vec::new();
        if let Some(qtext) = query {
            let ran = self.trg_raw(cx, qtext).await?;
            for r in &ran.rows {
                let mut row = Vec::with_capacity(targets.len());
                for (k, &i) in targets.iter().enumerate() {
                    let d = match ran.columns.get(k) {
                        Some(cd) => wire_datum(r.get(k).and_then(|v| v.as_deref()), cd),
                        None => Datum::Null,
                    };
                    let _ = i;
                    row.push(Some(d));
                }
                given.push(row);
            }
        } else {
            for r in rows {
                if r.len() != targets.len() {
                    return Err(SprigError::standard(
                        1136,
                        *b"21S01",
                        format!(
                            "Column count doesn't match value count at row {}",
                            given.len() + 1
                        ),
                    ));
                }
                let exprs: Vec<String> = r.iter().flatten().cloned().collect();
                let vals = if exprs.is_empty() {
                    Vec::new()
                } else {
                    let sql = format!("SELECT {}", exprs.join(", "));
                    let ran = self.trg_raw(cx, &sql).await?;
                    let one = ran.rows.into_iter().next().unwrap_or_default();
                    ran.columns
                        .iter()
                        .enumerate()
                        .map(|(i, c)| wire_datum(one.get(i).and_then(|v| v.as_deref()), c))
                        .collect()
                };
                let mut it = vals.into_iter();
                given.push(
                    r.iter()
                        .map(|e| e.as_ref().map(|_| it.next().unwrap_or(Datum::Null)))
                        .collect(),
                );
            }
        }
        let env = self.engine_vars(cx).env;
        let mut done = Done::default();
        for g in given {
            let mut conds = Vec::new();
            // `NEW` before the triggers: the given values converted as a
            // column assignment, defaults for the rest.
            let mut new: Vec<Datum> = def
                .columns
                .iter()
                .map(|c| match &c.default {
                    Some(ColDefault::Value(v)) => v.clone(),
                    _ if c.auto_inc => Datum::Int(0),
                    _ => Datum::Null,
                })
                .collect();
            let mut set = vec![false; def.columns.len()];
            for (k, &i) in targets.iter().enumerate() {
                if let Some(Some(v)) = g.get(k) {
                    new[i] = store(Some(&def.columns[i]), v.clone(), env, &mut conds)?;
                    set[i] = true;
                }
            }
            let start: Vec<String> = def
                .columns
                .iter()
                .enumerate()
                .map(|(i, c)| literal(&new[i], Some(c)))
                .collect();
            self.trg_fire(cx, def, db, before, &mut new, &[]).await?;
            self.val_check(cx, def, db, &new, None).await?;
            let mut names = Vec::new();
            let mut lits = Vec::new();
            for (i, c) in def.columns.iter().enumerate() {
                if c.generated.is_some() {
                    continue;
                }
                let lit = literal(&new[i], Some(c));
                if set[i] || lit != start[i] {
                    names.push(q(&c.name));
                    lits.push(lit);
                }
            }
            // `ON DUPLICATE KEY UPDATE`: a row holding one of the new row's
            // unique keys is changed instead (its `UPDATE` triggers fire).
            if !upsert.is_empty()
                && let Some(found) = self.trg_conflicts(cx, def, db, &new).await?
                && let Some(r) = found.rows.first()
            {
                let old = self.trg_row(cx, def, &found.columns, r)?;
                let n = Box::pin(self.trg_upsert(
                    cx,
                    def,
                    db,
                    upsert,
                    &new,
                    &old,
                    [del_before, del_after],
                ))
                .await?;
                done.affected += n;
                done.rows += 1;
                done.dups += 1;
                continue;
            }
            if mode == Mode::Replace {
                // The rows holding any of the new row's unique keys go first.
                if let Some(found) = self.trg_conflicts(cx, def, db, &new).await? {
                    for r in &found.rows {
                        let old = self.trg_row(cx, def, &found.columns, r)?;
                        self.trg_fire(cx, def, db, del_before, &mut [], &old)
                            .await?;
                        let sql = format!(
                            "DELETE FROM {}.{}{}",
                            q(db),
                            q(&def.name),
                            Self::trg_match(def, &old)
                        );
                        let gone = self.trg_raw(cx, &sql).await?;
                        done.affected += gone.affected;
                        done.dups += 1;
                        self.trg_fire(cx, def, db, del_after, &mut [], &old).await?;
                    }
                }
            }
            let sql = format!(
                "INSERT{} INTO {}.{} ({}) VALUES ({})",
                if mode == Mode::Ignore { " IGNORE" } else { "" },
                q(db),
                q(&def.name),
                names.join(", "),
                lits.join(", ")
            );
            let ran = self.trg_raw(cx, &sql).await?;
            done.affected += ran.affected;
            done.rows += 1;
            if done.first_id == 0 {
                done.first_id = ran.last_id;
            }
            if ran.affected == 0 {
                // `INSERT IGNORE` skipped it: no `AFTER` trigger.
                done.dups += 1;
                continue;
            }
            if after.is_empty() {
                continue;
            }
            if let Some(ai) = def.auto_inc
                && ran.last_id != 0
                && matches!(new[ai], Datum::Null | Datum::Int(0) | Datum::UInt(0))
            {
                new[ai] = Datum::UInt(ran.last_id);
            }
            let mut stored = if def.hidden {
                new.clone()
            } else {
                self.trg_reread(cx, def, db, &new).await?.unwrap_or(new)
            };
            self.trg_fire(cx, def, db, after, &mut stored, &[]).await?;
        }
        Ok(done)
    }

    #[allow(clippy::too_many_arguments)]
    async fn trg_update(
        &mut self,
        cx: &mut ExecCtx<'_>,
        def: &TableDef,
        db: &str,
        sets: &[(String, String)],
        tail: &str,
        before: &[Arc<Trigger>],
        after: &[Arc<Trigger>],
    ) -> Result<Done> {
        let mut targets = Vec::new();
        for (n, _) in sets {
            let Some(i) = def
                .columns
                .iter()
                .position(|c| c.name.eq_ignore_ascii_case(n))
            else {
                return Err(SprigError::standard(
                    1054,
                    *b"42S22",
                    format!("Unknown column '{n}' in 'field list'"),
                ));
            };
            targets.push(i);
        }
        // An assignment reading a column assigned before it sees the new
        // value on both servers: such ones run per row, after the others.
        let exprs: Vec<&str> = sets.iter().map(|(_, e)| e.as_str()).collect();
        let deps = reads_earlier(def, &targets, &exprs);
        let n = def.columns.len();
        let mut list = Self::trg_select_list(def);
        for (e, dep) in exprs.iter().zip(&deps) {
            list.push_str(", ");
            list.push_str(if *dep { "NULL" } else { e });
        }
        let sql = format!("SELECT {list}{tail} FOR UPDATE");
        let ran = self.trg_raw(cx, &sql).await?;
        let env = self.engine_vars(cx).env;
        let mut done = Done::default();
        for r in &ran.rows {
            let old = self.trg_row(cx, def, &ran.columns, r)?;
            let mut new = old.clone();
            let mut conds = Vec::new();
            for (k, &i) in targets.iter().enumerate() {
                let d = if deps[k] {
                    self.trg_eval_after(cx, def, db, &targets[..k], exprs[k], &new, &old)
                        .await?
                } else {
                    match ran.columns.get(n + k) {
                        Some(cd) => wire_datum(r.get(n + k).and_then(|v| v.as_deref()), cd),
                        None => Datum::Null,
                    }
                };
                new[i] = store(Some(&def.columns[i]), d, env, &mut conds)?;
            }
            self.trg_update_row(cx, def, db, &targets, new, &old, [before, after], &mut done)
                .await?;
        }
        Ok(done)
    }

    /// One row of an `UPDATE`: `new` (the assignments' values, `targets`
    /// their columns) through the `BEFORE` triggers, the change, then the
    /// `AFTER` triggers on the row as stored.
    #[allow(clippy::too_many_arguments)]
    async fn trg_update_row(
        &mut self,
        cx: &mut ExecCtx<'_>,
        def: &TableDef,
        db: &str,
        targets: &[usize],
        mut new: Vec<Datum>,
        old: &[Datum],
        [before, after]: [&[Arc<Trigger>]; 2],
        done: &mut Done,
    ) -> Result<()> {
        let (before, after) = (
            of_assigned(before, def, targets),
            of_assigned(after, def, targets),
        );
        let (before, after) = (before.as_slice(), after.as_slice());
        let start: Vec<String> = def
            .columns
            .iter()
            .enumerate()
            .map(|(i, c)| literal(&new[i], Some(c)))
            .collect();
        self.trg_fire(cx, def, db, before, &mut new, old).await?;
        self.val_check(cx, def, db, &new, Some(old)).await?;
        let mut parts = Vec::new();
        for (i, c) in def.columns.iter().enumerate() {
            if c.generated.is_some() {
                continue;
            }
            let value = literal(&new[i], Some(c));
            if targets.contains(&i) || value != start[i] {
                parts.push(format!("{} = {value}", q(&c.name)));
            }
        }
        done.matched += 1;
        if !parts.is_empty() {
            let sql = format!(
                "UPDATE {}.{} SET {}{}",
                q(db),
                q(&def.name),
                parts.join(", "),
                Self::trg_match(def, old)
            );
            let ran = self.trg_raw(cx, &sql).await?;
            done.affected += ran.affected;
        }
        if after.is_empty() {
            return Ok(());
        }
        let mut stored = if def.hidden {
            new.clone()
        } else {
            self.trg_reread(cx, def, db, &new).await?.unwrap_or(new)
        };
        self.trg_fire(cx, def, db, after, &mut stored, old).await
    }

    /// One row of a `DELETE`, between its triggers.
    async fn trg_delete_row(
        &mut self,
        cx: &mut ExecCtx<'_>,
        def: &TableDef,
        db: &str,
        old: &[Datum],
        [before, after]: [&[Arc<Trigger>]; 2],
        done: &mut Done,
    ) -> Result<()> {
        self.trg_fire(cx, def, db, before, &mut [], old).await?;
        let sql = format!(
            "DELETE FROM {}.{}{}",
            q(db),
            q(&def.name),
            Self::trg_match(def, old)
        );
        let ran = self.trg_raw(cx, &sql).await?;
        done.affected += ran.affected;
        self.trg_fire(cx, def, db, after, &mut [], old).await
    }

    /// A multi-table `UPDATE` / `DELETE` whose targets have triggers: run
    /// here (`None`: not one, or no trigger fires).
    #[allow(clippy::too_many_arguments)]
    async fn trg_multi_hook(
        &mut self,
        cx: &mut ExecCtx<'_>,
        cat: &Arc<sprig_catalog::TenantCatalog>,
        eng: &catalog::EngineCatalog,
        vals: &Arc<validation::Validations>,
        text: &str,
        more: bool,
        out: &mut dyn ResultSink,
    ) -> Result<Option<Step>> {
        let first = text.trim_start().get(..6).map(str::to_ascii_uppercase);
        if !matches!(first.as_deref(), Some("UPDATE" | "DELETE")) {
            return Ok(None);
        }
        let lctn = self
            .tenant
            .as_deref()
            .is_some_and(crate::tenant::session::TenantSession::lower_case_table_names);
        let current = self
            .current_logical()
            .and_then(|d| cat.database(&database_key(&d, lctn)))
            .map(|r| r.db_id);
        let lex = self.lex_opts().0;
        let mysql = self.persona == sprig_proto_mysql::Persona::MySql84;
        let found = {
            let names = crate::engine::plan::CatalogNames {
                cat,
                eng,
                lower_case: lctn,
            };
            let bcx = crate::engine::plan::BindCtx {
                tenant: cat.tenant,
                names: &names,
                current,
                backslash: !lex.no_backslash_escapes,
            };
            crate::engine::multiwrite::rows_select(&bcx, text, lex, mysql)
        };
        // An error is the statement's usual path's to give.
        let Ok(Some(rs)) = found else {
            return Ok(None);
        };
        let ev = if rs.update {
            Event::Update
        } else {
            Event::Delete
        };
        let trigs: Vec<[Vec<Arc<Trigger>>; 2]> = rs
            .targets
            .iter()
            .map(|t| {
                [
                    firing(eng.triggers.values(), t.def.object, true, ev),
                    firing(eng.triggers.values(), t.def.object, false, ev),
                ]
            })
            .collect();
        let checks = rs.update && rs.targets.iter().any(|t| vals.fires(t.def.object, true));
        if !checks && trigs.iter().all(|[b, a]| b.is_empty() && a.is_empty()) {
            return Ok(None);
        }
        if let Some(t) = rs.targets.iter().find(|t| on_stack(&busy(t.def.object))) {
            let e = SprigError::standard(
                1442,
                *b"HY000",
                format!(
                    "Can't update table '{}' in stored function/trigger because it is already used by statement which invoked this stored function/trigger",
                    t.def.name
                ),
            );
            return Ok(Some(self.fail(out, &e).await?));
        }
        let dbs: Vec<String> = rs
            .targets
            .iter()
            .map(|t| {
                cat.databases
                    .get(t.db)
                    .map_or_else(String::new, |d| d.name.clone())
            })
            .collect();
        let def = Arc::clone(&rs.targets[0].def);
        let db = dbs[0].clone();
        let dml = Dml::Multi {
            rs: Box::new(rs),
            trigs,
            dbs,
        };
        let fut = self.trg_statement(cx, def, db, dml, Default::default(), more, out);
        let fut = RIGHTS.scope(std::cell::RefCell::new(Vec::new()), fut);
        Ok(Some(
            Box::pin(validation::with(Arc::clone(vals), with_stack(fut))).await?,
        ))
    }

    /// Runs a multi-table write row by row: the rows found first (the
    /// values worked out from them as they were), then each target's rows
    /// in turn, each once, the first table's first, as both servers do.
    async fn trg_multi(
        &mut self,
        cx: &mut ExecCtx<'_>,
        rs: &crate::engine::multiwrite::RowsSelect,
        trigs: &[[Vec<Arc<Trigger>>; 2]],
        dbs: &[String],
    ) -> Result<Done> {
        // Every table the statement reads is in use: a trigger writing
        // one fails (1442).
        let marks: Vec<String> = rs.tables.iter().map(|d| busy(d.object)).collect();
        let _ = STACK.try_with(|s| s.borrow_mut().extend(marks.iter().cloned()));
        let r = self.trg_multi_rows(cx, rs, trigs, dbs).await;
        let _ = STACK.try_with(|s| {
            let mut s = s.borrow_mut();
            let n = s.len().saturating_sub(marks.len());
            s.truncate(n);
        });
        r
    }

    async fn trg_multi_rows(
        &mut self,
        cx: &mut ExecCtx<'_>,
        rs: &crate::engine::multiwrite::RowsSelect,
        trigs: &[[Vec<Arc<Trigger>>; 2]],
        dbs: &[String],
    ) -> Result<Done> {
        let ran = self
            .trg_raw(cx, &format!("{} FOR UPDATE", rs.select))
            .await?;
        let env = self.engine_vars(cx).env;
        let mut done = Done::default();
        for (k, t) in rs.targets.iter().enumerate() {
            let def = &*t.def;
            let db = &dbs[k];
            let [before, after] = &trigs[k];
            let targets: Vec<usize> = t.sets.iter().map(|s| s.0).collect();
            let mut seen = std::collections::HashSet::new();
            let cols = ran.columns.get(t.cols.clone()).unwrap_or(&[]);
            for r in &ran.rows {
                let old = self.trg_row(cx, def, cols, r.get(t.cols.clone()).unwrap_or(&[]))?;
                // A row matched again is written once, as its first match
                // gave.
                if !seen.insert(Self::trg_match(def, &old)) {
                    continue;
                }
                if rs.update {
                    let mut new = old.clone();
                    let mut conds = Vec::new();
                    for &(ci, pos) in &t.sets {
                        let d = match ran.columns.get(pos) {
                            Some(cd) => wire_datum(r.get(pos).and_then(|v| v.as_deref()), cd),
                            None => Datum::Null,
                        };
                        new[ci] = store(Some(&def.columns[ci]), d, env, &mut conds)?;
                    }
                    self.trg_update_row(
                        cx,
                        def,
                        db,
                        &targets,
                        new,
                        &old,
                        [before, after],
                        &mut done,
                    )
                    .await?;
                } else {
                    self.trg_delete_row(cx, def, db, &old, [before, after], &mut done)
                        .await?;
                }
            }
        }
        Ok(done)
    }

    async fn trg_delete(
        &mut self,
        cx: &mut ExecCtx<'_>,
        def: &TableDef,
        db: &str,
        tail: &str,
        before: &[Arc<Trigger>],
        after: &[Arc<Trigger>],
    ) -> Result<Done> {
        let sql = format!("SELECT {}{tail} FOR UPDATE", Self::trg_select_list(def));
        let ran = self.trg_raw(cx, &sql).await?;
        let mut done = Done::default();
        for r in &ran.rows {
            let old = self.trg_row(cx, def, &ran.columns, r)?;
            self.trg_delete_row(cx, def, db, &old, [before, after], &mut done)
                .await?;
        }
        Ok(done)
    }
}

/// The `UPDATE` triggers of `trigs` that an update assigning the columns
/// `assigned` (positions in `def`) fires: `UPDATE OF c, ...` ones only when
/// one of their columns is assigned.
fn of_assigned(trigs: &[Arc<Trigger>], def: &TableDef, assigned: &[usize]) -> Vec<Arc<Trigger>> {
    let names: Vec<&str> = assigned
        .iter()
        .filter_map(|&i| def.columns.get(i).map(|c| c.name.as_str()))
        .collect();
    trigs
        .iter()
        .filter(|t| t.fires_on(&names))
        .cloned()
        .collect()
}

/// Triggers list by event in the servers' order: INSERT, UPDATE, DELETE.
fn event_rank(t: &Trigger) -> u8 {
    match t.when().map(|w| w.1) {
        Some(Event::Insert) => 0,
        Some(Event::Update) => 1,
        Some(Event::Delete) => 2,
        None => 3,
    }
}

impl PassSession {
    /// `SHOW CREATE TRIGGER` and `SHOW TRIGGERS` on engine databases
    /// (`None`: not an engine database's).
    #[allow(clippy::too_many_lines)] // two answers, each its columns
    pub(in crate::exec) fn engine_show_trigger(
        &self,
        info: &sprig_sql::names::show::ShowInfo,
        sql: &[u8],
    ) -> Option<Result<crate::engine::show::Answer>> {
        use crate::engine::show::{Answer, coldef};
        use sprig_proto_mysql::resultset::encode_text_row;
        use sprig_sql::names::show::{ShowKind, like_match};
        let ts = self.tenant.as_deref()?;
        let cat = self.engine_cat()?;
        let eng = catalog::of(&cat)?;
        let mysql = self.persona == sprig_proto_mysql::Persona::MySql84;
        let cs = if mysql { 45u16 } else { 224 };
        // `Created`: MariaDB's is a DATETIME(2), MySQL's a TIMESTAMP(2).
        let created_ty = if mysql { 0x07 } else { 0x0c };
        let db = if let Some(s) = info.schema.as_deref() {
            s.parse::<sprig_common::names::SchemaName>().ok()?.db()?
        } else {
            let cur = self.current_logical()?;
            cat.database(&database_key(&cur, ts.lower_case_table_names()))?
                .db_id
        };
        if !eng.dbs.contains(&db) {
            return None;
        }
        let created = |t: &Trigger| format!("{}.00", t.cx.created);
        let c = |n: &str, len: u32| coldef("", "", n, "", cs, len, 0xfd, 0x1001, 0);
        let when = |t: &Trigger| {
            t.def.as_ref().map_or(("", ""), |d| {
                (d.event.word(), if d.before { "BEFORE" } else { "AFTER" })
            })
        };
        if info.kind == ShowKind::CreateTrigger {
            let name = info.object.clone()?;
            let Some(t) = eng.triggers.get(&(db, name.clone())) else {
                return Some(Err(SprigError::standard(
                    1360,
                    *b"HY000",
                    "Trigger does not exist",
                )));
            };
            let defs = vec![
                c("Trigger", 256),
                c("sql_mode", if mysql { 468 } else { 32768 }),
                c("SQL Original Statement", 1_048_576),
                c("character_set_client", 128),
                c("collation_connection", 256),
                c("Database Collation", 256),
                coldef("", "", "Created", "", 63, 22, created_ty, 0x1080, 2),
            ];
            let cr = created(t);
            let row = encode_text_row([
                Some(t.name.as_bytes()),
                Some(t.cx.sql_mode.as_bytes()),
                Some(t.show.as_bytes()),
                Some(t.cx.charset_client.as_bytes()),
                Some(t.cx.collation_connection.as_bytes()),
                Some(t.cx.db_collation.as_bytes()),
                Some(cr.as_bytes()),
            ]);
            return Some(Ok(Answer {
                defs,
                rows: vec![row],
                no_index: false,
            }));
        }
        // `SHOW TRIGGERS`: `LIKE` matches the table's name.
        let table = |t: &Trigger| {
            eng.tables
                .get(&t.table)
                .and_then(|p| p.as_ref().as_ref().ok().map(|d| d.name.clone()))
                .unwrap_or_default()
        };
        let mut list: Vec<&Arc<Trigger>> = eng
            .triggers
            .values()
            .filter(|t| t.db == db && t.def.is_ok())
            .filter(|t| {
                info.like
                    .as_deref()
                    .is_none_or(|p| like_match(p, &table(t), false))
            })
            .collect();
        list.sort_by_key(|t| (table(t), event_rank(t), when(t).1 == "AFTER", t.cx.order));
        let defs = vec![
            c("Trigger", 256),
            c("Event", 24),
            c("Table", 256),
            coldef("", "", "Statement", "", cs, 786_420, 0xfc, 0x1010, 0),
            c("Timing", 24),
            coldef("", "", "Created", "", 63, 22, created_ty, 0x1080, 2),
            c("sql_mode", if mysql { 468 } else { 32768 }),
            c("Definer", 1536),
            c("character_set_client", 128),
            c("collation_connection", 256),
            c("Database Collation", 256),
        ];
        let rows = list
            .iter()
            .map(|t| {
                let (ev, tm) = when(t);
                let body = t
                    .def
                    .as_ref()
                    .map(|d| d.body_text.clone())
                    .unwrap_or_default();
                let definer = format!("{}@{}", t.cx.definer.0, t.cx.definer.1);
                let vals: Vec<Vec<u8>> = vec![
                    t.name.clone().into_bytes(),
                    ev.as_bytes().to_vec(),
                    table(t).into_bytes(),
                    body.into_bytes(),
                    tm.as_bytes().to_vec(),
                    created(t).into_bytes(),
                    t.cx.sql_mode.clone().into_bytes(),
                    definer.into_bytes(),
                    t.cx.charset_client.clone().into_bytes(),
                    t.cx.collation_connection.clone().into_bytes(),
                    t.cx.db_collation.clone().into_bytes(),
                ];
                encode_text_row(vals.iter().map(|v| Some(v.as_slice())))
            })
            .collect();
        let answer = Answer {
            defs,
            rows,
            no_index: !mysql,
        };
        match info.filter.clone() {
            Some(range) => {
                let cond = std::str::from_utf8(sql.get(range)?).ok()?;
                crate::engine::infoschema::filter_answer(answer, cond).map(Ok)
            }
            None => Some(Ok(answer)),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::cols_subst;

    #[test]
    fn column_references_are_replaced_but_not_names_of_other_things() {
        let r = |n: &str| n.eq_ignore_ascii_case("a").then(|| "7".to_owned());
        let (s, hit) = cols_subst("a + t.a * `a` - A", &r);
        assert_eq!(s, "7 + 7 * 7 - 7");
        assert!(hit);
        let (s, hit) = cols_subst("a(1) + @a + 'a' + \"a\" + ab + b.a.c + 1a", &r);
        assert_eq!(s, "a(1) + @a + 'a' + \"a\" + ab + b.a.c + 1a");
        assert!(!hit);
        let (s, _) = cols_subst("CONCAT('it''s', a, db.t.a)", &r);
        assert_eq!(s, "CONCAT('it''s', 7, 7)");
    }
}
