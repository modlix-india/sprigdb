//! SQL triggers firing (`crate::engine::routine::trigger`). An `INSERT`,
//! `UPDATE` or `DELETE` on an engine table with triggers for its event
//! runs row by row: the rows are found first, then for each one its
//! `BEFORE` triggers (which may change `NEW`), the one-row change, and its
//! `AFTER` triggers (which see the row as stored). The whole statement
//! runs inside a transaction or savepoint of its own, so an error in any
//! row or trigger undoes all of it.

use sqlparser::ast::{
    AssignmentTarget, Expr, FromTable, ObjectName, ObjectNamePart, SetExpr, Spanned, Statement,
    TableFactor, TableObject,
};

use std::fmt::Write as _;

use super::{
    Arc, Bytes, ColumnDef, Datum, ExecCtx, Frame, ObjectId, OkPacket, PassSession, Ran, Result,
    ResultSink, STACK, SprigError, StatusFlags, Step, catalog, database_key, literal,
    not_on_engine, store, subst, wire_datum, with_stack,
};
use crate::engine::routine::parse::Event;
use crate::engine::routine::trigger::{Trigger, firing};
use crate::engine::table::{ColDefault, TableDef};

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
    },
    /// `UPDATE`: (column, expression) pairs; the `FROM ... WHERE ...
    /// ORDER BY ... LIMIT` tail.
    Update {
        sets: Vec<(String, String)>,
        tail: String,
    },
    /// `DELETE`: the tail.
    Delete { tail: String },
}

impl Dml {
    fn event(&self) -> Event {
        match self {
            Dml::Insert { .. } => Event::Insert,
            Dml::Update { .. } => Event::Update,
            Dml::Delete { .. } => Event::Delete,
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
            if ins.on.is_some() {
                return refuse("INSERT ... ON DUPLICATE KEY UPDATE on a table with triggers");
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
                },
            )))
        }
        Statement::Update(u) => {
            if !u.table.joins.is_empty() || u.from.is_some() {
                return refuse("a multi-table UPDATE of a table with triggers");
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
                return refuse("a multi-table DELETE of a table with triggers");
            }
            let f = &froms[0];
            if !f.joins.is_empty() {
                return refuse("a multi-table DELETE of a table with triggers");
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
        if eng.triggers.is_empty() || top_is_raw() {
            return Ok(None);
        }
        let text = match logical.or(self.eng.entry.as_deref()) {
            Some(t) => String::from_utf8_lossy(t).into_owned(),
            None => String::from_utf8_lossy(sql).into_owned(),
        };
        let text = text.as_str();
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
                if !self.trg_has(eng, &name, lctn) {
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
        let ev = dml.event();
        let before = firing(eng.triggers.values(), def.object, true, ev);
        let after = firing(eng.triggers.values(), def.object, false, ev);
        // `REPLACE` also fires the `DELETE` triggers of the rows it removes.
        let del = match &dml {
            Dml::Insert {
                mode: Mode::Replace,
                ..
            } => (
                firing(eng.triggers.values(), def.object, true, Event::Delete),
                firing(eng.triggers.values(), def.object, false, Event::Delete),
            ),
            _ => (Vec::new(), Vec::new()),
        };
        if before.is_empty() && after.is_empty() && del.0.is_empty() && del.1.is_empty() {
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
        Ok(Some(Box::pin(with_stack(fut)).await?))
    }

    /// `text` changes rows of an engine table that has triggers: a
    /// prepared statement the engine can't run yet.
    pub(in crate::exec) fn engine_prepared_fires(&self, text: &[u8]) -> bool {
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
        if eng.triggers.is_empty() {
            return false;
        }
        let lctn = self
            .tenant
            .as_deref()
            .is_some_and(crate::tenant::session::TenantSession::lower_case_table_names);
        table_of_text(&text).is_some_and(|n| self.trg_has(eng, &n, lctn))
    }

    fn trg_has(&self, eng: &catalog::EngineCatalog, name: &str, lctn: bool) -> bool {
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
            } => {
                Box::pin(self.trg_insert(
                    cx,
                    &def,
                    &db,
                    (cols.as_deref(), *mode),
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

    #[allow(clippy::too_many_arguments, clippy::too_many_lines)]
    async fn trg_insert(
        &mut self,
        cx: &mut ExecCtx<'_>,
        def: &TableDef,
        db: &str,
        (cols, mode): (Option<&[String]>, Mode),
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
            if mode == Mode::Replace {
                // The rows holding any of the new row's unique keys go first.
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
                if !ors.is_empty() {
                    let sql = format!(
                        "SELECT {} FROM {}.{} WHERE {} FOR UPDATE",
                        Self::trg_select_list(def),
                        q(db),
                        q(&def.name),
                        ors.join(" OR ")
                    );
                    let found = self.trg_raw(cx, &sql).await?;
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
        // value on both servers: not done here yet.
        for (k, (_, e)) in sets.iter().enumerate() {
            let lower = e.to_ascii_lowercase();
            if sets[..k]
                .iter()
                .any(|(n, _)| lower.contains(&n.to_ascii_lowercase()))
            {
                return Err(not_on_engine(
                    "an UPDATE of a table with triggers whose SET reads a column it assigned earlier",
                ));
            }
        }
        let exprs: Vec<&str> = sets.iter().map(|(_, e)| e.as_str()).collect();
        let n = def.columns.len();
        let mut list = Self::trg_select_list(def);
        if !exprs.is_empty() {
            list.push_str(", ");
            list.push_str(&exprs.join(", "));
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
                let d = match ran.columns.get(n + k) {
                    Some(cd) => wire_datum(r.get(n + k).and_then(|v| v.as_deref()), cd),
                    None => Datum::Null,
                };
                new[i] = store(Some(&def.columns[i]), d, env, &mut conds)?;
            }
            let start: Vec<String> = def
                .columns
                .iter()
                .enumerate()
                .map(|(i, c)| literal(&new[i], Some(c)))
                .collect();
            self.trg_fire(cx, def, db, before, &mut new, &old).await?;
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
                    Self::trg_match(def, &old)
                );
                let ran = self.trg_raw(cx, &sql).await?;
                done.affected += ran.affected;
            }
            if after.is_empty() {
                continue;
            }
            let mut stored = if def.hidden {
                new.clone()
            } else {
                self.trg_reread(cx, def, db, &new).await?.unwrap_or(new)
            };
            self.trg_fire(cx, def, db, after, &mut stored, &old).await?;
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
            self.trg_fire(cx, def, db, before, &mut [], &old).await?;
            let sql = format!(
                "DELETE FROM {}.{}{}",
                q(db),
                q(&def.name),
                Self::trg_match(def, &old)
            );
            let ran = self.trg_raw(cx, &sql).await?;
            done.affected += ran.affected;
            self.trg_fire(cx, def, db, after, &mut [], &old).await?;
        }
        Ok(done)
    }
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
