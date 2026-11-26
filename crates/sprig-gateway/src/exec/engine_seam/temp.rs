//! DDL on the session's temporary tables of engine databases, answered
//! here before the catalog's DDL path (`crate::engine::temp` keeps them):
//!
//! - `CREATE [OR REPLACE] TEMPORARY TABLE [IF NOT EXISTS] t (...)` and
//!   `... LIKE s`: the engine's definition, kept by the session; no
//!   implicit commit, as on the servers.
//! - `DROP [TEMPORARY] TABLE [IF EXISTS] t, ...` of temporary tables: a
//!   plain `DROP TABLE` commits first, `DROP TEMPORARY TABLE` doesn't.
//!   With a name missing, MariaDB drops the others and MySQL none.
//! - `TRUNCATE TABLE t`: commits, then the table starts empty (a new
//!   object number; the old rows are deleted in the background).
//! - `RENAME TABLE` (MariaDB) and `ALTER TABLE t RENAME TO u`.
//!
//! - `CREATE [OR REPLACE] [TEMPORARY] TABLE [IF NOT EXISTS] t [AS] SELECT
//!   ...`: the query's rows, in a table typed from its result columns. A
//!   base table is created through the catalog's DDL path (as a plain
//!   `CREATE TABLE`) after an implicit commit, and its rows are committed
//!   with it; a failure drops it again.
//!
//! `ALTER TABLE`, `CREATE INDEX` and `DROP INDEX` of a temporary table,
//! `CREATE TABLE` with its own columns and a `SELECT`, and statements
//! mixing temporary and other tables are refused (39960).

use sprig_catalog::ddl::database_key;
use sprig_common::ids::DbId;
use sprig_common::{Result, SprigError};
use sprig_proto_mysql::StatusFlags;
use sprig_proto_mysql::packets::OkPacket;
use sprig_sql::names::{DdlKind, DdlName, DdlTarget};

use super::diag;
use crate::engine::temp::{self, TempTable};
use crate::engine::{self, catalog, define, not_on_engine};
use crate::exec::PassSession;
use crate::exec::tenant::Step;
use crate::pipeline::{ExecCtx, ResultSink};
use crate::tenant::session::LocalWarning;

fn exists(name: &str) -> SprigError {
    SprigError::standard(1050, *b"42S01", format!("Table '{name}' already exists"))
}

fn unknown(names: &[String]) -> SprigError {
    SprigError::standard(
        1051,
        *b"42S02",
        format!("Unknown table '{}'", names.join(",")),
    )
}

fn no_table(db: &str, name: &str) -> SprigError {
    SprigError::standard(
        1146,
        *b"42S02",
        format!("Table '{db}.{name}' doesn't exist"),
    )
}

/// A foreign key on a temporary table: each server's refusal.
fn no_foreign_keys(mysql: bool, db: &str, name: &str) -> SprigError {
    if mysql {
        SprigError::standard(1215, *b"HY000", "Cannot add foreign key constraint")
    } else {
        SprigError::standard(
            1005,
            *b"HY000",
            format!(
                "Can't create table `{db}`.`{name}` (errno: 150 \"Foreign key constraint is incorrectly formed\")"
            ),
        )
    }
}

/// What a DDL statement does to the session's temporary tables.
enum Act {
    /// Not about them: the catalog's DDL path runs it.
    Pass,
    /// Answered with an OK and these notes.
    Done(Vec<LocalWarning>),
    /// Answered with an OK reporting rows copied (MySQL's `ALTER TABLE`).
    Copied(u64),
    /// Answered with an OK reporting rows inserted and how many
    /// conditions they raised (the diagnostics area holds them).
    Filled(u64, usize, bool),
}

impl PassSession {
    /// DDL statement `target` (client text `text`) on the session's
    /// temporary tables of engine databases, answered here: `None` when
    /// it isn't (the catalog's path runs it).
    pub(in crate::exec) async fn engine_temp_ddl(
        &mut self,
        cx: &mut ExecCtx<'_>,
        target: &DdlTarget,
        text: &[u8],
        status: StatusFlags,
        out: &mut dyn ResultSink,
    ) -> Option<Result<Step>> {
        // `CREATE` / `ALTER EVENT`: the schedule, evaluated here.
        if let Some(r) = Box::pin(self.engine_event_prepare(cx, target, text, status, out)).await {
            return Some(r);
        }
        // Partition DDL: the rows it drops or must fit.
        if let Some(r) = Box::pin(self.engine_partition_ddl(cx, target, text, status, out)).await {
            return Some(r);
        }
        // Plain indexes added to an engine table: built online.
        if let Some(r) = Box::pin(self.engine_online_index(cx, target, text, status, out)).await {
            return Some(r);
        }
        let ours = match target {
            DdlTarget::CreateObject {
                kind: DdlKind::Table,
                temporary: true,
                ..
            }
            | DdlTarget::CreateObject {
                kind: DdlKind::Table,
                as_select: true,
                ..
            }
            | DdlTarget::DropObjects {
                kind: DdlKind::Table,
                temporary: true,
                ..
            } => true,
            _ => !self.eng.temps.is_empty(),
        };
        if !ours {
            return None;
        }
        // A plain `DROP TABLE`, `TRUNCATE` and a rename of a temporary
        // table commit first (the servers' rule); the status says so.
        let mut status = status;
        let r = match Box::pin(self.temp_act(cx, target, text)).await {
            Ok((Act::Pass, _)) => return None,
            Ok((act, committed)) => {
                if committed {
                    status.remove(StatusFlags::IN_TRANS | StatusFlags::IN_TRANS_READONLY);
                }
                let mut ok = OkPacket::new(status);
                let notes = match act {
                    Act::Copied(n) => {
                        ok.affected_rows = n;
                        ok.info =
                            bytes::Bytes::from(format!("Records: {n}  Duplicates: 0  Warnings: 0"));
                        Vec::new()
                    }
                    Act::Filled(n, w, no_index) => {
                        if no_index {
                            ok.status |= StatusFlags::NO_INDEX_USED;
                        }
                        ok.affected_rows = n;
                        ok.info = bytes::Bytes::from(format!(
                            "Records: {n}  Duplicates: 0  Warnings: {w}"
                        ));
                        ok.warnings = u16::try_from(w).unwrap_or(u16::MAX);
                        return Some(match out.ok(ok).await {
                            Ok(()) => Ok(Step {
                                errored: false,
                                status: Some(status),
                            }),
                            Err(e) => Err(e),
                        });
                    }
                    Act::Done(notes) => notes,
                    Act::Pass => return None,
                };
                ok.warnings = u16::try_from(notes.len()).unwrap_or(u16::MAX);
                self.diag_set(notes, 0);
                match out.ok(ok).await {
                    Ok(()) => Ok(Step {
                        errored: false,
                        status: Some(status),
                    }),
                    Err(e) => Err(e),
                }
            }
            Err(e) => self.fail(out, &e).await,
        };
        Some(r)
    }

    /// The engine database `n` names, with its logical name.
    fn temp_db(&self, n: &DdlName) -> Option<(DbId, String)> {
        let ts = self.tenant.as_deref()?;
        let cat = ts.catalog()?;
        let eng = catalog::of(cat)?;
        let d = n.db.as_deref()?;
        let row = cat.database(&database_key(d, ts.lower_case_table_names()))?;
        eng.dbs
            .contains(&row.db_id)
            .then(|| (row.db_id, d.to_owned()))
    }

    /// The position of temporary table `n`, if it is one.
    fn temp_find(&self, n: &DdlName) -> Option<usize> {
        let (db, _) = self.temp_db(n)?;
        let lctn = self.tenant.as_deref()?.lower_case_table_names();
        self.eng.temps.find(db, &n.name, lctn)
    }

    fn temp_key(n: &DdlName) -> (Option<String>, String) {
        (n.db.clone(), n.name.to_ascii_lowercase())
    }

    /// MySQL: notes that the open transaction (or, with `autocommit = 0`,
    /// the one the session is in) created or dropped a temporary table.
    fn temp_note_txn(&mut self, bit: u8) {
        if self.persona != sprig_proto_mysql::Persona::MySql84 {
            return;
        }
        let autocommit_off = self.eng.vars.as_ref().is_some_and(|v| v.autocommit_off);
        if self.eng.txn.is_open() || autocommit_off {
            self.eng.temp_txn |= bit;
        }
    }

    /// Forgets the session's plans made before a change of its tables.
    fn temp_changed(&mut self) {
        self.eng.text_plans = crate::engine::plancache::TextPlans::default();
    }

    /// Drops the temporary table at `i` (named `n`): its rows are deleted
    /// in the background.
    fn temp_drop_at(&mut self, i: usize, n: &DdlName) {
        let tenant = self.tenant.as_deref().map_or(0, |t| t.tenant.0);
        if let Some(t) = self.eng.temps.remove(i) {
            temp::discard(tenant, vec![t.object]);
        }
        if let Some(ts) = self.tenant.as_deref_mut() {
            ts.temp.remove(&Self::temp_key(n));
        }
        self.temp_changed();
    }

    /// What `target` does; with whether it committed the session's
    /// transaction.
    #[allow(clippy::too_many_lines)] // one arm per statement
    async fn temp_act(
        &mut self,
        cx: &mut ExecCtx<'_>,
        target: &DdlTarget,
        text: &[u8],
    ) -> Result<(Act, bool)> {
        let mysql = self.persona == sprig_proto_mysql::Persona::MySql84;
        match target {
            // `CREATE TABLE ... SELECT` of a base table: commits first, as
            // all DDL does, then the query, the table and its rows.
            DdlTarget::CreateObject {
                kind: DdlKind::Table,
                temporary: false,
                as_select: true,
                name,
                if_not_exists,
                or_replace,
                ..
            } => {
                let Some((db, db_name)) = self.temp_db(name) else {
                    return Ok((Act::Pass, false));
                };
                self.engine_implicit_commit().await?;
                let present = self.tenant.as_deref().is_some_and(|ts| {
                    let lctn = ts.lower_case_table_names();
                    ts.catalog()
                        .and_then(|c| catalog::of(c))
                        .is_some_and(|eng| eng.table(db, &name.name, lctn).is_some())
                });
                if present && !*or_replace {
                    if *if_not_exists {
                        let e = exists(&name.name);
                        return Ok((
                            Act::Done(vec![diag::warning("Note", 1050, e.message.clone())]),
                            true,
                        ));
                    }
                    return Err(exists(&name.name));
                }
                let act = Box::pin(self.temp_create_select(
                    cx,
                    db,
                    &db_name,
                    name,
                    text,
                    mysql,
                    Some(*or_replace),
                ))
                .await?;
                Ok((act, true))
            }
            DdlTarget::CreateObject {
                kind: DdlKind::Table,
                temporary: true,
                name,
                if_not_exists,
                or_replace,
                like,
                as_select,
                ..
            } => {
                let Some((db, db_name)) = self.temp_db(name) else {
                    return Ok((Act::Pass, false));
                };
                if let Some(i) = self.temp_find(name) {
                    if *if_not_exists {
                        let e = exists(&name.name);
                        return Ok((
                            Act::Done(vec![diag::warning("Note", 1050, e.message.clone())]),
                            false,
                        ));
                    }
                    if !*or_replace {
                        return Err(exists(&name.name));
                    }
                    self.temp_drop_at(i, name);
                }
                if *as_select {
                    let act = Box::pin(
                        self.temp_create_select(cx, db, &db_name, name, text, mysql, None),
                    )
                    .await?;
                    return Ok((act, false));
                }
                let t = self
                    .temp_define(db, &db_name, name, like.as_ref(), text, mysql)
                    .await?;
                let tenant = self.tenant.as_deref().map_or(0, |t| t.tenant.0);
                self.eng.temps.add(tenant, t);
                self.temp_note_txn(1);
                if let Some(ts) = self.tenant.as_deref_mut() {
                    ts.temp.insert(Self::temp_key(name));
                }
                self.temp_changed();
                Ok((Act::Done(Vec::new()), false))
            }
            DdlTarget::DropObjects {
                kind: DdlKind::Table,
                names,
                if_exists,
                temporary,
            } => {
                let found: Vec<Option<usize>> = names.iter().map(|n| self.temp_find(n)).collect();
                if !*temporary {
                    if found.iter().all(Option::is_none) {
                        return Ok((Act::Pass, false));
                    }
                    if found.iter().any(Option::is_none) {
                        return Err(not_on_engine(
                            "DROP TABLE of temporary and other tables in one statement",
                        ));
                    }
                } else if names.iter().any(|n| self.temp_db(n).is_none()) {
                    return Ok((Act::Pass, false));
                }
                let missing: Vec<String> = names
                    .iter()
                    .zip(&found)
                    .filter(|(_, f)| f.is_none())
                    .map(|(n, _)| format!("{}.{}", n.db.as_deref().unwrap_or(""), n.name))
                    .collect();
                // MySQL drops none of them when one is missing.
                if !missing.is_empty() && !*if_exists && mysql {
                    return Err(unknown(&missing));
                }
                let committed = if *temporary {
                    false
                } else {
                    self.engine_implicit_commit().await?;
                    true
                };
                for n in names {
                    if let Some(i) = self.temp_find(n) {
                        self.temp_drop_at(i, n);
                        if *temporary {
                            self.temp_note_txn(2);
                        }
                    }
                }
                if missing.is_empty() {
                    return Ok((Act::Done(Vec::new()), committed));
                }
                if !*if_exists {
                    return Err(unknown(&missing));
                }
                let notes = missing
                    .iter()
                    .map(|m| diag::warning("Note", 1051, unknown(std::slice::from_ref(m)).message))
                    .collect();
                Ok((Act::Done(notes), committed))
            }
            DdlTarget::Truncate { name } => {
                let Some(i) = self.temp_find(name) else {
                    return Ok((Act::Pass, false));
                };
                self.engine_implicit_commit().await?;
                let Some(old) = self.eng.temps.get(i).cloned() else {
                    return Ok((Act::Pass, false));
                };
                let ts = self.tenant.as_deref().map_or(0, |t| t.tenant.0);
                let eng = engine::engine()?;
                let object = temp::new_object(eng, ts).await?;
                let fresh = TempTable::new(old.db, &old.name, object, old.text.to_string())?;
                self.eng.temps.replace(i, fresh);
                temp::discard(ts, vec![old.object]);
                self.temp_changed();
                Ok((Act::Done(Vec::new()), true))
            }
            DdlTarget::RenameTables { pairs, .. } => {
                let found: Vec<Option<usize>> =
                    pairs.iter().map(|(a, _)| self.temp_find(a)).collect();
                if found.iter().all(Option::is_none) {
                    return Ok((Act::Pass, false));
                }
                // MySQL's `RENAME TABLE` doesn't see temporary tables (its
                // `ALTER TABLE ... RENAME` does).
                let rename_stmt = String::from_utf8_lossy(text)
                    .trim_start()
                    .get(..6)
                    .is_some_and(|w| w.eq_ignore_ascii_case("RENAME"));
                if mysql && rename_stmt {
                    return Ok((Act::Pass, false));
                }
                if found.iter().any(Option::is_none) {
                    return Err(not_on_engine(
                        "RENAME TABLE of temporary and other tables in one statement",
                    ));
                }
                self.engine_implicit_commit().await?;
                let mut copied = 0;
                let lctn = self
                    .tenant
                    .as_deref()
                    .is_some_and(crate::tenant::session::TenantSession::lower_case_table_names);
                for (a, b) in pairs {
                    let Some(i) = self.temp_find(a) else {
                        return Err(no_table(a.db.as_deref().unwrap_or(""), &a.name));
                    };
                    let Some((db, _)) = self.temp_db(b) else {
                        return Err(not_on_engine("RENAME TABLE out of the engine"));
                    };
                    if self.eng.temps.find(db, &b.name, lctn).is_some() {
                        return Err(exists(&b.name));
                    }
                    let Some(t) = self.eng.temps.get(i) else {
                        continue;
                    };
                    if mysql {
                        let tenant = self.tenant.as_deref().map_or(0, |t| t.tenant.0);
                        copied += temp::row_count(engine::engine()?, tenant, t.object)?;
                    }
                    let moved = t.renamed(db, &b.name)?;
                    self.eng.temps.replace(i, moved);
                    if let Some(ts) = self.tenant.as_deref_mut() {
                        ts.temp.remove(&Self::temp_key(a));
                        ts.temp.insert(Self::temp_key(b));
                    }
                }
                self.temp_changed();
                if mysql {
                    return Ok((Act::Copied(copied), true));
                }
                Ok((Act::Done(Vec::new()), true))
            }
            DdlTarget::AlterObject {
                kind: DdlKind::Table,
                name,
                ..
            } if self.temp_find(name).is_some() => Err(not_on_engine(
                "ALTER TABLE, CREATE INDEX or DROP INDEX of a temporary table",
            )),
            _ => Ok((Act::Pass, false)),
        }
    }

    /// `CREATE [TEMPORARY] TABLE name [AS] SELECT ...`: the query runs, the
    /// table gets a column per result column (typed as the server types
    /// it), and the rows go in. A failed insert drops the table again.
    /// `base`: a base table (with `OR REPLACE` or not), created through
    /// the catalog's DDL path as a plain `CREATE TABLE`, its rows
    /// committed with it.
    #[allow(clippy::too_many_lines, clippy::too_many_arguments)] // the query, the definition, the rows
    async fn temp_create_select(
        &mut self,
        cx: &mut ExecCtx<'_>,
        db: DbId,
        db_name: &str,
        name: &DdlName,
        text: &[u8],
        mysql: bool,
        base: Option<bool>,
    ) -> Result<Act> {
        use sqlparser::ast::Statement;
        use sqlparser::parser::Parser;
        let sql = String::from_utf8_lossy(text).into_owned();
        let parsed = Parser::parse_sql(&sprig_plan::dialect::SprigMySql, &sql)
            .ok()
            .and_then(|mut v| (v.len() == 1).then(|| v.remove(0)));
        let Some(Statement::CreateTable(ct)) = parsed else {
            return Err(not_on_engine(
                "this form of CREATE TEMPORARY TABLE ... SELECT",
            ));
        };
        let Some(query) = ct.query.clone() else {
            return Err(not_on_engine(
                "this form of CREATE TEMPORARY TABLE ... SELECT",
            ));
        };
        if !ct.columns.is_empty() || !ct.constraints.is_empty() {
            return Err(not_on_engine(
                "CREATE TABLE with its own columns and a SELECT",
            ));
        }
        let (cat, current) = self
            .engine_scope()
            .ok_or_else(|| not_on_engine("CREATE TEMPORARY TABLE ... SELECT"))?;
        let tenant = cat.tenant.0;
        let lctn = self
            .tenant
            .as_deref()
            .is_some_and(crate::tenant::session::TenantSession::lower_case_table_names);
        let backslash = !self.lex_opts().0.no_backslash_escapes;
        let g = {
            let eng = catalog::of(&cat).ok_or_else(|| not_on_engine("no engine catalog"))?;
            let names = crate::engine::plan::CatalogNames {
                cat: &cat,
                eng,
                lower_case: lctn,
            };
            let bcx = crate::engine::plan::BindCtx {
                tenant: cat.tenant,
                names: &names,
                current,
                backslash,
            };
            crate::engine::general::bind_view(&bcx, &Statement::Query(query), None)?
                .ok_or_else(|| not_on_engine("CREATE TEMPORARY TABLE ... SELECT"))?
        };
        self.general_checks(&cat, &g)?;
        let sv = self.engine_vars(cx);
        let rows = self
            .general_run(
                tenant,
                &g,
                &[],
                &sv,
                std::sync::Arc::clone(cx.session.handle()),
            )
            .await?;
        let cols: Vec<String> = rows
            .columns
            .iter()
            .map(|c| temp::column_text(&c.name, &c.ty, mysql))
            .collect();
        let create = format!(
            "CREATE TABLE `{}` ({})",
            name.name.replace('`', "``"),
            cols.join(", ")
        );
        if let Some(or_replace) = base {
            let create = format!(
                "CREATE {}TABLE `{}`.{}",
                if or_replace { "OR REPLACE " } else { "" },
                db_name.replace('`', "``"),
                &create["CREATE TABLE ".len()..]
            );
            self.run_checked(cx, &create, false).await?;
        } else {
            let t = self
                .temp_define(db, db_name, name, None, create.as_bytes(), mysql)
                .await?;
            self.eng.temps.add(tenant, t);
            if let Some(ts) = self.tenant.as_deref_mut() {
                ts.temp.insert(Self::temp_key(name));
            }
            self.temp_changed();
            self.temp_note_txn(1);
        }
        // The rows, inserted as an `INSERT ... SELECT` inserts them.
        let filled = async {
            let cat = self
                .engine_cat()
                .ok_or_else(|| not_on_engine("CREATE TEMPORARY TABLE ... SELECT"))?;
            let eng = catalog::of(&cat).ok_or_else(|| not_on_engine("no engine catalog"))?;
            let names = crate::engine::plan::CatalogNames {
                cat: &cat,
                eng,
                lower_case: lctn,
            };
            let bcx = crate::engine::plan::BindCtx {
                tenant: cat.tenant,
                names: &names,
                current,
                backslash,
            };
            let ins_sql = format!(
                "INSERT INTO `{}`.`{}` SELECT 1",
                db_name.replace('`', "``"),
                name.name.replace('`', "``")
            );
            let Some(Statement::Insert(ins)) =
                Parser::parse_sql(&sprig_plan::dialect::SprigMySql, &ins_sql)
                    .ok()
                    .and_then(|mut v| v.pop())
            else {
                return Err(SprigError::internal("engine: the copy's INSERT"));
            };
            crate::engine::plan::bind_insert_rows(&bcx, &ins, &rows.rows, rows.columns.len())?
                .ok_or_else(|| not_on_engine("CREATE TEMPORARY TABLE ... SELECT"))
        }
        .await;
        let mut r = match filled {
            Ok(plan) => self.engine_run(tenant, &plan, &[], false, &sv).await,
            Err(e) => Err(e),
        };
        if base.is_some()
            && r.is_ok()
            && let Err(e) = self.engine_implicit_commit().await
        {
            r = Err(e);
        }
        match r {
            Ok(o) => {
                self.engine_diag(&o.conds);
                Ok(Act::Filled(o.affected, o.conds.len(), rows.no_index))
            }
            Err(e) => {
                if base.is_some() {
                    let drop = format!(
                        "DROP TABLE IF EXISTS `{}`.`{}`",
                        db_name.replace('`', "``"),
                        name.name.replace('`', "``")
                    );
                    let _ = self.run_internal(cx, &drop, false).await;
                } else if let Some(i) = self.temp_find(name) {
                    self.temp_drop_at(i, name);
                }
                Err(e)
            }
        }
    }

    /// The definition of new temporary table `name` in `db`: from the
    /// statement's column list, or from table `like`'s definition.
    async fn temp_define(
        &self,
        db: DbId,
        db_name: &str,
        name: &DdlName,
        like: Option<&DdlName>,
        text: &[u8],
        mysql: bool,
    ) -> Result<TempTable> {
        let ts = self
            .tenant
            .as_deref()
            .ok_or_else(|| SprigError::internal("no tenant"))?;
        let lctn = ts.lower_case_table_names();
        let base = ts
            .catalog()
            .ok_or_else(|| SprigError::internal("no catalog"))?;
        let cat = self.eng.temps.overlay(base);
        let eng_cat = catalog::of(&cat).ok_or_else(|| SprigError::internal("no engine catalog"))?;
        let text = String::from_utf8_lossy(text);
        let def = if let Some(src) = like {
            let src_db = src
                .db
                .as_deref()
                .and_then(|d| cat.database(&database_key(d, lctn)))
                .map(|d| d.db_id);
            let stored = src_db
                .and_then(|d| eng_cat.table(d, &src.name, lctn))
                .and_then(|p| p.as_ref().as_ref().ok().map(|td| td.object))
                .and_then(|o| eng_cat.texts.get(&o));
            let Some(stored) = stored else {
                return Err(no_table(src.db.as_deref().unwrap_or(""), &src.name));
            };
            define::like(&define::parse_stored(stored)?, &name.name)
        } else {
            let db_cs = cat
                .databases
                .get(db)
                .map(|d| (d.default_charset.clone(), d.default_collation.clone()))
                .filter(|(cs, co)| !cs.is_empty() && !co.is_empty())
                .unwrap_or_else(|| ("utf8mb4".into(), "utf8mb4_uca1400_ai_ci".into()));
            if let Err(why) = crate::engine::table::check_create(&text) {
                return Err(not_on_engine(&why));
            }
            match define::create(&text, &define::Ctx { mysql, db_cs })? {
                define::Created::Def(d) if d.partition.is_some() => {
                    return Err(if mysql {
                        SprigError::standard(
                            1562,
                            *b"HY000",
                            "Cannot create temporary table with partitions",
                        )
                    } else {
                        SprigError::standard(
                            1506,
                            *b"HY000",
                            "Partitioned tables do not support CREATE TEMPORARY TABLE",
                        )
                    });
                }
                define::Created::Def(d) => {
                    let mut d = *d;
                    name.name.clone_into(&mut d.name);
                    d
                }
                define::Created::Like(..) => {
                    return Err(not_on_engine("this form of CREATE TEMPORARY TABLE"));
                }
            }
        };
        if !def.fks.is_empty() {
            return Err(no_foreign_keys(mysql, db_name, &name.name));
        }
        let rendered = define::render(&def);
        // The engine must be able to serve it before a number is taken.
        TempTable::new(db, &name.name, temp::TEMP_BASE, rendered.clone())?;
        let object = temp::new_object(engine::engine()?, ts.tenant.0).await?;
        TempTable::new(db, &name.name, object, rendered)
    }
}
