//! The engine's part of a catalog DDL statement.
//!
//! The catalog's DDL path runs a statement in three phases: it marks its
//! targets, makes the physical change through a [`DdlExecutor`], and
//! publishes the definition the executor then shows. Every database is an
//! engine database, and the engine is the only physical side: [`EngineDdl`]
//! is the executor, and nothing about a table runs on MariaDB.
//!
//! ```text
//! CREATE TABLE [LIKE]: the engine works out the definition (`define`)
//!   and checks it can serve it; the catalog publishes that text
//! ALTER TABLE, CREATE INDEX, DROP INDEX: the engine works out the new
//!   definition from the table's current one, and stages the change
//!   (`Engine::alter_stage`: rows converted, unique keys checked); a
//!   failure is the statement's error, and nothing changed
//! RENAME TABLE: the definition under its new name (rows stay where they
//!   are: they are keyed by the table's object number)
//! TRUNCATE: the engine's rows
//! DROP TABLE, DROP DATABASE: the tables are noted
//! CREATE / ALTER / DROP DATABASE: the database's empty MariaDB schema
//!   (the session's backend connection uses it as its default database)
//! ```
//!
//! and once the catalog has answered ([`Hook::done`]): a published change
//! is applied (or, for a drop, the rows are deleted); one that wasn't
//! published is abandoned.

use std::sync::{Arc, Mutex, PoisonError};

use sprig_catalog::TenantCatalog;
use sprig_catalog::ddl::{
    DdlExecutor, Effect, PhysicalDdl, PhysicalObject, SchemaInfo, ShardTarget,
};
use sprig_catalog::store::{DatabaseRow, ObjectKind};
use sprig_common::ids::{ClusterId, DbId, ObjectId, TenantId};
use sprig_common::names::SchemaName;
use sprig_common::{Result, SprigError};

use super::assign::Condition;
use super::ddl::{Session, Staged};
use super::define::{self, Created, Ctx};
use super::table::{self, TableDef};
use super::{Engine, alter, catalog, not_on_engine, object_of};

/// What the engine reports for a finished statement.
#[derive(Debug, Default)]
pub(crate) struct Report {
    /// The rows the servers report as copied (`Records: n`), when they
    /// report any.
    pub rows: Option<u64>,
    /// The notes and warnings converting rows raised.
    pub conds: Vec<Condition>,
}

/// A routine a statement produced: (schema, function, name, its catalog
/// text; `None` for one it removed).
type ProducedRoutine = (SchemaName, ObjectKind, String, Option<String>);

/// One statement's engine state across the catalog's phases.
pub(crate) struct Hook {
    eng: &'static Engine,
    tenant: TenantId,
    /// The session's catalog snapshot: the definitions before the
    /// statement.
    cat: Arc<TenantCatalog>,
    /// Table names compare in lower case.
    lctn: bool,
    sess: Session,
    /// The statement creates a table that didn't exist (not `CREATE OR
    /// REPLACE`): one the engine can't serve is dropped again.
    creates: bool,
    staged: Mutex<Vec<Staged>>,
    dropped: Mutex<Vec<u32>>,
    /// The definitions this statement produced, latest last: (schema,
    /// table, its text; `None` for a table it removed).
    produced: Mutex<Vec<(SchemaName, String, Option<String>)>>,
    /// Sequences whose stored state the statement sets once its change is
    /// published (`None`: back to the start).
    seq_resets: Mutex<Vec<(ObjectId, Option<super::sequence::Record>)>>,
    /// The gateway's services: the newest catalog, for the check that a
    /// change starts from the table's current definition.
    svc: Option<Arc<crate::tenant::TenantServices>>,
    /// A routine statement's session context.
    routine: Option<super::routine::ddl::Session>,
    /// The routines this statement produced: (schema, function, name, its
    /// catalog text; `None` for one it removed).
    routines: Mutex<Vec<ProducedRoutine>>,
}

impl std::fmt::Debug for Hook {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Hook")
            .field("tenant", &self.tenant)
            .finish_non_exhaustive()
    }
}

impl Hook {
    /// The hook of a statement of `tenant`, whose session reads `cat`.
    pub(crate) fn new(
        eng: &'static Engine,
        tenant: TenantId,
        cat: Arc<TenantCatalog>,
        lctn: bool,
        sess: Session,
        creates: bool,
    ) -> Self {
        Self {
            eng,
            tenant,
            cat,
            lctn,
            sess,
            creates,
            staged: Mutex::new(Vec::new()),
            dropped: Mutex::new(Vec::new()),
            produced: Mutex::new(Vec::new()),
            seq_resets: Mutex::new(Vec::new()),
            svc: None,
            routine: None,
            routines: Mutex::new(Vec::new()),
        }
    }

    /// The same hook, for a routine statement of a session in `cx`.
    #[must_use]
    pub(crate) fn with_routine(mut self, cx: super::routine::ddl::Session) -> Self {
        self.routine = Some(cx);
        self
    }

    /// The catalog text of routine `name` (`function`) of `schema`, as
    /// this statement left it.
    fn routine_text(&self, schema: &SchemaName, kind: ObjectKind, name: &str) -> Option<String> {
        let trigger = kind == ObjectKind::Trigger;
        let produced = self.routines.lock().unwrap_or_else(PoisonError::into_inner);
        if let Some((_, _, _, t)) = produced.iter().rev().find(|(s, k, n, _)| {
            s == schema && *k == kind && (n == name || (!trigger && n.eq_ignore_ascii_case(name)))
        }) {
            return t.clone();
        }
        drop(produced);
        let db = schema.db()?;
        if trigger {
            let t = catalog::of(&self.cat)?
                .triggers
                .get(&(db, name.to_owned()))?;
            return Some(super::routine::stored(&t.cx, &t.show));
        }
        let function = kind == ObjectKind::Function;
        let kind = if function {
            super::routine::Kind::Function
        } else {
            super::routine::Kind::Procedure
        };
        let r = catalog::of(&self.cat)?.routine(db, kind, name)?;
        Some(super::routine::stored(&r.cx, &r.show))
    }

    fn produce_routine(&self, schema: SchemaName, kind: ObjectKind, name: &str, t: Option<String>) {
        self.routines
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .push((schema, kind, name.to_owned(), t));
    }

    /// The catalog text of `CREATE TRIGGER` `sql` in database `db`.
    fn trigger_text(&self, sql: &str, db: Option<DbId>, name: &str) -> Result<String> {
        let rs = self.routine.clone().unwrap_or_default();
        let text = rs.entry.as_deref().unwrap_or(sql);
        let (tdb, table) = super::routine::trigger::table_of(text)
            .or_else(|| super::routine::trigger::table_of(sql))
            .ok_or_else(|| SprigError::internal("CREATE TRIGGER without its table"))?;
        let cat = catalog::of(&self.cat);
        let db = match tdb {
            Some(d) => cat.and_then(|c| {
                c.dbs
                    .iter()
                    .copied()
                    .find(|x| self.db_name(*x).eq_ignore_ascii_case(&d))
            }),
            None => db,
        };
        let def = db
            .and_then(|d| cat?.table(d, &table, self.lctn))
            .and_then(|p| p.as_ref().as_ref().ok().cloned());
        let Some(def) = def else {
            return Err(SprigError::standard(
                1146,
                *b"42S02",
                format!("Table '{}.{table}' doesn't exist", self.sess.db),
            ));
        };
        let cols: Vec<String> = def.columns.iter().map(|c| c.name.clone()).collect();
        let others: Vec<(String, bool, super::routine::parse::Event, i64)> = cat
            .map(|c| {
                c.triggers
                    .values()
                    .filter(|t| t.table == def.object && t.name != name)
                    .filter_map(|t| t.when().map(|(b, e)| (t.name.clone(), b, e, t.cx.order)))
                    .collect()
            })
            .unwrap_or_default();
        let cx = db.map_or_else(|| self.ctx(DbId(0)), |d| self.ctx(d));
        super::routine::trigger::create(
            sql,
            &rs,
            &self.sess.account,
            self.sess.env.mysql,
            &cx.db_cs,
            &cols,
            &others,
        )
    }

    /// The same hook, able to read the newest catalog.
    #[must_use]
    pub(crate) fn with_services(mut self, svc: Arc<crate::tenant::TenantServices>) -> Self {
        self.svc = Some(svc);
        self
    }

    /// The executor the catalog's DDL path runs the statement through.
    pub(crate) fn over<'a>(&'a self, inner: &'a dyn DdlExecutor) -> EngineDdl<'a> {
        EngineDdl { hook: self, inner }
    }

    /// The engine table `name` of the database `schema` belongs to, as
    /// the session's catalog has it.
    fn table(&self, schema: &SchemaName, name: &str) -> Option<Arc<TableDef>> {
        let db = schema.db()?;
        let eng = catalog::of(&self.cat)?;
        eng.table(db, name, self.lctn)?
            .as_ref()
            .as_ref()
            .ok()
            .cloned()
    }

    /// The foreign keys of other tables that reference `def`, by the
    /// child's object id (oldest table first), unless `foreign_key_checks
    /// = 0`.
    fn children_of(&self, def: &TableDef) -> Vec<(Arc<TableDef>, usize)> {
        if self.sess.env.no_fk_checks {
            return Vec::new();
        }
        let Some(eng) = catalog::of(&self.cat) else {
            return Vec::new();
        };
        let names = super::plan::CatalogNames {
            cat: &self.cat,
            eng,
            lower_case: self.lctn,
        };
        let mut v: Vec<(Arc<TableDef>, usize)> =
            super::plan::Names::referencing(&names, def.db, &def.name)
                .into_iter()
                .filter(|(c, _)| c.object != def.object)
                .collect();
        v.sort_by_key(|(c, i)| (c.object, *i));
        v
    }

    /// The catalog's names as the binder reads them.
    fn names(&self) -> Option<super::plan::CatalogNames<'_>> {
        Some(super::plan::CatalogNames {
            cat: &self.cat,
            eng: catalog::of(&self.cat)?,
            lower_case: self.lctn,
        })
    }

    /// The database of the parent of foreign key `f` of a table of `db`.
    fn parent_db(&self, db: DbId, f: &define::Fk) -> Option<DbId> {
        match &f.ref_db {
            None => Some(db),
            // The statement's text names it physically (the gateway's
            // name translation); a stored definition, by its logical name.
            Some(n) => super::plan::physical_db(self.tenant, n)
                .or_else(|| super::plan::Names::db_by_name(&self.names()?, n)),
        }
    }

    /// Whether foreign key `f` of table `child` of `db` references `child`
    /// itself.
    fn self_reference(&self, db: DbId, child: &str, f: &define::Fk) -> bool {
        self.parent_db(db, f) == Some(db)
            && if self.lctn {
                f.ref_table.eq_ignore_ascii_case(child)
            } else {
                f.ref_table == child
            }
    }

    /// The stored definition of table `name` of database `db`, as the
    /// session's catalog has it.
    fn stored_def(&self, db: DbId, name: &str) -> Option<define::Def> {
        let eng = catalog::of(&self.cat)?;
        let o = if self.lctn {
            eng.by_lower.get(&(db, name.to_lowercase()))?
        } else {
            eng.by_name.get(&(db, name.to_owned()))?
        };
        define::parse_stored(eng.texts.get(o)?).ok()
    }

    /// Foreign keys `fks` of table `child` of `db` (being created, or
    /// altered to `child`) checked against their parents.
    ///
    /// # Errors
    /// MariaDB's 1005 errno 150; MySQL's 1824, 1830, 3734, 3780, 6125.
    fn check_fks(&self, db: DbId, child: &define::Def, fks: &[&define::Fk]) -> Result<()> {
        if fks.is_empty() {
            return Ok(());
        }
        let db_name = self.db_name(db);
        let cx = define::fkcheck::For {
            mysql: self.sess.env.mysql,
            checks: !self.sess.env.no_fk_checks,
            db: &db_name,
        };
        define::fkcheck::set_null(cx, child, fks)?;
        for f in fks {
            let stored;
            let parent = if self.self_reference(db, &child.name, f) {
                Some(child)
            } else {
                stored = self
                    .parent_db(db, f)
                    .and_then(|pdb| self.stored_def(pdb, &f.ref_table));
                stored.as_ref()
            };
            define::fkcheck::check(cx, child, f, parent)?;
        }
        Ok(())
    }

    /// Column changes of `ALTER TABLE` (`a`, from `old` to `new`, of table
    /// `d0`) that a foreign key depends on. MariaDB refuses any change of
    /// a key column's type (1832 on the child, 1833 on the parent), and a
    /// rename together with a type change (1846); MySQL refuses a type
    /// the other side doesn't match (3780) and any change it can't make
    /// in place (1832, 1833).
    #[allow(clippy::too_many_lines)] // the child's side, then the parents'
    fn fk_column_changes(
        &self,
        a: &alter::Alter,
        old: &define::Def,
        new: &define::Def,
        d0: &TableDef,
    ) -> Result<()> {
        let mysql = self.sess.env.mysql;
        let db = self.db_name(d0.db);
        let err = |code: u16, msg: String| SprigError::standard(code, *b"HY000", msg);
        let renamed_too = || {
            SprigError::standard(
                1846,
                *b"0A000",
                "ALGORITHM=COPY is not supported. Reason: Columns participating in a foreign key are renamed. Try ALGORITHM=INPLACE",
            )
        };
        let incompatible = |c: &str, r: &str, fk: &str| {
            err(
                3780,
                format!(
                    "Referencing column '{c}' and referenced column '{r}' in foreign key constraint '{fk}' are incompatible."
                ),
            )
        };
        for spec in &a.specs {
            let (oname, nname) = match spec {
                alter::Spec::Modify(n) => (n.as_str(), n.as_str()),
                alter::Spec::Rename { old, new } => (old.as_str(), new.as_str()),
                _ => continue,
            };
            let (Some(oi), Some(ni)) = (old.col(oname), new.col(nname)) else {
                continue;
            };
            let (ot, nt) = (&old.cols[oi].ty, &new.cols[ni].ty);
            if ot == nt {
                continue;
            }
            let oname = old.cols[oi].name.as_str();
            let renamed = !oname.eq_ignore_ascii_case(nname);
            let in_place = define::fkcheck::in_place(ot, nt, mysql);
            // The table's own foreign keys on the column.
            for f in &old.fks {
                let Some(pos) = f.cols.iter().position(|c| c.eq_ignore_ascii_case(oname)) else {
                    continue;
                };
                if !mysql {
                    if renamed {
                        return Err(renamed_too());
                    }
                    let fk = if in_place {
                        format!("{db}/{}", f.name)
                    } else {
                        f.name.clone()
                    };
                    return Err(err(
                        1832,
                        format!(
                            "Cannot change column '{oname}': used in a foreign key constraint '{fk}'"
                        ),
                    ));
                }
                let r = &f.ref_cols[pos];
                let pty = if self.self_reference(d0.db, &old.name, f) {
                    new.col(r).map(|i| new.cols[i].ty.clone())
                } else {
                    self.parent_db(d0.db, f)
                        .and_then(|pdb| self.stored_def(pdb, &f.ref_table))
                        .and_then(|p| p.col(r).map(|i| p.cols[i].ty.clone()))
                };
                if let Some(pty) = pty
                    && !define::fkcheck::compatible(nt, &pty)
                {
                    return Err(incompatible(new.cols[ni].name.as_str(), r, &f.name));
                }
                if !in_place {
                    return Err(err(
                        1832,
                        format!(
                            "Cannot change column '{oname}': used in a foreign key constraint '{}'",
                            f.name
                        ),
                    ));
                }
            }
            // Other tables' foreign keys on the column.
            for (child, i) in self.children_of(d0) {
                let cf = &child.fks[i];
                let Some(pos) = cf
                    .ref_cols
                    .iter()
                    .position(|c| c.eq_ignore_ascii_case(oname))
                else {
                    continue;
                };
                let cdb = self.db_name(child.db);
                if !mysql {
                    if renamed {
                        return Err(renamed_too());
                    }
                    let (fk, table) = if in_place {
                        (format!("{db}/{}", cf.name), format!("{cdb}/{}", child.name))
                    } else {
                        (cf.name.clone(), format!("{cdb}.{}", child.name))
                    };
                    return Err(err(
                        1833,
                        format!(
                            "Cannot change column '{oname}': used in a foreign key constraint '{fk}' of table '{table}'"
                        ),
                    ));
                }
                let ccol = child
                    .columns
                    .get(cf.cols[pos])
                    .map_or("", |c| c.name.as_str());
                let cty = self
                    .stored_def(child.db, &child.name)
                    .and_then(|c| c.col(ccol).map(|i| c.cols[i].ty.clone()));
                if let Some(cty) = cty
                    && !define::fkcheck::compatible(&cty, nt)
                {
                    return Err(incompatible(ccol, nname, &cf.name));
                }
                if !in_place {
                    return Err(err(
                        1833,
                        format!(
                            "Cannot change column '{oname}': used in a foreign key constraint '{}' of table '{cdb}.{}'",
                            cf.name, child.name
                        ),
                    ));
                }
            }
        }
        Ok(())
    }

    /// `ALTER TABLE ... ADD FOREIGN KEY` with `foreign_key_checks` on: the
    /// rows of `d0` must all have their parent under key `f` (the new
    /// definition `d1` names it in the error; `generated`: the statement
    /// didn't name it). Returns how many rows the table has (the servers
    /// copy the table to check them, and report the rows).
    ///
    /// # Errors
    /// 1452 naming the servers' temporary copy of the table.
    async fn rows_have_parents(
        &self,
        d0: &TableDef,
        d1: &TableDef,
        f: &define::Fk,
        generated: bool,
    ) -> Result<Option<u64>> {
        let parent = if self.self_reference(d0.db, &d0.name, f) {
            None
        } else {
            let Some(names) = self.names() else {
                return Ok(None);
            };
            let Some(pdb) = self.parent_db(d0.db, f) else {
                return Ok(None);
            };
            match super::plan::Names::table(&names, pdb, &f.ref_table)
                .and_then(|p| p.as_ref().as_ref().ok().cloned())
            {
                Some(p) => Some(p),
                None => return Ok(None),
            }
        };
        let pdef: &TableDef = parent.as_deref().unwrap_or(d0);
        let cols: Option<Vec<usize>> = f.cols.iter().map(|c| d0.column(c)).collect();
        let pcols: Option<Vec<usize>> = f.ref_cols.iter().map(|c| pdef.column(c)).collect();
        let (Some(cols), Some(pcols)) = (cols, pcols) else {
            return Ok(None);
        };
        if let Some(n) = self
            .eng
            .fk_rows_hold(self.tenant.0, d0, &cols, pdef, &pcols)
            .await?
        {
            return Ok(Some(n));
        }
        let mysql = self.sess.env.mysql;
        let mut text = d1
            .fks
            .iter()
            .find(|x| x.name.eq_ignore_ascii_case(&f.name))
            .map_or_else(String::new, |x| x.text(d1, mysql));
        // The servers check the rows while copying into a temporary table,
        // which the message names: `#sql-alter-<pid>-<connection>` on
        // MariaDB, `#sql-<pid>_<connection>` on MySQL (hexadecimal).
        let tmp = if mysql {
            format!("#sql-1_{:x}", self.sess.conn)
        } else {
            format!("#sql-alter-1-{:x}", self.sess.conn)
        };
        // MariaDB names a key the statement left unnamed after that copy.
        if generated
            && !mysql
            && let Some(n) = f.name.strip_prefix(&format!("{}_ibfk_", d0.name))
        {
            text = text.replacen(&format!("`{}`", f.name), &format!("`{tmp}_ibfk_{n}`"), 1);
        }
        Err(SprigError::standard(
            1452,
            *b"23000",
            format!(
                "Cannot add or update a child row: a foreign key constraint fails (`{}`.`{tmp}`, {text})",
                self.db_name(d0.db)
            ),
        ))
    }

    /// The logical name of database `db`.
    fn db_name(&self, db: DbId) -> String {
        self.cat
            .databases
            .get(db)
            .map_or_else(String::new, |d| d.name.clone())
    }

    /// 1701 for a `TRUNCATE` of a table another table's foreign key
    /// references. MariaDB names the newest such key, with the parent's
    /// database written out; MySQL the oldest, by name only.
    fn truncate_refused(&self, def: &TableDef) -> Option<SprigError> {
        let kids = self.children_of(def);
        let mysql = self.sess.env.mysql;
        let (child, i) = if mysql { kids.first()? } else { kids.last()? };
        let f = &child.fks[*i];
        let detail = if mysql {
            format!(
                "`{}`.`{}`, CONSTRAINT `{}`",
                self.db_name(child.db),
                child.name,
                f.name
            )
        } else {
            let mut q = f.clone();
            q.ref_db = Some(self.db_name(def.db));
            q.on_delete = None;
            q.on_update = None;
            format!(
                "`{}`.`{}`, {}",
                self.db_name(child.db),
                child.name,
                q.text(child, false)
            )
        };
        Some(SprigError::standard(
            1701,
            *b"42000",
            format!("Cannot truncate a table referenced in a foreign key constraint ({detail})"),
        ))
    }

    /// The refusal of a `DROP TABLE` of a table another table's foreign key
    /// references, unless that table goes in the same statement: MariaDB's
    /// 1451 without details, MySQL's 3730.
    fn drop_refused(&self, defs: &[Arc<TableDef>]) -> Option<SprigError> {
        for d in defs {
            let kids = self.children_of(d);
            let Some((child, i)) = kids
                .iter()
                .find(|(c, _)| !defs.iter().any(|x| x.object == c.object))
            else {
                continue;
            };
            return Some(if self.sess.env.mysql {
                SprigError::standard(
                    3730,
                    *b"HY000",
                    format!(
                        "Cannot drop table '{}' referenced by a foreign key constraint '{}' on table '{}'.",
                        d.name, child.fks[*i].name, child.name
                    ),
                )
            } else {
                SprigError::standard(
                    1451,
                    *b"23000",
                    "Cannot delete or update a parent row: a foreign key constraint fails",
                )
            });
        }
        None
    }

    /// The text of table `name` of `schema`: what this statement produced
    /// for it, else the session catalog's. `Some(None)`: this statement
    /// removed it.
    #[allow(clippy::option_option)] // a table produced, removed, or not touched
    fn text(&self, schema: &SchemaName, name: &str) -> Option<Option<String>> {
        let same = |a: &str, b: &str| {
            if self.lctn {
                a.eq_ignore_ascii_case(b)
            } else {
                a == b
            }
        };
        let produced = self.produced.lock().unwrap_or_else(PoisonError::into_inner);
        if let Some((_, _, t)) = produced
            .iter()
            .rev()
            .find(|(s, n, _)| s == schema && same(n, name))
        {
            return Some(t.clone());
        }
        drop(produced);
        let db = schema.db()?;
        let eng = catalog::of(&self.cat)?;
        let o = if self.lctn {
            eng.by_lower.get(&(db, name.to_lowercase()))?
        } else {
            eng.by_name.get(&(db, name.to_owned()))?
        };
        Some(eng.texts.get(o).map(ToString::to_string))
    }

    fn produce(&self, schema: SchemaName, name: &str, text: Option<String>) {
        self.produced
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .push((schema, name.to_owned(), text));
    }

    /// What a statement on database `db` is read against.
    fn ctx(&self, db: DbId) -> Ctx {
        let db_cs = self
            .cat
            .databases
            .get(db)
            .map(|d| (d.default_charset.clone(), d.default_collation.clone()))
            .filter(|(cs, co)| !cs.is_empty() && !co.is_empty())
            .unwrap_or_else(|| ("utf8mb4".into(), "utf8mb4_uca1400_ai_ci".into()));
        Ctx {
            mysql: self.sess.env.mysql,
            db_cs,
        }
    }

    /// The catalog answered.
    pub(crate) async fn done(self, outcome: Outcome<'_>) -> Report {
        let staged =
            std::mem::take(&mut *self.staged.lock().unwrap_or_else(PoisonError::into_inner));
        let dropped =
            std::mem::take(&mut *self.dropped.lock().unwrap_or_else(PoisonError::into_inner));
        let resets = std::mem::take(
            &mut *self
                .seq_resets
                .lock()
                .unwrap_or_else(PoisonError::into_inner),
        );
        if matches!(outcome, Outcome::Published(_)) {
            for (object, rec) in resets {
                if let Err(e) = self.eng.seq_store(self.tenant.0, object, rec) {
                    tracing::warn!(tenant = %self.tenant, error = %e, "a sequence's stored state wasn't set");
                }
            }
        }
        let mut report = Report::default();
        // Every change of a definition reports its rows (`Records: 0` when
        // none were copied).
        if !staged.is_empty() && matches!(outcome, Outcome::Published(_) | Outcome::Unknown) {
            report.rows = Some(0);
        }
        for mut s in staged {
            if let Some(n) = s.rows {
                report.rows = Some(n);
            }
            report.conds.append(&mut s.conds);
            match outcome {
                Outcome::Failed => {
                    if let Err(e) = self.eng.alter_abandon(s).await {
                        tracing::warn!(tenant = %self.tenant, error = %e, "a staged table change wasn't deleted; the table's next change deletes it");
                    }
                }
                // Without the new definition at hand, the next statement
                // on the table applies the change.
                Outcome::Unknown => {}
                Outcome::Published(cat) => {
                    let def = catalog::of(cat)
                        .and_then(|c| c.tables.get(&ObjectId(u64::from(s.object()))))
                        .and_then(|p| p.as_ref().as_ref().ok().cloned());
                    if let Some(def) = def
                        && let Err(e) = self.eng.alter_apply(s, &def).await
                    {
                        tracing::warn!(tenant = %self.tenant, error = %e, "a published table change wasn't applied yet; the table's next statement applies it");
                    }
                }
            }
        }
        // A dropped table's rows are deleted after the statement has
        // answered: the catalog no longer names the table, its keys carry
        // an object number no new table gets, and deleting millions of
        // rows a batch at a time would hold the client for minutes (the
        // servers answer at once).
        if matches!(outcome, Outcome::Published(_)) && !dropped.is_empty() {
            let (eng, tenant) = (self.eng, self.tenant);
            // Marked first, durably: a process that stops before the
            // deletion ends deletes the rest when it next serves the
            // tenant (`Engine::sweep_once`).
            if let Err(e) = eng.mark_dropped(tenant.0, &dropped).await {
                tracing::warn!(tenant = %tenant, error = %e, "dropped tables not marked; the catalog's record of the drop is the sweep's fallback");
            }
            tokio::spawn(async move {
                for object in dropped {
                    if let Err(e) = eng.drop_table(tenant.0, object).await {
                        tracing::warn!(tenant = %tenant, object, error = %e, "a dropped table's rows weren't all deleted");
                    }
                }
            });
        }
        report
    }
}

/// How the catalog answered a statement.
#[derive(Debug, Clone, Copy)]
pub(crate) enum Outcome<'a> {
    /// The change was published: the session's snapshot with it.
    Published(&'a TenantCatalog),
    /// The statement failed: nothing was published.
    Failed,
    /// The statement succeeded without the session learning a version (a
    /// statement that changes no definition, or one the controller
    /// finished).
    Unknown,
}

/// The session's executor with the engine's part added.
pub(crate) struct EngineDdl<'a> {
    hook: &'a Hook,
    inner: &'a dyn DdlExecutor,
}

impl EngineDdl<'_> {
    fn staged(&self, s: Staged) {
        self.hook
            .staged
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .push(s);
    }

    fn note_dropped(&self, objects: impl IntoIterator<Item = u32>) {
        self.hook
            .dropped
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .extend(objects);
    }

    /// `CREATE TABLE` (or `CREATE TABLE ... LIKE`) of `name` in `schema`.
    fn create(&self, stmt: &PhysicalDdl, name: &str) -> Result<()> {
        let Some(db) = stmt.schema.db() else {
            return Err(SprigError::internal(
                "engine: CREATE TABLE outside a database",
            ));
        };
        let cx = self.hook.ctx(db);
        let def = match define::create(&stmt.sql, &cx)? {
            Created::Def(d) => {
                let mut d = *d;
                name.clone_into(&mut d.name);
                d
            }
            Created::Like(src_db, src) => {
                let src_schema = match &src_db {
                    None => stmt.schema,
                    Some(s) => s
                        .parse::<SchemaName>()
                        .map_err(|_| unknown_table(s, &src))?,
                };
                let shown = src_db
                    .as_deref()
                    .map_or_else(|| stmt.schema.to_string(), str::to_owned);
                let Some(Some(text)) = self.hook.text(&src_schema, &src) else {
                    return Err(unknown_table(&shown, &src));
                };
                define::like(&define::parse_stored(&text)?, name)
            }
        };
        let fks: Vec<&define::Fk> = def.fks.iter().collect();
        self.hook.check_fks(db, &def, &fks)?;
        let text = define::render(&def);
        // The engine must be able to serve what results.
        if let Err(why) = table::parse(ObjectId(0), 1, db, &text) {
            return Err(not_on_engine(
                why.trim_start_matches("table definition not served by the engine: "),
            ));
        }
        // `CREATE OR REPLACE`: the table it replaces goes.
        if !self.hook.creates
            && let Some(old) = self.hook.table(&stmt.schema, name)
            && let Ok(o) = object_of(&old)
        {
            self.note_dropped([o]);
        }
        self.hook.produce(stmt.schema, name, Some(text));
        Ok(())
    }

    /// An `ALTER TABLE` (or `CREATE INDEX`, `DROP INDEX`) on engine table
    /// `d0`, named `name` in `schema`.
    async fn alter(&self, stmt: &PhysicalDdl, name: &str, d0: &TableDef) -> Result<()> {
        let Some(a) = alter::read(&stmt.sql) else {
            return Err(not_on_engine("this change of a table's definition"));
        };
        if a.specs
            .iter()
            .any(|s| matches!(s, alter::Spec::Other(o) if o == "RENAME"))
        {
            return Err(not_on_engine(
                "ALTER TABLE that renames the table among other changes (rename it in a statement of its own)",
            ));
        }
        // The change must start from the table's current definition:
        // another session may have changed it since this session's
        // catalog was read.
        if let Some(svc) = &self.hook.svc
            && let Ok(newest) = svc
                .cache
                .get(self.hook.tenant, sprig_catalog::Freshness::Latest)
                .await
            && catalog::of(&newest)
                .and_then(|c| c.tables.get(&d0.object))
                .and_then(|p| p.as_ref().as_ref().ok().map(|d| d.version))
                .is_some_and(|v| v > d0.version)
        {
            return Err(SprigError::standard(
                1412,
                *b"HY000",
                "Table definition has changed, please retry transaction",
            ));
        }
        let Some(Some(text)) = self.hook.text(&stmt.schema, name) else {
            return Err(SprigError::internal(
                "engine: a table's definition text is missing from the catalog",
            ));
        };
        let old = define::parse_stored(&text)?;
        let new = define::alter(&old, &stmt.sql, &self.hook.ctx(d0.db))?;
        self.hook.fk_column_changes(&a, &old, &new, d0)?;
        let new_text = define::render(&new);
        let d1 = table::parse(d0.object, d0.version.saturating_add(1), d0.db, &new_text).map_err(
            |why| {
                not_on_engine(why.trim_start_matches("table definition not served by the engine: "))
            },
        )?;
        // Foreign keys the statement adds: checked against their parents,
        // and (with `foreign_key_checks` on) against the rows.
        let added: Vec<&define::Fk> = new
            .fks
            .iter()
            .filter(|f| !old.fks.iter().any(|o| o.name.eq_ignore_ascii_case(&f.name)))
            .collect();
        self.hook.check_fks(d0.db, &new, &added)?;
        let mut copied = None;
        if !self.hook.sess.env.no_fk_checks {
            let lower = stmt.sql.to_lowercase();
            for f in &added {
                let generated = !lower.contains(&f.name.to_lowercase());
                copied = self
                    .hook
                    .rows_have_parents(d0, &d1, f, generated)
                    .await?
                    .or(copied);
            }
        }
        let mut s = self
            .hook
            .eng
            .alter_stage(self.hook.tenant.0, d0, &d1, &a, &self.hook.sess)
            .await?;
        if copied.is_some() && s.rows.is_none() {
            s.rows = copied;
        }
        self.staged(s);
        self.hook.produce(stmt.schema, name, Some(new_text));
        Ok(())
    }
}

/// 1146 for a table that isn't there.
impl EngineDdl<'_> {
    /// `CREATE VIEW` / `ALTER VIEW` of `name` in `schema`: its select is
    /// bound and described against the session's catalog, and the text the
    /// servers would store is produced (`crate::engine::view`).
    #[allow(clippy::too_many_lines)] // read, bind, describe, check, print
    fn view(&self, stmt: &PhysicalDdl, name: &str) -> Result<()> {
        use super::general::{self, Seed};
        use super::plan::{BindCtx, CatalogNames, Names as _, physical_db};
        use super::view::{self, Printer, errors};
        let Some(db) = stmt.schema.db() else {
            return Err(SprigError::internal(
                "engine: CREATE VIEW outside a database",
            ));
        };
        let Some(eng) = catalog::of(&self.hook.cat) else {
            return Err(SprigError::internal("engine: no engine catalog"));
        };
        let mysql = self.hook.sess.env.mysql;
        let mut h = view::parse_header(&stmt.sql)?;
        name.clone_into(&mut h.name);
        // A variable or parameter can't be part of a view.
        if let Ok(toks) = define::lex::lex(&h.body)
            && toks.iter().any(|t| {
                t.op("?") || matches!(&t.t, define::lex::T::Var(v) if !v.starts_with("@@"))
            })
        {
            return Err(errors::variable());
        }
        let names = CatalogNames {
            cat: &self.hook.cat,
            eng,
            lower_case: self.hook.lctn,
        };
        let db_name = names.db_name(db);
        // Physical database names, as the session's rewrite wrote them, in
        // their logical form.
        let mut body = h.body.clone();
        let tenant = self.hook.tenant;
        let prefix = format!("t{tenant}_d");
        if body.contains(&prefix) {
            let mut q = view::parse_query(&body)?;
            let _ = sqlparser::ast::visit_relations_mut(&mut q, |n| {
                if n.0.len() == 2
                    && let Some(sqlparser::ast::ObjectNamePart::Identifier(i)) = n.0.first_mut()
                    && let Some(d) = physical_db(tenant, &i.value)
                {
                    *i = sqlparser::ast::Ident::with_quote('`', names.db_name(d));
                }
                std::ops::ControlFlow::<()>::Continue(())
            });
            let _ = sqlparser::ast::visit_expressions_mut(&mut q, |e| {
                if let sqlparser::ast::Expr::CompoundIdentifier(p) = e
                    && p.len() == 3
                    && let Some(d) = physical_db(tenant, &p[0].value)
                {
                    p[0] = sqlparser::ast::Ident::with_quote('`', names.db_name(d));
                }
                std::ops::ControlFlow::<()>::Continue(())
            });
            body = q.to_string();
        }
        let query = view::parse_query(&body)?;
        let cx = BindCtx {
            tenant,
            names: &names,
            current: Some(db),
            backslash: true,
        };
        let stmt_q = sqlparser::ast::Statement::Query(Box::new(query.clone()));
        let described = match general::bind_view(&cx, &stmt_q, Some(&body))? {
            Some(g) => {
                let seed = Seed::from_vars(mysql, &self.hook.sess.env, |_| None);
                super::Engine::general_describe(&g, &seed)?
                    .into_iter()
                    .map(|c| c.name)
                    .collect::<Vec<_>>()
            }
            None => view::output_names(&query, &body),
        };
        let columns = match &h.columns {
            Some(c) if c.len() != described.len() => return Err(errors::column_counts(mysql)),
            Some(c) => c.clone(),
            None => described.clone(),
        };
        for (i, c) in columns.iter().enumerate() {
            if columns[..i].iter().any(|x| x.eq_ignore_ascii_case(c)) {
                return Err(errors::duplicate_column(c));
            }
        }
        let algorithm = h.algorithm.unwrap_or(view::Algorithm::Undefined);
        if h.check.is_some() && !view::updatable(&query, algorithm, mysql) {
            return Err(errors::check_on_non_updatable(&db_name, name, mysql));
        }
        let lctn = self.hook.lctn;
        let cols = |d: &str, t: &str| -> Option<Vec<String>> {
            let d = names.db_by_name(d)?;
            if let Some(p) = eng.table(d, t, lctn)
                && let Ok(def) = p.as_ref()
            {
                return Some(
                    def.columns
                        .iter()
                        .filter(|c| !c.invisible)
                        .map(|c| c.name.clone())
                        .collect(),
                );
            }
            match eng.view(d, t, lctn).map(AsRef::as_ref) {
                Some(Ok(v)) => Some(v.columns.clone()),
                _ => None,
            }
        };
        let top: &[String] = if mysql { &described } else { &columns };
        let printed = Printer::new(mysql, &db_name, &body, &cols).query(&query, Some(top));
        let definer = h
            .definer
            .clone()
            .unwrap_or_else(|| self.hook.sess.account.clone());
        let text = view::render(&h, &definer, mysql, &printed);
        // The stored text must read back.
        view::load(ObjectId(0), db, &db_name, &text)?;
        self.hook.produce(stmt.schema, name, Some(text));
        Ok(())
    }
}

impl Hook {
    /// The text of sequence `name` of `schema`, as this statement left it.
    fn seq_text(&self, schema: &SchemaName, name: &str) -> Option<String> {
        let same = |a: &str, b: &str| {
            if self.lctn {
                a.eq_ignore_ascii_case(b)
            } else {
                a == b
            }
        };
        let produced = self.produced.lock().unwrap_or_else(PoisonError::into_inner);
        if let Some((_, _, t)) = produced
            .iter()
            .rev()
            .find(|(s, n, _)| s == schema && same(n, name))
        {
            return t.clone();
        }
        drop(produced);
        let db = schema.db()?;
        catalog::of(&self.cat)?
            .sequence(db, name, self.lctn)
            .map(|e| e.text.clone())
    }
}

impl EngineDdl<'_> {
    /// `CREATE SEQUENCE` (`creating`) or `ALTER SEQUENCE` of `name` in
    /// `schema`: the options read, MariaDB's checks made, and the text
    /// `SHOW CREATE SEQUENCE` shows produced.
    fn sequence(&self, stmt: &PhysicalDdl, name: &str, creating: bool) -> Result<()> {
        use super::sequence::{self as seq, Record};
        let (Some(db), Some(eng)) = (stmt.schema.db(), catalog::of(&self.hook.cat)) else {
            return Err(SprigError::internal(
                "engine: a sequence outside a database",
            ));
        };
        let db_name = self
            .hook
            .cat
            .databases
            .get(db)
            .map_or_else(String::new, |d| d.name.clone());
        let st = seq::parse(&stmt.sql)?;
        let existing = eng.sequence(db, name, self.hook.lctn);
        let def = if creating {
            if st.options.restart.is_some() {
                return Err(SprigError::standard(
                    1064,
                    *b"42000",
                    "You have an error in your SQL syntax; RESTART belongs to ALTER SEQUENCE",
                ));
            }
            let d = seq::define(&db_name, name, &st.options)?;
            // `CREATE OR REPLACE` of a sequence: its values start over.
            if let Some(e) = existing {
                self.hook
                    .seq_resets
                    .lock()
                    .unwrap_or_else(PoisonError::into_inner)
                    .push((e.object, None));
            }
            d
        } else {
            let Some(e) = existing else {
                return Err(SprigError::standard(
                    4091,
                    *b"42S02",
                    format!("Unknown SEQUENCE: '{name}'"),
                ));
            };
            let old = e.def.as_ref().map_err(|w| not_on_engine(w))?;
            let d = seq::alter(&db_name, old, &st.options)?;
            if let Some(r) = st.options.restart {
                self.hook
                    .seq_resets
                    .lock()
                    .unwrap_or_else(PoisonError::into_inner)
                    .push((
                        e.object,
                        Some(Record {
                            next: r.unwrap_or(d.start),
                            round: 0,
                        }),
                    ));
            }
            d
        };
        self.hook
            .produce(stmt.schema, name, Some(seq::render(&def)));
        Ok(())
    }
}

fn unknown_table(schema: &str, name: &str) -> SprigError {
    SprigError::standard(
        1146,
        *b"42S02",
        format!("Table '{schema}.{name}' doesn't exist"),
    )
}

#[async_trait::async_trait]
impl DdlExecutor for EngineDdl<'_> {
    #[allow(clippy::too_many_lines)] // one arm per effect
    async fn execute(&self, stmt: &PhysicalDdl) -> Result<()> {
        match &stmt.effect {
            Effect::Alter {
                kind: ObjectKind::Table,
                name,
            } => match self.hook.table(&stmt.schema, name) {
                Some(d0) => self.alter(stmt, name, &d0).await,
                None => Err(unknown_table(&stmt.schema.to_string(), name)),
            },
            Effect::Create {
                kind: ObjectKind::Table,
                name,
            } => self.create(stmt, name),
            Effect::Truncate { name } => {
                let Some(def) = self.hook.table(&stmt.schema, name) else {
                    return Err(unknown_table(&stmt.schema.to_string(), name));
                };
                if let Some(e) = self.hook.truncate_refused(&def) {
                    return Err(e);
                }
                self.hook
                    .eng
                    .truncate(self.hook.tenant.0, &def, &self.hook.sess)
                    .await
            }
            Effect::Drop {
                kind: ObjectKind::Table,
                names,
            } => {
                // `DROP TABLE` of a view.
                if let (Some(db), Some(eng)) = (stmt.schema.db(), catalog::of(&self.hook.cat))
                    && let Some(n) = names
                        .iter()
                        .find(|n| eng.view(db, n, self.hook.lctn).is_some())
                {
                    let shown = self.hook.sess.db.clone();
                    return Err(if self.hook.sess.env.mysql {
                        SprigError::standard(
                            1051,
                            *b"42S02",
                            format!("Unknown table '{shown}.{n}'"),
                        )
                    } else {
                        SprigError::standard(1965, *b"42S02", format!("'{shown}.{n}' is a view"))
                    });
                }
                let defs: Vec<Arc<TableDef>> = names
                    .iter()
                    .filter_map(|n| self.hook.table(&stmt.schema, n))
                    .collect();
                if let Some(e) = self.hook.drop_refused(&defs) {
                    return Err(e);
                }
                let objects: Vec<u32> = defs.iter().filter_map(|d| object_of(d).ok()).collect();
                for n in names {
                    self.hook.produce(stmt.schema, n, None);
                }
                self.note_dropped(objects);
                Ok(())
            }
            Effect::Rename { moves } => {
                for (fs, fname, ts, tname) in moves {
                    let Some(Some(text)) = self.hook.text(fs, fname) else {
                        return Err(unknown_table(&fs.to_string(), fname));
                    };
                    let def = define::renamed(&define::parse_stored(&text)?, tname);
                    self.hook.produce(*fs, fname, None);
                    self.hook.produce(*ts, tname, Some(define::render(&def)));
                }
                Ok(())
            }
            Effect::DropSchema => {
                let objects: Vec<u32> = match (stmt.schema.db(), catalog::of(&self.hook.cat)) {
                    (Some(db), Some(eng)) => eng
                        .by_name
                        .iter()
                        .filter(|((d, _), _)| *d == db)
                        .filter_map(|(_, o)| u32::try_from(o.0).ok())
                        .collect(),
                    _ => Vec::new(),
                };
                self.inner.execute(stmt).await?;
                self.note_dropped(objects);
                Ok(())
            }
            Effect::Create {
                kind: ObjectKind::Sequence,
                name,
            } => self.sequence(stmt, name, true),
            Effect::Alter {
                kind: ObjectKind::Sequence,
                name,
            } => self.sequence(stmt, name, false),
            Effect::Drop {
                kind: ObjectKind::Sequence,
                names,
            } => {
                let (Some(db), Some(eng)) = (stmt.schema.db(), catalog::of(&self.hook.cat)) else {
                    return Err(SprigError::internal(
                        "engine: DROP SEQUENCE outside a database",
                    ));
                };
                let shown = self.hook.sess.db.clone();
                if let Some(n) = names
                    .iter()
                    .find(|n| eng.sequence(db, n, self.hook.lctn).is_none())
                {
                    return Err(SprigError::standard(
                        4090,
                        *b"42S02",
                        format!("'{shown}.{n}' is not a SEQUENCE"),
                    ));
                }
                let objects: Vec<u32> = names
                    .iter()
                    .filter_map(|n| eng.sequence(db, n, self.hook.lctn))
                    .filter_map(|e| u32::try_from(e.object.0).ok())
                    .collect();
                for n in names {
                    self.hook.produce(stmt.schema, n, None);
                }
                self.note_dropped(objects);
                Ok(())
            }
            Effect::Create {
                kind: ObjectKind::View,
                name,
            }
            | Effect::Alter {
                kind: ObjectKind::View,
                name,
            } => self.view(stmt, name),
            // A view has nothing but its catalog record.
            Effect::Drop {
                kind: ObjectKind::View,
                names,
            } => {
                // `DROP VIEW` of a table.
                if let Some(n) = names
                    .iter()
                    .find(|n| self.hook.table(&stmt.schema, n).is_some())
                {
                    let db = self.hook.sess.db.clone();
                    return Err(if self.hook.sess.env.mysql {
                        SprigError::standard(1347, *b"HY000", format!("'{db}.{n}' is not VIEW"))
                    } else {
                        SprigError::standard(4092, *b"42S02", format!("Unknown VIEW: '{db}.{n}'"))
                    });
                }
                for n in names {
                    self.hook.produce(stmt.schema, n, None);
                }
                Ok(())
            }
            Effect::Create {
                kind: kind @ (ObjectKind::Procedure | ObjectKind::Function),
                name,
            } => {
                let db = stmt.schema.db();
                let db_name =
                    db.map_or_else(|| self.hook.sess.db.clone(), |d| self.hook.db_name(d));
                let cx = db.map_or_else(|| self.hook.ctx(DbId(0)), |d| self.hook.ctx(d));
                let rs = self.hook.routine.clone().unwrap_or_default();
                let text = super::routine::ddl::create(
                    &stmt.sql,
                    &rs,
                    &db_name,
                    &self.hook.sess.account,
                    self.hook.sess.env.mysql,
                    &cx.db_cs,
                )?;
                self.hook
                    .produce_routine(stmt.schema, *kind, name, Some(text));
                Ok(())
            }
            Effect::Create {
                kind: ObjectKind::Trigger,
                name,
            } => {
                let text = self.hook.trigger_text(&stmt.sql, stmt.schema.db(), name)?;
                self.hook
                    .produce_routine(stmt.schema, ObjectKind::Trigger, name, Some(text));
                Ok(())
            }
            Effect::Alter {
                kind: kind @ (ObjectKind::Procedure | ObjectKind::Function),
                name,
            } => {
                let function = *kind == ObjectKind::Function;
                let Some(old) = self.hook.routine_text(&stmt.schema, *kind, name) else {
                    return Err(SprigError::standard(
                        1305,
                        *b"42000",
                        format!(
                            "{} {}.{name} does not exist",
                            if function { "FUNCTION" } else { "PROCEDURE" },
                            self.hook.sess.db
                        ),
                    ));
                };
                let rs = self.hook.routine.clone().unwrap_or_default();
                let sql = rs.entry.clone().unwrap_or_else(|| stmt.sql.clone());
                let text = super::routine::ddl::alter(&old, &sql, &rs)?;
                self.hook
                    .produce_routine(stmt.schema, *kind, name, Some(text));
                Ok(())
            }
            // A routine has nothing but its catalog record.
            Effect::Drop {
                kind: kind @ (ObjectKind::Procedure | ObjectKind::Function | ObjectKind::Trigger),
                names,
            } => {
                for n in names {
                    self.hook.produce_routine(stmt.schema, *kind, n, None);
                }
                Ok(())
            }
            Effect::CreateSchema | Effect::AlterSchema | Effect::Drop { .. } => {
                self.inner.execute(stmt).await
            }
            // Views, stored code, triggers, events and sequences aren't the
            // engine's yet.
            Effect::Create { .. } | Effect::Alter { .. } | Effect::Restore { .. } => Err(
                not_on_engine("views, stored routines, triggers, events and sequences"),
            ),
        }
    }

    async fn schema(&self, schema: &SchemaName) -> Result<Option<SchemaInfo>> {
        self.inner.schema(schema).await
    }

    async fn show_create(
        &self,
        schema: &SchemaName,
        kind: ObjectKind,
        name: &str,
    ) -> Result<Option<String>> {
        if kind == ObjectKind::Table || kind == ObjectKind::View {
            return Ok(self.hook.text(schema, name).flatten());
        }
        if kind == ObjectKind::Sequence {
            return Ok(self.hook.seq_text(schema, name));
        }
        if matches!(
            kind,
            ObjectKind::Procedure | ObjectKind::Function | ObjectKind::Trigger
        ) {
            return Ok(self.hook.routine_text(schema, kind, name));
        }
        self.inner.show_create(schema, kind, name).await
    }

    async fn objects(&self, schema: &SchemaName) -> Result<Vec<PhysicalObject>> {
        self.inner.objects(schema).await
    }

    async fn tenant_schemas(&self, tenant: TenantId) -> Result<Vec<SchemaName>> {
        self.inner.tenant_schemas(tenant).await
    }

    async fn busy(&self, schema: &SchemaName) -> Result<bool> {
        self.inner.busy(schema).await
    }

    async fn shards(
        &self,
        tenant: TenantId,
        db: &DatabaseRow,
        target: ShardTarget<'_>,
    ) -> Result<Vec<SchemaName>> {
        self.inner.shards(tenant, db, target).await
    }

    fn cluster_of(&self, schema: &SchemaName) -> Option<ClusterId> {
        self.inner.cluster_of(schema)
    }

    async fn wait_for_shard_jobs(&self, tenant: TenantId, shards: &[SchemaName]) -> Result<()> {
        self.inner.wait_for_shard_jobs(tenant, shards).await
    }
}
