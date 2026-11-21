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
    /// The gateway's services: the newest catalog, for the check that a
    /// change starts from the table's current definition.
    svc: Option<Arc<crate::tenant::TenantServices>>,
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
            svc: None,
        }
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

    /// The text of table `name` of `schema`: what this statement produced
    /// for it, else the session catalog's. `Some(None)`: this statement
    /// removed it.
    fn text(&self, schema: &SchemaName, name: &str) -> Option<Option<String>> {
        let same = |a: &str, b: &str| if self.lctn { a.eq_ignore_ascii_case(b) } else { a == b };
        let produced = self.produced.lock().unwrap_or_else(PoisonError::into_inner);
        if let Some((_, _, t)) = produced.iter().rev().find(|(s, n, _)| s == schema && same(n, name)) {
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
        Some(eng.texts.get(o).map(|t| t.to_string()))
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
        if matches!(outcome, Outcome::Published(_)) {
            for object in dropped {
                if let Err(e) = self.eng.drop_table(self.tenant.0, object).await {
                    tracing::warn!(tenant = %self.tenant, object, error = %e, "a dropped table's rows weren't all deleted");
                }
            }
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
    async fn create(&self, stmt: &PhysicalDdl, name: &str) -> Result<()> {
        let Some(db) = stmt.schema.db() else {
            return Err(SprigError::internal("engine: CREATE TABLE outside a database"));
        };
        let cx = self.hook.ctx(db);
        let def = match define::create(&stmt.sql, &cx)? {
            Created::Def(d) => {
                let mut d = *d;
                d.name = name.to_owned();
                d
            }
            Created::Like(src_db, src) => {
                let src_schema = match &src_db {
                    None => stmt.schema,
                    Some(s) => s.parse::<SchemaName>().map_err(|_| unknown_table(s, &src))?,
                };
                let shown = src_db.as_deref().map_or_else(|| stmt.schema.to_string(), str::to_owned);
                let Some(Some(text)) = self.hook.text(&src_schema, &src) else {
                    return Err(unknown_table(&shown, &src));
                };
                define::like(&define::parse_stored(&text)?, name)
            }
        };
        let text = define::render(&def);
        // The engine must be able to serve what results.
        if let Err(why) = table::parse(ObjectId(0), 1, db, &text) {
            return Err(not_on_engine(why.trim_start_matches(
                "table definition not served by the engine: ",
            )));
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
        let new_text = define::render(&new);
        let d1 = table::parse(d0.object, d0.version.saturating_add(1), d0.db, &new_text)
            .map_err(|why| {
                not_on_engine(why.trim_start_matches(
                    "table definition not served by the engine: ",
                ))
            })?;
        let s = self
            .hook
            .eng
            .alter_stage(self.hook.tenant.0, d0, &d1, &a, &self.hook.sess)
            .await?;
        self.staged(s);
        self.hook.produce(stmt.schema, name, Some(new_text));
        Ok(())
    }
}

/// 1146 for a table that isn't there.
fn unknown_table(schema: &str, name: &str) -> SprigError {
    SprigError::standard(
        1146,
        *b"42S02",
        format!("Table '{schema}.{name}' doesn't exist"),
    )
}

#[async_trait::async_trait]
impl DdlExecutor for EngineDdl<'_> {
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
            } => self.create(stmt, name).await,
            Effect::Truncate { name } => {
                let Some(def) = self.hook.table(&stmt.schema, name) else {
                    return Err(unknown_table(&stmt.schema.to_string(), name));
                };
                self.hook
                    .eng
                    .truncate(self.hook.tenant.0, &def, &self.hook.sess)
                    .await
            }
            Effect::Drop {
                kind: ObjectKind::Table,
                names,
            } => {
                let objects: Vec<u32> = names
                    .iter()
                    .filter_map(|n| self.hook.table(&stmt.schema, n))
                    .filter_map(|d| object_of(&d).ok())
                    .collect();
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
            Effect::CreateSchema | Effect::AlterSchema => self.inner.execute(stmt).await,
            // Views, stored code, triggers, events and sequences aren't the
            // engine's yet.
            Effect::Create { .. } | Effect::Alter { .. } | Effect::Restore { .. } => Err(
                not_on_engine("views, stored routines, triggers, events and sequences"),
            ),
            Effect::Drop { .. } => self.inner.execute(stmt).await,
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
        if kind == ObjectKind::Table {
            return Ok(self.hook.text(schema, name).flatten());
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
