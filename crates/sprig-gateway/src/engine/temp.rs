//! Temporary tables on engine databases (`CREATE TEMPORARY TABLE`).
//!
//! A temporary table belongs to one session and lives until the session
//! drops it or ends. Its definition is never published to the catalog:
//! the session keeps it ([`TempTables`]), and while it has any, the
//! session's engine statements read through its own copy of the catalog
//! snapshot with the temporary tables added ([`TempTables::overlay`]). In
//! that copy a temporary table hides a table, view or sequence of the same
//! name in its database, as on the servers, so every lookup of the
//! planner, the single-table path and `SHOW` finds it unchanged.
//!
//! Its rows are the engine's, keyed by an object number no catalog object
//! gets (from [`TEMP_BASE`] up, chosen at random and checked unused), so
//! they take part in the session's transactions like any table's.
//!
//! **Cleanup.** When the table is created, the engine records a drop mark
//! for its object whose value names the node and the process
//! ([`owner`]). Dropping the table, or the session's end, deletes its rows
//! and the mark. A process that stops before that leaves the mark: the
//! next process of the same node deletes the rows when it first serves
//! the tenant (`Engine::sweep`), and marks of another node's processes are
//! left to that node.

use std::collections::HashSet;
use std::sync::{Arc, Mutex, OnceLock, PoisonError};

use sprig_catalog::TenantCatalog;
use sprig_common::ids::{DbId, ObjectId};
use sprig_common::{Result, SprigError};

use super::catalog::{self, EngineCatalog, MODULE};
use super::table::{self, Parsed};
use super::{Engine, define};

/// The first object number of temporary tables: catalog objects are
/// numbered from 1 and never reach it.
pub(crate) const TEMP_BASE: u32 = 0xC000_0000;

/// This process's identity in its marks: a random number drawn at start.
fn incarnation() -> u64 {
    static ID: OnceLock<u64> = OnceLock::new();
    *ID.get_or_init(|| {
        let mut b = [0u8; 8];
        if getrandom::fill(&mut b).is_err() {
            // The clock is enough to tell two processes of a node apart.
            let t = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map_or(1, |d| d.as_nanos());
            #[allow(clippy::cast_possible_truncation)] // the low bits differ
            return t as u64 | 1;
        }
        u64::from_be_bytes(b) | 1
    })
}

/// This node's identity in marks: its replica id, or 0 for a single node.
fn node() -> u64 {
    crate::node_engine::raft_id_from_env().unwrap_or(0)
}

/// The value of a temporary table's drop mark: the node, then the process.
#[must_use]
pub(crate) fn owner() -> Vec<u8> {
    let mut v = Vec::with_capacity(16);
    v.extend_from_slice(&node().to_be_bytes());
    v.extend_from_slice(&incarnation().to_be_bytes());
    v
}

/// Whether a drop mark with value `v` (a temporary table's, [`owner`]) was
/// left by an earlier process of this node: its session is gone.
#[must_use]
pub(crate) fn orphaned(v: &[u8]) -> bool {
    let (Some(n), Some(p)) = (v.get(..8), v.get(8..16)) else {
        return false;
    };
    n == node().to_be_bytes() && p != incarnation().to_be_bytes()
}

/// The temporary tables of this process's sessions, by (tenant, object):
/// a new table's number is checked against them as well as the store.
fn live() -> &'static Mutex<HashSet<(u64, u32)>> {
    static LIVE: OnceLock<Mutex<HashSet<(u64, u32)>>> = OnceLock::new();
    LIVE.get_or_init(Mutex::default)
}

/// A fresh object number for a temporary table of `tenant`, marked as
/// this process's.
///
/// # Errors
/// The store's.
pub(crate) async fn new_object(eng: &Engine, tenant: u64) -> Result<u32> {
    for _ in 0..32 {
        let mut b = [0u8; 4];
        getrandom::fill(&mut b)
            .map_err(|e| SprigError::internal(format!("engine: no random number: {e}")))?;
        let o = TEMP_BASE | (u32::from_be_bytes(b) & !TEMP_BASE);
        if !live()
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .insert((tenant, o))
        {
            continue;
        }
        if eng.has_keys(tenant, o)? {
            forget(tenant, o);
            continue;
        }
        if let Err(e) = eng.mark_owned(tenant, o, owner()).await {
            forget(tenant, o);
            return Err(e);
        }
        return Ok(o);
    }
    Err(SprigError::internal(
        "engine: no free object number for a temporary table",
    ))
}

fn forget(tenant: u64, object: u32) {
    live()
        .lock()
        .unwrap_or_else(PoisonError::into_inner)
        .remove(&(tenant, object));
}

/// Deletes the rows and marks of temporary tables `objects` of `tenant`,
/// in the background (the statement or the session's end doesn't wait).
pub(crate) fn discard(tenant: u64, objects: Vec<u32>) {
    if objects.is_empty() {
        return;
    }
    let Ok(eng) = super::engine() else {
        return;
    };
    let Ok(rt) = tokio::runtime::Handle::try_current() else {
        return;
    };
    rt.spawn(async move {
        for o in objects {
            if let Err(e) = eng.drop_table(tenant, o).await {
                tracing::warn!(tenant, object = o, error = %e, "a temporary table's rows weren't all deleted; the node's next start deletes them");
            }
            forget(tenant, o);
        }
    });
}

/// The number of rows of temporary table `object` of `tenant` (MySQL's
/// `ALTER TABLE ... RENAME` reports them as copied).
///
/// # Errors
/// The store's.
pub(crate) fn row_count(eng: &Engine, tenant: u64, object: u32) -> Result<u64> {
    let p = super::keys::rows_prefix(tenant, object, 0);
    let mut tx = eng.begin(sprig_engine::txn::TxnOptions::default())?;
    let n = tx
        .scan(&p, &super::keys::prefix_end(&p), false, 1 << 24)?
        .len();
    tx.rollback();
    Ok(u64::try_from(n).unwrap_or(u64::MAX))
}

/// The column definition `CREATE TABLE ... SELECT` gives a result column
/// `name` of type `ty`, as each server makes it from the value's type.
#[must_use]
pub(crate) fn column_text(name: &str, ty: &sprig_exec::ExprType, mysql: bool) -> String {
    use sprig_engine::keys::collation::Collation;
    use sprig_exec::{FieldType as F, Kind, Persona};
    let m = ty.meta(if mysql {
        Persona::MySql
    } else {
        Persona::MariaDb
    });
    let u = if m.unsigned { " unsigned" } else { "" };
    let int = |n: &str| {
        if mysql {
            format!("{n}{u}")
        } else {
            format!("{n}({}){u}", m.len.max(1))
        }
    };
    let text = ty.kind == Kind::Str && ty.coll != Collation::Binary;
    let chars = ty.len;
    let frac = |n: &str| {
        if m.dec == 0 {
            n.to_owned()
        } else {
            format!("{n}({})", m.dec)
        }
    };
    let t = match m.field {
        F::Null => "binary(0)".to_owned(),
        F::Tiny => int("tinyint"),
        F::Short => int("smallint"),
        F::Int24 => int("mediumint"),
        F::Long => int("int"),
        // MySQL makes a computed integer of up to 10 digits an `int`.
        F::LongLong if mysql && ty.width == 0 && !ty.column && ty.len <= 10 => int("int"),
        F::LongLong => int("bigint"),
        F::Year => "year".to_owned(),
        F::Bit => format!("bit({})", m.len.max(1)),
        F::NewDecimal => format!("decimal({},{}){u}", ty.prec.max(1), ty.dec),
        F::Float => "float".to_owned(),
        F::Double => "double".to_owned(),
        F::VarString if text => format!("varchar({chars})"),
        F::VarString => format!("varbinary({chars})"),
        F::String if text => format!("char({chars})"),
        F::String => format!("binary({chars})"),
        F::Blob if text => "text".to_owned(),
        F::Blob => "blob".to_owned(),
        F::MediumBlob if text => "mediumtext".to_owned(),
        F::MediumBlob => "mediumblob".to_owned(),
        F::LongBlob if text => "longtext".to_owned(),
        F::LongBlob => "longblob".to_owned(),
        F::Date => "date".to_owned(),
        F::Time => frac("time"),
        F::DateTime => frac("datetime"),
        F::Timestamp => frac("timestamp"),
    };
    let coll = if text {
        format!(" COLLATE {}", ty.coll.name())
    } else {
        String::new()
    };
    let null = if ty.nullable { " NULL" } else { " NOT NULL" };
    // MySQL gives a computed `NOT NULL` column its type's zero as default.
    let default = match ty.kind {
        _ if !mysql || ty.nullable || ty.column => String::new(),
        Kind::Decimal if ty.dec > 0 => format!(" DEFAULT '0.{}'", "0".repeat(usize::from(ty.dec))),
        Kind::Int | Kind::Decimal | Kind::Double | Kind::Float => " DEFAULT '0'".to_owned(),
        Kind::Str => " DEFAULT ''".to_owned(),
        _ => String::new(),
    };
    format!("`{}` {t}{coll}{null}{default}", name.replace('`', "``"))
}

/// One temporary table.
#[derive(Debug, Clone)]
pub(crate) struct TempTable {
    /// Its database.
    pub db: DbId,
    /// Its name as written.
    pub name: String,
    /// Its object number in keys.
    pub object: u32,
    /// Its definition, as the catalog would store a table's.
    pub text: Arc<str>,
    /// The same, read.
    pub parsed: Arc<Parsed>,
}

impl TempTable {
    /// A table of `db` with definition `text` (a `CREATE TABLE` the engine
    /// produced) under object number `object`.
    ///
    /// # Errors
    /// 39960 when the engine can't serve the definition.
    pub(crate) fn new(db: DbId, name: &str, object: u32, text: String) -> Result<Self> {
        let parsed = table::parse(ObjectId(u64::from(object)), 1, db, &text);
        if let Err(why) = &parsed {
            return Err(super::not_on_engine(
                why.trim_start_matches("table definition not served by the engine: "),
            ));
        }
        Ok(Self {
            db,
            name: name.to_owned(),
            object,
            text: Arc::from(text),
            parsed: Arc::new(parsed),
        })
    }

    /// The same table under `name` in `db` (`RENAME TABLE`): its rows stay.
    ///
    /// # Errors
    /// The definition's.
    pub(crate) fn renamed(&self, db: DbId, name: &str) -> Result<Self> {
        let mut d = define::parse_stored(&self.text)?;
        name.clone_into(&mut d.name);
        Self::new(db, name, self.object, define::render(&d))
    }

    /// Whether the table is `name` of `db` (names compare in lower case
    /// when `lower_case`).
    fn is(&self, db: DbId, name: &str, lower_case: bool) -> bool {
        self.db == db
            && if lower_case {
                self.name.eq_ignore_ascii_case(name)
            } else {
                self.name == name
            }
    }
}

/// `SHOW CREATE TABLE` of a temporary table: its definition text with
/// `TEMPORARY`, as the servers show it.
#[must_use]
pub(crate) fn shown(text: &str) -> String {
    match text.strip_prefix("CREATE TABLE ") {
        Some(rest) => format!("CREATE TEMPORARY TABLE {rest}"),
        None => text.to_owned(),
    }
}

/// One session's temporary tables on engine databases.
#[derive(Debug, Default)]
pub(crate) struct TempTables {
    /// The tenant they belong to.
    tenant: u64,
    /// The tables, oldest first.
    list: Vec<TempTable>,
    /// Bumped by every change of the list.
    generation: u64,
    /// The last copy of the catalog made: for (catalog version,
    /// generation).
    overlay: Mutex<Option<(u64, u64, Arc<TenantCatalog>)>>,
}

impl Drop for TempTables {
    fn drop(&mut self) {
        let objects = self.list.drain(..).map(|t| t.object).collect();
        discard(self.tenant, objects);
    }
}

impl TempTables {
    /// Whether the session has none.
    #[must_use]
    pub(crate) fn is_empty(&self) -> bool {
        self.list.is_empty()
    }

    /// A number that changes with every change of the tables (plans made
    /// before a change are made again).
    #[must_use]
    pub(crate) fn generation(&self) -> u64 {
        self.generation
    }

    /// The tables, oldest first.
    pub(crate) fn iter(&self) -> impl Iterator<Item = &TempTable> {
        self.list.iter()
    }

    /// The position of table `name` of `db`.
    #[must_use]
    pub(crate) fn find(&self, db: DbId, name: &str, lower_case: bool) -> Option<usize> {
        self.list.iter().position(|t| t.is(db, name, lower_case))
    }

    /// The table at `i`.
    #[must_use]
    pub(crate) fn get(&self, i: usize) -> Option<&TempTable> {
        self.list.get(i)
    }

    /// Adds `t`, a table of `tenant`.
    pub(crate) fn add(&mut self, tenant: u64, t: TempTable) {
        self.tenant = tenant;
        self.list.push(t);
        self.changed();
    }

    /// Replaces the table at `i` with `t`.
    pub(crate) fn replace(&mut self, i: usize, t: TempTable) {
        if let Some(slot) = self.list.get_mut(i) {
            *slot = t;
            self.changed();
        }
    }

    /// Removes the table at `i`; the caller deletes its rows.
    pub(crate) fn remove(&mut self, i: usize) -> Option<TempTable> {
        if i >= self.list.len() {
            return None;
        }
        let t = self.list.remove(i);
        self.changed();
        Some(t)
    }

    /// Drops every table (the session ends or is reset).
    pub(crate) fn clear(&mut self) {
        let objects: Vec<u32> = self.list.drain(..).map(|t| t.object).collect();
        if !objects.is_empty() {
            discard(self.tenant, objects);
            self.changed();
        }
    }

    fn changed(&mut self) {
        self.generation += 1;
        *self
            .overlay
            .get_mut()
            .unwrap_or_else(PoisonError::into_inner) = None;
    }

    /// `base` as this session sees it: with its temporary tables added,
    /// each hiding a table, view or sequence of the same name in its
    /// database. `base` itself when the session has none.
    pub(crate) fn overlay(&self, base: &Arc<TenantCatalog>) -> Arc<TenantCatalog> {
        if self.list.is_empty() {
            return Arc::clone(base);
        }
        let mut memo = self.overlay.lock().unwrap_or_else(PoisonError::into_inner);
        if let Some((v, g, c)) = &*memo
            && *v == base.version
            && *g == self.generation
            && c.tenant == base.tenant
        {
            return Arc::clone(c);
        }
        let Some(eng) = catalog::of(base) else {
            return Arc::clone(base);
        };
        let mut e: EngineCatalog = eng.clone();
        for t in &self.list {
            add_to(&mut e, t);
        }
        let mut c = (**base).clone();
        c.ext = base.ext.with(vec![(MODULE, Arc::new(e))]);
        let c = Arc::new(c);
        *memo = Some((base.version, self.generation, Arc::clone(&c)));
        c
    }
}

/// Adds temporary table `t` to `e`, hiding what has its name.
fn add_to(e: &mut EngineCatalog, t: &TempTable) {
    let o = ObjectId(u64::from(t.object));
    let lower = t.name.to_lowercase();
    // What the name named before.
    if let Some(old) = e.by_name.get(&(t.db, t.name.clone())).copied() {
        e.by_lower.retain(|_, v| *v != old);
    }
    if let Some(old) = e.by_lower.get(&(t.db, lower.clone())).copied() {
        e.by_name.retain(|_, v| *v != old);
    }
    if let Some(k) = e.views_lower.remove(&(t.db, lower.clone())) {
        e.views.remove(&k);
    }
    e.views.remove(&(t.db, t.name.clone()));
    e.views_lower.retain(|_, k| *k != (t.db, t.name.clone()));
    if let Some(s) = e.seq_lower.remove(&(t.db, lower.clone())) {
        e.seq_names.retain(|_, v| *v != s);
    }
    if let Some(s) = e.seq_names.remove(&(t.db, t.name.clone())) {
        e.seq_lower.retain(|_, v| *v != s);
    }
    e.tables.insert(o, Arc::clone(&t.parsed));
    e.texts.insert(o, Arc::clone(&t.text));
    e.by_name.insert((t.db, t.name.clone()), o);
    e.by_lower.insert((t.db, lower), o);
    e.temps.insert(o);
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn marks_of_this_process_and_other_nodes_are_not_orphans() {
        assert!(!orphaned(&owner()));
        let mut other_process = owner();
        other_process[15] ^= 0xff;
        assert!(orphaned(&other_process));
        let mut other_node = owner();
        other_node[0] ^= 0xff;
        assert!(!orphaned(&other_node));
        assert!(!orphaned(&[]));
    }

    const TEXT: &str = "CREATE TABLE `t` (\n  `a` int(11) DEFAULT NULL\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_uca1400_ai_ci";

    #[test]
    fn a_temporary_table_hides_what_has_its_name() {
        let db = DbId(3);
        let mut e = EngineCatalog::default();
        let base = ObjectId(20);
        e.tables.insert(
            base,
            Arc::new(table::parse(base, 1, db, &TEXT.replace("`a`", "`b`"))),
        );
        e.by_name.insert((db, "t".into()), base);
        e.by_lower.insert((db, "t".into()), base);
        e.views.insert((db, "v".into()), Arc::new(Err("x".into())));
        e.views_lower.insert((db, "v".into()), (db, "v".into()));
        let t = TempTable::new(db, "t", TEMP_BASE + 1, TEXT.into()).unwrap();
        let v = TempTable::new(db, "v", TEMP_BASE + 2, TEXT.replace("`t`", "`v`")).unwrap();
        add_to(&mut e, &t);
        add_to(&mut e, &v);
        let found = e.table(db, "T", true).unwrap().as_ref().as_ref().unwrap();
        assert_eq!(found.object, ObjectId(u64::from(TEMP_BASE + 1)));
        assert!(e.view(db, "v", false).is_none());
        assert!(e.table(db, "v", false).is_some());
        assert!(e.temps.contains(&ObjectId(u64::from(TEMP_BASE + 2))));
        // The base table's definition stays, for plans bound before.
        assert!(e.tables.contains_key(&base));
        // Renamed: the rows (object) stay, the name moves.
        let r = t.renamed(DbId(4), "u").unwrap();
        assert_eq!((r.object, r.db, r.name.as_str()), (t.object, DbId(4), "u"));
        assert!(r.text.starts_with("CREATE TABLE `u`"));
    }

    #[test]
    fn shown_with_temporary() {
        assert_eq!(
            shown("CREATE TABLE `t` (\n  `a` int(11) DEFAULT NULL\n)"),
            "CREATE TEMPORARY TABLE `t` (\n  `a` int(11) DEFAULT NULL\n)"
        );
    }
}
