//! Foreign keys on engine tables, enforced row by row inside the
//! statement's transaction, as InnoDB enforces them (both servers' rules,
//! observed through their clients):
//!
//! - **A child row** (inserted, or updated in its key's columns) with no
//!   `NULL` in the key needs its parent: the parent's entry is looked up
//!   by a locking read in shared mode (the entry alone when it is there;
//!   the place where it would be, at REPEATABLE READ and SERIALIZABLE,
//!   when it is not), and a missing parent fails the statement with 1452.
//!   The child's own write comes first, so a duplicate key is reported
//!   before a missing parent.
//! - **A parent row** that is deleted, or whose referenced columns change,
//!   has its children looked up through the child's index on the key:
//!   `RESTRICT`, `NO ACTION` and `SET DEFAULT` fail with 1451 while one
//!   exists (read in shared mode); `CASCADE` deletes them or gives them
//!   the new key, `SET NULL` sets their key to `NULL` (each child row
//!   locked exclusively), recursively, at most 15 levels deep. A cascaded
//!   update that reaches a table already being updated higher up the
//!   same chain fails with 1451.
//! - `foreign_key_checks = 0` turns every check and action off.
//!
//! The tables a statement may reach are found when it is bound
//! ([`Graph::build`]), from the catalog's definitions; a statement on a
//! table with no foreign key either way carries no graph and pays
//! nothing.

use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use bytes::Bytes;
use sprig_common::ids::{DbId, ObjectId};
use sprig_common::{Result, SprigError};
use sprig_engine::txn::{LockMode, TxnOptions, Wait};

use super::keys::{self, Space};
use super::plan::BindCtx;
use super::row::{self, Datum};
use super::store::Tx;
use super::table::{FkAction, TableDef};
use super::{Engine, assign};
use super::{Pace, WALK_BATCH, exec};

/// The most tables one statement's foreign keys may reach.
const MAX_TABLES: usize = 64;

/// InnoDB's cascade depth limit.
const MAX_DEPTH: u32 = 15;

/// Rows read per batch of a scan of a child's index.
const BATCH: usize = 256;

/// A key of a table: its primary key, or a secondary index.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum KeyOf {
    /// The primary key.
    Primary,
    /// Secondary index `ix` (a position in the definition).
    Index(usize),
}

/// The parent of one foreign key.
#[derive(Debug, Clone)]
pub(crate) struct Parent {
    /// The parent table.
    pub def: Arc<TableDef>,
    /// The parent's key whose first parts are the referenced columns.
    pub key: KeyOf,
}

/// A foreign key of another table (or of the same one) that references a
/// table.
#[derive(Debug, Clone)]
pub(crate) struct Child {
    /// The child table.
    pub object: ObjectId,
    /// The foreign key's position among the child's.
    pub fk: usize,
    /// The child's key whose first parts are the foreign key's columns.
    pub key: KeyOf,
    /// The referenced (parent's) columns, in the key's order.
    pub ref_cols: Vec<usize>,
}

/// One table a statement's foreign keys reach.
#[derive(Debug, Clone)]
pub(crate) struct Node {
    /// Its definition.
    pub def: Arc<TableDef>,
    /// Its database's logical name (errors name it).
    pub db_name: String,
    /// Per foreign key of the table: its parent, when the parent table
    /// exists and has a key the child's values can be looked up in.
    pub parents: Vec<Option<Parent>>,
    /// The foreign keys that reference the table.
    pub children: Vec<Child>,
}

/// The tables a statement's foreign keys reach, by object.
#[derive(Debug, Clone, Default)]
pub(crate) struct Graph {
    /// The statement's table and every table a cascade may reach.
    pub tables: HashMap<ObjectId, Node>,
}

impl Graph {
    /// Whether table `object` references itself (a cascade of one of its
    /// rows can reach its other rows).
    pub(crate) fn refers_to_itself(&self, object: ObjectId) -> bool {
        self.tables
            .get(&object)
            .is_some_and(|n| n.children.iter().any(|c| c.object == object))
    }
}

/// The key of `def` whose first parts are exactly columns `cols`, in that
/// order (whole columns, not prefixes).
fn key_on(def: &TableDef, cols: &[usize]) -> Option<KeyOf> {
    let leads = |key_cols: &[usize], parts: &[sprig_engine::keys::typed::KeyColumn]| {
        key_cols.len() >= cols.len()
            && key_cols.iter().zip(cols).all(|(a, b)| a == b)
            && parts.iter().take(cols.len()).all(|p| p.prefix.is_none())
    };
    if !def.hidden && leads(&def.pk, &def.pk_parts) {
        return Some(KeyOf::Primary);
    }
    def.indexes
        .iter()
        .position(|x| leads(&x.cols, &x.parts))
        .map(KeyOf::Index)
}

impl Graph {
    /// The foreign keys statement on table `def` (of database `db`) must
    /// keep; `None` when the table has none either way.
    pub(crate) fn build(cx: &BindCtx<'_>, def: &Arc<TableDef>, db: DbId) -> Option<Arc<Self>> {
        if def.fks.is_empty() && cx.names.referencing(db, &def.name).is_empty() {
            return None;
        }
        let mut g = Graph::default();
        let mut queue = vec![(Arc::clone(def), db)];
        while let Some((d, db)) = queue.pop() {
            if g.tables.contains_key(&d.object) || g.tables.len() >= MAX_TABLES {
                continue;
            }
            let mut parents = Vec::with_capacity(d.fks.len());
            for f in &d.fks {
                let pdb = match &f.ref_db {
                    None => Some(db),
                    Some(n) => cx.names.db_by_name(n),
                };
                let parent = pdb
                    .and_then(|pdb| cx.names.table(pdb, &f.ref_table))
                    .and_then(|p| p.as_ref().as_ref().ok().cloned())
                    .and_then(|p| {
                        let cols: Option<Vec<usize>> =
                            f.ref_cols.iter().map(|c| p.column(c)).collect();
                        let key = key_on(&p, &cols?)?;
                        Some(Parent { def: p, key })
                    });
                parents.push(parent);
            }
            let mut children = Vec::new();
            for (child, fk) in cx.names.referencing(db, &d.name) {
                let f = &child.fks[fk];
                let Some(ref_cols) = f
                    .ref_cols
                    .iter()
                    .map(|c| d.column(c))
                    .collect::<Option<Vec<usize>>>()
                else {
                    continue;
                };
                let Some(key) = key_on(&child, &f.cols) else {
                    continue;
                };
                children.push(Child {
                    object: child.object,
                    fk,
                    key,
                    ref_cols,
                });
                let cdb = child.db;
                queue.push((child, cdb));
            }
            g.tables.insert(
                d.object,
                Node {
                    def: Arc::clone(&d),
                    db_name: cx.names.db_name(db),
                    parents,
                    children,
                },
            );
        }
        Some(Arc::new(g))
    }
}

/// 1452: a child row without its parent.
fn no_parent(node: &Node, fk: usize, mysql: bool) -> SprigError {
    let def = &node.def;
    SprigError::standard(
        1452,
        *b"23000",
        format!(
            "Cannot add or update a child row: a foreign key constraint fails (`{}`.`{}`, {})",
            node.db_name,
            def.name,
            def.fks[fk].text(def, mysql)
        ),
    )
}

/// 1451: a parent row its children still reference.
fn referenced(child: &Node, fk: usize, mysql: bool) -> SprigError {
    let def = &child.def;
    SprigError::standard(
        1451,
        *b"23000",
        format!(
            "Cannot delete or update a parent row: a foreign key constraint fails (`{}`.`{}`, {})",
            child.db_name,
            def.name,
            def.fks[fk].text(def, mysql)
        ),
    )
}

/// A cascade that would reach its fifteenth level: MariaDB's 1296 naming
/// the constraint where it stops, MySQL's 3008.
fn too_deep(child: &Node, fk: usize, mysql: bool) -> SprigError {
    if mysql {
        return SprigError::standard(
            3008,
            *b"HY000",
            "Foreign key cascade delete/update exceeds max depth of 15.",
        );
    }
    let def = &child.def;
    SprigError::standard(
        1296,
        *b"HY000",
        format!(
            "Got error 193 '`{}`.`{}`, {}' from InnoDB",
            child.db_name,
            def.name,
            def.fks[fk].text(def, false)
        ),
    )
}

/// The prefix of `key` of `def` in `sp`, and its parts.
fn key_space<'d>(
    sp: &Space,
    def: &'d TableDef,
    key: KeyOf,
) -> (Vec<u8>, &'d [sprig_engine::keys::typed::KeyColumn]) {
    match key {
        KeyOf::Primary => (sp.rows(), &def.pk_parts[..]),
        KeyOf::Index(ix) => (sp.index(def, ix), &def.indexes[ix].parts[..]),
    }
}

/// `vals` as the first parts of a key: `None` when a value can't be one
/// (no row can hold it, so none matches).
fn key_prefix(
    mut prefix: Vec<u8>,
    parts: &[sprig_engine::keys::typed::KeyColumn],
    vals: &[&Datum],
) -> Option<Vec<u8>> {
    for (v, kc) in vals.iter().zip(parts) {
        if !keys::put_part(&mut prefix, v, kc) {
            return None;
        }
    }
    Some(prefix)
}

/// What a statement's cascades are in the middle of.
#[derive(Debug, Default)]
pub(crate) struct Chain {
    /// The tables being updated higher up (the statement's own table for
    /// an `UPDATE`): a cascaded update that comes back to one of them is
    /// refused.
    updating: Vec<ObjectId>,
    /// The rows being deleted or updated higher up: a cascade that comes
    /// back to one of them (a row that references itself) leaves it be.
    rows: Vec<Bytes>,
}

impl Chain {
    /// The chain of a statement's change of its row at `key` of table
    /// `object` (`update`: an `UPDATE`).
    pub(crate) fn of(object: ObjectId, key: &Bytes, update: bool) -> Self {
        Self {
            updating: if update { vec![object] } else { Vec::new() },
            rows: vec![key.clone()],
        }
    }
}

type Pending<'a> = Pin<Box<dyn Future<Output = Result<()>> + Send + 'a>>;

impl Engine {
    /// The keyspace of `def` for the statement's transaction (held by it).
    async fn space_in(&self, tenant: u64, tx: &mut Tx, def: &Arc<TableDef>) -> Result<Space> {
        if let Some(sp) = tx.held(def) {
            return Ok(sp);
        }
        let sp = self.space(tenant, def).await?;
        tx.hold(def, &sp);
        Ok(sp)
    }

    /// Whether every row of `child` whose columns `cols` hold no `NULL`
    /// has a row of `parent` with the same values in columns `pcols` (read
    /// at a snapshot; `ALTER TABLE ... ADD FOREIGN KEY` holds the table):
    /// the number of rows when they all do, `None` at the first that
    /// doesn't.
    ///
    /// # Errors
    /// A store error.
    pub(crate) async fn fk_rows_hold(
        &self,
        tenant: u64,
        child: &TableDef,
        cols: &[usize],
        parent: &TableDef,
        pcols: &[usize],
    ) -> Result<Option<u64>> {
        let csp = self.space(tenant, child).await?;
        let Some(pkey) = key_on(parent, pcols) else {
            return Ok(Some(self.row_count(tenant, child, u64::MAX).await?));
        };
        let psp = self.space(tenant, parent).await?;
        let (pprefix, parts) = key_space(&psp, parent, pkey);
        let whole = match pkey {
            KeyOf::Primary => parent.pk.len() == pcols.len(),
            KeyOf::Index(ix) => {
                let x = &parent.indexes[ix];
                x.unique && x.cols.len() == pcols.len()
            }
        };
        let mut tx = self.begin(TxnOptions::default())?;
        let rows = csp.rows();
        let end = keys::prefix_end(&rows);
        let mut lo = rows;
        let mut pace = Pace::default();
        let mut n = 0u64;
        loop {
            let batch = tx.scan(&lo, &end, false, WALK_BATCH)?;
            n += batch.len() as u64;
            for (k, v) in &batch {
                let r = row::decode(child, &csp, k, v)?;
                if cols.iter().any(|&c| r[c].is_null()) {
                    continue;
                }
                let vals: Vec<&Datum> = cols.iter().map(|&c| &r[c]).collect();
                let Some(key) = key_prefix(pprefix.clone(), parts, &vals) else {
                    return Ok(None);
                };
                let found = if whole {
                    tx.get(&key)?.is_some()
                } else {
                    !tx.scan(&key, &keys::prefix_end(&key), false, 1)?.is_empty()
                };
                if !found {
                    return Ok(None);
                }
            }
            pace.add(batch.len()).await;
            match batch.last() {
                Some((k, _)) if batch.len() == WALK_BATCH => lo = keys::prefix_end(k),
                _ => return Ok(Some(n)),
            }
        }
    }

    /// Child side: `row` of table `object` was inserted (`old` is `None`)
    /// or updated from `old`. Each foreign key whose columns the statement
    /// set (on an update: changed) and hold no `NULL` needs its parent.
    ///
    /// # Errors
    /// 1452; a lock error.
    pub(crate) async fn fk_parents(
        &self,
        tenant: u64,
        tx: &mut Tx,
        g: &Graph,
        object: ObjectId,
        row: &[Datum],
        old: Option<&[Datum]>,
    ) -> Result<()> {
        let Some(node) = g.tables.get(&object) else {
            return Ok(());
        };
        let mysql = tx.env().mysql;
        for (i, f) in node.def.fks.iter().enumerate() {
            if f.cols.iter().any(|&c| row[c].is_null()) {
                continue;
            }
            if let Some(old) = old
                && f.cols.iter().all(|&c| old[c] == row[c])
            {
                continue;
            }
            let Some(parent) = &node.parents[i] else {
                return Err(no_parent(node, i, mysql));
            };
            let psp = self.space_in(tenant, tx, &parent.def).await?;
            let (prefix, parts) = key_space(&psp, &parent.def, parent.key);
            let vals: Vec<&Datum> = f.cols.iter().map(|&c| &row[c]).collect();
            let Some(k) = key_prefix(prefix, parts, &vals) else {
                return Err(no_parent(node, i, mysql));
            };
            // The whole primary key, or a whole unique key without NULLs:
            // one entry. Otherwise the first entry that starts with the
            // values.
            let whole = match parent.key {
                KeyOf::Primary => parent.def.pk.len() == f.cols.len(),
                KeyOf::Index(ix) => {
                    let x = &parent.def.indexes[ix];
                    x.unique && x.cols.len() == f.cols.len()
                }
            };
            let found = if whole {
                matches!(
                    tx.lock_key(&Bytes::from(k), LockMode::Shared).await?,
                    Some(Some(_))
                )
            } else {
                let hi = keys::prefix_end(&k);
                !tx.scan_lock_entries(&k, &hi, 1, LockMode::Shared, Wait::Block)
                    .await?
                    .is_empty()
            };
            if !found {
                return Err(no_parent(node, i, mysql));
            }
        }
        Ok(())
    }

    /// Parent side: row `old` of table `object` is deleted (`new` is
    /// `None`) or updated to `new`. Each child that references a key the
    /// change removes is refused (1451) or acted on, before the parent's
    /// own write. `chain`: what the cascades are in the middle of.
    ///
    /// # Errors
    /// 1451; a child's own errors (a duplicate key a cascaded update
    /// makes); a lock error.
    #[allow(clippy::too_many_arguments, clippy::too_many_lines)] // one pass per referencing key
    pub(crate) fn fk_children<'a>(
        &'a self,
        tenant: u64,
        tx: &'a mut Tx,
        g: &'a Graph,
        object: ObjectId,
        old: &'a [Datum],
        new: Option<&'a [Datum]>,
        depth: u32,
        chain: &'a mut Chain,
    ) -> Pending<'a> {
        Box::pin(async move {
            let Some(node) = g.tables.get(&object) else {
                return Ok(());
            };
            let mysql = tx.env().mysql;
            for ch in &node.children {
                // Rows whose key holds a NULL are referenced by nothing; an
                // update that keeps the key's bytes changes nothing.
                if ch.ref_cols.iter().any(|&c| old[c].is_null()) {
                    continue;
                }
                if let Some(new) = new
                    && ch.ref_cols.iter().all(|&c| old[c] == new[c])
                {
                    continue;
                }
                let Some(cnode) = g.tables.get(&ch.object) else {
                    continue;
                };
                let f = &cnode.def.fks[ch.fk];
                let action = if new.is_some() {
                    f.update_action()
                } else {
                    f.delete_action()
                };
                let csp = self.space_in(tenant, tx, &cnode.def).await?;
                let (prefix, parts) = key_space(&csp, &cnode.def, ch.key);
                let vals: Vec<&Datum> = ch.ref_cols.iter().map(|&c| &old[c]).collect();
                let Some(lo) = key_prefix(prefix, parts, &vals) else {
                    continue;
                };
                let hi = keys::prefix_end(&lo);
                if !action.acts() {
                    let any = tx
                        .scan_lock_entries(&lo, &hi, 1, LockMode::Shared, Wait::Block)
                        .await?;
                    if !any.is_empty() {
                        return Err(referenced(cnode, ch.fk, mysql));
                    }
                    continue;
                }
                // A cascaded update (or `SET NULL`) into a table already
                // being updated higher up the chain acts as `RESTRICT`.
                let updates_child =
                    action == FkAction::SetNull || (new.is_some() && action == FkAction::Cascade);
                if updates_child && chain.updating.contains(&ch.object) {
                    let any = tx
                        .scan_lock_entries(&lo, &hi, 1, LockMode::Shared, Wait::Block)
                        .await?;
                    if !any.is_empty() {
                        return Err(referenced(cnode, ch.fk, mysql));
                    }
                    continue;
                }
                // The child rows, each locked exclusively.
                let mut targets: Vec<Bytes> = Vec::new();
                let mut from = lo;
                let mut pace = Pace::default();
                loop {
                    let batch = tx
                        .scan_lock_entries(&from, &hi, BATCH, LockMode::Exclusive, Wait::Block)
                        .await?;
                    let more = batch.len() == BATCH;
                    for (k, v) in &batch {
                        targets.push(match ch.key {
                            KeyOf::Primary => k.clone(),
                            KeyOf::Index(_) => {
                                let tail = keys::entry_target(&csp, &cnode.def, k, v).ok_or_else(
                                    || {
                                        SprigError::internal(
                                            "engine: an index entry doesn't decode",
                                        )
                                    },
                                )?;
                                let mut rk = csp.rows();
                                rk.extend_from_slice(tail);
                                Bytes::from(rk)
                            }
                        });
                    }
                    pace.add(batch.len()).await;
                    match batch.last() {
                        Some((k, _)) if more => from = keys::prefix_end(k),
                        _ => break,
                    }
                }
                targets.retain(|rk| !chain.rows.contains(rk));
                // The fifteenth level of a cascade is one too many.
                if !targets.is_empty() && depth + 1 >= MAX_DEPTH {
                    return Err(too_deep(cnode, ch.fk, mysql));
                }
                for rk in targets {
                    pace.add(1).await;
                    let Some(Some(raw)) = tx.lock_key(&rk, LockMode::Exclusive).await? else {
                        // Gone meanwhile (a cascade of this statement).
                        continue;
                    };
                    let crow = row::decode(&cnode.def, &csp, &rk, &raw)?;
                    let cnew: Option<Vec<Datum>> = match (new, action) {
                        (None, FkAction::Cascade) => None,
                        (Some(new), FkAction::Cascade) => {
                            let mut r = crow.clone();
                            for (&cc, &pc) in f.cols.iter().zip(&ch.ref_cols) {
                                let c = &cnode.def.columns[cc];
                                r[cc] = assign::constant(c, new[pc].clone()).ok_or_else(|| {
                                    SprigError::internal(
                                        "engine: a parent's key doesn't fit its child's column",
                                    )
                                })?;
                            }
                            Some(r)
                        }
                        _ => {
                            let mut r = crow.clone();
                            for &cc in &f.cols {
                                r[cc] = Datum::Null;
                            }
                            Some(r)
                        }
                    };
                    // The child's own children first.
                    if cnew.is_some() {
                        chain.updating.push(ch.object);
                    }
                    chain.rows.push(rk.clone());
                    let deeper = self
                        .fk_children(
                            tenant,
                            tx,
                            g,
                            ch.object,
                            &crow,
                            cnew.as_deref(),
                            depth + 1,
                            chain,
                        )
                        .await;
                    chain.rows.pop();
                    if cnew.is_some() {
                        chain.updating.pop();
                    }
                    deeper?;
                    match cnew {
                        None => exec::delete_row(&csp, &cnode.def, tx, rk, &crow).await?,
                        Some(r) => exec::replace_row(&csp, &cnode.def, tx, rk, &crow, &r).await?,
                    }
                }
            }
            Ok(())
        })
    }
}
