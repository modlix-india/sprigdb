//! Row-level security on the store: the policies that bind the session
//! running a command, per collection, as filters the store applies itself.
//!
//! ```text
//! the front end builds a RowGuard from the session's policies
//!   (their MongoDB forms, bound to the session's values) and runs the
//!   command inside guard::scope(Some(g), ...)
//! reads   every document a scan yields passes the collection's SELECT
//!         filter first (read::Scan::run): find, count, distinct,
//!         aggregate and its $lookup/$graphLookup/$unionWith reads, views,
//!         getMore, and the targets of update and delete
//! update  targets also pass UPDATE's USING; each new version passes
//!         UPDATE's WITH CHECK (else 39363)
//! delete  targets also pass DELETE's USING
//! insert  each new document passes INSERT's WITH CHECK (an upsert's too)
//! ```
//!
//! As in PostgreSQL, a document is visible when one applicable permissive
//! policy and every applicable restrictive one match it; no applicable
//! permissive policy shows nothing. Policies compare under the simple
//! collation, so a command's collation can't widen them. Collections that
//! don't bind the session aren't in the guard. Work outside a guarded
//! command (the TTL sweeper, index builds) is never filtered.

use std::collections::HashMap;
use std::future::Future;
use std::sync::Arc;

use bson::Document;
use sprig_proto_mongo::errors::MongoError;

use super::Coll;
use crate::filter::Pred;

/// What a set of policies lets through: a document passes when one of
/// `permissive` and every one of `restrictive` match it.
#[derive(Debug, Clone, Default)]
pub struct Filter {
    /// OR-ed (none: nothing passes).
    pub permissive: Vec<Pred>,
    /// AND-ed.
    pub restrictive: Vec<Pred>,
}

impl Filter {
    /// Nothing passes.
    #[must_use]
    pub fn hidden() -> Self {
        Self::default()
    }

    /// Whether `d` passes, compared under the simple collation.
    #[must_use]
    pub fn passes(&self, d: &Document) -> bool {
        crate::collation::simple(|| {
            self.permissive.iter().any(|p| p.matches(d))
                && self.restrictive.iter().all(|p| p.matches(d))
        })
    }
}

/// One collection's policies for the session, per command and clause.
#[derive(Debug, Clone, Default)]
pub struct CollPolicies {
    /// `SELECT`'s `USING`: what every read sees.
    pub select: Filter,
    /// `UPDATE`'s `USING`: what an update may touch.
    pub update_using: Filter,
    /// `UPDATE`'s `WITH CHECK` (else its `USING`): new versions.
    pub update_check: Filter,
    /// `DELETE`'s `USING`: what a delete may remove.
    pub delete_using: Filter,
    /// `INSERT`'s `WITH CHECK`: new documents.
    pub insert_check: Filter,
}

/// The collections whose policies bind a session, by engine object.
#[derive(Debug, Clone, Default)]
pub struct RowGuard {
    colls: HashMap<u32, Arc<CollPolicies>>,
}

impl RowGuard {
    /// A guard over `colls` (engine object → policies).
    #[must_use]
    pub fn new(colls: HashMap<u32, Arc<CollPolicies>>) -> Self {
        Self { colls }
    }

    /// Whether the collection with engine object `object` binds the
    /// session.
    #[must_use]
    pub fn binds(&self, object: u32) -> bool {
        self.colls.contains_key(&object)
    }

    /// Whether no collection binds the session.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.colls.is_empty()
    }

    /// The policies of the collection with engine object `object`.
    #[must_use]
    pub fn of(&self, object: u32) -> Option<&Arc<CollPolicies>> {
        self.colls.get(&object)
    }
}

/// What a scan's documents are read for.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Op {
    /// A read: `SELECT`'s policies.
    Read,
    /// An update's targets: `SELECT`'s and `UPDATE`'s.
    Update,
    /// A delete's targets: `SELECT`'s and `DELETE`'s.
    Delete,
}

tokio::task_local! {
    static GUARD: Arc<RowGuard>;
    static OP: Op;
}

/// Runs `f` (a command, or a cursor's next batch) under guard `g` (none:
/// unguarded).
pub async fn scope<F: Future>(g: Option<Arc<RowGuard>>, f: F) -> F::Output {
    match g.filter(|g| !g.is_empty()) {
        Some(g) => GUARD.scope(g, f).await,
        None => f.await,
    }
}

/// The guard of the command running on this task.
#[must_use]
pub fn current() -> Option<Arc<RowGuard>> {
    GUARD.try_with(Arc::clone).ok()
}

/// Runs the synchronous `f` (an update's or a delete's target scan) with
/// its scans read for `op`.
pub fn with_op<R>(op: Op, f: impl FnOnce() -> R) -> R {
    OP.sync_scope(op, f)
}

fn policies_of(c: &Coll) -> Option<Arc<CollPolicies>> {
    GUARD
        .try_with(|g| g.of(c.space.object).cloned())
        .ok()
        .flatten()
}

/// Whether the running command's policies bind collection `c`.
#[must_use]
pub fn binds(c: &Coll) -> bool {
    policies_of(c).is_some()
}

/// Whether document `d` of `c` shows to the scan running now (for the
/// operation [`with_op`] set; a read otherwise).
#[must_use]
pub fn sees(c: &Coll, d: &Document) -> bool {
    sees_for(c, d, OP.try_with(|o| *o).unwrap_or(Op::Read))
}

/// Whether document `d` of `c` shows to `op`.
#[must_use]
pub fn sees_for(c: &Coll, d: &Document, op: Op) -> bool {
    let Some(p) = policies_of(c) else {
        return true;
    };
    p.select.passes(d)
        && match op {
            Op::Read => true,
            Op::Update => p.update_using.passes(d),
            Op::Delete => p.delete_using.passes(d),
        }
}

/// 39363 unless new document `d` of `c` passes the `WITH CHECK` of an
/// insert (`insert`) or an update.
///
/// # Errors
/// 39363 `ER_SPRIG_RLS_CHECK_FAILED`.
pub fn check(c: &Coll, d: &Document, insert: bool) -> Result<(), MongoError> {
    let Some(p) = policies_of(c) else {
        return Ok(());
    };
    let f = if insert {
        &p.insert_check
    } else {
        &p.update_check
    };
    if f.passes(d) {
        return Ok(());
    }
    Err(MongoError::new(
        RLS_CHECK_FAILED,
        format!(
            "ER_SPRIG_RLS_CHECK_FAILED: new row violates row-level security policy for table '{}'",
            c.meta.name
        ),
    ))
}

/// `ER_SPRIG_RLS_CHECK_FAILED`.
pub const RLS_CHECK_FAILED: i32 = 39363;
/// `ER_SPRIG_RLS_UNSUPPORTED`.
pub const RLS_UNSUPPORTED: i32 = 39366;
