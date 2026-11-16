//! Counts and facets over the rows a caller may see.
//!
//! A copy is filled without row-level security, so its counts and
//! facet buckets include rows a policy hides from the caller. Hits are
//! already re-read through the caller's session; counts and facets are
//! not, so for a caller that a policy on the indexed object binds:
//!
//! ```text
//! every hit of the query (39827 past search.max_window, without the count)
//! visible := the hits whose rows the caller's session reads
//! count  := |visible|
//! facets := the copy's facets over (query AND _key IN visible)
//! ```
//!
//! Ranked hits for such a caller are scored over the hits it may see, so
//! no score moves with rows hidden from it (the copy's term statistics
//! count every row):
//!
//! ```text
//! candidates(query): every hit; None past search.max_window
//! the front end keeps the hits whose rows its session reads
//! rescore(query, visible, k): the best k of them, with BM25 statistics
//!   of the visible hits alone (Scoring::Matched)
//! past the window: plain_top(query, k): the best k with no statistics of
//!   the index (Scoring::Plain); the front end leaves out hidden hits
//! ```
//!
//! Callers no policy binds keep the single call to the copy. A front end
//! that can't tell whether a policy binds (`None`) takes the visible path.

use std::sync::Arc;

use sprig_catalog::TenantCatalog;
use sprig_common::ids::TenantId;
use sprig_common::{Result, SprigCode, SprigError};
use sprig_search::def::IndexDef;
use sprig_search::query::SearchQuery;
use sprig_search::rpc::Op;
use sprig_search::schema::Key;
use sprig_search::{Hit, Scoring};

use super::{Consistency, SearchService};

/// Whether `binds` (a session's answer for the index's object) means the
/// visible path: bound, or unknown, or an index whose object isn't known.
#[must_use]
pub fn needs_visible(def: &IndexDef, binds: Option<bool>) -> bool {
    def.object_id == 0 || binds != Some(false)
}

/// The refusal past the window, worded without the number of matches
/// (which counts hidden rows).
#[must_use]
pub fn too_many(max: usize) -> SprigError {
    SprigError::new(
        SprigCode::SearchWindowExceeded,
        format!(
            "counts and facets for a user bound by row-level security on this table are taken \
             over the rows the user may see, which needs the query to match at most \
             search.max_window ({max}) rows; narrow the query or ask for no facets"
        ),
    )
}

/// Every hit of `query` (for re-reading), 39827 as [`too_many`].
///
/// # Errors
/// [`too_many`]; the copy's.
pub async fn all_hits(
    svc: &Arc<SearchService>,
    tenant: TenantId,
    cat: &TenantCatalog,
    def: &Arc<IndexDef>,
    query: SearchQuery,
    consistency: Consistency,
) -> Result<Vec<Hit>> {
    let max = svc.knobs().max_window;
    crate::exec::search::run_search(svc, tenant, cat, def, query, None, consistency)
        .await
        .map_err(|e| {
            if e.code() == Some(SprigCode::SearchWindowExceeded) {
                metrics::counter!("sprig_search_window_exceeded_total").increment(1);
                too_many(max)
            } else {
                e
            }
        })
}

/// Every hit of `query` for a bound caller to re-read; `None` when it
/// matches more than `search.max_window` documents.
///
/// # Errors
/// The copy's, other than 39827.
pub async fn candidates(
    svc: &Arc<SearchService>,
    tenant: TenantId,
    cat: &TenantCatalog,
    def: &Arc<IndexDef>,
    query: SearchQuery,
    consistency: Consistency,
) -> Result<Option<Vec<Hit>>> {
    match crate::exec::search::run_search(svc, tenant, cat, def, query, None, consistency).await {
        Ok(h) => Ok(Some(h)),
        Err(e) if e.code() == Some(SprigCode::SearchWindowExceeded) => Ok(None),
        Err(e) => Err(e),
    }
}

/// The best `k` (every one: `None`) of the `visible` hits of `query`,
/// scored with statistics of those hits alone.
///
/// # Errors
/// The copy's.
#[allow(clippy::too_many_arguments)]
pub async fn rescore(
    svc: &Arc<SearchService>,
    tenant: TenantId,
    cat: &TenantCatalog,
    def: &Arc<IndexDef>,
    query: SearchQuery,
    visible: Vec<Key>,
    k: Option<usize>,
    consistency: Consistency,
) -> Result<Vec<Hit>> {
    if visible.is_empty() {
        return Ok(Vec::new());
    }
    metrics::counter!("sprig_rls_search_rechecks_total", "scoring" => "matched").increment(1);
    let k = k.unwrap_or(visible.len()).min(visible.len());
    let op = Op::Top {
        k,
        scoring: Scoring::Matched,
    };
    crate::exec::search::run_op(
        svc,
        tenant,
        cat,
        def,
        within(query, visible),
        op,
        consistency,
    )
    .await
}

/// The best `k` hits of `query` with no statistics of the index (a bound
/// caller's search past the window); hidden ones included, for the front
/// end to leave out.
///
/// # Errors
/// The copy's.
pub async fn plain_top(
    svc: &Arc<SearchService>,
    tenant: TenantId,
    cat: &TenantCatalog,
    def: &Arc<IndexDef>,
    query: SearchQuery,
    k: usize,
    consistency: Consistency,
) -> Result<Vec<Hit>> {
    metrics::counter!("sprig_rls_search_rechecks_total", "scoring" => "plain").increment(1);
    let op = Op::Top {
        k,
        scoring: Scoring::Plain,
    };
    crate::exec::search::run_op(svc, tenant, cat, def, query, op, consistency).await
}

/// `query` restricted to the documents with these keys.
#[must_use]
pub fn within(query: SearchQuery, visible: Vec<Key>) -> SearchQuery {
    SearchQuery::Bool {
        must: vec![query],
        should: Vec::new(),
        must_not: Vec::new(),
        filter: vec![SearchQuery::Keys(visible)],
        min_should: 0,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use sprig_search::def::{Flavor, Model};

    #[test]
    fn only_known_unbound_callers_skip_the_visible_path() {
        let mut d = IndexDef::new("ix", "db", "t", Model::Table, Flavor::Sql);
        d.object_id = 42;
        assert!(!needs_visible(&d, Some(false)));
        assert!(needs_visible(&d, Some(true)));
        assert!(needs_visible(&d, None));
        d.object_id = 0;
        assert!(needs_visible(&d, Some(false)));
        let e = too_many(10_000);
        assert_eq!(e.code(), Some(SprigCode::SearchWindowExceeded));
        assert!(
            e.message.contains("search.max_window (10000)"),
            "{}",
            e.message
        );
        match within(SearchQuery::All, vec![Key(vec![1])]) {
            SearchQuery::Bool { must, filter, .. } => {
                assert_eq!(must, vec![SearchQuery::All]);
                assert_eq!(filter, vec![SearchQuery::Keys(vec![Key(vec![1])])]);
            }
            other => panic!("{other:?}"),
        }
    }
}
