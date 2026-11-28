//! Retrieval indexes of engine databases.
//!
//! ```text
//! catalog  one record per index (sprig_ai::retrieval::rec), in the
//!          tenant's catalog records; no feed subscription
//! chunks   one engine table `db`.`__sprig_rx_<id>` beside the source
//!          (rx::create_engine_table_sql): the embedding a LONGBLOB of
//!          f32 values, FULLTEXT (text, title) the engine's own index
//! filling  the in-node job (super::sync) on the unit's leader
//! search   keyword: MATCH(text, title) on the chunk table, as the engine
//!            answers it (InnoDB's scoring); a caller bound by row-level
//!            security: every chunk with a query word (boolean mode, at
//!            most 1000), ranked by BM25 over the visible ones alone
//!          vector: an exact scan of the chunk table's vectors, cosine
//!            distance, in process (no vector index on the engine yet)
//!          then, as on MariaDB: the candidates' rows read back through
//!          the caller's session, fused (RRF, k = 60), at most two per row
//! ```
//!
//! The chunk table is read and written by the gateway's own statements
//! (internal mode) in the caller's session for searches and DDL, and in
//! the index creator's session for the job; no client statement can name
//! it.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use serde_json::json;
use sprig_ai::retrieval::rec;
use sprig_ai::retrieval::store::{NewIndex, RetrievalIndex};
use sprig_ai::retrieval::{self as rx};
use sprig_catalog::TenantCatalog;
use sprig_catalog::records::{RecordStore, RecordTx};
use sprig_common::ids::DbId;
use sprig_common::names::quote;
use sprig_common::{Result, SprigError};
use sprig_sql::ai_fn::string_literal;
use sprig_sql::retrieval::Call;

use super::{Candidate, Found, MAX_CANDIDATES, fuse, match_words, rank_visible};
use crate::exec::PassSession;
use crate::pipeline::{ExecCtx, Wire};

/// The tenant's catalog records for writes: a transaction runs on the node
/// that leads the tenant's unit, here or through its peer service, so a
/// replica that doesn't lead it never fails the commit (39070).
pub(crate) fn writers(svc: &crate::tenant::TenantServices) -> Arc<dyn RecordStore> {
    Arc::clone(&svc.writers)
}

/// Whether database `db` of the tenant is an engine database.
pub(crate) fn engine_db(cat: &TenantCatalog, db: DbId) -> bool {
    crate::engine::catalog::of(cat).is_some_and(|e| e.dbs.contains(&db))
}

/// Milliseconds since the epoch now.
pub(crate) fn now_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |d| i64::try_from(d.as_millis()).unwrap_or(i64::MAX))
}

/// One row of a chunk-table read as a candidate: key columns, then
/// `chunk_no`, `title`, `text` from column `nk` on.
pub(super) fn candidate(keys: &[rx::KeyColumn], r: &[Option<Vec<u8>>]) -> Candidate {
    let nk = keys.len();
    let text_of = |i: usize| {
        r.get(i)
            .and_then(Option::as_ref)
            .map(|b| String::from_utf8_lossy(b).into_owned())
    };
    Candidate {
        key: (0..nk)
            .map(|i| keys[i].text(r.get(i).and_then(Option::as_deref)))
            .collect(),
        chunk_no: text_of(nk).and_then(|s| s.parse().ok()).unwrap_or(0),
        title: text_of(nk + 1),
        text: text_of(nk + 2).unwrap_or_default(),
    }
}

/// The cosine distance of two unit vectors (1 − their dot product); 2 for
/// vectors of different lengths.
pub(crate) fn distance(a: &[f32], b: &[f32]) -> f64 {
    if a.len() != b.len() || a.is_empty() {
        return 2.0;
    }
    let dot: f32 = a.iter().zip(b).map(|(x, y)| x * y).sum();
    1.0 - f64::from(dot)
}

/// A chunk by its distance: (distance, the row's key as text, `chunk_no`).
type Near = (f64, Vec<Option<String>>, u32);

/// The `k` chunks of `rows` (key columns, `chunk_no`, `embedding`) nearest
/// `q`, nearest first; ties by key and number.
fn top_near(
    keys: &[rx::KeyColumn],
    rows: &[Vec<Option<Vec<u8>>>],
    q: &[f32],
    k: usize,
) -> Vec<Near> {
    let nk = keys.len();
    let mut all: Vec<Near> = rows
        .iter()
        .map(|r| {
            let key: Vec<Option<String>> = (0..nk)
                .map(|i| keys[i].text(r.get(i).and_then(Option::as_deref)))
                .collect();
            let no = r
                .get(nk)
                .and_then(Option::as_ref)
                .and_then(|b| std::str::from_utf8(b).ok()?.parse().ok())
                .unwrap_or(0);
            let d = r
                .get(nk + 1)
                .and_then(Option::as_deref)
                .and_then(rx::vector_of)
                .map_or(2.0, |v| distance(&v, q));
            (d, key, no)
        })
        .collect();
    let cmp = |a: &Near, b: &Near| {
        a.0.total_cmp(&b.0)
            .then_with(|| a.1.cmp(&b.1))
            .then(a.2.cmp(&b.2))
    };
    if all.len() > k && k > 0 {
        all.select_nth_unstable_by(k - 1, cmp);
        all.truncate(k);
    }
    all.sort_by(cmp);
    all
}

/// Vectors read per page of the exact scan.
const SCAN_PAGE: usize = 2_000;

/// The `k` nearest of `v`, nearest first.
fn keep_nearest(mut v: Vec<Near>, k: usize) -> Vec<Near> {
    v.sort_by(|a, b| {
        a.0.total_cmp(&b.0)
            .then_with(|| a.1.cmp(&b.1))
            .then(a.2.cmp(&b.2))
    });
    v.truncate(k);
    v
}

/// "After the chunk of row `r`" (its key columns, then `chunk_no`) in the
/// chunk table's key order.
fn keyset_after(keys: &[rx::KeyColumn], r: &[Option<Vec<u8>>]) -> String {
    let mut cols: Vec<(String, String)> = keys
        .iter()
        .enumerate()
        .map(|(i, k)| {
            (
                quote(&rx::key_column(&k.name)),
                k.literal(r.get(i).and_then(Option::as_deref)),
            )
        })
        .collect();
    let no = r
        .get(keys.len())
        .and_then(Option::as_ref)
        .and_then(|b| std::str::from_utf8(b).ok())
        .unwrap_or("0")
        .to_owned();
    cols.push(("`chunk_no`".into(), no));
    let mut ors = Vec::new();
    for i in 0..cols.len() {
        let mut and: Vec<String> = cols[..i]
            .iter()
            .map(|(c, v)| format!("{c} = {v}"))
            .collect();
        and.push(format!("{} > {}", cols[i].0, cols[i].1));
        ors.push(format!("({})", and.join(" AND ")));
    }
    ors.join(" OR ")
}

/// What a search over an engine chunk table needs from the statement.
pub(super) struct EngineSearch {
    /// The filter over the chunk table's copies.
    pub chunk_filter: Option<String>,
    /// The filter over the source table.
    pub table_filter: Option<String>,
    /// The query's embedding.
    pub q_vec: Option<Vec<f32>>,
    /// Candidates per list (`ai.retrieval.candidates`).
    pub per_list: usize,
    /// Row-level security binds the caller on the source table.
    pub bound: bool,
    /// Hits wanted.
    pub limit: usize,
    /// The keyword list's weight in fusion.
    pub weight: f64,
    /// The session's `NO_BACKSLASH_ESCAPES`.
    pub nbe: bool,
    /// `ai.retrieval.max_scan_chunks`.
    pub max_scan: u64,
}

impl PassSession {
    /// A transaction on the tenant's catalog records, on the unit's
    /// leader ([`writers`]).
    pub(super) async fn rx_records(&self) -> Result<Box<dyn RecordTx>> {
        let ts = self
            .tenant
            .as_deref()
            .ok_or_else(|| SprigError::internal("no tenant"))?;
        writers(ts.services()).begin(ts.tenant).await
    }

    /// `sql`'s rows, run as the gateway's own statement in this session
    /// (so it may name a chunk table); the statement's error returned.
    pub(super) async fn rx_internal_rows(
        &mut self,
        cx: &mut ExecCtx<'_>,
        wire: Wire,
        sql: &str,
    ) -> Result<Vec<Vec<Option<Vec<u8>>>>> {
        let before = self.tenant.as_deref_mut().map(|ts| {
            let b = ts.types.internal;
            ts.types.internal = true;
            b
        });
        let r = self.sub_rows(cx, wire, sql).await;
        if let (Some(b), Some(ts)) = (before, self.tenant.as_deref_mut()) {
            ts.types.internal = b;
        }
        r
    }

    /// `CREATE RETRIEVAL INDEX` on an engine database: the record, then
    /// the chunk table; the job fills it.
    pub(super) async fn rx_create_engine(
        &mut self,
        cx: &mut ExecCtx<'_>,
        wire: Wire,
        db: &str,
        table: &str,
        mut n: NewIndex,
        if_not_exists: bool,
    ) -> Result<()> {
        if n.created_by.is_none() {
            return Err(sprig_ai::embedding::failed(format!(
                "retrieval index {}: an engine database's index is filled as the user who \
                 creates it; this login has no tenant user",
                n.name
            )));
        }
        n.progress = json!({"since_ms": now_ms()});
        let mut tx = self.rx_records().await?;
        let Some(id) = rec::insert(tx.as_mut(), &n).await? else {
            drop(tx);
            if if_not_exists {
                return Ok(());
            }
            return Err(SprigError::standard(
                1061,
                *b"42000",
                format!(
                    "Duplicate key name '{}': {db} has a retrieval index of that name",
                    n.name
                ),
            ));
        };
        tx.commit().await?;
        let ddl = rx::create_engine_table_sql(db, id, &n.key_columns, &n.filters);
        if let Err(e) = self.rx_internal_rows(cx, wire, &ddl).await {
            if let Ok(mut tx) = self.rx_records().await
                && rec::delete(tx.as_mut(), id).await.is_ok()
            {
                let _ = tx.commit().await;
            }
            return Err(e);
        }
        super::sync::wake();
        let tenant = self.tenant.as_deref().map_or(0, |ts| ts.tenant.0);
        tracing::info!(tenant, %db, %table, index = id, name = %n.name,
            "retrieval index created on an engine database");
        Ok(())
    }

    /// `DROP RETRIEVAL INDEX` on an engine database: the record goes (so
    /// searches stop at once), then the chunk table.
    pub(super) async fn rx_drop_engine(
        &mut self,
        cx: &mut ExecCtx<'_>,
        wire: Wire,
        db: &str,
        ix: &RetrievalIndex,
    ) -> Result<()> {
        // Marked first (searches and listings stop at once), the chunk
        // table dropped, then the record: a failure leaves the record
        // marked, and the job finishes the drop.
        let mut tx = self.rx_records().await?;
        if !rec::mark_dropping(tx.as_mut(), ix.id).await? {
            return Ok(());
        }
        tx.commit().await?;
        let sql = format!(
            "DROP TABLE IF EXISTS {}.{}",
            quote(db),
            quote(&rx::table_name(ix.id))
        );
        self.rx_internal_rows(cx, wire, &sql).await?;
        let mut tx = self.rx_records().await?;
        rec::delete(tx.as_mut(), ix.id).await?;
        tx.commit().await?;
        let tenant = self.tenant.as_deref().map_or(0, |ts| ts.tenant.0);
        tracing::info!(event = "retrieval.chunks.dropped", tenant, db, index = %ix.name,
            id = ix.id, "retrieval index and its chunk table dropped");
        Ok(())
    }

    /// One `RETRIEVE()`'s search over an engine chunk table (see the
    /// module docs).
    #[allow(clippy::too_many_lines)] // the two lists, the check, the fusion
    #[allow(clippy::too_many_arguments)] // the call, the index and the statement's parts
    pub(super) async fn rx_search_engine(
        &mut self,
        cx: &mut ExecCtx<'_>,
        wire: Wire,
        call: &Call,
        ix: RetrievalIndex,
        db: String,
        table: String,
        s: EngineSearch,
    ) -> Result<(Found, &'static str)> {
        let keys = ix.key_columns.clone();
        let nk = keys.len();
        let kcols: Vec<String> = keys
            .iter()
            .map(|k| quote(&rx::key_column(&k.name)))
            .collect();
        let kc = kcols.join(", ");
        let t = format!("{}.{}", quote(&db), quote(&rx::table_name(ix.id)));
        let head = format!("{kc}, `chunk_no`, `title`, `text`");
        let q_lit = string_literal(&call.query, false);
        let m_lit = string_literal(&match_words(&call.query), false);
        let and_filter = s
            .chunk_filter
            .as_deref()
            .map_or_else(String::new, |f| format!(" AND ({f})"));
        let mode = if s.q_vec.is_some() {
            "hybrid"
        } else {
            "keyword"
        };
        // Every chunk's distance, nearest first, read once (an exact scan).
        // The nearest chunks, read once (an exact scan of at most
        // `max_scan` chunks; a larger index answers without a vector list).
        let mut nearest: Option<Vec<Near>> = None;
        let mut vector_skipped = None;
        if let Some(q) = &s.q_vec {
            // Pages in key order, so at most a page of vectors is held.
            let mut best: Vec<Near> = Vec::new();
            let mut after: Option<String> = None;
            let mut seen = 0u64;
            loop {
                let mut conds: Vec<String> =
                    s.chunk_filter.iter().map(|f| format!("({f})")).collect();
                if let Some(a) = &after {
                    conds.push(format!("({a})"));
                }
                let wh = if conds.is_empty() {
                    String::new()
                } else {
                    format!(" WHERE {}", conds.join(" AND "))
                };
                let rows = self
                    .rx_internal_rows(
                        cx,
                        wire,
                        &format!(
                            "SELECT {kc}, `chunk_no`, `embedding` FROM {t}{wh} \
                             ORDER BY {kc}, `chunk_no` LIMIT {SCAN_PAGE}"
                        ),
                    )
                    .await?;
                seen += rows.len() as u64;
                if seen > s.max_scan {
                    vector_skipped = Some(s.max_scan);
                    break;
                }
                best.extend(top_near(&keys, &rows, q, MAX_CANDIDATES));
                best = keep_nearest(best, MAX_CANDIDATES);
                if rows.len() < SCAN_PAGE {
                    nearest = Some(best);
                    break;
                }
                after = rows.last().map(|r| keyset_after(&keys, r));
            }
        }
        let mut per_list = s.per_list.max(1);
        let depth = per_list;
        let mut bound_kw: Option<(Vec<(f64, Candidate)>, usize)> = None;
        let mut texts: HashMap<(Vec<Option<String>>, u32), Candidate> = HashMap::new();
        loop {
            let mut kw: Vec<(f64, Candidate)> = Vec::new();
            let mut full = false;
            let mut over = 0usize;
            if !s.bound || bound_kw.is_none() {
                let sql = if s.bound {
                    format!(
                        "SELECT {head}, 0 AS s FROM {t} WHERE MATCH(`text`, `title`) AGAINST \
                         ({m_lit} IN BOOLEAN MODE){and_filter} ORDER BY {kc}, `chunk_no` LIMIT {}",
                        MAX_CANDIDATES + 1
                    )
                } else {
                    format!(
                        "SELECT {head}, MATCH(`text`, `title`) AGAINST ({q_lit} IN NATURAL LANGUAGE \
                         MODE) AS s FROM {t} WHERE MATCH(`text`, `title`) AGAINST ({q_lit} IN \
                         NATURAL LANGUAGE MODE){and_filter} ORDER BY s DESC LIMIT {per_list}"
                    )
                };
                let rows = self.rx_internal_rows(cx, wire, &sql).await?;
                if s.bound && rows.len() > MAX_CANDIDATES {
                    over = 1;
                } else {
                    if !s.bound && rows.len() >= per_list {
                        full = true;
                    }
                    for r in &rows {
                        let score: f64 = r
                            .get(nk + 3)
                            .and_then(Option::as_ref)
                            .and_then(|b| std::str::from_utf8(b).ok()?.parse().ok())
                            .unwrap_or(0.0);
                        kw.push((score, candidate(&keys, r)));
                    }
                }
            }
            if s.bound {
                match &bound_kw {
                    Some((k, o)) => {
                        kw.clone_from(k);
                        over = *o;
                    }
                    None => bound_kw = Some((kw.clone(), over)),
                }
                if over > 0 {
                    kw.clear();
                }
            }
            // The vector list: the first per_list of the scan, their text
            // read once.
            let mut vec_edge: Option<f64> = None;
            let mut vecs: Vec<(f64, Candidate)> = Vec::new();
            if let Some(all) = &nearest {
                let take: Vec<&Near> = all.iter().take(per_list).collect();
                if take.len() >= per_list {
                    full = true;
                    if s.bound {
                        vec_edge = take.last().map(|x| x.0);
                    }
                }
                let missing: Vec<&Near> = take
                    .iter()
                    .copied()
                    .filter(|x| !texts.contains_key(&(x.1.clone(), x.2)))
                    .collect();
                for part in missing.chunks(200) {
                    let pred: Vec<String> = part
                        .iter()
                        .map(|(_, key, no)| {
                            let mut eq: Vec<String> = keys
                                .iter()
                                .zip(key)
                                .map(|(c, v)| {
                                    format!(
                                        "{} = {}",
                                        quote(&rx::key_column(&c.name)),
                                        super::key_literal(c, v.as_deref(), false)
                                    )
                                })
                                .collect();
                            eq.push(format!("`chunk_no` = {no}"));
                            format!("({})", eq.join(" AND "))
                        })
                        .collect();
                    let rows = self
                        .rx_internal_rows(
                            cx,
                            wire,
                            &format!("SELECT {head} FROM {t} WHERE {}", pred.join(" OR ")),
                        )
                        .await?;
                    for r in &rows {
                        let c = candidate(&keys, r);
                        texts.insert((c.key.clone(), c.chunk_no), c);
                    }
                }
                for (d, key, no) in take {
                    if let Some(c) = texts.get(&(key.clone(), *no)) {
                        vecs.push((*d, c.clone()));
                    }
                }
            }
            kw.sort_by(|a, b| b.0.total_cmp(&a.0));
            // The rows the caller may see, through the session.
            let mut rows: Vec<Vec<Option<String>>> = Vec::new();
            let mut seen: HashSet<Vec<Option<String>>> = HashSet::new();
            for (_, c) in kw.iter().chain(&vecs) {
                if seen.insert(c.key.clone()) {
                    rows.push(c.key.clone());
                }
            }
            let visible = self
                .rx_visible(
                    cx,
                    wire,
                    &ix,
                    &db,
                    &table,
                    &rows,
                    s.table_filter.as_deref(),
                    s.nbe,
                )
                .await?;
            if s.bound {
                let kw = rank_visible(
                    &call.query,
                    kw.into_iter()
                        .map(|(_, c)| c)
                        .filter(|c| visible.contains(&c.key))
                        .collect(),
                    depth,
                );
                let mut vecs: Vec<Candidate> = vecs
                    .into_iter()
                    .filter(|(d, _)| vec_edge.is_none_or(|e| *d <= e))
                    .map(|(_, c)| c)
                    .filter(|c| visible.contains(&c.key))
                    .collect();
                if vecs.len() < depth && vec_edge.is_some() && per_list < MAX_CANDIDATES {
                    per_list = (per_list * 4).min(MAX_CANDIDATES);
                    continue;
                }
                vecs.truncate(depth);
                return Ok((
                    Found {
                        index: ix,
                        db,
                        table,
                        hits: fuse(&kw, &vecs, s.weight, s.limit),
                        shards: 1,
                        keyword_skipped: over,
                        vector_skipped,
                    },
                    mode,
                ));
            }
            let kw: Vec<Candidate> = kw
                .into_iter()
                .map(|(_, c)| c)
                .filter(|c| visible.contains(&c.key))
                .collect();
            let vecs: Vec<Candidate> = vecs
                .into_iter()
                .map(|(_, c)| c)
                .filter(|c| visible.contains(&c.key))
                .collect();
            let hits = fuse(&kw, &vecs, s.weight, s.limit);
            let hidden = rows.len() > visible.len();
            if hits.len() >= s.limit || !full || !hidden || per_list >= MAX_CANDIDATES {
                return Ok((
                    Found {
                        index: ix,
                        db,
                        table,
                        hits,
                        shards: 1,
                        keyword_skipped: 0,
                        vector_skipped,
                    },
                    mode,
                ));
            }
            per_list = (per_list * 4).min(MAX_CANDIDATES);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn distances_and_candidates() {
        assert!((distance(&[1.0, 0.0], &[1.0, 0.0])).abs() < 1e-9);
        assert!((distance(&[1.0, 0.0], &[0.0, 1.0]) - 1.0).abs() < 1e-9);
        assert!((distance(&[1.0], &[1.0, 0.0]) - 2.0).abs() < 1e-9);
        let keys = vec![
            rx::KeyColumn {
                name: "id".into(),
                column_type: "int".into(),
                binary: false,
                numeric: true,
            },
            rx::KeyColumn {
                name: "b".into(),
                column_type: "binary(2)".into(),
                binary: true,
                numeric: false,
            },
        ];
        let r = vec![
            Some(b"7".to_vec()),
            Some(vec![0xab, 0x01]),
            Some(b"3".to_vec()),
            None,
            Some(b"chunk".to_vec()),
        ];
        assert_eq!(
            keyset_after(&keys, &r),
            "(`k_id` > 7) OR (`k_id` = 7 AND `k_b` > X'ab01') OR (`k_id` = 7 AND `k_b` = X'ab01' AND `chunk_no` > 3)"
        );
        let c = candidate(&keys, &r);
        assert_eq!(c.key, vec![Some("7".into()), Some("ab01".into())]);
        assert_eq!(c.chunk_no, 3);
        assert_eq!(c.title, None);
        assert_eq!(c.text, "chunk");
    }
}
