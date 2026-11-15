//! Global lookup indexes in the sharding loop (chapters 13, 16 and 19):
//! the advisor's proposals and the recovery of stopped builds.
//!
//! ```text
//! lookup_tick(tenant), after the groups' ticks:
//!   1. resume: every lookup job no one touched for lookup.build_lease_seconds
//!      (its gateway or controller died) is taken over and finished, and
//!      lookups whose table was dropped are dropped
//!   2. at most every advisor.lookup_eval_minutes (5), the advisor:
//!      fingerprints of the last advisor.lookup_window_minutes (60) that ran
//!      at least advisor.lookup_min_runs (100) times, at least
//!      auto_index.lookup_scatter_pct (80%) of them on every shard of a
//!      sharded group, with `=` or `IN` on one column of a single table;
//!      a column that carries a declared single-column UNIQUE key without
//!      the distribution key and has no lookup yet is a candidate
//!   3. each candidate goes through the approvals gate as `advisor.lookup`
//!      (advisor.mode: on builds at once; approve and recommend raise an
//!      item and build once it is approved); the build is
//!      sprig_shard::lookup::admin's, as for ADD UNIQUE
//!   4. a build that fails on values repeating across shards raises an
//!      `advisor.lookup` item `.../lookup/<key>/duplicates` with a sample of
//!      the values, held for an approval; while it is open its column is
//!      not proposed again (no build loop); once approved (the values were
//!      fixed), it is built at the next evaluation and the item gets the
//!      result
//! ```

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime};

use mysql_async::prelude::Queryable as _;
use mysql_async::{Conn, Pool};
use sprig_common::ids::{ClusterId, DbId, GroupId, ObjectId, TenantId};
use sprig_common::names::{SchemaName, quote};
use sprig_common::{Result, SprigError};
use sprig_shard::lookup::admin::{
    Admin, DUPLICATES_TARGET_SUFFIX, DuplicateReport, LookupHost, NewLookup, lookup_keys,
    owner_shape,
};
use sprig_shard::lookup::direct::Pools;
use sprig_shard::lookup::{LookupCatalog, LookupOrigin};
use sprig_shard::shard_schema;
use sprig_workload::shape::PredOp;
use sprig_workload::store::WorkloadTables;

use super::{ShardingLoop, TenantSettings, lock};

/// How long a publish waits for gateways to load the new catalog version
/// when their acknowledgements can't be used.
pub(super) const PUBLISH_SETTLE: Duration = Duration::from_millis(2500);

/// A column the workload reads by equality across every shard.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(super) struct Scatter {
    /// Runs in the window.
    pub(super) runs: u64,
    /// Of them, on every shard.
    pub(super) scattered: u64,
    /// Their time, µs.
    pub(super) total_us: u64,
    /// Fingerprints.
    pub(super) fingerprints: Vec<u64>,
}

/// Folds one fingerprint into the per-column tally: single-table shapes
/// with `=` or `IN` on a column, by `(object, column)`.
pub(super) fn tally(
    out: &mut BTreeMap<(ObjectId, String), Scatter>,
    fp: u64,
    shape: &sprig_workload::shape::Shape,
    runs: u64,
    scattered: u64,
    total_us: u64,
) {
    let [t] = shape.tables.as_slice() else {
        return;
    };
    let Some(object) = t.object_id else {
        return;
    };
    let mut cols: Vec<String> = shape
        .predicates
        .iter()
        .filter(|p| matches!(p.op, PredOp::Eq | PredOp::In { .. }))
        .map(|p| p.column.col.to_ascii_lowercase())
        .collect();
    cols.sort();
    cols.dedup();
    for c in cols {
        let e = out.entry((object, c)).or_default();
        e.runs += runs;
        e.scattered += scattered;
        e.total_us += total_us;
        e.fingerprints.push(fp);
    }
}

/// Whether a tally qualifies (`advisor.lookup_min_runs`,
/// `auto_index.lookup_scatter_pct`).
#[must_use]
pub(super) fn qualifies(s: &Scatter, a: &sprig_advisor::sharding::settings::LookupAdvice) -> bool {
    a.qualifies(s.runs, s.scattered)
}

/// [`LookupHost`] for the controller: the catalog on the tenant's home
/// cluster, the loop's writer pools, and the loop's publish.
struct CtlHost<'a> {
    l: &'a ShardingLoop,
    tenant: TenantId,
    home: ClusterId,
    /// `lookup.publish_timeout_ms`.
    publish_timeout: Duration,
}

#[async_trait::async_trait]
impl LookupHost for CtlHost<'_> {
    async fn catalog(&self) -> Result<Conn> {
        let mut c = self.l.conn(&self.home).await?;
        c.query_drop(format!(
            "USE {}",
            quote(&SchemaName::catalog(self.tenant).to_string())
        ))
        .await
        .map_err(|e| SprigError::internal(format!("catalog: {e}")))?;
        Ok(c)
    }

    async fn pools(&self, clusters: &[ClusterId]) -> Result<Pools> {
        let mut m: std::collections::HashMap<ClusterId, Pool> = std::collections::HashMap::new();
        for c in clusters {
            m.insert(c.clone(), self.l.pools.pool(c).await?);
        }
        Ok(Arc::new(m))
    }

    async fn publish(&self) -> Result<()> {
        let Some(p) = &self.l.publish else {
            return Ok(());
        };
        let Some(v) = p.catalog_changed_to(self.tenant).await? else {
            tokio::time::sleep(PUBLISH_SETTLE).await;
            return Ok(());
        };
        // Every gateway of the DC acknowledges the version; laggards are
        // fenced past lookup.publish_timeout_ms.
        let store = Arc::clone(self.l.maps.store());
        match sprig_shard::lookup::publish::wait_for_gateways(
            &store,
            self.tenant,
            v,
            self.publish_timeout,
        )
        .await
        {
            Ok(o) if o.fenced.is_empty() => {}
            Ok(o) => {
                tracing::warn!(tenant = %self.tenant, version = v, fenced = ?o.fenced,
                    "lookup job: gateways fenced");
            }
            Err(e) => {
                tracing::warn!(tenant = %self.tenant, error = %e,
                    "lookup job: acknowledgements unavailable; a fixed wait instead");
                tokio::time::sleep(PUBLISH_SETTLE).await;
            }
        }
        Ok(())
    }

    async fn duplicates(&self, r: &DuplicateReport) {
        let Some(a) = self.l.approvals.get() else {
            return;
        };
        let db_name = self
            .db_name(r.db)
            .await
            .unwrap_or_else(|| format!("d{}", r.db.0));
        let mut subject = a.subject(self.tenant).await;
        subject.db = Some(r.db);
        subject.db_name = Some(db_name.clone());
        subject.object = Some(r.owner);
        subject.object_name = Some(r.table.clone());
        #[allow(clippy::cast_precision_loss)] // a count of values
        let score = r.found.values as f64;
        let p = sprig_approvals::Proposal {
            kind: sprig_approvals::Kind::AdvisorLookup,
            subject,
            target: r.target(self.tenant),
            action: sprig_approvals::Action {
                text: r.action_text(&db_name),
                machine: r.machine(),
            },
            evidence: r.evidence(),
            score,
            summary: r.summary(&db_name, false),
            emergency: false,
        };
        let v = a.gate(&p, &duplicates_policy()).await;
        tracing::info!(tenant = %self.tenant, table = %r.table, key = %r.key,
            values = r.found.values, verdict = ?v, "approvals item raised for a lookup's duplicates");
    }
}

/// How a duplicates item is held: for an approval, with no timers, under
/// any `advisor.mode` (it reports a failure; only a person can fix it).
fn duplicates_policy() -> sprig_approvals::Policy {
    sprig_approvals::Policy::fixed(
        sprig_approvals::Mode::Approve,
        sprig_approvals::model::Level::Database,
    )
}

impl CtlHost<'_> {
    /// Database `db`'s name, from the catalog.
    async fn db_name(&self, db: DbId) -> Option<String> {
        let mut c = self.catalog().await.ok()?;
        c.exec_first("SELECT name FROM `databases` WHERE db_id = ?", (db.0,))
            .await
            .ok()
            .flatten()
    }
}

/// A member row as the candidate check reads it: group, database,
/// routing, key columns, `rk_state`, table, database name.
type MemberText = (u32, u32, String, Option<String>, String, String, String);

/// One proposal.
#[derive(Debug, Clone)]
struct Candidate {
    object: ObjectId,
    db: DbId,
    db_name: String,
    table: String,
    group: GroupId,
    key: String,
    column: String,
    scatter: Scatter,
}

impl ShardingLoop {
    /// Step 4 of a tick: lookup jobs to resume, then the advisor's
    /// lookup proposals (module docs).
    ///
    /// # Errors
    /// Catalog errors; a failed build is logged and reported to its
    /// approval item.
    pub(super) async fn lookup_tick(
        &self,
        tenant: TenantId,
        home: &ClusterId,
        ts: &TenantSettings,
    ) -> Result<()> {
        let host = CtlHost {
            l: self,
            tenant,
            home: home.clone(),
            publish_timeout: ts.lookup.publish_timeout,
        };
        let admin = Admin {
            tenant,
            maps: &self.maps,
            host: &host,
            chunk: ts.lookup.chunk_rows,
            lease: ts.lookup.build_lease,
        };
        for (id, what) in admin.resume().await? {
            tracing::info!(tenant = %tenant, lookup = id.0, outcome = %what, "lookup job resumed");
        }
        {
            let mut last = lock(&self.lookup_runs);
            if last
                .get(&tenant)
                .is_some_and(|t| t.elapsed() < ts.lookup_advice.every)
            {
                return Ok(());
            }
            last.insert(tenant, Instant::now());
        }
        // Duplicates items: approved ones are built; open ones hold their
        // column back from new proposals.
        let held = self.duplicates_items(tenant, &admin).await;
        if ts.advisor.mode == sprig_advisor::sharding::AdvisorMode::Off {
            return Ok(());
        }
        for c in self.lookup_candidates(tenant, home, ts).await? {
            if held.contains(&(c.object, c.key.to_ascii_lowercase())) {
                tracing::debug!(tenant = %tenant, table = %c.table, key = %c.key,
                    "lookup proposal held: its duplicates item is open");
                continue;
            }
            self.propose_lookup(tenant, ts, &admin, &c).await;
        }
        Ok(())
    }

    /// The tenant's live `advisor.lookup` items that report duplicates:
    /// each approved one is built now (the item gets the result; a build
    /// that fails again raises a new item); returns the `(table, key)` of
    /// every one still open, whose columns aren't proposed again.
    async fn duplicates_items(
        &self,
        tenant: TenantId,
        admin: &Admin<'_>,
    ) -> std::collections::BTreeSet<(ObjectId, String)> {
        use sprig_approvals::{Kind, Proposal, State, Verdict};
        let mut held = std::collections::BTreeSet::new();
        let Some(approvals) = self.approvals.get() else {
            return held;
        };
        let hub = approvals.hub();
        let subject = approvals.subject(tenant).await;
        let items = match hub.store_of(Kind::AdvisorLookup, &subject) {
            Ok(store) => {
                let f = sprig_approvals::Filter {
                    kinds: vec![Kind::AdvisorLookup.as_str().to_owned()],
                    ..sprig_approvals::Filter::default()
                };
                hub.list(store.as_ref(), &f).await.unwrap_or_default()
            }
            Err(_) => Vec::new(),
        };
        for r in items
            .into_iter()
            .filter(|r| r.target.ends_with(DUPLICATES_TARGET_SUFFIX))
        {
            let m = &r.action.machine;
            let (Some(object), Some(key)) = (
                m.get("object").and_then(serde_json::Value::as_u64),
                m.get("key").and_then(serde_json::Value::as_str),
            ) else {
                continue;
            };
            let object = ObjectId(object);
            if r.state != State::Approved {
                held.insert((object, key.to_ascii_lowercase()));
                continue;
            }
            // Approved: the gate hands out its ticket, then the build runs.
            let p = Proposal {
                kind: r.kind,
                subject: r.subject.clone(),
                target: r.target.clone(),
                action: r.action.clone(),
                evidence: r.evidence.clone(),
                score: r.score,
                summary: r.summary.clone(),
                emergency: false,
            };
            let Verdict::Proceed(ticket) = approvals.gate(&p, &duplicates_policy()).await else {
                held.insert((object, key.to_ascii_lowercase()));
                continue;
            };
            let columns: Vec<String> = m
                .get("columns")
                .and_then(serde_json::Value::as_array)
                .map(|v| {
                    v.iter()
                        .filter_map(|c| c.as_str().map(str::to_owned))
                        .collect()
                })
                .unwrap_or_default();
            let r = self
                .build_for_item(tenant, admin, object, key, columns)
                .await;
            let (ok, detail) = match &r {
                Ok(t) => (true, t.clone()),
                Err(e) => (false, format!("lookup index {key} failed: {e}")),
            };
            tracing::info!(tenant = %tenant, key, ok, detail = %detail, "approved duplicates item built");
            self.approval_done(Some(&ticket), ok, &detail).await;
        }
        held
    }

    /// Builds `object`'s unique lookup `key` on `columns`, as an approved
    /// item asks.
    async fn build_for_item(
        &self,
        tenant: TenantId,
        admin: &Admin<'_>,
        object: ObjectId,
        key: &str,
        columns: Vec<String>,
    ) -> Result<String> {
        let mut c = admin.host.catalog().await?;
        let member: Option<(u32, u32, String, String, String)> = c
            .exec_first(
                "SELECT m.group_id, m.db_id, CAST(m.routing AS CHAR), CAST(m.rk_state AS CHAR), \
                 o.name FROM group_members m JOIN table_groups g ON g.group_id = m.group_id \
                 JOIN objects o ON o.object_id = m.object_id \
                 WHERE m.object_id = ? AND g.kind = 'sharded' AND o.state = 'active'",
                (object.0,),
            )
            .await
            .map_err(|e| SprigError::internal(format!("catalog: {e}")))?;
        drop(c);
        let Some((group, db, routing, rk, table)) = member else {
            return Err(SprigError::internal(format!(
                "table {} isn't a sharded table any more",
                object.0
            )));
        };
        if routing != "columns" || rk != "ready" || columns.is_empty() {
            return Err(SprigError::internal(format!(
                "{table} can't hold a lookup index now (routing {routing}, rk {rk})"
            )));
        }
        // Built meanwhile (the statement ran again): done.
        let mut c = admin.host.catalog().await?;
        let built = LookupCatalog
            .rows(&mut c)
            .await?
            .into_iter()
            .any(|r| r.owner == object && r.index_name.eq_ignore_ascii_case(key));
        drop(c);
        if built {
            return Ok(format!("lookup index {key} exists already"));
        }
        let (map, _) = self.maps.load(tenant, GroupId(group)).await?;
        // The key must still be declared on the shards (a failed ADD
        // UNIQUE dropped it): then its lookup makes it unique across them.
        let declared = match map.ranges.first() {
            Some(r) => {
                let schema = shard_schema(tenant, DbId(db), GroupId(group), r.shard);
                let mut sc = self.conn(&r.cluster).await?;
                owner_shape(&mut sc, &schema, &table)
                    .await
                    .map(|shape| {
                        lookup_keys(&shape, &[])
                            .iter()
                            .any(|(k, cols)| k.eq_ignore_ascii_case(key) && *cols == columns)
                    })
                    .unwrap_or(false)
            }
            None => false,
        };
        if !declared {
            return Err(SprigError::internal(format!(
                "{table} has no unique key {key} ({}) now; add it with ALTER TABLE ... ADD \
                 UNIQUE KEY once the values are unique, which builds its lookup index",
                columns.join(", ")
            )));
        }
        let (l, rep) = admin
            .create(&NewLookup {
                db: DbId(db),
                owner: object,
                owner_table: table,
                owner_map: Arc::new(map),
                columns,
                key: Some(key.to_owned()),
                origin: LookupOrigin::UniqueKey,
            })
            .await?;
        Ok(format!(
            "lookup index {} built: {} rows, {} repaired",
            l.index_name, rep.rows, rep.repaired
        ))
    }

    /// The columns worth a unique lookup now (module docs).
    async fn lookup_candidates(
        &self,
        tenant: TenantId,
        home: &ClusterId,
        ts: &TenantSettings,
    ) -> Result<Vec<Candidate>> {
        let mut c = self.conn(home).await?;
        let cat = SchemaName::catalog(tenant).to_string();
        let has: Option<u64> = c
            .exec_first(
                "SELECT COUNT(*) FROM information_schema.TABLES WHERE TABLE_SCHEMA = ? \
                 AND TABLE_NAME IN ('workload_stats', 'lookup_indexes')",
                (cat.as_str(),),
            )
            .await
            .map_err(|e| SprigError::internal(format!("catalog: {e}")))?;
        if has.unwrap_or(0) < 2 {
            return Ok(Vec::new());
        }
        let now = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .map_or(0, |d| d.as_secs());
        let wl = WorkloadTables::for_tenant(tenant);
        let stats = wl
            .merged_stats(
                &mut c,
                now.saturating_sub(ts.lookup_advice.window.as_secs()),
                now + 60,
            )
            .await?;
        let busy: Vec<(u64, u64, u64, u64)> = stats
            .iter()
            .filter(|(_, s)| s.exec_count >= 1 && s.scatter_count > 0)
            .map(|(id, s)| (*id, s.exec_count, s.scatter_count, s.total_us))
            .collect();
        if busy.is_empty() {
            return Ok(Vec::new());
        }
        let ids: Vec<sprig_common::ids::Fingerprint> = busy
            .iter()
            .map(|(id, ..)| sprig_common::ids::Fingerprint(*id))
            .collect();
        let fps = wl.fingerprints(&mut c, &ids).await?;
        let mut by_col: BTreeMap<(ObjectId, String), Scatter> = BTreeMap::new();
        for f in &fps {
            let Some(&(id, runs, scattered, us)) = busy.iter().find(|b| b.0 == f.id.0) else {
                continue;
            };
            tally(&mut by_col, id, &f.shape, runs, scattered, us);
        }
        c.query_drop(format!("USE {}", quote(&cat)))
            .await
            .map_err(|e| SprigError::internal(format!("catalog: {e}")))?;
        let existing = LookupCatalog.rows(&mut c).await?;
        let mut out = Vec::new();
        for ((object, column), s) in by_col {
            if !qualifies(&s, &ts.lookup_advice) {
                continue;
            }
            if existing
                .iter()
                .any(|l| l.owner == object && l.columns.as_slice() == [column.clone()])
            {
                continue;
            }
            if let Some(cand) = self.candidate(tenant, &mut c, object, column, s).await? {
                out.push(cand);
            }
        }
        Ok(out)
    }

    /// `object.column` as a candidate: an active, ready column member of
    /// a group with two or more shards, whose column carries a declared
    /// single-column unique key without the distribution key.
    async fn candidate(
        &self,
        tenant: TenantId,
        c: &mut Conn,
        object: ObjectId,
        column: String,
        scatter: Scatter,
    ) -> Result<Option<Candidate>> {
        let member: Option<MemberText> = c
            .exec_first(
                "SELECT m.group_id, m.db_id, CAST(m.routing AS CHAR), \
                 CAST(m.key_columns AS CHAR), CAST(m.rk_state AS CHAR), o.name, d.name \
                 FROM group_members m JOIN table_groups g ON g.group_id = m.group_id \
                 JOIN objects o ON o.object_id = m.object_id \
                 JOIN `databases` d ON d.db_id = m.db_id \
                 WHERE m.object_id = ? AND g.kind = 'sharded' AND g.state = 'active'",
                (object.0,),
            )
            .await
            .map_err(|e| SprigError::internal(format!("catalog: {e}")))?;
        let Some((group, db, routing, key, rk, table, db_name)) = member else {
            return Ok(None);
        };
        if routing != "columns" || rk != "ready" {
            return Ok(None);
        }
        let key: Vec<String> = key
            .and_then(|k| serde_json::from_str::<Vec<String>>(&k).ok())
            .unwrap_or_default()
            .into_iter()
            .map(|k| k.to_ascii_lowercase())
            .collect();
        let Ok((map, _)) = self.maps.load(tenant, GroupId(group)).await else {
            return Ok(None);
        };
        // One shard: nothing scatters.
        let (true, Some(r)) = (map.ranges.len() >= 2, map.ranges.first()) else {
            return Ok(None);
        };
        let schema = shard_schema(tenant, DbId(db), GroupId(group), r.shard);
        let Ok(mut sc) = self.conn(&r.cluster).await else {
            return Ok(None);
        };
        let Ok(shape) = owner_shape(&mut sc, &schema, &table).await else {
            return Ok(None);
        };
        Ok(lookup_keys(&shape, &key)
            .into_iter()
            .find(|(_, cols)| cols.as_slice() == std::slice::from_ref(&column))
            .map(|(k, _)| Candidate {
                object,
                db: DbId(db),
                db_name,
                table,
                group: GroupId(group),
                key: k,
                column,
                scatter,
            }))
    }

    /// The approvals gate for one candidate, and the build once it may go.
    async fn propose_lookup(
        &self,
        tenant: TenantId,
        ts: &TenantSettings,
        admin: &Admin<'_>,
        c: &Candidate,
    ) {
        use sprig_approvals::{Action, Kind, Proposal, Verdict};
        let ticket = match self.approvals.get() {
            Some(a) => {
                let policy = a
                    .policy(
                        Kind::AdvisorLookup,
                        Some(tenant),
                        Some(c.db),
                        Some(c.object),
                        None,
                    )
                    .await;
                let mut subject = a.subject(tenant).await;
                subject.db = Some(c.db);
                subject.db_name = Some(c.db_name.clone());
                subject.object = Some(c.object);
                subject.object_name = Some(c.table.clone());
                #[allow(clippy::cast_precision_loss)] // microseconds of an hour
                let ms = c.scatter.total_us as f64 / 1000.0;
                let p = Proposal {
                    kind: Kind::AdvisorLookup,
                    subject,
                    target: format!("t{tenant}/o{}/lookup/{}", c.object.0, c.key),
                    action: Action {
                        text: format!(
                            "SPRIG REBUILD LOOKUP INDEX `{}` ON `{}`.`{}`",
                            c.key, c.db_name, c.table
                        ),
                        machine: serde_json::json!({
                            "object": c.object.0,
                            "group": c.group.0,
                            "key": c.key,
                            "column": c.column,
                        }),
                    },
                    evidence: serde_json::json!({
                        "runs": c.scatter.runs,
                        "scattered": c.scatter.scattered,
                        "total_ms": ms,
                        "fingerprints": c.scatter.fingerprints,
                        "window_hours": 1,
                    }),
                    score: ms,
                    summary: format!(
                        "global lookup index on {}.{} ({}, unique key {}): {} of {} reads in \
                         the last hour went to every shard ({:.0} ms)",
                        c.db_name,
                        c.table,
                        c.column,
                        c.key,
                        c.scatter.scattered,
                        c.scatter.runs,
                        ms
                    ),
                    emergency: false,
                };
                match a.gate(&p, &policy).await {
                    Verdict::Proceed(t) => Some(t),
                    other => {
                        tracing::debug!(tenant = %tenant, table = %c.table, verdict = ?other,
                            "lookup proposal held");
                        return;
                    }
                }
            }
            None if ts.advisor.mode == sprig_advisor::sharding::AdvisorMode::On => None,
            None => return,
        };
        let r = async {
            let (map, _) = self.maps.load(tenant, c.group).await?;
            admin
                .create(&NewLookup {
                    db: c.db,
                    owner: c.object,
                    owner_table: c.table.clone(),
                    owner_map: Arc::new(map),
                    columns: vec![c.column.clone()],
                    key: Some(c.key.clone()),
                    origin: LookupOrigin::UniqueKey,
                })
                .await
        }
        .await;
        let (ok, detail) = match &r {
            Ok((l, rep)) => (
                true,
                format!(
                    "lookup index {} built: {} rows, {} repaired",
                    l.index_name, rep.rows, rep.repaired
                ),
            ),
            Err(e) => (false, format!("lookup index {} failed: {e}", c.key)),
        };
        tracing::info!(tenant = %tenant, table = %c.table, ok, detail = %detail, "advisor lookup");
        self.approval_done(ticket.as_ref().filter(|t| t.id.is_some()), ok, &detail)
            .await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use sprig_workload::shape::{Shape, StmtType};

    #[test]
    fn scatter_reads_by_column_are_tallied() {
        let mut out = BTreeMap::new();
        let s = Shape {
            tables: vec![sprig_workload::shape::TableRef {
                alias: "customers".into(),
                object_id: Some(ObjectId(7)),
                db: None,
                table: "customers".into(),
                db_id: None,
                derived: false,
            }],
            predicates: vec![
                sprig_workload::shape::Predicate {
                    column: sprig_workload::shape::ColumnRef {
                        table: None,
                        col: "Email".into(),
                    },
                    op: PredOp::Eq,
                    args: vec![0],
                },
                sprig_workload::shape::Predicate {
                    column: sprig_workload::shape::ColumnRef {
                        table: None,
                        col: "age".into(),
                    },
                    op: PredOp::Range {
                        lo: true,
                        hi: false,
                    },
                    args: vec![1],
                },
            ],
            ..Shape::empty(StmtType::Select)
        };
        tally(&mut out, 1, &s, 90, 90, 9000);
        tally(&mut out, 2, &s, 30, 10, 1000);
        let e = &out[&(ObjectId(7), "email".to_owned())];
        assert_eq!((e.runs, e.scattered, e.total_us), (120, 100, 10_000));
        assert_eq!(out.len(), 1, "ranges don't count");
        let a = sprig_advisor::sharding::settings::LookupAdvice::default();
        assert!(qualifies(e, &a));
        assert!(!qualifies(
            &Scatter {
                runs: 120,
                scattered: 90,
                ..Scatter::default()
            },
            &a
        ));
        assert!(!qualifies(
            &Scatter {
                runs: 99,
                scattered: 99,
                ..Scatter::default()
            },
            &a
        ));
        // A tenant that lowers advisor.lookup_min_runs proposes sooner.
        let low = sprig_advisor::sharding::settings::LookupAdvice { min_runs: 20, ..a };
        assert!(qualifies(
            &Scatter {
                runs: 20,
                scattered: 20,
                ..Scatter::default()
            },
            &low
        ));
        // Two tables (a join): not a lookup's read.
        let mut j = s.clone();
        j.tables.push(j.tables[0].clone());
        let mut none = BTreeMap::new();
        tally(&mut none, 3, &j, 500, 500, 1);
        assert!(none.is_empty());
    }
}
