//! The periodic run's `Rekey` verdict: for a tenant whose tables are
//! already in table groups, every `advisor.interval_hours`, score the
//! design it has against the same design with one group re-keyed, and
//! re-key when a better key wins by a sustained margin.
//!
//! ```text
//! periodic(tenant):                          (the sharding loop decides when)
//!   current ← the groups as the catalog has them (members' routing), the
//!             other tables unsharded
//!   per group g (one key column, no pinned member):
//!     candidates: a column name n such that every member has a column of
//!       its own for it (named n, or the foreign-key column that references
//!       a member's n), all of one class the router routes (integer or
//!       decimal, string under one collation, binary), n passing the key
//!       filters on the largest member holding it, and not g's key now
//!     D_n ← current with g keyed by n, every member a column member
//!     gain = (score(D_n) − score(current)) in points; confidence = share of
//!       200 Poisson resamples where the gain is ≥ advisor.rekey_min_gain_pct
//!   best ← the candidate with the largest gain (ties: fewer bytes, lower id)
//!   qualifies ← gain ≥ advisor.rekey_min_gain_pct, confidence ≥ 0.95, and
//!               nothing blocks the re-key (shards on several clusters)
//!   apply ← qualifies, and the last advisor.rekey_sustain_runs − 1
//!           periodic runs proposed the same re-key and qualified, no re-key
//!           of g within advisor.rekey_cooldown_days, and g's bytes ≤
//!           migration.auto_size_limit_gb
//!   verdict: apply; else recommend when the gain is ≥ 5 points; else
//!            no_change. `advisor.mode = recommend` or `approve` turns apply
//!            into recommend (the loop raises an `advisor.rekey` item)
//! ```
//!
//! Everything here is pure; the loop in `sprig-controller::sharding`
//! assembles the input, keeps the history and starts the re-key.

use std::collections::BTreeMap;
use std::time::Duration;

use serde::{Deserialize, Serialize};
use sprig_shard::KeyClass;

use super::design::{
    self, Design, GroupDesign, Input, Member, Obj, Routing, Verdict, gain_confidence, key_quality,
};
use super::settings::{AdvisorMode, AdvisorSettings, RekeyAdvice};

/// The confidence a re-key needs.
pub const REKEY_CONFIDENCE: f64 = 0.95;

/// Below `advisor.rekey_min_gain_pct`, a gain of this many points is still
/// recommended.
pub const RECOMMEND_GAIN_PTS: f64 = 5.0;

/// A table group as the catalog has it now.
#[derive(Debug, Clone, PartialEq)]
pub struct LiveGroup {
    /// The group id.
    pub id: u32,
    /// Its key's classes.
    pub key: Vec<KeyClass>,
    /// Its members and how each routes now.
    pub members: Vec<Member>,
    /// Data and index bytes over every shard.
    pub bytes: f64,
    /// Shards on more than one cluster (a re-key reads one cluster).
    pub several_clusters: bool,
    /// A member is pinned (`SPRIG PIN`): the advisor doesn't change it.
    pub pinned: bool,
}

/// One proposed re-key.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Proposal {
    /// The group.
    pub group: u32,
    /// Each member's table name and its column for the new key.
    pub columns: BTreeMap<String, String>,
    /// The name the candidate was found by (the column most members use).
    pub column: String,
    /// The design's score now, in [0, 1].
    pub current: f64,
    /// The score with the group re-keyed.
    pub proposed: f64,
    /// `proposed − current` in points.
    pub gain_pts: f64,
    /// The bootstrap confidence of a gain of at least
    /// `advisor.rekey_min_gain_pct`.
    pub confidence: f64,
    /// Gain and confidence clear the bar and nothing blocks the re-key.
    pub qualifies: bool,
    /// Why it can't be applied as is (it can still be recommended).
    pub blockers: Vec<String>,
    /// The group's bytes.
    pub bytes: f64,
}

impl Proposal {
    /// Whether `o` proposes the same re-key: the same group and columns.
    #[must_use]
    pub fn same_as(&self, o: &Proposal) -> bool {
        self.group == o.group && self.columns == o.columns
    }
}

/// What a periodic run found.
#[derive(Debug, Clone, PartialEq, Default)]
pub struct Evaluation {
    /// The current design.
    pub current: Design,
    /// Its score.
    pub current_score: f64,
    /// The best candidate of each group that has one, best first.
    pub proposals: Vec<Proposal>,
    /// The design of the best proposal (the current design without one).
    pub best: Design,
    /// Lines for the evidence: groups skipped and candidates rejected.
    pub notes: Vec<String>,
}

/// The design the groups make now; every other table stays unsharded.
#[must_use]
pub fn current_design(input: &Input, groups: &[LiveGroup]) -> Design {
    let mut d = Design::default();
    for g in groups {
        let name = g
            .members
            .first()
            .and_then(|m| input.table(m.object))
            .map_or_else(|| format!("g{}", g.id), |t| t.name.clone());
        d.groups.push(GroupDesign {
            name,
            key: g.key.clone(),
            members: g.members.clone(),
        });
    }
    let grouped: Vec<Obj> = groups
        .iter()
        .flat_map(|g| g.members.iter().map(|m| m.object))
        .collect();
    d.unsharded = input
        .tables
        .iter()
        .map(|t| t.object)
        .filter(|o| !grouped.contains(o))
        .collect();
    d
}

fn routable(k: KeyClass) -> bool {
    matches!(
        k,
        KeyClass::Numeric | KeyClass::String { .. } | KeyClass::Binary
    )
}

/// A member's column for a key the group's members call `n`: its own
/// column `n`, else the column of a foreign key that references another
/// member's `n`; with its class.
fn member_column(input: &Input, g: &LiveGroup, object: Obj, n: &str) -> Option<(String, KeyClass)> {
    let t = input.table(object)?;
    if let Some(c) = t.column(n)
        && let Some(k) = c.class
    {
        return Some((n.to_owned(), k));
    }
    input
        .fks
        .iter()
        .filter(|f| f.child == object && f.child_cols.len() == 1)
        .filter(|f| g.members.iter().any(|m| m.object == f.parent))
        .filter(|f| f.parent_cols.len() == 1 && f.parent_cols[0] == n)
        .find_map(|f| {
            let c = t.column(&f.child_cols[0])?;
            Some((c.name.clone(), c.class?))
        })
}

/// The group's current key column of each member (`None` for parent
/// members).
fn current_columns(g: &LiveGroup) -> BTreeMap<Obj, Option<String>> {
    g.members
        .iter()
        .map(|m| {
            let c = match &m.routing {
                Routing::Columns { columns } if columns.len() == 1 => Some(columns[0].clone()),
                _ => None,
            };
            (m.object, c)
        })
        .collect()
}

/// Candidate re-keys: the column name, the group's design, each member
/// table's column.
pub type Candidates = Vec<(String, GroupDesign, BTreeMap<String, String>)>;

/// The candidate re-keys of `g`: for each column name, its group design;
/// with the evidence lines for names that were rejected.
#[must_use]
pub fn candidates(input: &Input, g: &LiveGroup, s: &AdvisorSettings) -> (Candidates, Vec<String>) {
    let mut out = Vec::new();
    let mut notes = Vec::new();
    let now = current_columns(g);
    let mut names: Vec<String> = g
        .members
        .iter()
        .filter_map(|m| input.table(m.object))
        .flat_map(|t| t.columns.iter())
        .filter(|c| c.class.is_some_and(routable))
        .filter(|c| !c.name.starts_with("_sprig"))
        .map(|c| c.name.clone())
        .collect();
    names.sort();
    names.dedup();
    'names: for n in names {
        let mut cols: Vec<(Obj, String, KeyClass)> = Vec::new();
        for m in &g.members {
            match member_column(input, g, m.object, &n) {
                Some((c, k)) => cols.push((m.object, c, k)),
                None => continue 'names,
            }
        }
        let class = cols[0].2;
        if cols.iter().any(|(_, _, k)| *k != class) || !routable(class) {
            notes.push(format!(
                "group {}: {n} isn't one class in every member",
                g.id
            ));
            continue;
        }
        if cols
            .iter()
            .all(|(o, c, _)| now.get(o).and_then(Option::as_deref) == Some(c.as_str()))
        {
            continue; // the key it has
        }
        // The key filters on the largest member holding the column itself.
        let largest = cols
            .iter()
            .filter(|(_, c, _)| *c == n)
            .filter_map(|(o, _, _)| input.table(*o))
            .max_by(|a, b| {
                a.size_bytes
                    .total_cmp(&b.size_bytes)
                    .then(b.object.cmp(&a.object))
            });
        if let Some(t) = largest
            && let Err(why) = key_quality(t, std::slice::from_ref(&n), g.bytes, input, s)
        {
            notes.push(format!("group {}: {why}", g.id));
            continue;
        }
        let named: BTreeMap<String, String> = cols
            .iter()
            .filter_map(|(o, c, _)| Some((input.table(*o)?.name.clone(), c.clone())))
            .collect();
        let members = cols
            .iter()
            .map(|(o, c, _)| Member {
                object: *o,
                routing: Routing::Columns {
                    columns: vec![c.clone()],
                },
            })
            .collect();
        let name = g
            .members
            .first()
            .and_then(|m| input.table(m.object))
            .map_or_else(|| format!("g{}", g.id), |t| t.name.clone());
        out.push((
            n,
            GroupDesign {
                name,
                key: vec![class],
                members,
            },
            named,
        ));
    }
    (out, notes)
}

/// Scores every group's candidate re-keys against the current design.
#[must_use]
pub fn evaluate(
    input: &Input,
    groups: &[LiveGroup],
    s: &AdvisorSettings,
    r: &RekeyAdvice,
) -> Evaluation {
    let mut input = input.clone();
    input.normalize();
    let current = current_design(&input, groups);
    let current_score = design::score(&input, &current, s).total;
    let mut notes = Vec::new();
    let mut proposals: Vec<(Proposal, Design)> = Vec::new();
    for (i, g) in groups.iter().enumerate() {
        if g.pinned {
            notes.push(format!("group {}: a member is pinned; not re-keyed", g.id));
            continue;
        }
        if g.key.len() != 1 {
            notes.push(format!(
                "group {}: a key of several parts; re-keys move single-column keys",
                g.id
            ));
            continue;
        }
        let (cands, why) = candidates(&input, g, s);
        notes.extend(why);
        let mut best: Option<(Proposal, Design)> = None;
        for (n, gd, named) in cands {
            let mut d = current.clone();
            d.groups[i] = gd;
            let proposed = design::score(&input, &d, s).total;
            let gain_pts = (proposed - current_score) * 100.0;
            if gain_pts <= 0.0 {
                continue;
            }
            let better = best.as_ref().is_none_or(|(b, _)| {
                gain_pts > b.gain_pts + 1e-9
                    || ((gain_pts - b.gain_pts).abs() <= 1e-9 && n < b.column)
            });
            if !better {
                continue;
            }
            let mut blockers = Vec::new();
            if g.several_clusters {
                blockers.push("the group's shards are on several clusters".to_owned());
            }
            best = Some((
                Proposal {
                    group: g.id,
                    columns: named,
                    column: n,
                    current: current_score,
                    proposed,
                    gain_pts,
                    confidence: 0.0,
                    qualifies: false,
                    blockers,
                    bytes: g.bytes,
                },
                d,
            ));
        }
        if let Some((mut p, d)) = best {
            p.confidence = gain_confidence(&input, &d, &current, r.min_gain_pts / 100.0, s);
            p.qualifies = p.gain_pts >= r.min_gain_pts - 1e-9
                && p.confidence >= REKEY_CONFIDENCE
                && p.blockers.is_empty();
            proposals.push((p, d));
        }
    }
    proposals.sort_by(|(a, _), (b, _)| {
        b.qualifies
            .cmp(&a.qualifies)
            .then(b.gain_pts.total_cmp(&a.gain_pts))
            .then(a.bytes.total_cmp(&b.bytes))
            .then(a.group.cmp(&b.group))
    });
    let best = proposals
        .first()
        .map_or_else(|| current.clone(), |(_, d)| d.clone());
    Evaluation {
        current,
        current_score,
        proposals: proposals.into_iter().map(|(p, _)| p).collect(),
        best,
        notes,
    }
}

/// What the loop knows about earlier runs and the group.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct History {
    /// The proposals of the previous periodic runs, newest run first.
    pub earlier: Vec<Vec<Proposal>>,
    /// Since the group's last re-key, if it had one.
    pub since_rekey: Option<Duration>,
}

/// Why a qualifying proposal isn't applied yet (empty: apply).
#[must_use]
pub fn holds(p: &Proposal, h: &History, r: &RekeyAdvice) -> Vec<String> {
    let mut why = Vec::new();
    let need = usize::try_from(r.sustain_runs.saturating_sub(1)).unwrap_or(usize::MAX);
    let agreeing = h
        .earlier
        .iter()
        .take_while(|run| run.iter().any(|q| q.qualifies && q.same_as(p)))
        .count();
    if agreeing < need {
        why.push(format!(
            "proposed by {} consecutive run(s), {} needed (advisor.rekey_sustain_runs)",
            agreeing + 1,
            r.sustain_runs
        ));
    }
    if let Some(t) = h.since_rekey
        && t < r.cooldown
    {
        why.push(format!(
            "re-keyed {:.1} days ago, {:.1} needed (advisor.rekey_cooldown_days)",
            t.as_secs_f64() / 86_400.0,
            r.cooldown.as_secs_f64() / 86_400.0
        ));
    }
    if p.bytes > r.size_limit_bytes {
        why.push(format!(
            "{:.1} GB is above migration.auto_size_limit_gb",
            p.bytes / (1024.0 * 1024.0 * 1024.0)
        ));
    }
    why
}

/// The periodic run's verdict for its best proposal: `apply` when it
/// qualifies and nothing holds it ([`holds`]); `recommend` for a gain of
/// at least [`RECOMMEND_GAIN_PTS`]; else `no_change`. Under
/// `advisor.mode = recommend` or `approve`, `apply` is `recommend`.
#[must_use]
pub fn decide(best: Option<&Proposal>, held: &[String], s: &AdvisorSettings) -> Verdict {
    let Some(p) = best else {
        return Verdict::NoChange;
    };
    let v = if p.qualifies && held.is_empty() {
        Verdict::Apply
    } else if p.gain_pts >= RECOMMEND_GAIN_PTS {
        Verdict::Recommend
    } else {
        Verdict::NoChange
    };
    match (s.mode, v) {
        (AdvisorMode::Recommend | AdvisorMode::Approve, Verdict::Apply) => Verdict::Recommend,
        _ => v,
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::float_cmp)]

    use sprig_shard::colocate::{SqlFamily, SqlType};

    use super::*;
    use crate::sharding::design::{Column, ForeignKey, Stmt, Table, Touch, Unit};

    const ORDERS: Obj = 1;
    const ITEMS: Obj = 2;
    const EVENTS: Obj = 3;

    fn col(n: &str, ndv: f64) -> Column {
        Column {
            name: n.into(),
            class: Some(KeyClass::Numeric),
            ndv: Some(ndv),
            top_share: None,
            sql: Some(SqlType::of(SqlFamily::Integer)),
        }
    }

    fn table(o: Obj, name: &str, cols: &[&str]) -> Table {
        Table {
            object: o,
            db: 1,
            name: name.into(),
            pk: vec![cols[0].to_owned()],
            columns: cols.iter().map(|c| col(c, 1e6)).collect(),
            uniques: Vec::new(),
            size_bytes: 1e9,
        }
    }

    fn eq(o: Obj, c: &str, w: f64, write: bool) -> Stmt {
        Stmt {
            weight: w,
            fingerprint: o * 100 + u64::try_from(c.len()).unwrap() + u64::from(write),
            touches: vec![Touch {
                object: o,
                eq: vec![c.into()],
                write,
                ..Touch::default()
            }],
            joins: Vec::new(),
        }
    }

    /// Orders, their items and events keyed by `store_id`; the workload
    /// reads and writes by `customer_id`.
    fn shop(by_customer: f64, by_store: f64) -> (Input, Vec<LiveGroup>) {
        let input = Input {
            tables: vec![
                table(ORDERS, "orders", &["id", "store_id", "customer_id"]),
                table(ITEMS, "order_items", &["id", "order_id", "store_id"]),
                table(EVENTS, "events", &["id", "store_id", "customer_id"]),
            ],
            fks: vec![ForeignKey {
                name: "fk_items_order".into(),
                child: ITEMS,
                child_cols: vec!["order_id".into()],
                parent: ORDERS,
                parent_cols: vec!["id".into()],
            }],
            stmts: vec![
                eq(ORDERS, "customer_id", by_customer, false),
                eq(EVENTS, "customer_id", by_customer, true),
                eq(ORDERS, "store_id", by_store, false),
            ],
            units: vec![Unit {
                weight: 10.0,
                writes: vec![ORDERS, EVENTS],
            }],
            split_bytes: 10e9,
            ..Input::default()
        };
        let cols = |c: &str| Routing::Columns {
            columns: vec![c.into()],
        };
        let g = LiveGroup {
            id: 7,
            key: vec![KeyClass::Numeric],
            members: vec![
                Member {
                    object: ORDERS,
                    routing: cols("store_id"),
                },
                Member {
                    object: ITEMS,
                    routing: cols("store_id"),
                },
                Member {
                    object: EVENTS,
                    routing: cols("store_id"),
                },
            ],
            bytes: 3e9,
            several_clusters: false,
            pinned: false,
        };
        (input, vec![g])
    }

    #[test]
    fn a_better_key_is_proposed_with_each_members_column() {
        let (input, groups) = shop(1000.0, 10.0);
        let s = AdvisorSettings::default();
        let r = RekeyAdvice::default();
        let e = evaluate(&input, &groups, &s, &r);
        // order_items has no customer_id and no foreign key to one: the
        // only candidates are the members' common columns.
        assert!(e.proposals.is_empty(), "{:?}", e.proposals);
        let mut input = input;
        input.tables[1].columns.push(col("customer_id", 1e6));
        let e = evaluate(&input, &groups, &s, &r);
        let [p] = e.proposals.as_slice() else {
            panic!("{:?} {:?}", e.proposals, e.notes)
        };
        assert_eq!(p.column, "customer_id");
        assert_eq!(p.columns["orders"], "customer_id");
        assert_eq!(p.columns["order_items"], "customer_id");
        assert!(p.gain_pts >= 15.0, "{p:?}");
        assert!(p.confidence >= REKEY_CONFIDENCE, "{p:?}");
        assert!(p.qualifies);
        assert_eq!(e.best.groups[0].key, vec![KeyClass::Numeric]);
        // Not sustained yet: recommend; sustained: apply.
        let held = holds(p, &History::default(), &r);
        assert_eq!(held.len(), 1, "{held:?}");
        assert_eq!(decide(Some(p), &held, &s), Verdict::Recommend);
        let h = History {
            earlier: vec![vec![p.clone()], vec![p.clone()]],
            since_rekey: None,
        };
        assert!(holds(p, &h, &r).is_empty());
        assert_eq!(decide(Some(p), &holds(p, &h, &r), &s), Verdict::Apply);
        // Under recommend or approve, apply is a recommendation.
        let rec = AdvisorSettings {
            mode: AdvisorMode::Approve,
            ..s
        };
        assert_eq!(decide(Some(p), &[], &rec), Verdict::Recommend);
        // Cooldown and size hold it.
        let h2 = History {
            since_rekey: Some(Duration::from_secs(86_400)),
            ..h.clone()
        };
        assert_eq!(holds(p, &h2, &r).len(), 1);
        let small = RekeyAdvice {
            size_limit_bytes: 1e9,
            ..r
        };
        assert_eq!(holds(p, &h, &small).len(), 1);
        // A run in between that proposed something else breaks the streak.
        let other = Proposal {
            column: "id".into(),
            columns: BTreeMap::from([("orders".into(), "id".into())]),
            ..p.clone()
        };
        let h3 = History {
            earlier: vec![vec![p.clone()], vec![other]],
            since_rekey: None,
        };
        assert_eq!(holds(p, &h3, &r).len(), 1);
    }

    #[test]
    fn a_member_may_take_the_key_through_its_foreign_key() {
        let (mut input, mut groups) = shop(1000.0, 10.0);
        // order_items references orders.customer_ref, the key's name in
        // orders.
        input.tables[0].columns.push(col("customer_ref", 1e6));
        input.tables[2].columns.push(col("customer_ref", 1e6));
        input.tables[1].columns.push(col("cref", 1e6));
        input.fks.push(ForeignKey {
            name: "fk_items_cref".into(),
            child: ITEMS,
            child_cols: vec!["cref".into()],
            parent: ORDERS,
            parent_cols: vec!["customer_ref".into()],
        });
        input.stmts.push(eq(ORDERS, "customer_ref", 5000.0, false));
        groups[0].members[1].routing = Routing::Parent {
            parent: ORDERS,
            fk: "fk_items_order".into(),
        };
        let s = AdvisorSettings::default();
        let e = evaluate(&input, &groups, &s, &RekeyAdvice::default());
        let p = e
            .proposals
            .first()
            .unwrap_or_else(|| panic!("{:?}", e.notes));
        assert_eq!(p.column, "customer_ref");
        assert_eq!(p.columns["order_items"], "cref");
    }

    #[test]
    fn small_gains_pinned_groups_and_several_clusters() {
        // The workload already fits the key: nothing to propose.
        let (mut input, groups) = shop(1.0, 1000.0);
        input.tables[1].columns.push(col("customer_id", 1e6));
        let s = AdvisorSettings::default();
        let r = RekeyAdvice::default();
        let e = evaluate(&input, &groups, &s, &r);
        assert!(
            e.proposals.iter().all(|p| !p.qualifies),
            "{:?}",
            e.proposals
        );
        assert_eq!(
            decide(e.proposals.first(), &[], &s),
            if e.proposals
                .first()
                .is_some_and(|p| p.gain_pts >= RECOMMEND_GAIN_PTS)
            {
                Verdict::Recommend
            } else {
                Verdict::NoChange
            }
        );
        let (mut input, mut groups) = shop(1000.0, 10.0);
        input.tables[1].columns.push(col("customer_id", 1e6));
        groups[0].pinned = true;
        let e = evaluate(&input, &groups, &s, &r);
        assert!(e.proposals.is_empty());
        assert!(
            e.notes.iter().any(|n| n.contains("pinned")),
            "{:?}",
            e.notes
        );
        groups[0].pinned = false;
        groups[0].several_clusters = true;
        let e = evaluate(&input, &groups, &s, &r);
        let p = &e.proposals[0];
        assert!(!p.qualifies && p.blockers.len() == 1, "{p:?}");
        assert_eq!(decide(Some(p), &[], &s), Verdict::Recommend);
        // Too few distinct values per expected shard: rejected.
        let (mut input, groups) = shop(1000.0, 10.0);
        for t in &mut input.tables {
            t.columns.push(col("region", 3.0));
        }
        input.stmts.push(eq(ORDERS, "region", 100_000.0, false));
        let e = evaluate(&input, &groups, &s, &r);
        assert!(e.proposals.iter().all(|p| p.column != "region"));
        assert!(
            e.notes.iter().any(|n| n.contains("region")),
            "{:?}",
            e.notes
        );
    }
}
