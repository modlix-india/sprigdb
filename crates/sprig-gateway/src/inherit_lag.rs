//! Exact reads of materialized copies on engine databases: a statement
//! reads a copy only once the copy has applied every commit that was
//! visible when the statement began; otherwise, after a short wait, it
//! reads through the levels above for that statement.
//!
//! ```text
//! the copy task (crate::inherit_orphans), per family, after each look
//! that left nothing to do (no change, or the passes ran):
//!   publish: each family tenant's place in the change log it read up to
//!   statement in tenant c, before the inheritance layer plans it:
//!   for each serving copy of an object the statement names:
//!     chain = the copy's source tenant and the tenants above it
//!     target(t) = the highest log index handed out in t's unit now
//!                 (every commit the statement can see is at or below it)
//!     current when the copy's family published place(t) >= target(t)
//!     for every t of the chain
//!   none behind: read the copies
//!   some behind: ask the task to look now (after the change log has
//!     reached the targets), wait up to inherit.copy_wait_ms for it to
//!     publish; the copies still behind are left out: the statement reads
//!     through the levels above instead (always exact)
//! ```
//!
//! A family this process's task hasn't looked at (another node leads it,
//! or it is new) has nothing published: its copies are left out at once.
//! Inside a client's transaction copies are left out too: its snapshot may
//! predate the copy's last writes. Only statements that read or write rows
//! (`SELECT`, `WITH`, `TABLE`, `VALUES`, `INSERT`, `REPLACE`, `UPDATE`,
//! `DELETE`, `SPRIG ...`) are looked at; others (`SHOW SHARES`, `EXPLAIN`,
//! DDL) keep every copy as it is. Copies of objects the statement doesn't
//! name are left out (a link check reading one reads through instead).

use std::collections::{HashMap, HashSet};
use std::sync::{Mutex, OnceLock, PoisonError};
use std::time::Duration;

use sprig_common::ids::{ObjectId, TenantId};
use sprig_inherit::copy::{CopyKey, CopyStates};

/// A family: the tree root, the logical database and the table.
pub(crate) type Family = (u64, String, String);

/// What the copy task has published.
#[derive(Default)]
struct Inner {
    /// Each family tenant's object of the family's table.
    objects: HashMap<(u64, u64), Family>,
    /// Per family, each tenant's change-log index its copies have applied.
    applied: HashMap<Family, HashMap<u64, u64>>,
    /// Log indexes statements wait for, per unit: the task waits for the
    /// change log to reach them before it looks.
    wanted: HashMap<u64, u64>,
}

struct Lag {
    inner: Mutex<Inner>,
    /// Bumped on every publish.
    changed: tokio::sync::watch::Sender<u64>,
    nudge: tokio::sync::Notify,
}

fn lag() -> &'static Lag {
    static L: OnceLock<Lag> = OnceLock::new();
    L.get_or_init(|| Lag {
        inner: Mutex::default(),
        changed: tokio::sync::watch::channel(0).0,
        nudge: tokio::sync::Notify::new(),
    })
}

fn inner() -> std::sync::MutexGuard<'static, Inner> {
    lag().inner.lock().unwrap_or_else(PoisonError::into_inner)
}

/// The copy task: family `f`'s copies have applied each tenant's change
/// log up to the index given; `objects` are the family's tenants and their
/// objects of its table.
pub(crate) fn publish(f: &Family, objects: &[(TenantId, ObjectId)], applied: &[(u64, u64)]) {
    {
        let mut i = inner();
        i.objects.retain(|_, g| g != f);
        for (t, o) in objects {
            i.objects.insert((t.0, o.0), f.clone());
        }
        let a = i.applied.entry(f.clone()).or_default();
        for (t, idx) in applied {
            let e = a.entry(*t).or_insert(0);
            *e = (*e).max(*idx);
        }
    }
    lag().changed.send_modify(|g| *g = g.wrapping_add(1));
}

/// The copy task: family `f` is no longer watched here.
pub(crate) fn forget(f: &Family) {
    let mut i = inner();
    i.objects.retain(|_, g| g != f);
    i.applied.remove(f);
}

/// Resolves when a statement asks the copy task to look now.
pub(crate) async fn nudged() {
    lag().nudge.notified().await;
}

/// The log indexes statements wait for, per unit (taken).
pub(crate) fn take_wanted() -> Vec<(u64, u64)> {
    inner().wanted.drain().collect()
}

/// One copy a statement may read, with the tenants whose commits it
/// depends on.
#[derive(Debug, Clone)]
pub(crate) struct Need {
    pub key: CopyKey,
    /// The copy's source tenant and the tenants above it.
    pub chain: Vec<TenantId>,
}

/// Where one copy stands for a statement.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Stand {
    Current,
    Behind,
    Unknown,
}

/// Whether `n`'s copy has applied `targets` (tenant → log index).
fn stand(i: &Inner, n: &Need, targets: &HashMap<u64, u64>) -> Stand {
    let Some(f) = i.objects.get(&(n.key.tenant.0, n.key.object.0)) else {
        return Stand::Unknown;
    };
    let Some(a) = i.applied.get(f) else {
        return Stand::Unknown;
    };
    for t in &n.chain {
        let want = targets.get(&t.0).copied().unwrap_or(0);
        match a.get(&t.0) {
            Some(&have) if have >= want => {}
            Some(_) => return Stand::Behind,
            None => return Stand::Unknown,
        }
    }
    Stand::Current
}

/// The highest log index handed out in tenant `t`'s unit (0 without an
/// engine).
fn target_of(t: TenantId) -> u64 {
    crate::node_engine::engine().map_or(0, |e| {
        e.consensus()
            .proposed_index(sprig_engine::Engine::unit_of_tenant(t.0))
    })
}

fn count(outcome: &'static str, n: usize) {
    if n > 0 {
        metrics::counter!("sprig_inherit_copy_exact_reads_total", "outcome" => outcome)
            .increment(n as u64);
    }
}

/// `copies` for one statement: those of `needs` that are current, or
/// become current within `wait`; every other copy is left out, so the
/// statement reads through instead (module docs).
pub(crate) async fn exact(mut copies: CopyStates, needs: &[Need], wait: Duration) -> CopyStates {
    let keep_keys: HashSet<CopyKey> = needs.iter().map(|n| n.key).collect();
    copies.0.retain(|k, i| i.serving && keep_keys.contains(k));
    if copies.0.is_empty() {
        return copies;
    }
    let needs: Vec<&Need> = needs
        .iter()
        .filter(|n| copies.0.contains_key(&n.key))
        .collect();
    let mut targets: HashMap<u64, u64> = HashMap::new();
    for n in &needs {
        for t in &n.chain {
            targets.entry(t.0).or_insert_with(|| target_of(*t));
        }
    }
    let deadline = tokio::time::Instant::now() + wait;
    let mut rx = lag().changed.subscribe();
    let mut waited = false;
    let mut asked = false;
    loop {
        rx.borrow_and_update();
        let mut behind = Vec::new();
        {
            let i = inner();
            for n in &needs {
                match stand(&i, n, &targets) {
                    Stand::Current => {}
                    Stand::Behind => behind.push(n.key),
                    Stand::Unknown => {
                        copies.0.remove(&n.key);
                        count("read_through", 1);
                    }
                }
            }
        }
        if behind.is_empty() {
            count(if waited { "waited" } else { "current" }, copies.0.len());
            return copies;
        }
        if tokio::time::Instant::now() >= deadline {
            for k in &behind {
                copies.0.remove(k);
            }
            count("read_through", behind.len());
            count("waited", copies.0.len());
            return copies;
        }
        if !asked {
            asked = true;
            let mut i = inner();
            for (t, idx) in &targets {
                let u = sprig_engine::Engine::unit_of_tenant(*t).0;
                let e = i.wanted.entry(u).or_insert(0);
                *e = (*e).max(*idx);
            }
        }
        lag().nudge.notify_one();
        waited = true;
        // At the deadline the loop takes one last look at what was
        // published.
        let _ = tokio::time::timeout_at(deadline, rx.changed()).await;
    }
}

/// Whether statement `sql` reads or writes rows (module docs).
pub(crate) fn reads_rows(sql: &str) -> bool {
    let first = sql
        .trim_start_matches(|c: char| c.is_whitespace() || c == '(')
        .split(|c: char| !c.is_ascii_alphabetic())
        .next()
        .unwrap_or("");
    [
        "SELECT", "WITH", "TABLE", "VALUES", "INSERT", "REPLACE", "UPDATE", "DELETE", "SPRIG",
    ]
    .iter()
    .any(|k| first.eq_ignore_ascii_case(k))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn key(t: u64, o: u64) -> CopyKey {
        CopyKey {
            tenant: TenantId(t),
            object: ObjectId(o),
            share: 0,
        }
    }

    #[test]
    fn a_copy_is_current_once_its_family_applied_every_tenant_of_the_chain() {
        let f: Family = (9_100, "d".into(), "t".into());
        let mut i = Inner::default();
        i.objects.insert((9_102, 7), f.clone());
        i.applied
            .insert(f, HashMap::from([(9_100, 10), (9_101, 5)]));
        let n = Need {
            key: key(9_102, 7),
            chain: vec![TenantId(9_101), TenantId(9_100)],
        };
        let t = |a, b| HashMap::from([(9_100u64, a), (9_101u64, b)]);
        assert_eq!(stand(&i, &n, &t(10, 5)), Stand::Current);
        assert_eq!(stand(&i, &n, &t(11, 5)), Stand::Behind);
        assert_eq!(stand(&i, &n, &t(3, 6)), Stand::Behind);
        // Another tenant's object, or a tenant the family has no place for.
        let other = Need {
            key: key(9_102, 8),
            chain: vec![TenantId(9_101)],
        };
        assert_eq!(stand(&i, &other, &t(0, 0)), Stand::Unknown);
        let far = Need {
            key: key(9_102, 7),
            chain: vec![TenantId(9_103)],
        };
        assert_eq!(stand(&i, &far, &t(0, 0)), Stand::Unknown);
    }

    #[test]
    fn statements_that_read_or_write_rows_are_known() {
        assert!(reads_rows("SELECT * FROM t"));
        assert!(reads_rows("  (select 1)"));
        assert!(reads_rows("delete FROM t WHERE id = 1"));
        assert!(reads_rows("SPRIG RESET t WHERE id = 1"));
        assert!(!reads_rows("SHOW SHARES"));
        assert!(!reads_rows("EXPLAIN SELECT * FROM t"));
        assert!(!reads_rows("ALTER TABLE t ADD COLUMN c INT"));
        assert!(!reads_rows("SELECTX"));
    }
}
