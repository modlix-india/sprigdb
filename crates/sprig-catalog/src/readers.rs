//! Reader formats: which format of each catalog module every running
//! process reads, so the controller allows a contract migration only once
//! no running process needs what it removes.
//!
//! ```text
//! process start: declare(every module of the build)      (the binary)
//!                Migrator::new also notes its modules
//! keep():        /sprig/dc/<dc>/catalog-readers/<role>/<node>
//!                {role, node, build, formats: {module: newest}}
//!                bound to a lease; rewritten when the formats change
//! scan():        every DC's catalog-readers/, and its gateways/,
//!                workers/ and controllers/: a running process without a
//!                reader record (an older build) is "unregistered"
//! contract_ceilings(): per module, contracts up to the oldest format any
//!                reader of the module reads; none at all while a process
//!                is unregistered or no reader lists the module
//! ```
//!
//! A process lists the newest format of every module its build knows, so
//! a reader that doesn't list a module (a build from before the module)
//! doesn't read it. A module a build forgets to declare is listed once the
//! process builds a [`Migrator`](crate::Migrator) with it; until then its
//! contracts wait, the safe side. The contract delay
//! (`catalog.contract_delay_days`) is the migrator's own and still applies
//! on top of these ceilings.

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex, PoisonError};
use std::time::Duration;

use serde::{Deserialize, Serialize};
use sprig_common::ids::DcId;
use sprig_common::time::Clock;
use sprig_common::{CancellationToken, Result};
use sprig_meta::session::SessionOpts;
use sprig_meta::{ListOpts, MetaStore, Op, Record, Session, Tier, TierView, Txn, Versioned};

use crate::migrate::{Ceiling, Module, Phase};

/// The DC-tier prefix of reader records.
pub const PREFIX: &str = "catalog-readers/";

/// The registration lease.
pub const TTL: Duration = Duration::from_secs(10);

/// How often a registered process checks whether its formats changed.
pub const REFRESH: Duration = Duration::from_secs(5);

/// The modules this process reads, by name.
static DECLARED: Mutex<BTreeMap<&'static str, Module>> = Mutex::new(BTreeMap::new());

/// Notes `modules` as read by this process (module docs). A name seen
/// twice keeps the module with the newer format.
pub fn declare(modules: &[Module]) {
    let mut d = DECLARED.lock().unwrap_or_else(PoisonError::into_inner);
    for m in modules {
        match d.get(m.name) {
            Some(old) if old.newest() >= m.newest() => {}
            _ => {
                d.insert(m.name, *m);
            }
        }
    }
}

/// Every module this process reads.
#[must_use]
pub fn declared() -> Vec<Module> {
    DECLARED
        .lock()
        .unwrap_or_else(PoisonError::into_inner)
        .values()
        .copied()
        .collect()
}

/// The newest format of every module this process reads.
#[must_use]
pub fn formats() -> BTreeMap<String, u32> {
    DECLARED
        .lock()
        .unwrap_or_else(PoisonError::into_inner)
        .values()
        .map(|m| (m.name.to_owned(), m.newest()))
        .collect()
}

/// `catalog-readers/<role>/<node>`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReaderRecord {
    /// `gateway`, `worker` or `controller`.
    pub role: String,
    /// The process's node id, as in its `gateways/`, `workers/` or
    /// `controllers/` registration.
    pub node: String,
    /// Its build.
    #[serde(default)]
    pub build: String,
    /// The newest format it reads, per module.
    #[serde(default)]
    pub formats: BTreeMap<String, u32>,
}

impl Versioned for ReaderRecord {
    const V: u32 = 1;
}

/// The key of `role`/`node`'s record, relative to the DC tier.
#[must_use]
pub fn key_of(role: &str, node: &str) -> String {
    format!("{PREFIX}{role}/{node}")
}

async fn put(
    store: &Arc<dyn MetaStore>,
    session: &Session,
    key: &str,
    rec: &ReaderRecord,
) -> Result<()> {
    store
        .txn(Txn {
            when: vec![],
            then: vec![Op::Put {
                key: store.tier().key(key)?,
                value: Record::new(rec.clone()).encode()?,
                lease: Some(session.lease()),
            }],
            otherwise: vec![],
        })
        .await?;
    Ok(())
}

/// Keeps this process's reader record registered in `store` (its DC tier)
/// until `stop`, writing it again when its formats change or its lease is
/// lost.
pub async fn keep(
    store: Arc<dyn MetaStore>,
    clock: Arc<dyn Clock>,
    role: &'static str,
    node: String,
    stop: CancellationToken,
) {
    let key = key_of(role, &node);
    loop {
        let session = tokio::select! {
            () = stop.cancelled() => return,
            r = Session::open(Arc::clone(&store), Arc::clone(&clock), SessionOpts::new(TTL)) => r,
        };
        let session = match session {
            Ok(s) => s,
            Err(e) => {
                tracing::warn!(error = %e, "catalog reader registration failed; retrying");
                tokio::select! {
                    () = stop.cancelled() => return,
                    () = tokio::time::sleep(Duration::from_secs(1)) => continue,
                }
            }
        };
        let mut written: Option<BTreeMap<String, u32>> = None;
        loop {
            let now = formats();
            if written.as_ref() != Some(&now) {
                let rec = ReaderRecord {
                    role: role.to_owned(),
                    node: node.clone(),
                    build: env!("CARGO_PKG_VERSION").to_owned(),
                    formats: now.clone(),
                };
                match put(&store, &session, &key, &rec).await {
                    Ok(()) => {
                        tracing::debug!(modules = now.len(), "catalog reader formats registered");
                        written = Some(now);
                    }
                    Err(e) => tracing::warn!(error = %e, "writing catalog reader formats failed"),
                }
            }
            tokio::select! {
                () = stop.cancelled() => {
                    let _ = session.close().await;
                    return;
                }
                () = session.lost() => {
                    tracing::warn!("catalog reader registration lease lost; registering again");
                    break;
                }
                () = tokio::time::sleep(REFRESH) => {}
            }
        }
    }
}

/// What [`scan`] found.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReaderScan {
    /// Every reader record, with its DC.
    pub readers: Vec<(String, ReaderRecord)>,
    /// Running processes without a reader record, as `<role> <node> (dc
    /// <dc>)`: builds from before registration, whose formats are unknown.
    pub unregistered: Vec<String>,
}

/// Every key of a listing, all pages.
async fn list_all(store: &dyn MetaStore, rel: &str) -> Result<Vec<sprig_meta::KeyValue>> {
    let prefix = store.tier().key(rel)?;
    let mut out = Vec::new();
    let mut opts = ListOpts::default();
    loop {
        let page = store.list(&prefix, opts.clone()).await?;
        out.extend(page.kvs.iter().cloned());
        if !page.more {
            return Ok(out);
        }
        opts.at = Some(page.revision);
        opts.after = page.kvs.last().map(|kv| kv.key.clone());
    }
}

/// What the scan reads of a DC record.
#[derive(Debug, Clone, Deserialize, Serialize)]
struct DcState {
    #[serde(default)]
    state: String,
}

impl Versioned for DcState {
    const V: u32 = 1;
}

/// The DCs to look at: `dc`'s own and every DC in `/sprig/g/dcs/` that
/// isn't retired.
async fn dcs(global: &dyn MetaStore, dc: &dyn MetaStore) -> Result<Vec<DcId>> {
    let mut out: Vec<DcId> = Vec::new();
    if let Tier::Dc(own) = dc.tier() {
        out.push(own.clone());
    }
    for kv in list_all(global, "dcs/").await? {
        let Some(id) = kv
            .key
            .as_str()
            .rsplit('/')
            .next()
            .and_then(|s| DcId::new(s).ok())
        else {
            continue;
        };
        let retired = Record::<DcState>::decode(&kv.value).is_ok_and(|r| r.body.state == "retired");
        if !retired && !out.contains(&id) {
            out.push(id);
        }
    }
    Ok(out)
}

/// Reads every DC's reader records and running processes, through `dc` (a
/// DC tier store viewed as each DC's tier, which reaches every DC in
/// single-etcd mode and through the relay in two tiers).
///
/// # Errors
/// The stores' errors: a scan that can't read everything allows nothing.
pub async fn scan(global: &dyn MetaStore, dc: &Arc<dyn MetaStore>) -> Result<ReaderScan> {
    let mut out = ReaderScan::default();
    for id in dcs(global, dc.as_ref()).await? {
        let tier = TierView::over(dc, Tier::Dc(id.clone()));
        let mut known: Vec<(String, String)> = Vec::new();
        for kv in list_all(tier.as_ref(), PREFIX).await? {
            match Record::<ReaderRecord>::decode(&kv.value) {
                Ok(r) => {
                    known.push((r.body.role.clone(), r.body.node.clone()));
                    out.readers.push((id.to_string(), r.body));
                }
                Err(_) => out
                    .unregistered
                    .push(format!("{} (dc {id}, unreadable reader record)", kv.key)),
            }
        }
        for (role, rel) in [
            ("gateway", "gateways/"),
            ("worker", "workers/"),
            ("controller", "controllers/"),
        ] {
            for kv in list_all(tier.as_ref(), rel).await? {
                let Some(node) = kv.key.as_str().rsplit('/').next() else {
                    continue;
                };
                if !known.iter().any(|(r, n)| r == role && n == node) {
                    out.unregistered.push(format!("{role} {node} (dc {id})"));
                }
            }
        }
    }
    Ok(out)
}

/// One module's contract ceiling and why.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ModuleCeiling {
    /// The module.
    pub module: String,
    /// The newest format this build knows.
    pub newest: u32,
    /// The formats of its contract migrations.
    pub contracts: Vec<u32>,
    /// The highest format a contract may reach now (0: none).
    pub contract_ceiling: u32,
    /// Readers that hold it back, as `<role> <node> (dc <dc>) reads <n>`,
    /// or why nothing is allowed.
    pub held_by: Vec<String>,
}

/// The contract ceiling of each of `modules` given `scan` (module docs):
/// the oldest format a reader listing the module reads; 0 while any
/// process is unregistered or no reader lists the module.
#[must_use]
pub fn contract_ceilings(scan: &ReaderScan, modules: &[Module]) -> Vec<ModuleCeiling> {
    modules
        .iter()
        .map(|m| {
            let contracts: Vec<u32> = m
                .migrations
                .iter()
                .filter(|x| x.phase == Phase::Contract)
                .map(|x| x.format)
                .collect();
            let readers: Vec<(String, &ReaderRecord, u32)> = scan
                .readers
                .iter()
                .filter_map(|(dc, r)| r.formats.get(m.name).map(|f| (dc.clone(), r, *f)))
                .collect();
            let oldest = readers.iter().map(|(_, _, f)| *f).min();
            let (ceiling, held_by) = if !scan.unregistered.is_empty() {
                (
                    0,
                    scan.unregistered
                        .iter()
                        .map(|u| format!("{u}: no reader record"))
                        .collect(),
                )
            } else if let Some(oldest) = oldest {
                let held = readers
                    .iter()
                    .filter(|(_, _, f)| *f < m.newest())
                    .map(|(dc, r, f)| format!("{} {} (dc {dc}) reads {f}", r.role, r.node))
                    .collect();
                (oldest, held)
            } else {
                (0, vec!["no registered reader lists the module".to_owned()])
            };
            ModuleCeiling {
                module: m.name.to_owned(),
                newest: m.newest(),
                contracts,
                contract_ceiling: ceiling,
                held_by,
            }
        })
        .collect()
}

/// The [`Ceiling`] a migration run gets for one module: expands as far as
/// they go, contracts up to `contract`.
#[must_use]
pub fn ceiling(contract: u32) -> Ceiling {
    Ceiling {
        expand: u32::MAX,
        contract,
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]

    use sprig_common::time::SystemClock;
    use sprig_meta::{Consistency, MemStore};

    use super::*;
    use crate::migrate::Migration;

    static TWO: &[Migration] = &[
        Migration {
            format: 1,
            phase: Phase::Expand,
            name: "one",
            statements: &[],
        },
        Migration {
            format: 2,
            phase: Phase::Expand,
            name: "two",
            statements: &[],
        },
        Migration {
            format: 3,
            phase: Phase::Contract,
            name: "three",
            statements: &[],
        },
    ];

    const M: Module = Module {
        name: "readers_test_m",
        migrations: TWO,
    };

    fn rec(role: &str, node: &str, f: Option<u32>) -> ReaderRecord {
        ReaderRecord {
            role: role.into(),
            node: node.into(),
            build: "t".into(),
            formats: f
                .map(|f| BTreeMap::from([(M.name.to_owned(), f)]))
                .unwrap_or_default(),
        }
    }

    #[test]
    fn ceilings_follow_the_oldest_reader() {
        let mut s = ReaderScan {
            readers: vec![
                ("dc-a".into(), rec("gateway", "g1", Some(3))),
                ("dc-b".into(), rec("worker", "w1", Some(2))),
                // A build from before the module: it doesn't read it.
                ("dc-a".into(), rec("gateway", "g0", None)),
            ],
            unregistered: vec![],
        };
        let c = &contract_ceilings(&s, &[M])[0];
        assert_eq!(c.contracts, vec![3]);
        assert_eq!(c.contract_ceiling, 2);
        assert_eq!(c.held_by, vec!["worker w1 (dc dc-b) reads 2"]);
        s.readers[1].1.formats.insert(M.name.into(), 3);
        let c = &contract_ceilings(&s, &[M])[0];
        assert_eq!((c.contract_ceiling, c.held_by.len()), (3, 0));
        // A running process without a record holds every contract back.
        s.unregistered.push("gateway g9 (dc dc-a)".into());
        assert_eq!(contract_ceilings(&s, &[M])[0].contract_ceiling, 0);
        // So does a module no reader lists.
        let none = ReaderScan::default();
        assert_eq!(contract_ceilings(&none, &[M])[0].contract_ceiling, 0);
    }

    #[test]
    fn declared_modules_keep_the_newest() {
        static OLD: &[Migration] = &[Migration {
            format: 1,
            phase: Phase::Expand,
            name: "one",
            statements: &[],
        }];
        let old = Module {
            name: "readers_test_decl",
            migrations: OLD,
        };
        let new = Module {
            name: "readers_test_decl",
            migrations: TWO,
        };
        declare(&[new]);
        declare(&[old]);
        assert_eq!(formats().get("readers_test_decl"), Some(&3));
        assert!(declared().iter().any(|m| m.name == "readers_test_decl"));
    }

    #[tokio::test]
    async fn records_are_kept_and_scanned() {
        let dc: Arc<dyn MetaStore> = Arc::new(MemStore::new(Tier::Dc(DcId::new("dc-t").unwrap())));
        let global = MemStore::new(Tier::Global);
        declare(&[M]);
        let stop = CancellationToken::new();
        let task = tokio::spawn(keep(
            Arc::clone(&dc),
            Arc::new(SystemClock),
            "gateway",
            "g1".into(),
            stop.clone(),
        ));
        // A worker without a record.
        dc.txn(Txn {
            when: vec![],
            then: vec![Op::Put {
                key: dc.tier().key("workers/w1").unwrap(),
                value: b"{}".to_vec(),
                lease: None,
            }],
            otherwise: vec![],
        })
        .await
        .unwrap();
        let key = dc.tier().key(&key_of("gateway", "g1")).unwrap();
        for _ in 0..100 {
            if dc
                .get(&key, Consistency::Linearizable)
                .await
                .unwrap()
                .is_some()
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        let s = scan(&global, &dc).await.unwrap();
        assert_eq!(s.readers.len(), 1);
        assert_eq!(s.readers[0].1.formats.get(M.name), Some(&3));
        assert_eq!(s.unregistered, vec!["worker w1 (dc dc-t)"]);
        stop.cancel();
        task.await.unwrap();
        assert!(
            dc.get(&key, Consistency::Linearizable)
                .await
                .unwrap()
                .is_none()
        );
    }
}
