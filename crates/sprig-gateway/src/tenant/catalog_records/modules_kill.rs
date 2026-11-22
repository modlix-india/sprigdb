//! kill -9 of a process writing the catalog on engine records: the core
//! rows, the catalog version and the modules' rows (graphs, collections,
//! search), on a real engine (RocksDB), each change one engine transaction.
//!
//! A child process (this test binary again) commits catalog changes in a
//! loop. The change at version `v` writes, in one catalog change, what the
//! gateway writes for each module (a graph `G+v` with a native label, a
//! relationship type and an index; a collection `C+v` with its table's
//! object row, a setting on it and an index; a search index `S+v` on that
//! table and an analyzer), and every even `v` drops the graph, the
//! collection and the table of `v - 3`; it prints every acknowledged
//! version. The parent kills it at a varying moment, reopens the engine,
//! loads a snapshot with the modules' own catalog extensions, and checks
//! that the snapshot holds exactly the rows of the catalog's committed
//! version (every acknowledged change included, core rows too), and that
//! the next change commits on top of it. After each change the child also writes the
//! unversioned records (an ID block, a decision, an approvals item, a
//! workload flush), each
//! in a short engine transaction of its own: every acknowledged one must
//! be there afterwards, and no two ID blocks may ever overlap.
//!
//! `SPRIG_TEST_MARIADB_SINGLE` (`mysql://root:...@127.0.0.1:<port>`) names
//! the MariaDB the loader reads the catalog format from; without it the
//! test reports what it needs and passes.

#![allow(
    clippy::unwrap_used,
    clippy::too_many_lines,
    clippy::many_single_char_names
)]

use std::collections::{BTreeMap, BTreeSet};
use std::io::{BufRead, BufReader};
use std::path::Path;
use std::process::{Command, Stdio};
use std::sync::Arc;
use std::time::{Duration, SystemTime};

use mysql_async::prelude::Queryable;
use serde_json::json;
use sprig_approvals::store::RecStore as _;
use sprig_balancer::decisions::DecisionLog as _;
use sprig_catalog::cache::{CatalogExtension, CatalogSource, ExtensionStates, MariaSource, Node};
use sprig_catalog::ddl::DdlTx;
use sprig_catalog::migrate::MigrateOpts;
use sprig_catalog::records::RecordStore;
use sprig_catalog::rows::SettingsTx;
use sprig_catalog::store::rec::CoreRecords;
use sprig_catalog::store::{
    CatalogEndpoint, CatalogLocator, Namespace, ObjectFlags, ObjectKind, ObjectRow, ObjectState,
    SettingRow, SettingScope, Snapshot, StaticLocator, Store,
};
use sprig_catalog::{
    DirectoryAllocator, MariaBackend, MariaCatalog, Migrator, VersionKind, Versioner,
};
use sprig_common::ids::{DbId, ObjectId, TenantId};
use sprig_doc::catalog_records::DocTables;
use sprig_doc::meta::{CollMeta, IndexDef};
use sprig_engine::{EngineConfig, StoreKind};
use sprig_graph::catalog::{GraphCatalog, GraphExtension, GraphState};
use sprig_meta::{MemStore, MetaStore, Op, Tier, Txn};
use sprig_shard::ids::{Ordering, SeqId, SequenceSource as _};

use super::EngineRecords;
use crate::mongo::catalog::{DocExtension, DocState};
use crate::search::catalog::{SearchExtension, SearchState, SearchTables};

const T: TenantId = TenantId(57_406_000_901);
const CHILD: &str = "SPRIG_MODULES_KILL_CHILD";
const G: u64 = 1_000_000;
const C: u64 = 2_000_000;
const S: u64 = 3_000_000;

fn open(dir: &Path) -> sprig_engine::Engine {
    let mut cfg = EngineConfig::new(dir);
    cfg.store = StoreKind::Rocks;
    sprig_engine::Engine::open(cfg).unwrap()
}

fn locator(pool: &mysql_async::Pool, schema: &str) -> Arc<dyn CatalogLocator> {
    StaticLocator::new()
        .with(
            T,
            CatalogEndpoint {
                pool: pool.clone(),
                store: Store::new(schema),
            },
        )
        .shared()
}

/// A directory whose catalog version starts at the catalog's.
async fn directory(at: u64) -> Arc<dyn MetaStore> {
    let g = Arc::new(MemStore::new(Tier::Global));
    let key = Tier::Global.key(&format!("tenants/{T}")).unwrap();
    let body = json!({"v": 1, "tenant_id": T.0, "name": "kill", "state": "active",
                      "home_dc": "dc-a", "catalog_version": at, "auth_version": 0});
    g.txn(Txn {
        then: vec![Op::Put {
            key,
            value: serde_json::to_vec(&body).unwrap(),
            lease: None,
        }],
        ..Txn::default()
    })
    .await
    .unwrap();
    g
}

async fn catalog_version(records: &Arc<dyn RecordStore>) -> u64 {
    let mut t = records.begin(T).await.unwrap();
    CoreRecords::versions(&mut *t).await.unwrap().catalog
}

/// The versions whose rows a snapshot at `at` holds.
fn expected(at: u64) -> BTreeSet<u64> {
    (1..=at)
        .filter(|v| !(v + 3 <= at && (v + 3) % 2 == 0))
        .collect()
}

fn coll(v: u64) -> CollMeta {
    CollMeta {
        name: format!("c{v}"),
        table: format!("t{v}"),
        uuid: [1; 16],
        array_paths: BTreeSet::new(),
        arrays_full: false,
        pending_paths: BTreeSet::new(),
        indexes: vec![IndexDef {
            id: 1,
            name: format!("i{v}"),
            key: vec![("a".into(), 1)],
            unique: false,
            sparse: false,
            expire_after: None,
            origin: "user".into(),
            extra: bson::Document::new(),
            mariadb: None,
            auto_index_id: None,
        }],
        options: bson::Document::new(),
        next_index: 2,
    }
}

fn graph_index(v: u64) -> sprig_graph::store::IndexDef {
    sprig_graph::store::IndexDef {
        id: 0,
        name: format!("ix{v}"),
        unique: false,
        kind: sprig_graph::ast::IndexKind::Range,
        entity: sprig_graph::ast::Entity::Node,
        token: String::new(),
        token_id: 1,
        props: vec!["p".into()],
        state: String::new(),
    }
}

fn search_index(v: u64) -> sprig_search::def::IndexDef {
    use sprig_search::def::{FieldDef, FieldKind, Flavor, IndexDef, Model, Source};
    let mut d = IndexDef::new(
        &format!("s{v}"),
        "db",
        &format!("t{v}"),
        Model::Table,
        Flavor::Sql,
    );
    d.index_id = S + v;
    d.object_id = C + v;
    d.push(FieldDef::new("body", Source::Column, FieldKind::Text))
        .unwrap();
    d
}

fn search_analyzer(v: u64) -> sprig_search::analyze::CustomAnalyzer {
    sprig_search::analyze::CustomAnalyzer {
        name: format!("an{v}"),
        tokenizer: "standard".into(),
        tokenizer_opts: serde_json::json!({}),
        filters: vec!["lowercase".into()],
        synonyms: Vec::new(),
        version: 1,
    }
}

async fn change(cat: &MariaCatalog) -> u64 {
    let mut c = cat.change(T, VersionKind::Catalog).await.unwrap();
    let v = c.version();
    {
        let tx = c.tx();
        // The collection's table, as the engine's `CREATE TABLE` makes it.
        let obj = ObjectRow {
            object_id: ObjectId(C + v),
            db_id: DbId(1),
            namespace: Namespace::Relation,
            name: format!("t{v}"),
            name_key: format!("t{v}"),
            kind: ObjectKind::Table,
            parent_id: None,
            state: ObjectState::Active,
            schema_version: 1,
            flags: ObjectFlags::default(),
            pending_job_id: None,
            created_at: SystemTime::now(),
            changed_version: v,
        };
        DdlTx::insert_object(tx, &obj).await.unwrap();
        SettingsTx::put_setting(
            tx,
            &SettingRow {
                scope: SettingScope::Object(ObjectId(C + v)),
                key: "kill.v".into(),
                value: Some(json!(v)),
                bounds: None,
                set_by: "system".into(),
                set_at: SystemTime::now(),
                version: v,
            },
        )
        .await
        .unwrap();
        if v > 3 && v % 2 == 0 {
            assert!(
                tx.set_object_state(ObjectId(C + v - 3), ObjectState::Dropped, v)
                    .await
                    .unwrap()
            );
        }
        let rec = tx.records().await.unwrap();
        DocTables::insert(rec, C + v, 1, &coll(v)).await.unwrap();
        GraphCatalog::insert_native_as(rec, G + v, 1, Some(7), 1)
            .await
            .unwrap();
        assert_eq!(
            GraphCatalog::add_source(rec, G + v, true, &format!("L{v}"))
                .await
                .unwrap(),
            1
        );
        GraphCatalog::add_source(rec, G + v, false, &format!("R{v}"))
            .await
            .unwrap();
        GraphCatalog::insert_index(rec, G + v, &graph_index(v))
            .await
            .unwrap();
        SearchTables::put_index(rec, 1, &search_index(v), "user")
            .await
            .unwrap();
        assert!(
            SearchTables::add_analyzer(rec, &search_analyzer(v))
                .await
                .unwrap()
        );
        if v > 3 && v % 2 == 0 {
            GraphCatalog::delete(rec, G + v - 3).await.unwrap();
            DocTables::delete(rec, C + v - 3).await.unwrap();
            SearchTables::delete_index(rec, S + v - 3).await.unwrap();
            assert!(
                SearchTables::remove_analyzer(rec, &format!("an{}", v - 3))
                    .await
                    .unwrap()
            );
        }
    }
    c.commit().await.unwrap()
}

/// What a snapshot at `at` holds of the core rows and the modules,
/// checked row by row.
fn check(round: u64, at: u64, snap: &Snapshot, states: &ExtensionStates) {
    let want = expected(at);
    let live: BTreeSet<u64> = snap
        .objects
        .iter()
        .filter(|o| o.state == ObjectState::Active)
        .map(|o| o.object_id.0 - C)
        .collect();
    assert_eq!(live, want, "round {round}: live tables at v{at}");
    let all: BTreeSet<u64> = snap.objects.iter().map(|o| o.object_id.0 - C).collect();
    assert_eq!(
        all,
        (1..=at).collect(),
        "round {round}: table rows at v{at}"
    );
    let set: BTreeSet<u64> = snap
        .settings
        .iter()
        .filter(|r| r.key == "kill.v")
        .map(|r| {
            r.value
                .as_ref()
                .and_then(serde_json::Value::as_u64)
                .unwrap()
        })
        .collect();
    assert_eq!(set, (1..=at).collect(), "round {round}: settings at v{at}");
    let graphs = states
        .get::<GraphState>(sprig_graph::catalog::MODULE)
        .unwrap();
    let have: BTreeSet<u64> = graphs.graphs.keys().map(|g| g - G).collect();
    assert_eq!(have, want, "round {round}: graphs at v{at}");
    for v in &want {
        let g = &graphs.graphs[&(G + v)];
        assert_eq!(
            g.labels,
            BTreeMap::from([(1, format!("L{v}"))]),
            "round {round}: graph {v}'s labels"
        );
        assert_eq!(g.types.len(), 1, "round {round}: graph {v}'s types");
        assert_eq!(g.indexes.len(), 1, "round {round}: graph {v}'s indexes");
        assert_eq!(g.indexes[0].token, format!("L{v}"));
    }
    let docs = states
        .get::<DocState>(crate::mongo::catalog::MODULE)
        .unwrap();
    let have: BTreeSet<u64> = docs
        .of(1)
        .map(|m| m.name[1..].parse::<u64>().unwrap())
        .collect();
    assert_eq!(have, want, "round {round}: collections at v{at}");
    for v in &want {
        let m = docs.get(1, &format!("c{v}")).unwrap();
        assert_eq!(**m, coll(*v), "round {round}: collection {v}");
    }
    let search = states
        .get::<SearchState>(crate::search::catalog::MODULE)
        .unwrap();
    let have: BTreeSet<u64> = search.of_db(1).iter().map(|d| d.index_id - S).collect();
    assert_eq!(have, want, "round {round}: search indexes at v{at}");
    for d in search.of_db(1) {
        assert_eq!(
            **d,
            search_index(d.index_id - S),
            "round {round}: search index"
        );
    }
    let have: BTreeSet<u64> = search
        .analyzers
        .iter()
        .map(|a| a.name[2..].parse::<u64>().unwrap())
        .collect();
    assert_eq!(have, want, "round {round}: analyzers at v{at}");
}

fn extensions() -> Vec<Arc<dyn CatalogExtension>> {
    vec![
        Arc::new(GraphExtension),
        Arc::new(DocExtension),
        Arc::new(SearchExtension),
    ]
}

/// The child: changes until killed, printing each acknowledged version.
async fn child(url: &str, schema: &str, dir: &Path) {
    let pool = mysql_async::Pool::new(url);
    let records: Arc<dyn RecordStore> = Arc::new(EngineRecords::on(open(dir)));
    let at = catalog_version(&records).await;
    let cat: MariaCatalog = Versioner::new(
        MariaBackend::new(locator(&pool, schema)).with_records(Arc::clone(&records)),
        Arc::new(DirectoryAllocator::new(directory(at).await)),
    );
    let seqs = sequences(&records);
    let _ = seqs
        .create(SeqId(SEQ), 1, Some("id"), 1, Ordering::Blocks)
        .await;
    let log = sprig_balancer::decisions::CatalogDecisionLog::new(Arc::clone(&records));
    let recs = sprig_approvals::catalog::CatalogRecStore::new(Arc::clone(&records), T);
    println!("ready");
    loop {
        let v = change(&cat).await;
        println!("ok {v}");
        // The unversioned records, each in a short transaction of its own.
        let (a, z) = seqs.fetch(SeqId(SEQ), 7).await.unwrap();
        println!("blk {a} {z}");
        let id = log.record(T, &decision(v)).await.unwrap();
        println!("dec {id} {v}");
        let r = recs
            .raise(&proposal(v), &policy(), &[], i64::try_from(v).unwrap())
            .await
            .unwrap();
        if let sprig_approvals::Verdict::Wait(id) = r.verdict {
            println!("rec {} {v}", id.number());
        }
        // A workload flush: one stats row in bucket `v`.
        let mut tx = records.begin(T).await.unwrap();
        sprig_workload::store::WorkloadTables::replace_stats(&mut *tx, 1, &[flush_row(v)])
            .await
            .unwrap();
        tx.commit().await.unwrap();
        println!("wl {v} {v}");
    }
}

fn flush_row(v: u64) -> sprig_workload::recorder::FlushRow {
    let mut stats = sprig_workload::recorder::Stats {
        exec_count: v,
        ..sprig_workload::recorder::Stats::default()
    };
    stats.latency_hist.record(v);
    sprig_workload::recorder::FlushRow {
        tenant: T,
        fingerprint: sprig_common::ids::Fingerprint(7),
        bucket_start: v,
        part: 0,
        stats,
        last_seen: SystemTime::now(),
    }
}

const SEQ: u64 = 77;

fn sequences(records: &Arc<dyn RecordStore>) -> sprig_shard::ids::records::RecordSequences {
    sprig_shard::ids::records::RecordSequences::new(Arc::clone(records), T)
        .held_by("kill-child", Duration::from_secs(60))
}

fn decision(v: u64) -> sprig_balancer::decisions::NewDecision {
    sprig_balancer::decisions::NewDecision {
        decided_at_us: i64::try_from(v).unwrap(),
        component: "balancer".into(),
        kind: "shard.move".into(),
        subject_type: "shard".into(),
        subject_id: format!("s{v}"),
        mode: sprig_balancer::decisions::Mode::Applied,
        outcome: sprig_balancer::decisions::Outcome::Scheduled,
        reason_code: "test".into(),
        summary: format!("decision {v}"),
        evidence: serde_json::json!({}),
        alternatives: None,
        job_id: None,
        parent_id: None,
        actor: "kill".into(),
        dc_id: "dc-a".into(),
    }
}

fn proposal(v: u64) -> sprig_approvals::model::Proposal {
    use sprig_approvals::model::{Action, Kind, Proposal, Subject};
    Proposal {
        kind: Kind::IndexCreate,
        subject: Subject {
            tenant: Some(T),
            ..Subject::default()
        },
        target: format!("t/o{v}"),
        action: Action {
            text: format!("ALTER TABLE t{v} ADD INDEX (a)"),
            machine: serde_json::json!({ "v": v }),
        },
        evidence: serde_json::json!({}),
        score: 1.0,
        summary: format!("item {v}"),
        emergency: false,
    }
}

fn policy() -> sprig_approvals::policy::Policy {
    sprig_approvals::policy::Policy::fixed(
        sprig_approvals::model::Mode::Approve,
        sprig_approvals::model::Level::Table,
    )
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn live_module_catalogs_survive_kill_9() {
    let Ok(url) = std::env::var("SPRIG_TEST_MARIADB_SINGLE") else {
        eprintln!("skipped: set SPRIG_TEST_MARIADB_SINGLE to run it");
        return;
    };
    if let Ok(c) = std::env::var(CHILD) {
        let (schema, dir) = c.split_once('|').unwrap();
        child(&url, schema, Path::new(dir)).await;
        return;
    }
    let schema = format!("__sprig_test_modules_kill_{}", std::process::id());
    let dir = std::env::temp_dir().join(format!("sprig-modules-kill-{}", std::process::id()));
    let pool = mysql_async::Pool::new(url.as_str());
    {
        let mut c = pool.get_conn().await.unwrap();
        Migrator::new(&[])
            .unwrap()
            .run(&mut c, &schema, &MigrateOpts::default())
            .await
            .unwrap();
    }
    std::fs::create_dir_all(&dir).unwrap();
    let rounds: u64 = std::env::var("SPRIG_MODULES_KILL_ROUNDS")
        .ok()
        .and_then(|r| r.parse().ok())
        .unwrap_or(8);
    let mut acked_total = 0;
    let mut report = Vec::new();
    // Every acknowledged ID block of every round: never two overlapping.
    let mut blocks: Vec<(i128, i128)> = Vec::new();
    for round in 0..rounds {
        let exe = std::env::current_exe().unwrap();
        let mut ch = Command::new(exe)
            .args([
                "--exact",
                "tenant::catalog_records::modules_kill::live_module_catalogs_survive_kill_9",
                "--nocapture",
                "--test-threads",
                "1",
            ])
            .env(CHILD, format!("{schema}|{}", dir.display()))
            .stdout(Stdio::piped())
            .stderr(Stdio::null())
            .spawn()
            .unwrap();
        let out = ch.stdout.take().unwrap();
        let (tx, rx) = std::sync::mpsc::channel();
        let reader = std::thread::spawn(move || {
            for line in BufReader::new(out).lines().map_while(std::io::Result::ok) {
                if tx.send(line).is_err() {
                    break;
                }
            }
        });
        loop {
            let l = rx.recv_timeout(Duration::from_secs(60)).unwrap();
            if l.ends_with("ready") {
                break;
            }
        }
        let wait = 150 + (round * 137) % 700;
        tokio::time::sleep(Duration::from_millis(wait)).await;
        ch.kill().unwrap(); // SIGKILL
        let _ = ch.wait();
        reader.join().unwrap();
        let lines: Vec<String> = rx.try_iter().collect();
        let acked: Vec<u64> = lines
            .iter()
            .filter_map(|l| l.strip_prefix("ok ").and_then(|v| v.parse().ok()))
            .collect();
        let pairs = |p: &str| -> Vec<(i128, i128)> {
            lines
                .iter()
                .filter_map(|l| {
                    let mut it = l.strip_prefix(p)?.split(' ');
                    Some((it.next()?.parse().ok()?, it.next()?.parse().ok()?))
                })
                .collect()
        };
        blocks.extend(pairs("blk "));
        let decisions = pairs("dec ");
        let items = pairs("rec ");
        let flushes = pairs("wl ");
        let last_acked = acked.last().copied().unwrap_or(0);
        acked_total += acked.len();

        // Reopen, load through the modules' own extensions, and check.
        let eng = open(&dir);
        let records: Arc<dyn RecordStore> = Arc::new(EngineRecords::on(eng.clone()));
        let loc = locator(&pool, &schema);
        let source = MariaSource::new(Arc::clone(&loc), extensions())
            .unwrap()
            .with_records(Arc::clone(&records));
        let l = source.load(T, None, Node::Writer).await.unwrap();
        let at = l.snapshot.versions.catalog;
        assert_eq!(at, catalog_version(&records).await);
        assert!(
            at >= last_acked,
            "round {round}: acknowledged v{last_acked} lost (at v{at})"
        );
        let states = ExtensionStates::default().with(l.ext);
        let ok = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            check(round, at, &l.snapshot, &states);
        }));
        if let Err(e) = ok {
            eprintln!("round {round}: reload at v{at} (last acked v{last_acked})");
            std::panic::resume_unwind(e);
        }
        // Acknowledged unversioned records are all there.
        let log = sprig_balancer::decisions::CatalogDecisionLog::new(Arc::clone(&records));
        for (id, v) in &decisions {
            let d = log.get(T, u64::try_from(*id).unwrap()).await.unwrap();
            assert_eq!(
                d.map(|d| d.d.subject_id),
                Some(format!("s{v}")),
                "round {round}: decision {id}"
            );
        }
        let recs = sprig_approvals::catalog::CatalogRecStore::new(Arc::clone(&records), T);
        for (n, v) in &items {
            let r = recs.get(u64::try_from(*n).unwrap()).await.unwrap();
            assert_eq!(
                r.map(|r| r.target),
                Some(format!("t/o{v}")),
                "round {round}: item {n}"
            );
        }
        // Every acknowledged workload flush is there, with its counts.
        let mut tx = records.begin(T).await.unwrap();
        for (v, _) in &flushes {
            let v = u64::try_from(*v).unwrap();
            let m = sprig_workload::store::WorkloadTables::merged_stats(&mut *tx, v, v + 1)
                .await
                .unwrap();
            assert_eq!(
                m.get(&7).map(|s| s.exec_count),
                Some(v),
                "round {round}: workload flush {v}"
            );
        }
        drop(tx);
        // A block after the crash starts past every block handed out.
        let (a, z) = sequences(&records).fetch(SeqId(SEQ), 3).await.unwrap();
        blocks.push((a, z));
        let mut sorted = blocks.clone();
        sorted.sort_unstable();
        for w in sorted.windows(2) {
            assert!(w[0].1 <= w[1].0, "round {round}: blocks {w:?} overlap");
        }
        // The next change commits on top; a load after it agrees, and an
        // incremental load from the crash's version equals a full one.
        let cat: MariaCatalog = Versioner::new(
            MariaBackend::new(Arc::clone(&loc)).with_records(Arc::clone(&records)),
            Arc::new(DirectoryAllocator::new(directory(at).await)),
        );
        let v = change(&cat).await;
        let l = source.load(T, None, Node::Writer).await.unwrap();
        assert_eq!(l.snapshot.versions.catalog, v);
        let states = ExtensionStates::default().with(l.ext);
        let ok = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            check(round, v, &l.snapshot, &states);
        }));
        if let Err(e) = ok {
            eprintln!("round {round}: first load at v{at}, change v{v}");
            std::panic::resume_unwind(e);
        }
        let mut t = records.begin(T).await.unwrap();
        let inc = CoreRecords::snapshot(&mut *t, Some(at)).await.unwrap();
        drop(t);
        let mut full: Vec<_> = l
            .snapshot
            .objects
            .iter()
            .filter(|o| o.changed_version > at)
            .cloned()
            .collect();
        full.sort_by_key(|o| o.object_id);
        assert_eq!(
            inc.objects, full,
            "round {round}: incremental objects since v{at}"
        );
        report.push(format!(
            "round {round}: killed after {wait} ms, {} acked (last v{last_acked}), committed v{at}",
            acked.len()
        ));
        drop(source);
        drop(records);
        drop(cat);
        drop(eng);
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
    for r in &report {
        eprintln!("{r}");
    }
    eprintln!("{rounds} rounds, {acked_total} acknowledged changes, 0 inconsistencies");
    let mut c = pool.get_conn().await.unwrap();
    c.query_drop(format!("DROP DATABASE `{schema}`"))
        .await
        .unwrap();
    let _ = std::fs::remove_dir_all(&dir);
}
