//! The apply-time full-text index over a real engine: a build of the rows
//! already there, changes followed from the log, a restart that replays
//! from the files' position, and the same scores as the in-memory index.

use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use bytes::Bytes;
use sprig_common::ids::{DbId, ObjectId};
use sprig_engine::txn::TxnOptions;
use sprig_engine::{EngineConfig, StoreKind};
use sprig_exec::{FtMode, FtSpec, FullText};
use sprig_search::innodb::{self, Analyzer, LowerFold, MemIndex, Rules};

use super::*;
use crate::engine::keys::Space;
use crate::engine::row::{self, Datum};
use crate::engine::table;

const TENANT: u64 = 61_306_000_777;

struct TempDir(PathBuf);

impl TempDir {
    fn new() -> Self {
        static N: AtomicU64 = AtomicU64::new(0);
        let p = std::env::temp_dir().join(format!(
            "sprig-gw-ft-{}-{}",
            std::process::id(),
            N.fetch_add(1, Ordering::Relaxed)
        ));
        let _ = std::fs::remove_dir_all(&p);
        std::fs::create_dir_all(&p).unwrap_or_else(|e| panic!("{e}"));
        Self(p)
    }
}

impl Drop for TempDir {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

fn open(dir: &std::path::Path) -> sprig_engine::Engine {
    let mut cfg = EngineConfig::new(dir);
    cfg.store = StoreKind::Rocks;
    cfg.checkpoint_interval = Duration::ZERO;
    sprig_engine::Engine::open(cfg).unwrap_or_else(|e| panic!("{e}"))
}

const T: &str = "CREATE TABLE `docs` (\n  `id` int(11) NOT NULL,\n  `title` varchar(200) DEFAULT NULL,\n  `body` text DEFAULT NULL,\n  PRIMARY KEY (`id`),\n  FULLTEXT KEY `ft` (`title`,`body`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_uca1400_ai_ci";

fn def() -> Arc<TableDef> {
    table::parse(ObjectId(40), 3, DbId(1), T).unwrap_or_else(|e| panic!("{e}"))
}

async fn put(eng: &sprig_engine::Engine, def: &TableDef, sp: &Space, rows: &[(i64, &str, &str)]) {
    let mut tx = Tx::begin(eng, TxnOptions::default()).unwrap_or_else(|e| panic!("{e}"));
    for (id, t, b) in rows {
        let r = vec![
            Datum::Int(*id),
            Datum::Str(Bytes::from(t.to_string())),
            Datum::Str(Bytes::from(b.to_string())),
        ];
        let k = keys::row_key(sp, def, &r[..1]).unwrap_or_else(|| panic!("key"));
        let v = row::encode(def, sp, &r).unwrap_or_else(|e| panic!("{e}"));
        tx.put(Bytes::from(k), v)
            .await
            .unwrap_or_else(|e| panic!("{e}"));
    }
    tx.finish().await.unwrap_or_else(|e| panic!("{e}"));
}

async fn del(eng: &sprig_engine::Engine, def: &TableDef, sp: &Space, id: i64) {
    let mut tx = Tx::begin(eng, TxnOptions::default()).unwrap_or_else(|e| panic!("{e}"));
    let k = keys::row_key(sp, def, &[Datum::Int(id)]).unwrap_or_else(|| panic!("key"));
    tx.delete(Bytes::from(k))
        .await
        .unwrap_or_else(|e| panic!("{e}"));
    tx.finish().await.unwrap_or_else(|e| panic!("{e}"));
}

fn search(
    m: &Arc<Manager>,
    def: &Arc<TableDef>,
    sp: &Space,
    mode: FtMode,
    q: &str,
) -> Vec<(i64, f32)> {
    let s = StmtSearch::new(
        m,
        vec![StmtTable {
            schema: "db".into(),
            name: "docs".into(),
            tenant: TENANT,
            def: Arc::clone(def),
            sp: sp.clone(),
        }],
    )
    .unwrap_or_else(|| panic!("no search"));
    let spec = FtSpec {
        schema: "db".into(),
        table: "docs".into(),
        columns: vec!["body".into(), "title".into()],
        mode,
    };
    s.check(&spec).unwrap_or_else(|e| panic!("{e:?}"));
    let (name, keys) = s
        .hits(&spec, q.as_bytes())
        .unwrap_or_else(|e| panic!("{e:?}"))
        .unwrap_or_else(|| panic!("no hits"));
    assert_eq!(name, "ft");
    keys.iter()
        .map(|k| {
            let Datum::Int(id) = k[0] else {
                panic!("{k:?}")
            };
            #[allow(clippy::cast_possible_truncation)]
            let sc = s
                .score(&spec, q.as_bytes(), k)
                .unwrap_or_else(|e| panic!("{e:?}")) as f32;
            (id, sc)
        })
        .collect()
}

/// The same history in memory: the expected answer. `hist` is every
/// write in order: a row's new words, or `None` for its delete.
fn expect(hist: &[(i64, Option<(&str, &str)>)], mode: FtMode, q: &str) -> Vec<(i64, f32)> {
    let a = Analyzer::new(Rules::default(), Arc::new(LowerFold));
    let mut ix = MemIndex::default();
    let mut ids: Vec<i64> = Vec::new();
    let mut live: std::collections::HashMap<i64, u64> = std::collections::HashMap::new();
    for (id, row) in hist {
        if let Some(d) = live.remove(id) {
            ix.remove(d);
        }
        if let Some((t, b)) = row {
            let d = ix.add(&a, &[t, b]);
            ids.push(*id);
            live.insert(*id, d);
        } else {
            ids.push(*id);
            let d = ix.add(&a, &[""]);
            ix.remove(d);
        }
    }
    // The placeholder documents of deletes hold no words: no effect.
    let node = match mode {
        FtMode::Boolean => innodb::parse_boolean(q, &a).unwrap_or_else(|e| panic!("{e}")),
        _ => innodb::parse_natural(q, &a),
    };
    let mut hits: Vec<(i64, f32, u64)> = innodb::rank(&node, &ix)
        .into_iter()
        .map(|(d, s)| (ids[usize::try_from(d).unwrap_or(0)], s, d))
        .collect();
    innodb::order_hits(&mut hits);
    hits.into_iter().map(|(i, s, _)| (i, s)).collect()
}

fn by_id(mut v: Vec<(i64, f32)>) -> Vec<(i64, f32)> {
    v.sort_by_key(|x| x.0);
    v
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[allow(clippy::too_many_lines)] // one scenario: build, follow, restart, drop
async fn builds_follows_and_recovers() {
    let dir = TempDir::new();
    let def = def();
    assert_eq!(def.fulltext.len(), 1);
    let mut dels: Vec<usize> = Vec::new();
    let hist = |rows: &[(i64, &'static str, &'static str)], dels: &[usize]| {
        let mut h: Vec<(i64, Option<(&str, &str)>)> = Vec::new();
        for (i, (id, t, b)) in rows.iter().enumerate() {
            if dels.contains(&i) {
                h.push((2, None));
            }
            h.push((*id, Some((*t, *b))));
        }
        if dels.contains(&rows.len()) {
            h.push((2, None));
        }
        h
    };
    let mut rows: Vec<(i64, &'static str, &'static str)> = vec![
        (1, "MySQL tutorial", "DBMS stands for DataBase"),
        (
            2,
            "How To Use MySQL Well",
            "After you went through a tutorial",
        ),
        (3, "Optimizing MySQL", "In this tutorial we show"),
    ];
    {
        let eng = open(&dir.0);
        let m = Manager::open(&eng, false).unwrap_or_else(|e| panic!("{e}"));
        let sp = Space::plain(TENANT, 40, Vec::new(), false, &def);
        // Rows before the index: the build reads them.
        put(&eng, &def, &sp, &rows).await;
        let ixs = m
            .sync_table(TENANT, &def, &sp)
            .unwrap_or_else(|e| panic!("{e}"));
        assert_eq!(ixs.len(), 1);
        // Rows after: the log brings them.
        let more = [
            (4, "1001 MySQL Tricks", "1. Never run mysqld as root"),
            (
                5,
                "MySQL vs. YourSQL",
                "In the following database comparison",
            ),
            (6, "MySQL Security", "When configured properly, MySQL"),
        ];
        put(&eng, &def, &sp, &more).await;
        rows.extend(more);
        // An update and a delete.
        put(
            &eng,
            &def,
            &sp,
            &[(3, "Optimizing Postgres", "In this guide")],
        )
        .await;
        rows.push((3, "Optimizing Postgres", "In this guide"));
        del(&eng, &def, &sp, 2).await;
        dels.push(rows.len());
        for (mode, q) in [
            (FtMode::Natural, "database"),
            (FtMode::Natural, "mysql tutorial"),
            (FtMode::Boolean, "+mysql -yoursql"),
            (FtMode::Boolean, "\"never run\" tutor*"),
        ] {
            assert_eq!(
                by_id(search(&m, &def, &sp, mode, q)),
                by_id(expect(&hist(&rows, &dels), mode, q)),
                "{q}"
            );
        }
        m.shutdown();
    }
    // A restart: the index reopens at its files' position and replays.
    {
        let eng = open(&dir.0);
        let m = Manager::open(&eng, false).unwrap_or_else(|e| panic!("{e}"));
        let sp = Space::plain(TENANT, 40, Vec::new(), false, &def);
        put(
            &eng,
            &def,
            &sp,
            &[(7, "Database tuning", "tutorial on database")],
        )
        .await;
        rows.push((7, "Database tuning", "tutorial on database"));
        for (mode, q) in [
            (FtMode::Natural, "database"),
            (FtMode::Boolean, "tutorial >database"),
        ] {
            assert_eq!(
                by_id(search(&m, &def, &sp, mode, q)),
                by_id(expect(&hist(&rows, &dels), mode, q)),
                "{q}"
            );
        }
        // The key goes: the index and its files go.
        let mut plain = (*def).clone();
        plain.fulltext.clear();
        plain.version += 1;
        assert!(
            m.sync_table(TENANT, &plain, &sp)
                .unwrap_or_default()
                .is_empty()
        );
        assert!(std::fs::read_dir(dir.0.join("idx")).is_ok_and(|mut u| {
            u.all(|u| {
                u.ok().is_none_or(|u| {
                    std::fs::read_dir(u.path())
                        .map(|t| {
                            t.flatten().all(|t| {
                                std::fs::read_dir(t.path())
                                    .map(|i| i.count() == 0)
                                    .unwrap_or(true)
                            })
                        })
                        .unwrap_or(true)
                })
            })
        }));
        m.shutdown();
    }
}

#[test]
fn collation_terms_fold_and_keep_prefixes() {
    let ai = CollationFold::new(
        Collation::from_name("utf8mb4_uca1400_ai_ci").unwrap_or_else(|| panic!("coll")),
    );
    assert_eq!(ai.term("Café"), ai.term("cafe"));
    assert_eq!(ai.term("ÜBER"), ai.term("uber"));
    assert!(ai.term("tutorial").starts_with(&ai.term("tu")));
    let bin =
        CollationFold::new(Collation::from_name("utf8mb4_bin").unwrap_or_else(|| panic!("coll")));
    assert_ne!(bin.term("Cafe"), bin.term("cafe"));
}
