//! Test bed for the engine document store: a store on the engine's
//! in-memory path or on RocksDB, a cursor table, and the index check.

#![allow(
    dead_code,
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::many_single_char_names,
    clippy::cast_precision_loss,
    clippy::cast_sign_loss,
    clippy::cast_possible_truncation,
    clippy::cast_possible_wrap,
    clippy::float_cmp,
    clippy::doc_markdown,
    clippy::unused_async,
    clippy::map_entry,
    clippy::single_match_else,
    clippy::match_wildcard_for_single_variants
)]

use std::collections::{BTreeSet, HashMap};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use bson::{Bson, Document, doc};
use sprig_doc::docstore::cmd::Reply;
use sprig_doc::docstore::read::StoreCursor;
use sprig_doc::docstore::{DocStore, Name, keys, layout};
use sprig_engine::txn::testing::MemPath;
use sprig_engine::txn::{CommitPath, Dir, UnitConfig};
use sprig_engine::{Engine, EngineConfig, StoreKind};

/// The tenant every test uses.
pub(crate) const TENANT: u64 = 42;

pub(crate) struct Bed {
    pub(crate) store: Arc<DocStore>,
    cursors: Mutex<HashMap<i64, StoreCursor>>,
    next: Mutex<i64>,
}

/// A fresh directory under the build's temporary folder.
pub(crate) fn dir(tag: &str) -> PathBuf {
    let d = PathBuf::from(env!("CARGO_TARGET_TMPDIR"))
        .join(format!("m-docs-{tag}-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&d);
    d
}

/// A store on the engine over RocksDB in `d`, opened as after a crash
/// (no flush on close).
pub(crate) fn rocks_store(d: &Path) -> Arc<DocStore> {
    let mut c = EngineConfig::new(d);
    c.store = StoreKind::Rocks;
    c.budget.engine_mb = 64;
    c.rocks.avoid_flush_on_close = true;
    c.checkpoint_interval = Duration::ZERO;
    let e = Engine::open(c).unwrap();
    Arc::new(DocStore::new(Arc::new(e) as Arc<dyn CommitPath>))
}

/// The ids of documents (int32 `_id`s).
pub(crate) fn ids(d: &[Document]) -> Vec<i32> {
    d.iter().map(|x| x.get_i32("_id").unwrap()).collect()
}

impl Bed {
    pub(crate) fn mem() -> Self {
        let p = MemPath::start(UnitConfig::default(), Duration::from_micros(200));
        Self::on(Arc::new(DocStore::new(p as Arc<dyn CommitPath>)))
    }

    pub(crate) fn on(store: Arc<DocStore>) -> Self {
        Self {
            store,
            cursors: Mutex::new(HashMap::new()),
            next: Mutex::new(1),
        }
    }

    /// Runs a command; errors come back as MongoDB's error reply.
    pub(crate) async fn run(&self, db: &str, body: Document) -> Document {
        let name = body.keys().next().cloned().unwrap();
        if name == "getMore" {
            let id = body.get_i64("getMore").unwrap();
            let n = usize::try_from(body.get_i64("batchSize").unwrap_or(101)).unwrap();
            let cur = self.cursors.lock().unwrap().remove(&id);
            let Some(mut cur) = cur else {
                return doc! {"ok": 0.0, "code": 43, "errmsg": "cursor not found"};
            };
            let batch = cur.next_batch(&self.store, n).await.unwrap();
            let ns = cur.coll().ns();
            let out_id = if cur.exhausted() {
                0
            } else {
                self.cursors.lock().unwrap().insert(id, cur);
                id
            };
            return doc! {"cursor": {"nextBatch": batch, "id": out_id, "ns": ns}, "ok": 1.0};
        }
        let mut body = body;
        body.insert("$db", db);
        let coll = sprig_doc::collation::Collation::of(&body).unwrap();
        let r =
            sprig_doc::collation::scope(coll, self.store.command(TENANT, "u", db, &name, &body))
                .await;
        match r {
            Ok(Reply::Doc(d)) => d,
            Ok(c @ Reply::Cursor { .. }) => {
                let id = {
                    let mut g = self.next.lock().unwrap();
                    *g += 1;
                    *g
                };
                if let Reply::Cursor { ns, first, rest } = c {
                    let has = rest.is_some();
                    if let Some(r) = rest {
                        self.cursors.lock().unwrap().insert(id, r);
                    }
                    let id = if has { id } else { 0 };
                    doc! {"cursor": {"firstBatch": first, "id": id, "ns": ns}, "ok": 1.0}
                } else {
                    unreachable!()
                }
            }
            Err(e) => e.to_doc(),
        }
    }

    /// Every document of a cursor command, through `getMore`s.
    pub(crate) async fn find_all_cmd(&self, db: &str, body: Document) -> Vec<Document> {
        let r = self.run(db, body).await;
        let c = r.get_document("cursor").unwrap_or_else(|_| panic!("{r}"));
        let mut out: Vec<Document> = c
            .get_array("firstBatch")
            .unwrap()
            .iter()
            .map(|x| x.as_document().unwrap().clone())
            .collect();
        let mut id = c.get_i64("id").unwrap();
        while id != 0 {
            let r = self
                .run(
                    db,
                    doc! {"getMore": id, "collection": "x", "batchSize": 7i64},
                )
                .await;
            let c = r.get_document("cursor").unwrap();
            out.extend(
                c.get_array("nextBatch")
                    .unwrap()
                    .iter()
                    .map(|x| x.as_document().unwrap().clone()),
            );
            id = c.get_i64("id").unwrap();
        }
        out
    }

    pub(crate) async fn find_all(&self, db: &str, body: Document) -> Vec<Document> {
        self.find_all_cmd(db, body).await
    }
}

/// Every index's entries equal the entries of the live documents.
pub(crate) async fn check_indexes(b: &Bed, db: &str) {
    for c in b.store.list(TENANT, Some(db)).unwrap() {
        let c = b
            .store
            .coll(TENANT, &Name::new(db, &c.meta.name))
            .unwrap()
            .unwrap();
        let mut t = b.store.begin(false);
        let p = c.space.docs();
        let rows = t
            .scan(&p, &layout::after_prefix(&p), Dir::Forward, usize::MAX)
            .unwrap();
        for idx in &c.meta.indexes {
            let mut want = BTreeSet::new();
            for (k, v) in &rows {
                let d = sprig_doc::docstore::read::decode(v).unwrap();
                let id = &k[p.len()..];
                let (es, _) = keys::entries(&d, idx).unwrap();
                for e in es {
                    let mut key = c.space.index(idx.id, idx.unique);
                    key.extend_from_slice(&e);
                    if !idx.unique {
                        key.extend_from_slice(id);
                    }
                    want.insert((key, id.to_vec()));
                }
            }
            let ip = c.space.index(idx.id, idx.unique);
            let got: BTreeSet<(Vec<u8>, Vec<u8>)> = t
                .scan(&ip, &layout::after_prefix(&ip), Dir::Forward, usize::MAX)
                .unwrap()
                .into_iter()
                .map(|(k, v)| (k, v.to_vec()))
                .collect();
            assert_eq!(got, want, "index {} of {}", idx.name, c.meta.name);
        }
        t.rollback();
    }
}

/// `Bson` helper for tests.
pub(crate) fn int(n: i32) -> Bson {
    Bson::Int32(n)
}
