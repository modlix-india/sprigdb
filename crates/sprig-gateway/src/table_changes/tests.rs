use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use sprig_common::ids::{DbId, ObjectId, TenantId};
use sprig_engine::log::SyncMode;
use sprig_engine::txn::TxnOptions;
use sprig_engine::{EngineConfig, StoreKind};
use sprig_streams::changes::{ChangeConfig, ChangeLog};
use sqlparser::dialect::MySqlDialect;
use sqlparser::parser::Parser;

use super::*;
use crate::engine::plan::{self, BindCtx, Names};
use crate::engine::table::{self, Parsed};

const TENANT: TenantId = TenantId(64_306_000_101);
const DB: DbId = DbId(1);

const ITEMS: &str = "CREATE TABLE `items` (\n  `id` int(11) NOT NULL,\n  `name` varchar(20) DEFAULT NULL,\n  `price` decimal(30,2) DEFAULT NULL,\n  `raw` varbinary(8) DEFAULT NULL,\n  PRIMARY KEY (`id`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_uca1400_ai_ci";

const OTHER: &str = "CREATE TABLE `other` (\n  `id` int(11) NOT NULL,\n  PRIMARY KEY (`id`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_uca1400_ai_ci";

struct Fake(HashMap<String, Arc<Parsed>>);

impl Names for Fake {
    fn engine_db(&self, db: DbId) -> bool {
        db == DB
    }
    fn table(&self, _db: DbId, name: &str) -> Option<Arc<Parsed>> {
        self.0.get(&name.to_lowercase()).cloned()
    }
    fn other_relation(&self, _db: DbId, _name: &str) -> bool {
        false
    }
    fn db_name(&self, _db: DbId) -> String {
        "shop".into()
    }
    fn db_by_name(&self, name: &str) -> Option<DbId> {
        (name == "shop").then_some(DB)
    }
    fn version(&self) -> u64 {
        1
    }
}

struct Bed {
    eng: crate::engine::Engine,
    log: ChangeLog,
    names: Fake,
    dir: std::path::PathBuf,
}

impl Drop for Bed {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.dir);
    }
}

fn bed(tag: &str) -> Bed {
    let dir = std::env::temp_dir().join(format!("sprig-gw-tchanges-{tag}-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    let mut cfg = EngineConfig::new(dir.join("engine"));
    cfg.store = StoreKind::Mem;
    cfg.log.sync = SyncMode::Off;
    cfg.checkpoint_interval = Duration::ZERO;
    let inner = sprig_engine::Engine::open(cfg).unwrap();
    let log = ChangeLog::open(&inner, dir.join("changes"), ChangeConfig::default()).unwrap();
    let mut t = HashMap::new();
    t.insert(
        "items".to_owned(),
        Arc::new(table::parse(ObjectId(10), 1, DB, ITEMS)),
    );
    t.insert(
        "other".to_owned(),
        Arc::new(table::parse(ObjectId(11), 1, DB, OTHER)),
    );
    Bed {
        eng: crate::engine::Engine::new(inner),
        log,
        names: Fake(t),
        dir,
    }
}

impl Bed {
    async fn sql(&self, sql: &str) {
        let stmt = Parser::parse_sql(&MySqlDialect {}, sql).unwrap().remove(0);
        let cx = BindCtx {
            tenant: TENANT,
            names: &self.names,
            current: Some(DB),
            backslash: true,
        };
        let p = plan::bind(&cx, &stmt).unwrap().unwrap();
        self.eng
            .run(TENANT.0, &p, &[], false, TxnOptions::default())
            .await
            .unwrap();
    }

    fn watched(&self) -> HashMap<u32, Watched> {
        let def = match &*self.names.0["items"] {
            Ok(d) => Arc::clone(d),
            Err(e) => panic!("{e}"),
        };
        HashMap::from([(
            10,
            Watched {
                def,
                db: "shop".into(),
            },
        )])
    }

    /// Reads until `n` events came (or 5 s passed), from `token`.
    async fn events(
        &self,
        token: Option<String>,
        from_ms: i64,
        n: usize,
        consumer: &str,
    ) -> (Vec<Event>, String) {
        let w = self.watched();
        let mut out = Vec::new();
        let mut tk = token;
        let until = std::time::Instant::now() + Duration::from_secs(5);
        loop {
            let b = read(
                &self.eng,
                &self.log,
                TENANT.0,
                &w,
                tk.as_deref(),
                from_ms,
                PAGE_BYTES,
                Duration::from_millis(200),
                consumer,
            )
            .await
            .unwrap();
            out.extend(b.events);
            tk = Some(b.next);
            if out.len() >= n || std::time::Instant::now() > until {
                return (out, tk.unwrap_or_default());
            }
        }
    }
}

#[tokio::test]
async fn row_changes_become_events_with_images() {
    let b = bed("images");
    b.sql("INSERT INTO items VALUES (1, 'one', 1.50, X'00ff'), (2, 'two', NULL, NULL)")
        .await;
    b.sql("INSERT INTO other VALUES (7)").await;
    b.sql("UPDATE items SET name = 'uno', price = 1234567890123456789.10 WHERE id = 1")
        .await;
    b.sql("DELETE FROM items WHERE id = 2").await;
    let (ev, next) = b.events(None, 0, 4, "c1").await;
    let ops: Vec<&str> = ev.iter().map(|e| e.op).collect();
    assert_eq!(ops, ["c", "c", "u", "d"], "{ev:#?}");
    assert_eq!(ev[0].key, json!({"id": 1}));
    assert_eq!(
        ev[0].after,
        Some(json!({"id": 1, "name": "one", "price": 1.5, "raw": "AP8="}))
    );
    assert_eq!(ev[0].before, None);
    assert_eq!(
        ev[2].before,
        Some(json!({"id": 1, "name": "one", "price": 1.5, "raw": "AP8="}))
    );
    // A decimal a double can't hold exactly is its text.
    assert_eq!(
        ev[2].after,
        Some(json!({"id": 1, "name": "uno", "price": "1234567890123456789.10", "raw": "AP8="}))
    );
    assert_eq!(ev[3].after, None);
    assert_eq!(
        ev[3].before,
        Some(json!({"id": 2, "name": "two", "price": null, "raw": null}))
    );
    assert_eq!(ev[3].envelope["source"]["object"], json!("items"));
    assert_eq!(ev[3].envelope["source"]["db"], json!("shop"));
    assert_eq!(ev[3].envelope["op"], json!("d"));
    // Tokens order the events; resuming after one gives the rest.
    let (rest, _) = b.events(Some(ev[1].token.clone()), 0, 2, "c2").await;
    assert_eq!(rest, ev[2..].to_vec());
    // Nothing new after the last token.
    let (none, _) = b.events(Some(next.clone()), 0, 1, "c1").await;
    assert!(none.is_empty());
    // A later change comes after it (the kept reader).
    b.sql("INSERT INTO items VALUES (3, 'three', 3, NULL)")
        .await;
    let (more, _) = b.events(Some(next), 0, 1, "c1").await;
    assert_eq!(more.len(), 1);
    assert_eq!(more[0].key, json!({"id": 3}));
}

#[tokio::test]
async fn a_start_time_leaves_out_earlier_changes() {
    let b = bed("from");
    b.sql("INSERT INTO items VALUES (1, 'one', 1, NULL)").await;
    let (first, _) = b.events(None, 0, 1, "").await;
    assert_eq!(first.len(), 1);
    let from = first[0].ts_ms + 1;
    tokio::time::sleep(Duration::from_millis(5)).await;
    b.sql("INSERT INTO items VALUES (2, 'two', 2, NULL)").await;
    let (ev, _) = b.events(None, from, 1, "").await;
    assert_eq!(ev.len(), 1, "{ev:#?}");
    assert_eq!(ev[0].key, json!({"id": 2}));
}

#[test]
fn decimals_and_tokens() {
    assert_eq!(decimal("1.50"), json!(1.5));
    assert_eq!(decimal("10"), json!(10.0));
    assert_eq!(
        decimal("12345678901234567890.1234567891"),
        json!("12345678901234567890.1234567891")
    );
    assert_eq!(ms_of(cts_of_ms(1_762_508_100_123)), 1_762_508_100_123);
}

#[test]
fn delete_keys_decode_from_the_row_key() {
    let def = match table::parse(ObjectId(10), 1, DB, ITEMS) {
        Ok(d) => d,
        Err(e) => panic!("{e}"),
    };
    let sp = crate::engine::keys::Space::plain(TENANT.0, 10, vec![], false, &def);
    let k = crate::engine::keys::row_key(&sp, &def, &[Datum::Int(42)]).unwrap();
    assert_eq!(key_of_bytes(&def, false, &k), json!({"id": 42}));
}
