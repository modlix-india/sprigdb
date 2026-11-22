//! Tests of catalog records on the in-memory store: reads, locks, typed
//! rows, counters, and versioned changes (one transaction with their
//! version bump; a crash at any step leaves no row and no version).

#![allow(clippy::unwrap_used, clippy::many_single_char_names)]

use std::sync::Arc;
use std::time::Duration;

use serde::{Deserialize, Serialize};
use serde_json::json;
use sprig_common::SprigCode;
use sprig_common::ids::TenantId;
use sprig_meta::{MemStore, MetaStore, Op, Tier, Txn};

use super::mem::MemRecords;
use super::*;
use crate::version::{DirectoryAllocator, Failpoint, Step};

const T: TenantId = TenantId(42);
const U: TenantId = TenantId(43);

async fn seed(store: &dyn MetaStore, tenant: TenantId) {
    let key = Tier::Global.key(&format!("tenants/{tenant}")).unwrap();
    let body = json!({"v": 1, "tenant_id": tenant.0, "name": "acme", "state": "active",
                      "home_dc": "mum1", "catalog_version": 0, "auth_version": 0});
    let r = store
        .txn(Txn {
            then: vec![Op::Put {
                key,
                value: serde_json::to_vec(&body).unwrap(),
                lease: None,
            }],
            ..Txn::default()
        })
        .await
        .unwrap();
    assert!(r.succeeded);
}

async fn setup() -> (MemRecords, AuthCatalog) {
    let g = Arc::new(MemStore::new(Tier::Global));
    seed(&*g, T).await;
    seed(&*g, U).await;
    let recs = MemRecords::new();
    let alloc = DirectoryAllocator::new(g).with_timeout(Duration::from_millis(300));
    let cat = Versioner::new(RecBackend::new(Arc::new(recs.clone())), Arc::new(alloc));
    (recs, cat)
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
struct Row {
    id: u32,
    name: String,
    changed_version: u64,
}

const ROWS: RowTable = RowTable::new("test", "rows");

#[tokio::test]
async fn reads_see_own_writes_and_a_stable_view() {
    let recs = MemRecords::new();
    let mut a = recs.begin(T).await.unwrap();
    a.put(b"k1", b"one").await.unwrap();
    assert_eq!(a.get(b"k1").await.unwrap().as_deref(), Some(&b"one"[..]));
    // Not visible before the commit.
    let mut b = recs.begin(T).await.unwrap();
    assert_eq!(b.get(b"k1").await.unwrap(), None);
    a.commit().await.unwrap();
    // b's read view was taken at its first read.
    assert_eq!(b.get(b"k1").await.unwrap(), None);
    // A current read sees the commit.
    assert_eq!(
        b.get_for_update(b"k1").await.unwrap().as_deref(),
        Some(&b"one"[..])
    );
    b.rollback().await.unwrap();
    // Other tenants see nothing.
    let mut c = recs.begin(U).await.unwrap();
    assert_eq!(c.get(b"k1").await.unwrap(), None);
}

#[tokio::test]
async fn scans_merge_own_writes_in_key_order() {
    let recs = MemRecords::new();
    let mut a = recs.begin(T).await.unwrap();
    for k in [b"a/3", b"a/1", b"b/1"] {
        a.put(k, k).await.unwrap();
    }
    a.commit().await.unwrap();
    let mut b = recs.begin(T).await.unwrap();
    b.delete(b"a/1").await.unwrap();
    b.put(b"a/2", b"x").await.unwrap();
    let got = scan_all(&mut *b, b"a/", &prefix_end(b"a/")).await.unwrap();
    let keys: Vec<_> = got.iter().map(|(k, _)| k.clone()).collect();
    assert_eq!(keys, vec![b"a/2".to_vec(), b"a/3".to_vec()]);
}

#[tokio::test]
async fn locks_wait_then_time_out_as_busy() {
    let recs = MemRecords::new().with_lock_wait(Duration::from_millis(50));
    let mut a = recs.begin(T).await.unwrap();
    a.get_for_update(b"v").await.unwrap();
    let mut b = recs.begin(T).await.unwrap();
    let e = b.get_for_update(b"v").await.unwrap_err();
    assert_eq!(e.code(), Some(SprigCode::CatalogConflict));
    // Another tenant's record of the same name is another lock.
    let mut c = recs.begin(U).await.unwrap();
    c.get_for_update(b"v").await.unwrap();
    a.commit().await.unwrap();
    let mut d = recs.begin(T).await.unwrap();
    d.get_for_update(b"v").await.unwrap();
}

#[tokio::test]
async fn typed_rows_and_counters() {
    let recs = MemRecords::new();
    let mut tx = recs.begin(T).await.unwrap();
    let first = next_ids(&mut *tx, "row", 2).await.unwrap();
    assert_eq!(first, 1);
    assert_eq!(next_ids(&mut *tx, "row", 1).await.unwrap(), 3);
    for (id, name) in [(10u32, "ten"), (2, "two"), (300, "three hundred")] {
        let r = Row {
            id,
            name: name.into(),
            changed_version: 1,
        };
        ROWS.put(&mut *tx, &Pk::new().u32(id), &r).await.unwrap();
    }
    tx.commit().await.unwrap();
    let mut tx = recs.begin(T).await.unwrap();
    let all: Vec<Row> = ROWS.rows(&mut *tx, &Pk::new()).await.unwrap();
    assert_eq!(
        all.iter().map(|r| r.id).collect::<Vec<_>>(),
        vec![2, 10, 300]
    );
    let one: Option<Row> = ROWS.get(&mut *tx, &Pk::new().u32(10)).await.unwrap();
    assert_eq!(one.unwrap().name, "ten");
    counter_at_least(&mut *tx, "row", 100).await.unwrap();
    assert_eq!(next_ids(&mut *tx, "row", 1).await.unwrap(), 101);
}

#[test]
fn byte_string_keys_are_prefix_free_and_ordered() {
    let a = Pk::new().bytes(b"ab").u32(9);
    let b = Pk::new().bytes(b"ab\0").u32(1);
    let c = Pk::new().bytes(b"abc").u32(1);
    assert!(a.0 < b.0 && b.0 < c.0);
    assert_eq!(prefix_end(b"m/a/"), b"m/a0".to_vec());
}

#[tokio::test]
async fn a_change_commits_its_rows_with_the_version() {
    let (recs, cat) = setup().await;
    let mut ch = cat.change(T, VersionKind::Auth).await.unwrap();
    let v = ch.version();
    let r = Row {
        id: 1,
        name: "x".into(),
        changed_version: v,
    };
    ROWS.put(ch.tx().records(), &Pk::new().u32(1), &r)
        .await
        .unwrap();
    assert_eq!(ch.commit().await.unwrap(), v);
    let mut tx = recs.begin(T).await.unwrap();
    assert_eq!(read_version(&mut *tx, VersionKind::Auth).await.unwrap(), v);
    assert_eq!(read_version(&mut *tx, VersionKind::Catalog).await.unwrap(), 0);
    let got: Option<Row> = ROWS.get(&mut *tx, &Pk::new().u32(1)).await.unwrap();
    assert_eq!(got, Some(r));
}

fn crash_at(step: Step) -> Failpoint {
    Arc::new(move |_, _, s| {
        if s == step {
            Err(SprigError::internal(format!("injected crash at {s:?}")))
        } else {
            Ok(())
        }
    })
}

#[tokio::test]
async fn a_crash_at_any_step_leaves_no_row_and_the_gap_settles() {
    for step in [Step::Locked, Step::Allocated, Step::BeforeCommit] {
        let (recs, cat) = setup().await;
        let cat = cat.with_failpoint(crash_at(step));
        let r = async {
            let mut ch = cat.change(T, VersionKind::Auth).await?;
            let row = Row {
                id: 7,
                name: "lost".into(),
                changed_version: ch.version(),
            };
            ROWS.put(ch.tx().records(), &Pk::new().u32(7), &row).await?;
            ch.commit().await
        }
        .await;
        assert!(r.is_err(), "{step:?}");
        let mut tx = recs.begin(T).await.unwrap();
        let rows: Vec<Row> = ROWS.rows(&mut *tx, &Pk::new()).await.unwrap();
        assert!(rows.is_empty(), "{step:?}");
        assert_eq!(read_version(&mut *tx, VersionKind::Auth).await.unwrap(), 0);
        // The version record isn't left locked: the next change goes
        // through, above any gap.
        let plain = Versioner::new(cat.backend().clone(), Arc::clone(cat.allocator()));
        let settled = plain.settle(T, VersionKind::Auth).await.unwrap();
        let ch = plain.change(T, VersionKind::Auth).await.unwrap();
        assert!(ch.version() > settled, "{step:?}");
        ch.commit().await.unwrap();
    }
}

#[tokio::test]
async fn changes_of_one_tenant_queue_on_the_version_record() {
    let (_recs, cat) = setup().await;
    let first = cat.change(T, VersionKind::Auth).await.unwrap();
    let cat2 = cat.clone();
    let waiter = tokio::spawn(async move {
        let ch = cat2.change(T, VersionKind::Auth).await.unwrap();
        ch.commit().await.unwrap()
    });
    tokio::time::sleep(Duration::from_millis(30)).await;
    assert!(!waiter.is_finished());
    // Another tenant isn't held up.
    let other = cat.change(U, VersionKind::Auth).await.unwrap();
    other.commit().await.unwrap();
    let v1 = first.commit().await.unwrap();
    let v2 = waiter.await.unwrap();
    assert!(v2 > v1);
}
