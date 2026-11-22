#![allow(
    clippy::unwrap_used,
    clippy::cast_possible_truncation,
    clippy::cast_possible_wrap,
    clippy::many_single_char_names,
    clippy::too_many_lines,
    clippy::manual_is_multiple_of,
    clippy::needless_borrow,
    clippy::range_plus_one
)]

use std::sync::Arc;
use std::time::{Duration, UNIX_EPOCH};

use sprig_common::ids::TenantId;

use super::*;
use crate::cache::mem::MemSource;
use crate::ddl::{DdlBackend, DdlTx};
use crate::records::RecordStore;
use crate::records::mem::MemRecords;
use crate::rows::{SettingsBackend, SettingsTx};
use crate::store::{StaticLocator, Store};
use crate::version::{MariaBackend, VersionBackend, VersionKind};

const T: TenantId = TenantId(7);

fn backend() -> (MariaBackend, Arc<MemRecords>) {
    let recs = Arc::new(MemRecords::new());
    let b = MariaBackend::new(StaticLocator::new().shared())
        .with_records(Arc::clone(&recs) as Arc<dyn RecordStore>);
    (b, recs)
}

fn at(s: u64) -> SystemTime {
    UNIX_EPOCH + Duration::from_micros(1_700_000_000_000_000 + s)
}

fn db(id: u32, name: &str, state: DbState, v: u64) -> DatabaseRow {
    DatabaseRow {
        db_id: DbId(id),
        name: name.to_uppercase(),
        name_key: name.to_owned(),
        default_charset: "utf8mb4".into(),
        default_collation: "utf8mb4_bin".into(),
        default_group_id: GroupId(1),
        state,
        options: serde_json::json!({"o": id}),
        created_at: at(u64::from(id)),
        created_by: Some(UserId(3)),
        changed_version: v,
    }
}

fn obj(id: u64, db: u32, kind: ObjectKind, name: &str, state: ObjectState, v: u64) -> ObjectRow {
    ObjectRow {
        object_id: ObjectId(id),
        db_id: DbId(db),
        namespace: kind.namespace(),
        name: name.to_owned(),
        name_key: name.to_owned(),
        kind,
        parent_id: (id % 3 == 0).then_some(ObjectId(id - 1)),
        state,
        schema_version: id,
        flags: ObjectFlags::RLS | ObjectFlags::INHERITABLE,
        pending_job_id: None,
        created_at: at(id),
        changed_version: v,
    }
}

fn setting(scope: SettingScope, key: &str, value: Option<i64>, v: u64) -> SettingRow {
    SettingRow {
        scope,
        key: key.to_owned(),
        value: value.map(|n| serde_json::json!(n)),
        bounds: None,
        set_by: "user:1".into(),
        set_at: at(v),
        version: v,
    }
}

fn code(r: &Result<impl Sized>) -> Option<u16> {
    r.as_ref().err().map(|e| crate::ddl::errno(&e).unwrap_or(1))
}

/// A tiny deterministic generator (the test needs no extra crate).
struct Rng(u64);

impl Rng {
    fn next(&mut self) -> u64 {
        self.0 = self
            .0
            .wrapping_mul(6_364_136_223_846_793_005)
            .wrapping_add(1);
        self.0 >> 33
    }

    fn below(&mut self, n: u64) -> u64 {
        self.next() % n
    }
}

fn sorted(mut s: Snapshot) -> Snapshot {
    s.databases.sort_by_key(|d| d.db_id);
    s.objects.sort_by_key(|o| o.object_id);
    s.settings
        .sort_by(|a, b| (a.scope, &a.key).cmp(&(b.scope, &b.key)));
    s
}

/// Runs the same random changes on the in-memory catalog model and on the
/// catalog on records: every call answers the same, and full and
/// incremental snapshots after each change are equal.
async fn differential(seed: u64) {
    let mem = MemSource::new();
    mem.bootstrap(T);
    let (rec, _recs) = backend();
    let mut rng = Rng(seed);
    let names = ["a", "b", "c", "d"];
    let mut next_db = 1u32;
    let mut next_obj = 1u64;
    let mut versions: Vec<u64> = Vec::new();
    for round in 0..60u64 {
        let (mut mt, mv) = mem.begin_locked(T, VersionKind::Catalog).await.unwrap();
        let (mut rt, rv) = rec.begin_locked(T, VersionKind::Catalog).await.unwrap();
        assert_eq!(mv, rv, "committed versions differ in round {round}");
        let v = mv + 1;
        for _ in 0..1 + rng.below(6) {
            match rng.below(11) {
                0 | 1 => {
                    let r = db(next_db, names[rng.below(4) as usize], DbState::Active, v);
                    next_db += 1;
                    let a = DdlTx::insert_database(&mut mt, &r).await;
                    let b = DdlTx::insert_database(&mut rt, &r).await;
                    assert_eq!(code(&a), code(&b), "insert db {r:?}");
                }
                2 => {
                    let Some(d) = DdlTx::databases(&mut rt)
                        .await
                        .unwrap()
                        .into_iter()
                        .nth(rng.below(u64::from(next_db)) as usize)
                    else {
                        continue;
                    };
                    let state = [DbState::Dropped, DbState::Dropping, DbState::Active]
                        [rng.below(3) as usize];
                    let r = DatabaseRow {
                        state,
                        name_key: names[rng.below(4) as usize].into(),
                        changed_version: v,
                        ..d
                    };
                    let a = DdlTx::update_database(&mut mt, &r).await;
                    let b = DdlTx::update_database(&mut rt, &r).await;
                    assert_eq!(code(&a), code(&b), "update db {r:?}");
                }
                3..=5 => {
                    let dbs = DdlTx::databases(&mut rt).await.unwrap();
                    if dbs.is_empty() {
                        continue;
                    }
                    let d = &dbs[rng.below(dbs.len() as u64) as usize];
                    let kind = [ObjectKind::Table, ObjectKind::Function, ObjectKind::View]
                        [rng.below(3) as usize];
                    let r = obj(
                        next_obj,
                        d.db_id.0,
                        kind,
                        names[rng.below(4) as usize],
                        ObjectState::Active,
                        v,
                    );
                    next_obj += 1;
                    let a = DdlTx::insert_object(&mut mt, &r).await;
                    let b = DdlTx::insert_object(&mut rt, &r).await;
                    assert_eq!(code(&a), code(&b), "insert object {r:?}");
                    if b.is_ok() && rng.below(2) == 0 {
                        let sv = SchemaVersionRow {
                            object_id: r.object_id,
                            version: 1 + rng.below(3),
                            ddl: "CREATE".into(),
                            create_sql: Some("CREATE TABLE x (a INT)".into()),
                            create_sql_hash: Some([7u8; 32]),
                            ddl_job_id: None,
                            applied_by: Some(UserId(2)),
                            applied_at: at(9),
                            changed_version: v,
                        };
                        let a = DdlTx::insert_schema_version(&mut mt, &sv).await;
                        let b = DdlTx::insert_schema_version(&mut rt, &sv).await;
                        assert_eq!(code(&a), code(&b));
                        assert_eq!(
                            DdlTx::latest_schema_version(&mut mt, r.object_id)
                                .await
                                .unwrap(),
                            DdlTx::latest_schema_version(&mut rt, r.object_id)
                                .await
                                .unwrap()
                        );
                    }
                }
                6 => {
                    let Some(o) = DdlTx::object(&mut rt, ObjectId(1 + rng.below(next_obj)))
                        .await
                        .unwrap()
                    else {
                        continue;
                    };
                    let r = ObjectRow {
                        state: [
                            ObjectState::Dropped,
                            ObjectState::Altering,
                            ObjectState::Active,
                        ][rng.below(3) as usize],
                        name_key: names[rng.below(4) as usize].into(),
                        changed_version: v,
                        ..o
                    };
                    let a = DdlTx::update_object(&mut mt, &r).await;
                    let b = DdlTx::update_object(&mut rt, &r).await;
                    assert_eq!(code(&a), code(&b), "update object {r:?}");
                }
                7 => {
                    let id = ObjectId(1 + rng.below(next_obj));
                    DdlTx::delete_object(&mut mt, id).await.unwrap();
                    DdlTx::delete_object(&mut rt, id).await.unwrap();
                }
                8 | 9 => {
                    let scope = match rng.below(3) {
                        0 => SettingScope::Tenant,
                        1 => SettingScope::Database(DbId(1 + rng.below(3) as u32)),
                        _ => SettingScope::Object(ObjectId(1 + rng.below(4))),
                    };
                    let key = ["s.one", "s.two", "sprig.internal"][rng.below(3) as usize];
                    // (The model also purges empty internal rows; the
                    // catalog keeps them, as its SQL did.)
                    if rng.below(3) == 0 && !key.starts_with("sprig.") {
                        let a = SettingsTx::reset_setting(
                            &mut mt,
                            scope,
                            key,
                            true,
                            true,
                            "user:2",
                            at(v),
                            v,
                        )
                        .await
                        .unwrap();
                        let b = SettingsTx::reset_setting(
                            &mut rt,
                            scope,
                            key,
                            true,
                            true,
                            "user:2",
                            at(v),
                            v,
                        )
                        .await
                        .unwrap();
                        assert_eq!(a, b);
                    } else {
                        let r = setting(scope, key, Some(rng.below(100) as i64), v);
                        SettingsTx::put_setting(&mut mt, &r).await.unwrap();
                        SettingsTx::put_setting(&mut rt, &r).await.unwrap();
                    }
                    assert_eq!(
                        SettingsTx::key_rows(&mut mt, key).await.unwrap(),
                        SettingsTx::key_rows(&mut rt, key).await.unwrap()
                    );
                }
                _ => {
                    for n in names {
                        let a = DdlTx::database_by_name(&mut mt, n).await.unwrap();
                        let b = DdlTx::database_by_name(&mut rt, n).await.unwrap();
                        assert_eq!(a, b, "database by name {n}");
                        if let Some(d) = a {
                            let a = DdlTx::object_by_name(&mut mt, d.db_id, Namespace::Relation, n)
                                .await
                                .unwrap();
                            let b = DdlTx::object_by_name(&mut rt, d.db_id, Namespace::Relation, n)
                                .await
                                .unwrap();
                            assert_eq!(a, b, "object by name {n}");
                            assert_eq!(
                                DdlTx::objects_in(&mut mt, d.db_id).await.unwrap(),
                                DdlTx::objects_in(&mut rt, d.db_id).await.unwrap()
                            );
                        }
                    }
                }
            }
        }
        if rng.below(8) == 0 {
            mem.rollback(mt).await.unwrap();
            rec.rollback(rt).await.unwrap();
        } else {
            mem.commit(mt, VersionKind::Catalog, v).await.unwrap();
            rec.commit(rt, VersionKind::Catalog, v).await.unwrap();
            versions.push(v);
        }
        if rng.below(10) == 0 && versions.len() > 2 {
            let up_to = versions[rng.below(versions.len() as u64) as usize];
            let a = DdlBackend::purge(&mem, T, up_to).await.unwrap();
            let b = DdlBackend::purge(&rec, T, up_to).await.unwrap();
            assert_eq!(a, b, "purge up to {up_to}");
        }
        let full_m = sorted(mem.tables(T).unwrap().snapshot(None));
        let full_r = sorted(DdlBackend::full_snapshot(&rec, T).await.unwrap());
        assert_eq!(full_m.databases, full_r.databases, "round {round}");
        assert_eq!(full_m.objects, full_r.objects, "round {round}");
        assert_eq!(full_m.settings, full_r.settings, "round {round}");
        assert_eq!(full_m.versions.catalog, full_r.versions.catalog);
        assert_eq!(full_m.versions.purged, full_r.versions.purged);
        for &since in &versions {
            let a = sorted(mem.tables(T).unwrap().snapshot(Some(since)));
            let mut rtx = rec.records().unwrap().begin(T).await.unwrap();
            let b = sorted(CoreRecords::snapshot(&mut *rtx, Some(since)).await.unwrap());
            assert_eq!(a, b, "incremental since {since}, round {round}");
        }
        let a = SettingsBackend::tenant_scope(&mem, T).await.unwrap();
        let b = SettingsBackend::tenant_scope(&rec, T).await.unwrap();
        assert_eq!(a, b);
    }
}

#[tokio::test]
async fn the_core_on_records_answers_like_the_catalog_model() {
    for seed in [1, 2, 3, 7, 42, 123, 2024, 4242, 77_777, 99_999] {
        differential(seed).await;
    }
}

#[tokio::test]
async fn names_are_unique_while_live_and_free_once_dropped() {
    let (b, _) = backend();
    let (mut tx, v0) = b.begin_locked(T, VersionKind::Catalog).await.unwrap();
    assert_eq!(v0, 0);
    DdlTx::insert_database(&mut tx, &db(1, "x", DbState::Active, 1))
        .await
        .unwrap();
    let e = DdlTx::insert_database(&mut tx, &db(2, "x", DbState::Active, 1))
        .await
        .unwrap_err();
    assert_eq!(crate::ddl::errno(&e), Some(1062));
    let e = DdlTx::insert_database(&mut tx, &db(1, "y", DbState::Active, 1))
        .await
        .unwrap_err();
    assert_eq!(crate::ddl::errno(&e), Some(1062));
    DdlTx::insert_object(
        &mut tx,
        &obj(5, 1, ObjectKind::Table, "t", ObjectState::Active, 1),
    )
    .await
    .unwrap();
    // Another namespace may use the name.
    DdlTx::insert_object(
        &mut tx,
        &obj(6, 1, ObjectKind::Function, "t", ObjectState::Active, 1),
    )
    .await
    .unwrap();
    let e = DdlTx::insert_object(
        &mut tx,
        &obj(7, 1, ObjectKind::View, "t", ObjectState::Active, 1),
    )
    .await
    .unwrap_err();
    assert_eq!(crate::ddl::errno(&e), Some(1062));
    b.commit(tx, VersionKind::Catalog, 1).await.unwrap();

    let (mut tx, v1) = b.begin_locked(T, VersionKind::Catalog).await.unwrap();
    assert_eq!(v1, 1);
    let r = tx.records().await.unwrap();
    assert!(
        CoreRecords::set_object_state(r, ObjectId(5), ObjectState::Dropped, 2)
            .await
            .unwrap()
    );
    assert!(
        CoreRecords::object_by_name(r, DbId(1), Namespace::Relation, "t")
            .await
            .unwrap()
            .is_none()
    );
    CoreRecords::insert_object(r, &obj(7, 1, ObjectKind::View, "t", ObjectState::Active, 2))
        .await
        .unwrap();
    // A rename onto a live name is refused; away from one frees it.
    let mut moved = obj(6, 1, ObjectKind::Function, "u", ObjectState::Active, 2);
    assert!(CoreRecords::update_object(r, &moved).await.unwrap());
    moved.object_id = ObjectId(8);
    CoreRecords::insert_object(
        r,
        &obj(8, 1, ObjectKind::Function, "t", ObjectState::Active, 2),
    )
    .await
    .unwrap();
    let e = CoreRecords::update_object(
        r,
        &obj(8, 1, ObjectKind::Function, "u", ObjectState::Active, 2),
    )
    .await
    .unwrap_err();
    assert_eq!(crate::ddl::errno(&e), Some(1062));
    b.commit(tx, VersionKind::Catalog, 2).await.unwrap();
}

#[tokio::test]
async fn ids_counters_and_versions_survive_and_never_repeat() {
    let (b, recs) = backend();
    let first = b.next_ids(T, Counter::Object, 3).await.unwrap();
    assert_eq!(first, 1);
    assert_eq!(b.next_object_id(T).await.unwrap(), ObjectId(4));
    assert_eq!(b.next_ids(T, Counter::Database, 1).await.unwrap(), 1);
    assert_eq!(b.next_ids(T, Counter::Other("group"), 1).await.unwrap(), 1);
    // A counter an older build wrote under the unterminated key is read.
    let mut tx = recs.begin(T).await.unwrap();
    tx.put(b"ctr/core.shard", &41u64.to_be_bytes())
        .await
        .unwrap();
    tx.commit().await.unwrap();
    assert_eq!(b.next_ids(T, Counter::Other("shard"), 1).await.unwrap(), 42);
    assert_eq!(b.next_ids(T, Counter::Other("shard"), 1).await.unwrap(), 43);
    let (tx, v) = b.begin_locked(T, VersionKind::Catalog).await.unwrap();
    assert_eq!(v, 0);
    b.commit(tx, VersionKind::Catalog, 5).await.unwrap();
    let (v, when) = DdlBackend::version_mark(&b, T).await.unwrap();
    assert_eq!(v, 5);
    assert!(when > UNIX_EPOCH);
    let (tx, v) = b.begin_locked(T, VersionKind::Catalog).await.unwrap();
    assert_eq!(v, 5);
    b.rollback(tx).await.unwrap();
}

#[tokio::test]
async fn a_rolled_back_change_leaves_nothing() {
    let (b, recs) = backend();
    let (mut tx, _) = b.begin_locked(T, VersionKind::Catalog).await.unwrap();
    DdlTx::insert_database(&mut tx, &db(1, "x", DbState::Active, 1))
        .await
        .unwrap();
    b.rollback(tx).await.unwrap();
    assert!(recs.dump(T).is_empty());
    // A MariaDB-only catalog has no records.
    let maria = MariaBackend::new(StaticLocator::new().shared());
    assert!(maria.records().is_none());
    let _ = Store::for_tenant(T);
}

#[tokio::test]
async fn the_reader_snapshot_is_what_a_change_committed() {
    let (b, recs) = backend();
    let (mut tx, _) = b.begin_locked(T, VersionKind::Catalog).await.unwrap();
    DdlTx::insert_database(&mut tx, &db(1, "x", DbState::Active, 1))
        .await
        .unwrap();
    DdlTx::insert_object(
        &mut tx,
        &obj(5, 1, ObjectKind::Table, "t", ObjectState::Active, 1),
    )
    .await
    .unwrap();
    SettingsTx::put_setting(
        &mut tx,
        &setting(SettingScope::Tenant, "ai.digest", Some(1), 1),
    )
    .await
    .unwrap();
    b.commit(tx, VersionKind::Catalog, 1).await.unwrap();
    let reader = CoreReader::new(Arc::clone(&recs) as Arc<dyn RecordStore>, T);
    let snap = reader.snapshot().await.unwrap();
    assert_eq!(snap.versions.catalog, 1);
    assert!(snap.since.is_none());
    assert_eq!(snap.databases.len(), 1);
    assert_eq!(snap.objects.len(), 1);
    assert_eq!(snap.settings.len(), 1);
    assert_eq!(snap.settings[0].key, "ai.digest");
    // Another tenant's reader sees nothing.
    let other = CoreReader::new(recs, TenantId(8)).snapshot().await.unwrap();
    assert_eq!(other.versions.catalog, 0);
    assert!(other.databases.is_empty() && other.objects.is_empty() && other.settings.is_empty());
}

#[test]
fn namespace_bytes_are_fixed_and_distinct() {
    // The bytes the first build stored (each namespace's place then).
    for (i, ns) in Namespace::ALL.iter().enumerate() {
        assert_eq!(usize::from(ns_code(*ns)), i, "{ns:?}");
    }
}
