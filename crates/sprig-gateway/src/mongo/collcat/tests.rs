use std::time::SystemTime;

use sprig_catalog::cache::Loaded;
use sprig_catalog::store::{DatabaseRow, DbState, ObjectFlags, ObjectKind, ObjectRow, ObjectState};
use sprig_catalog::{Snapshot, TenantCatalog, Versions};
use sprig_common::ids::{DbId, GroupId, ObjectId, TenantId};
use sprig_doc::docstore::Namespace as StoreNs;
use sprig_settings::Registry;

use super::plan;

const C: u32 = sprig_doc::docstore::layout::CATALOG;

fn db(id: u32, name: &str) -> DatabaseRow {
    DatabaseRow {
        db_id: DbId(id),
        name: name.into(),
        name_key: name.into(),
        default_charset: "utf8mb4".into(),
        default_collation: "utf8mb4_uca1400_ai_ci".into(),
        default_group_id: GroupId(0),
        state: DbState::Active,
        options: serde_json::json!({ "shards": [1] }),
        created_at: SystemTime::UNIX_EPOCH,
        created_by: None,
        changed_version: 1,
    }
}

fn obj(id: u64, db: u32, name: &str, kind: ObjectKind, state: ObjectState) -> ObjectRow {
    ObjectRow {
        object_id: ObjectId(id),
        db_id: DbId(db),
        namespace: kind.namespace(),
        name: name.into(),
        name_key: name.into(),
        kind,
        parent_id: None,
        state,
        schema_version: 1,
        flags: ObjectFlags::empty(),
        pending_job_id: None,
        created_at: SystemTime::UNIX_EPOCH,
        changed_version: 1,
    }
}

fn coll(id: u64, db: u32, name: &str) -> ObjectRow {
    obj(id, db, name, ObjectKind::Collection, ObjectState::Active)
}

fn catalog(objects: Vec<ObjectRow>) -> TenantCatalog {
    static R: std::sync::OnceLock<Registry> = std::sync::OnceLock::new();
    let reg = R.get_or_init(|| Registry::new([]).unwrap());
    TenantCatalog::build(
        reg,
        TenantId(42),
        Loaded {
            snapshot: Snapshot {
                versions: Versions {
                    catalog: 1,
                    auth: 1,
                    purged: 0,
                },
                since: None,
                databases: vec![db(1, "shop"), db(2, "crm")],
                objects,
                settings: Vec::new(),
            },
            ext: Vec::new(),
        },
    )
}

fn ns(db: &str, name: &str, n: u32) -> StoreNs {
    StoreNs {
        db: db.into(),
        name: name.into(),
        object: C + n,
        view: false,
    }
}

fn id(n: u32) -> u64 {
    u64::from(C + n)
}

#[test]
fn new_collections_get_objects_numbered_by_their_engine_object() {
    let cat = catalog(vec![obj(
        5,
        1,
        "orders",
        ObjectKind::Table,
        ObjectState::Active,
    )]);
    let mut v = ns("shop", "views1", 3);
    v.view = true;
    let p = plan(
        &cat,
        false,
        &[
            ns("shop", "people", 1),
            ns("crm", "leads", 2),
            v,
            ns("newdb", "x", 4),
        ],
    );
    let adds: Vec<(u64, u32, &str)> = p
        .adds
        .iter()
        .map(|r| (r.object_id.0, r.db_id.0, r.name.as_str()))
        .collect();
    assert_eq!(adds, vec![(id(1), 1, "people"), (id(2), 2, "leads")]);
    assert!(p.adds.iter().all(|r| r.kind == ObjectKind::Collection
        && r.namespace == sprig_catalog::store::Namespace::Relation));
    // A database the catalog lacks is made first; views get no object.
    assert_eq!(p.dbs, vec!["newdb".to_owned()]);
    assert!(p.drops.is_empty() && p.renames.is_empty() && p.conflicts.is_empty());
}

#[test]
fn objects_follow_drops_and_renames_and_stay_put_otherwise() {
    let cat = catalog(vec![
        coll(id(1), 1, "a"),
        coll(id(2), 1, "b"),
        coll(id(3), 1, "gone"),
        coll(id(4), 1, "same"),
    ]);
    // a and b swap names; gone is dropped; same is unchanged; c moves to crm.
    let p = plan(
        &cat,
        false,
        &[
            ns("shop", "b", 1),
            ns("shop", "a", 2),
            ns("shop", "same", 4),
        ],
    );
    assert!(p.adds.is_empty(), "{p:?}");
    assert_eq!(
        p.drops.iter().map(|o| o.object_id.0).collect::<Vec<_>>(),
        vec![id(3)]
    );
    let mut r: Vec<(u64, &str, &str)> = p
        .renames
        .iter()
        .map(|(n, o)| (n.object_id.0, o.name.as_str(), n.name.as_str()))
        .collect();
    r.sort_unstable();
    assert_eq!(r, vec![(id(1), "a", "b"), (id(2), "b", "a")]);
    assert!(p.conflicts.is_empty());

    let p = plan(&cat, false, &[ns("crm", "a", 1)]);
    assert_eq!(p.renames.len(), 1);
    assert_eq!(p.renames[0].0.db_id, DbId(2));
    assert_eq!(p.drops.len(), 3);
}

#[test]
fn a_name_another_relation_holds_gives_no_object() {
    let cat = catalog(vec![
        obj(5, 1, "orders", ObjectKind::Table, ObjectState::Active),
        obj(
            id(7),
            1,
            "old",
            ObjectKind::Collection,
            ObjectState::Dropped,
        ),
        // A collection object whose collection was dropped and made again
        // (another number): the old object leaves, the new one comes.
        coll(id(8), 1, "again"),
    ]);
    let p = plan(
        &cat,
        false,
        &[
            ns("shop", "orders", 1),
            ns("shop", "old", 7),
            ns("shop", "again", 9),
        ],
    );
    assert_eq!(p.conflicts, vec![("shop".into(), "orders".into())]);
    let adds: Vec<u64> = p.adds.iter().map(|r| r.object_id.0).collect();
    assert_eq!(adds, vec![id(7), id(9)]);
    assert_eq!(
        p.drops.iter().map(|o| o.object_id.0).collect::<Vec<_>>(),
        vec![id(8)]
    );
}

#[test]
fn names_that_meet_without_case_conflict_when_names_are_case_insensitive() {
    let cat = catalog(Vec::new());
    let p = plan(
        &cat,
        true,
        &[ns("shop", "Users", 1), ns("shop", "users", 2)],
    );
    assert_eq!(p.adds.len(), 1);
    assert_eq!(p.adds[0].name_key, "users");
    assert_eq!(p.conflicts, vec![("shop".into(), "users".into())]);
    let p = plan(
        &cat,
        false,
        &[ns("shop", "Users", 1), ns("shop", "users", 2)],
    );
    assert_eq!(p.adds.len(), 2);
}
