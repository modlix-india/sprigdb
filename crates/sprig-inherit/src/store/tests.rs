//! The inheritance records on the in-memory record store.

#![allow(clippy::unwrap_used)]

use sprig_catalog::records::RecordStore;
use sprig_catalog::records::mem::MemRecords;
use sprig_common::ids::DbId;

use super::*;
use crate::store::NewSpace;

const T: TenantId = TenantId(57_306_000_004);

async fn apply(m: &MemRecords, c: &InheritChange, v: u64) -> Result<()> {
    let mut tx = m.begin(T).await?;
    InheritStore::apply(&mut *tx, c, v).await?;
    tx.commit().await
}

async fn load(m: &MemRecords, names: &ObjectNames) -> InheritState {
    let mut tx = m.begin(T).await.unwrap();
    InheritStore::load(&mut *tx, names).await.unwrap()
}

fn obj(id: u64) -> InheritableObject {
    InheritableObject {
        object_id: ObjectId(id),
        db: DbId(1),
        table: format!("t{id}"),
        key: vec!["id".into()],
        columns: vec!["id".into(), "v".into()],
        slots: BTreeMap::from([("v".to_owned(), 1)]),
        uniques: Vec::new(),
        tag_field: None,
        version: 1,
        auto: Some("id".into()),
        links: Vec::new(),
        adopted: true,
        type_root: None,
    }
}

fn share(id: u64, name: &str, object: u64, to: ShareTarget) -> Share {
    Share {
        id,
        name: name.into(),
        object_id: ObjectId(object),
        from: ROOT_SPACE,
        to,
        filter: Some("region = 'eu'".into()),
        columns: None,
        take: None,
        reshare: false,
        version: 1,
        created_by: 7,
        mode: MergeMode::default(),
        via: None,
    }
}

#[tokio::test]
async fn statements_follow_the_sql_rules() {
    let m = MemRecords::new();
    let names: ObjectNames = [10, 11]
        .into_iter()
        .map(|i| (ObjectId(i), (DbId(1), format!("t{i}"))))
        .collect();
    apply(
        &m,
        &InheritChange {
            root_name: Some("root".into()),
            add_spaces: vec![
                NewSpace {
                    name: "eu".into(),
                    parent: ROOT_SPACE,
                    created_by: 7,
                },
                NewSpace {
                    name: "fr".into(),
                    parent: SpaceId(2),
                    created_by: 7,
                },
            ],
            upsert_objects: vec![obj(10), obj(11)],
            upsert_shares: vec![
                share(0, "all", 10, ShareTarget::AllChildren),
                share(0, "to_eu", 11, ShareTarget::Space(SpaceId(2))),
            ],
            upsert_takes: vec![Take {
                share_id: 77,
                peer_tenant: TenantId(5),
                object_id: ObjectId(10),
                take: Some("x > 1".into()),
            }],
            homes: vec![(9, Some(SpaceId(3)))],
            ..InheritChange::default()
        },
        4,
    )
    .await
    .unwrap();
    let st = load(&m, &names).await;
    assert_eq!(st.spaces().count(), 3);
    let fr = st.spaces().find(|s| s.name == "fr").unwrap();
    assert_eq!(
        (fr.id, fr.parent, fr.depth),
        (SpaceId(3), Some(SpaceId(2)), 2)
    );
    assert_eq!(st.objects().count(), 2);
    assert!(st.object(ObjectId(10)).unwrap().adopted);
    let ids: Vec<u64> = st.shares().map(|s| s.id).collect();
    assert_eq!(ids, vec![2, 3]);
    // A space name is unique in the tenant.
    let e = apply(
        &m,
        &InheritChange {
            add_spaces: vec![NewSpace {
                name: "eu".into(),
                parent: ROOT_SPACE,
                created_by: 7,
            }],
            ..InheritChange::default()
        },
        5,
    )
    .await
    .unwrap_err();
    assert_eq!(e.code(), Some(SprigCode::SpaceExists));
    // An object changed bumps its version; a dropped share stays as
    // `dropping` until purged; dropping a space takes its shares and
    // homes; dropping an object takes its shares.
    apply(
        &m,
        &InheritChange {
            upsert_objects: vec![obj(10)],
            drop_shares: vec![2],
            ..InheritChange::default()
        },
        6,
    )
    .await
    .unwrap();
    let st = load(&m, &names).await;
    assert_eq!(st.object(ObjectId(10)).unwrap().version, 2);
    assert_eq!(st.shares().count(), 1);
    assert_eq!(st.dropping().count(), 1);
    apply(
        &m,
        &InheritChange {
            purge_shares: vec![2],
            drop_spaces: vec![SpaceId(2)],
            homes: vec![(8, Some(SpaceId(2)))],
            ..InheritChange::default()
        },
        7,
    )
    .await
    .unwrap();
    let st = load(&m, &names).await;
    assert_eq!(st.dropping().count(), 0);
    // A purged share's id isn't handed out again.
    apply(
        &m,
        &InheritChange {
            upsert_shares: vec![share(0, "again", 10, ShareTarget::AllChildren)],
            ..InheritChange::default()
        },
        7,
    )
    .await
    .unwrap();
    let st = load(&m, &names).await;
    let again = st.shares().find(|s| s.name == "again").unwrap().id;
    assert!(again > 77, "share id {again} handed out before");
    apply(
        &m,
        &InheritChange {
            drop_shares: vec![again],
            ..InheritChange::default()
        },
        7,
    )
    .await
    .unwrap();
    apply(
        &m,
        &InheritChange {
            purge_shares: vec![again],
            ..InheritChange::default()
        },
        7,
    )
    .await
    .unwrap();
    apply(
        &m,
        &InheritChange {
            upsert_shares: vec![share(0, "again", 10, ShareTarget::AllChildren)],
            ..InheritChange::default()
        },
        7,
    )
    .await
    .unwrap();
    let st = load(&m, &names).await;
    let made_again = st.shares().find(|s| s.name == "again").unwrap().id;
    assert_eq!(made_again, again + 1, "the dropped share's id came back");
    apply(
        &m,
        &InheritChange {
            drop_shares: vec![made_again],
            purge_shares: vec![made_again],
            ..InheritChange::default()
        },
        7,
    )
    .await
    .unwrap();
    let st = load(&m, &names).await;
    assert_eq!(st.dropping().count(), 0);
    assert_eq!(
        st.shares().count(),
        0,
        "the share to the dropped space went"
    );
    apply(
        &m,
        &InheritChange {
            delete_objects: vec![ObjectId(10)],
            homes: vec![(9, None)],
            ..InheritChange::default()
        },
        8,
    )
    .await
    .unwrap();
    let mut tx = m.begin(T).await.unwrap();
    let shares = SHARES.rows::<ShareRow>(&mut *tx, &Pk::new()).await.unwrap();
    assert!(shares.iter().all(|r| r.object_id != 10), "{shares:?}");
    // User 9's home went; user 8's was set after the space was dropped in
    // the same change (the order the SQL ran them in).
    let homes = HOMES.rows::<HomeRow>(&mut *tx, &Pk::new()).await.unwrap();
    assert_eq!(homes.len(), 1, "{homes:?}");
    assert_eq!(homes[0].space_id, 2);
    // Objects whose table is gone aren't loaded.
    drop(tx);
    let st = load(&m, &ObjectNames::new()).await;
    assert_eq!(st.objects().count(), 0);
}
