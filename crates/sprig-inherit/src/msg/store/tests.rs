//! The inherited messaging records on the in-memory record store.

#![allow(clippy::unwrap_used)]

use sprig_catalog::records::mem::MemRecords;

use super::*;
use crate::msg::model::{Cause, DefKind, DefState, Scope};

const T: TenantId = TenantId(57_306_000_005);

fn def(id: u64, name: &str) -> Definition {
    Definition {
        declarer: TenantId(0),
        object_id: id,
        kind: DefKind::Stream,
        db_name: "shop".into(),
        name: name.into(),
        scope: Scope::default(),
        definition: serde_json::json!({"partitions": 3}),
        grants: None,
        def_version: 1,
        state: DefState::Active,
        drop_after_ms: None,
        created_by: 7,
        created_at_ms: 1,
        changed_version: 0,
    }
}

fn instance(object: u64, id: u64) -> Instance {
    Instance {
        def: DefKey {
            tenant: TenantId(9),
            object,
        },
        instance_id: id,
        kind: DefKind::Trigger,
        def_version: 1,
        definition: serde_json::json!({}),
        state: InstanceState::Creating,
        cause: Cause::Declare,
        physical: Some(serde_json::json!({"binding": 44})),
        created_at_ms: 0,
    }
}

#[tokio::test]
async fn definitions_overrides_and_instances() {
    let m = MemRecords::new();
    let mut tx = m.begin(T).await.unwrap();
    MsgStore::put_def(&mut *tx, &def(10, "ev"), 3).await.unwrap();
    // A definition of the same name under another id replaces it.
    MsgStore::put_def(&mut *tx, &def(11, "ev"), 4).await.unwrap();
    MsgStore::put_def(&mut *tx, &def(12, "other"), 4).await.unwrap();
    let key = DefKey {
        tenant: TenantId(1),
        object: 12,
    };
    for what in [What::Settings, What::State] {
        let o = Override {
            made_in: TenantId(0),
            def: key,
            target: TenantId(9),
            applies_to: AppliesTo::Subtree,
            what,
            value: serde_json::json!("disabled"),
            set_by: 7,
            set_at_ms: 0,
        };
        MsgStore::put_override(&mut *tx, &o, 4).await.unwrap();
    }
    tx.commit().await.unwrap();
    let mut tx = m.begin(T).await.unwrap();
    let st = MsgStore::load(&mut *tx, T).await.unwrap();
    assert_eq!(st.defs.len(), 2);
    assert!(st.defs.values().all(|d| d.declarer == T && d.changed_version == 4));
    assert_eq!(st.overrides.len(), 2);
    assert_eq!(
        MsgStore::delete_override(&mut *tx, key, TenantId(9), AppliesTo::Subtree, Some(What::State))
            .await
            .unwrap(),
        1
    );
    assert_eq!(
        MsgStore::delete_override(&mut *tx, key, TenantId(9), AppliesTo::Own, None)
            .await
            .unwrap(),
        0
    );
    MsgStore::delete_def(&mut *tx, 12).await.unwrap();
    tx.commit().await.unwrap();

    let store = MsgStore::new(Arc::new(m.clone()), T);
    let i = instance(20, 500);
    assert!(store.claim(&i).await.unwrap());
    // A second claim of the definition, or of the instance id, loses.
    assert!(!store.claim(&i).await.unwrap());
    assert!(!store.claim(&instance(21, 500)).await.unwrap());
    // A fresh `creating` row isn't taken over; an old one is.
    assert!(!store.take_over(i.def, 60_000).await.unwrap());
    tokio::time::sleep(std::time::Duration::from_millis(5)).await;
    assert!(store.take_over(i.def, 0).await.unwrap());
    store
        .set_instance(
            i.def,
            InstanceState::Active,
            2,
            &serde_json::json!({"v": 2}),
            Some(&serde_json::json!({"binding": 44, "module": 3})),
        )
        .await
        .unwrap();
    store.mark_blocked(44, Some("no rights")).await.unwrap();
    let got = store.instance(i.def).await.unwrap().unwrap();
    assert_eq!(got.state, InstanceState::Active);
    assert_eq!(got.def_version, 2);
    assert_eq!(got.physical.as_ref().unwrap()["blocked"], "no rights");
    store.mark_blocked(44, None).await.unwrap();
    let got = store.instance(i.def).await.unwrap().unwrap();
    assert!(got.physical.as_ref().unwrap().get("blocked").is_none());
    assert_eq!(store.instances().await.unwrap().len(), 1);
    store.delete_instance(i.def).await.unwrap();
    assert!(store.instance(i.def).await.unwrap().is_none());
    let mut tx = m.begin(T).await.unwrap();
    let st = MsgStore::load(&mut *tx, T).await.unwrap();
    assert_eq!(st.defs.len(), 1);
    assert_eq!(st.overrides.len(), 1);
    assert!(st.instances.is_empty());
}
