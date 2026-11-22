//! Users, passwords and their cache loads on catalog records (the
//! in-memory store), through the same calls as production: auth changes,
//! tombstones, verifier slots and history, login state, and incremental
//! loads equal to full ones.

#![allow(clippy::unwrap_used, clippy::too_many_lines, clippy::many_single_char_names)]

use std::sync::Arc;
use std::time::{Duration, UNIX_EPOCH};

use serde_json::json;
use sprig_catalog::records::mem::MemRecords;
use sprig_catalog::records::{RecBackend, RecordStore, RecordTx, read_version};
use sprig_catalog::{AuthCatalog, DirectoryAllocator, VersionKind, Versioner};
use sprig_common::ids::{TenantId, UserId};
use sprig_common::time::SystemClock;
use sprig_meta::{MemStore, MetaStore, Op, Tier, Txn};

use crate::admin::{self, NewUser, SetPassword};
use crate::cache::load_auth;
use crate::store::{AuthStore, LoginStateRow, Slot, UserState, VerifierKind};

const T: TenantId = TenantId(77);

async fn setup() -> (MemRecords, AuthCatalog) {
    let g = Arc::new(MemStore::new(Tier::Global));
    let key = Tier::Global.key(&format!("tenants/{T}")).unwrap();
    let body = json!({"v": 1, "tenant_id": T.0, "name": "acme", "state": "active",
                      "home_dc": "dc-a", "catalog_version": 0, "auth_version": 0});
    let r = g
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
    let recs = MemRecords::new();
    let alloc = DirectoryAllocator::new(g as Arc<dyn MetaStore>);
    let cat = Versioner::new(RecBackend::new(Arc::new(recs.clone())), Arc::new(alloc));
    (recs, cat)
}

async fn rd(recs: &MemRecords) -> Box<dyn RecordTx> {
    recs.begin(T).await.unwrap()
}

#[tokio::test]
async fn users_and_passwords_through_auth_changes() {
    let (recs, cat) = setup().await;
    let clock = SystemClock;
    let created = admin::create_user(&cat, T, &NewUser::new("alice", b"pw1"), &clock)
        .await
        .unwrap();
    assert_eq!(created.user_id, UserId(1));
    assert_eq!(created.auth_version, 1);
    // The same live account again: MySQL's 1396, and nothing is left.
    let e = admin::create_user(&cat, T, &NewUser::new("alice", b"x"), &clock)
        .await
        .unwrap_err();
    assert!(
        matches!(e.kind, sprig_common::ErrorKind::Backend { errno: 1396, .. }),
        "{e}"
    );
    // Another host is another account.
    let mut office = NewUser::new("alice", b"office");
    office.host = "10.%".into();
    let office = admin::create_user(&cat, T, &office, &clock).await.unwrap();
    assert_eq!(
        office.user_id,
        UserId(2),
        "a rolled-back create reserved nothing"
    );
    // The name compares exactly: ALICE is another account.
    let mut caps = NewUser::new("ALICE", b"x");
    caps.host = "10.%".into();
    assert_eq!(
        admin::create_user(&cat, T, &caps, &clock)
            .await
            .unwrap()
            .user_id,
        UserId(3)
    );

    let store = AuthStore;
    let mut r = rd(&recs).await;
    let row = store.user(&mut *r, UserId(1)).await.unwrap().unwrap();
    assert_eq!(
        (row.name.as_str(), row.host.as_str(), row.changed_version),
        ("alice", "%", 1)
    );
    assert!(row.password_changed_at.is_some());
    assert_eq!(
        store
            .user_by_account(&mut *r, "alice", "10.%")
            .await
            .unwrap()
            .unwrap()
            .user_id,
        UserId(2)
    );
    assert_eq!(
        admin::find_user(&cat, T, "alice", "%").await.unwrap(),
        Some(UserId(1))
    );

    // RETAIN CURRENT PASSWORD: primary to secondary; again: the old
    // secondary goes to history.
    let keep = SetPassword {
        retain_current: true,
        keep_history: 5,
        ..SetPassword::default()
    };
    admin::set_password(&cat, T, UserId(1), b"pw2", keep, &clock)
        .await
        .unwrap();
    let c = admin::set_password(&cat, T, UserId(1), b"pw3", keep, &clock)
        .await
        .unwrap();
    let mut r = rd(&recs).await;
    let vs = store.verifiers_of(&mut *r, &[UserId(1)]).await.unwrap();
    let slots: Vec<(Slot, u64)> = vs
        .iter()
        .filter(|v| v.kind == VerifierKind::CachingSha2)
        .map(|v| (v.slot, v.generation))
        .collect();
    assert_eq!(
        slots,
        [(Slot::Primary, 3), (Slot::Secondary, 2), (Slot::History, 1)]
    );
    for (slot, generation) in slots {
        let mut kinds: Vec<VerifierKind> = vs
            .iter()
            .filter(|v| v.slot == slot && v.generation == generation)
            .map(|v| v.kind)
            .collect();
        kinds.sort();
        assert_eq!(
            kinds,
            [
                VerifierKind::CachingSha2,
                VerifierKind::ScramSha256,
                VerifierKind::ScramSha512
            ]
        );
    }
    let salt = |slot: Slot, kind: VerifierKind| {
        vs.iter()
            .find(|v| v.slot == slot && v.kind == kind)
            .and_then(|v| v.salt.clone())
            .unwrap()
    };
    for kind in [VerifierKind::ScramSha256, VerifierKind::ScramSha512] {
        assert_eq!(salt(Slot::Primary, kind), salt(Slot::Secondary, kind));
        assert_eq!(salt(Slot::Secondary, kind), salt(Slot::History, kind));
    }
    // Verifier ids are never reused and increase.
    let mut ids: Vec<u64> = vs.iter().map(|v| v.verifier_id).collect();
    ids.sort_unstable();
    ids.dedup();
    assert_eq!(ids.len(), vs.len());
    let row = store.user(&mut *r, UserId(1)).await.unwrap().unwrap();
    assert_eq!(row.changed_version, c.auth_version);
    // DISCARD OLD PASSWORD, keeping no history.
    admin::discard_old_password(&cat, T, UserId(1), 0)
        .await
        .unwrap();
    let mut r = rd(&recs).await;
    let vs = store.verifiers_of(&mut *r, &[UserId(1)]).await.unwrap();
    assert_eq!(
        vs.iter().map(|v| v.slot).collect::<Vec<_>>(),
        [Slot::Primary; 3],
        "caching_sha2_password and both SCRAM verifiers"
    );

    // Native verifiers when the tenant enables them.
    let mut n = SetPassword::default();
    n.hashing.native = true;
    admin::set_password(&cat, T, UserId(2), b"office2", n, &clock)
        .await
        .unwrap();
    let mut r = rd(&recs).await;
    let vs = store.verifiers_of(&mut *r, &[UserId(2)]).await.unwrap();
    let primary: Vec<VerifierKind> = vs
        .iter()
        .filter(|v| v.slot == Slot::Primary)
        .map(|v| v.kind)
        .collect();
    assert_eq!(
        primary,
        [
            VerifierKind::CachingSha2,
            VerifierKind::Native,
            VerifierKind::ScramSha256,
            VerifierKind::ScramSha512
        ]
    );

    // DROP USER: a tombstone; the name is free again, with a new id.
    admin::drop_user(&cat, T, UserId(1)).await.unwrap();
    let mut r = rd(&recs).await;
    let row = store.user(&mut *r, UserId(1)).await.unwrap().unwrap();
    assert_eq!(row.state, UserState::Dropped);
    assert!(
        store
            .verifiers_of(&mut *r, &[UserId(1)])
            .await
            .unwrap()
            .is_empty()
    );
    let again = admin::create_user(&cat, T, &NewUser::new("alice", b"pw"), &clock)
        .await
        .unwrap();
    assert_eq!(
        again.user_id,
        UserId(4),
        "ids of dropped users are never reused"
    );
    assert!(
        admin::set_account_locked(&cat, T, UserId(1), true)
            .await
            .is_err(),
        "dropped"
    );

    // login_state round trip (unversioned).
    let ls = LoginStateRow {
        user_id: UserId(4),
        consecutive_failures: 2,
        locked_until: Some(UNIX_EPOCH + Duration::from_micros(1_900_000_000_123_456)),
        last_failure_at: None,
        last_success_at: None,
        last_success_ip: Some(vec![10, 0, 0, 1]),
        updated_at: UNIX_EPOCH + Duration::from_secs(1_762_560_000),
    };
    let before = read_version(&mut *rd(&recs).await, VersionKind::Auth)
        .await
        .unwrap();
    let mut w = rd(&recs).await;
    store.put_login_state(&mut *w, &ls).await.unwrap();
    w.commit().await.unwrap();
    let mut r = rd(&recs).await;
    assert_eq!(store.login_state(&mut *r, UserId(4)).await.unwrap(), Some(ls));
    assert_eq!(store.temp_locks(&mut *r).await.unwrap().len(), 1);
    assert_eq!(
        read_version(&mut *r, VersionKind::Auth).await.unwrap(),
        before,
        "login state writes bump no version"
    );
    // ACCOUNT UNLOCK clears the temporary lock in the same change.
    admin::set_account_locked(&cat, T, UserId(4), false)
        .await
        .unwrap();
    let mut r = rd(&recs).await;
    let ls = store.login_state(&mut *r, UserId(4)).await.unwrap().unwrap();
    assert_eq!(
        (ls.locked_until, ls.consecutive_failures),
        (None, 0),
        "unlocked"
    );
    assert!(store.temp_locks(&mut *r).await.unwrap().is_empty());

    // Hosts compare without case: the same account.
    let mut lower = NewUser::new("erin", b"x");
    lower.host = "db.example.com".into();
    admin::create_user(&cat, T, &lower, &clock).await.unwrap();
    let mut upper = NewUser::new("erin", b"y");
    upper.host = "DB.EXAMPLE.COM".into();
    let e = admin::create_user(&cat, T, &upper, &clock).await.unwrap_err();
    assert!(
        matches!(e.kind, sprig_common::ErrorKind::Backend { errno: 1396, .. }),
        "{e}"
    );
}

#[tokio::test]
async fn incremental_loads_equal_full_loads() {
    let (recs, cat) = setup().await;
    let clock = SystemClock;
    let mut r = rd(&recs).await;
    let first = load_auth(&mut *r, None, None).await.unwrap();
    assert!(first.is_empty());
    let mut at = read_version(&mut *r, VersionKind::Auth).await.unwrap();
    drop(r);
    let mut prev = first;
    for i in 0..6_u32 {
        admin::create_user(&cat, T, &NewUser::new(format!("u{i}"), b"pw"), &clock)
            .await
            .unwrap();
    }
    let step = |prev: crate::cache::TenantAuth, at: u64| {
        let recs = recs.clone();
        async move {
            let mut r = recs.begin(T).await.unwrap();
            let next = load_auth(&mut *r, Some(at), Some(&prev)).await.unwrap();
            let v = read_version(&mut *r, VersionKind::Auth).await.unwrap();
            let mut f = recs.begin(T).await.unwrap();
            let full = load_auth(&mut *f, None, None).await.unwrap();
            assert_eq!(next, full, "incremental equals full");
            (next, v)
        }
    };
    (prev, at) = step(prev, at).await;
    admin::set_password(
        &cat,
        T,
        UserId(2),
        b"new",
        SetPassword {
            retain_current: true,
            ..SetPassword::default()
        },
        &clock,
    )
    .await
    .unwrap();
    admin::set_account_locked(&cat, T, UserId(3), true)
        .await
        .unwrap();
    admin::drop_user(&cat, T, UserId(4)).await.unwrap();
    admin::set_password_expired(&cat, T, UserId(5), true)
        .await
        .unwrap();
    let (auth, _) = step(prev, at).await;
    assert_eq!(auth.len(), 5);
    assert_eq!(auth.user(UserId(2)).unwrap().caching_sha2.len(), 2);
    assert!(auth.user(UserId(3)).unwrap().row.account_locked);
    assert!(auth.user(UserId(4)).is_none());
    assert!(auth.user(UserId(5)).unwrap().row.password_expired);
}

#[tokio::test]
async fn a_failed_change_leaves_nothing_and_the_next_one_goes_through() {
    let (recs, cat) = setup().await;
    let clock = SystemClock;
    admin::create_user(&cat, T, &NewUser::new("bob", b"pw"), &clock)
        .await
        .unwrap();
    // A user that doesn't exist: refused, rolled back, gap settled.
    assert!(
        admin::set_password(&cat, T, UserId(9), b"x", SetPassword::default(), &clock)
            .await
            .is_err()
    );
    let mut r = rd(&recs).await;
    assert!(AuthStore.user(&mut *r, UserId(9)).await.unwrap().is_none());
    let next = admin::create_user(&cat, T, &NewUser::new("carol", b"pw"), &clock)
        .await
        .unwrap();
    assert_eq!(next.user_id, UserId(2));
}
