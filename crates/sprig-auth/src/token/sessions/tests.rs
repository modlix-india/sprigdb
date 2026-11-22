//! Token sessions on catalog records (the in-memory store): creation in an
//! auth change with its purge, the column rules MariaDB enforced, the
//! incremental and full loads, the refresh's compare-and-set, revocation
//! and touches.

#![allow(clippy::unwrap_used)]

use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use sprig_catalog::records::mem::MemRecords;
use sprig_catalog::records::{RecordStore as _, read_version};
use sprig_catalog::{AuthCatalog, DirectoryAllocator, RecBackend, VersionKind, Versioner};
use sprig_common::ids::{TenantId, UserId};
use sprig_meta::{MemStore, MetaStore, Op, Tier, Txn};

use super::*;
use crate::store::{AuthStore, UserRow, UserSource};

/// The tenant of the tests.
pub(crate) const T: TenantId = TenantId(42);

/// An auth catalog on in-memory records, its tenant `T` in a directory.
pub(crate) async fn catalog() -> (MemRecords, AuthCatalog) {
    let g = Arc::new(MemStore::new(Tier::Global));
    let key = Tier::Global.key(&format!("tenants/{T}")).unwrap();
    let body = serde_json::json!({"v": 1, "tenant_id": T.0, "name": "acme", "state": "active",
                                  "home_dc": "mum1", "catalog_version": 0, "auth_version": 0});
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
    let alloc = DirectoryAllocator::new(g).with_timeout(Duration::from_millis(300));
    let cat = Versioner::new(RecBackend::new(Arc::new(recs.clone())), Arc::new(alloc));
    (recs, cat)
}

/// The committed auth version of `T`.
pub(crate) async fn auth_version(recs: &MemRecords) -> u64 {
    let mut tx = recs.begin(T).await.unwrap();
    read_version(&mut *tx, VersionKind::Auth).await.unwrap()
}

/// Adds the live user `name`@`%` with id `id` in an auth change.
pub(crate) async fn add_user(cat: &AuthCatalog, id: u32, name: &str) {
    let mut c = cat.change(T, VersionKind::Auth).await.unwrap();
    let v = c.version();
    let row = UserRow::new(UserId(id), name, "%", UserSource::Api, at(1), v);
    AuthStore::new()
        .insert_user(c.tx().records(), &row)
        .await
        .unwrap();
    c.commit().await.unwrap();
}

/// `secs` seconds after the epoch, plus a few microseconds and nanoseconds
/// (stored times keep microseconds only).
pub(crate) fn at(secs: u64) -> SystemTime {
    UNIX_EPOCH + Duration::from_secs(1_800_000_000 + secs) + Duration::from_nanos(123_456_789)
}

fn principal(user: u32) -> Principal {
    Principal {
        tenant: Some(T),
        tenant_name: Some("acme".into()),
        user_id: Some(UserId(user)),
        user: "app".into(),
        host: "%".into(),
        client_ip: "127.0.0.1".parse().unwrap(),
        must_change_password: false,
        limits: crate::store::AccountLimits::default(),
        auth_version: 0,
        backend: crate::BackendTarget {
            cluster: sprig_common::ids::ClusterId::new("dev-c1").unwrap(),
            user: "t42_u3".into(),
            credentials: "file:/dev/null".parse().unwrap(),
        },
    }
}

fn new_session(sid: u8, user: u32, now: SystemTime) -> NewSession {
    NewSession {
        sid: [sid; 16],
        principal: principal(user),
        refresh_sha256: [1; 32],
        amr: vec!["pwd".into()],
        client: "studio".into(),
        expires_at: now + Duration::from_secs(3600),
        now,
        impersonator: None,
    }
}

async fn row(recs: &MemRecords, sid: u8) -> Option<SessionRow> {
    let mut tx = recs.begin(T).await.unwrap();
    TOKEN_SESSIONS
        .get(&mut *tx, &session_pk(&[sid; 16]))
        .await
        .unwrap()
}

async fn load(recs: &MemRecords, since: Option<u64>) -> Vec<[u8; 16]> {
    let mut tx = recs.begin(T).await.unwrap();
    sessions_since(&mut *tx, since)
        .await
        .unwrap()
        .into_iter()
        .map(|s| s.sid)
        .collect()
}

const RULES: RefreshRules = RefreshRules {
    idle: Duration::from_secs(600),
    max_age: Duration::from_secs(7200),
};

#[tokio::test]
async fn a_session_is_created_in_an_auth_change_and_loads_incrementally() {
    let (recs, cat) = catalog().await;
    let now = SystemTime::now();
    let mut s = new_session(1, 3, now);
    s.amr = vec!["otp".into(), "pwd".into(), "otp".into()];
    s.impersonator = Some(Impersonator {
        tenant_id: 9,
        user_id: 4,
        reason: "x".repeat(300),
    });
    insert(&cat, T, &s).await.unwrap();
    let v = auth_version(&recs).await;
    assert!(v > 0);
    let r = row(&recs, 1).await.unwrap();
    // The SET column's order, each member once.
    assert_eq!(r.amr, vec!["pwd".to_owned(), "otp".to_owned()]);
    assert_eq!(r.changed_version, v);
    assert_eq!(r.reason.as_deref().map(str::len), Some(256));
    assert_eq!(r.created_at, crate::rec::to_micros(now));
    assert_eq!(r.last_used_at, Some(r.created_at));
    assert_eq!(r.refresh_prev, None);
    assert_eq!(load(&recs, None).await, vec![[1; 16]]);
    assert_eq!(load(&recs, Some(v - 1)).await, vec![[1; 16]]);
    assert!(load(&recs, Some(v)).await.is_empty());
}

#[tokio::test]
async fn values_the_columns_couldnt_hold_are_refused_and_nothing_stays() {
    let (recs, cat) = catalog().await;
    let now = SystemTime::now();
    let mut bad_amr = new_session(1, 3, now);
    bad_amr.amr = vec!["pwd".into(), "sms".into()];
    let e = insert(&cat, T, &bad_amr).await.unwrap_err();
    assert!(matches!(
        e.kind,
        sprig_common::ErrorKind::Backend { errno: 1265, .. }
    ));
    let mut bad_client = new_session(2, 3, now);
    bad_client.client = "web".into();
    let e = insert(&cat, T, &bad_client).await.unwrap_err();
    assert!(matches!(
        e.kind,
        sprig_common::ErrorKind::Backend { errno: 1265, .. }
    ));
    assert!(row(&recs, 1).await.is_none() && row(&recs, 2).await.is_none());
    // A refused change rolls back and settles the version it was
    // given (a closed gap): no row moves to it.
    // A session id already in use is a duplicate key.
    insert(&cat, T, &new_session(3, 3, now)).await.unwrap();
    let e = insert(&cat, T, &new_session(3, 4, now)).await.unwrap_err();
    assert!(crate::rec::is_duplicate(&e));
    assert_eq!(row(&recs, 3).await.unwrap().user_id, UserId(3));
}

#[tokio::test]
async fn a_full_load_lists_live_sessions_only() {
    let (recs, cat) = catalog().await;
    let now = SystemTime::now();
    insert(&cat, T, &new_session(1, 3, now)).await.unwrap();
    let mut expired = new_session(2, 3, now - Duration::from_secs(7200));
    expired.expires_at = now - Duration::from_secs(60);
    insert(&cat, T, &expired).await.unwrap();
    insert(&cat, T, &new_session(3, 3, now)).await.unwrap();
    assert!(revoke_in_change(&cat, T, [3; 16], now).await.unwrap());
    assert_eq!(load(&recs, None).await, vec![[1; 16]]);
    // Incrementally every changed row comes, for the cache to drop.
    assert_eq!(load(&recs, Some(0)).await, vec![[1; 16], [2; 16], [3; 16]]);
}

async fn put_row(tx: &mut dyn RecordTx, sid: [u8; 16], r: &SessionRow) {
    let mut r = r.clone();
    r.session_id = sid;
    TOKEN_SESSIONS.put(tx, &session_pk(&sid), &r).await.unwrap();
}

#[tokio::test]
async fn a_sign_in_purges_at_most_100_rows_ended_over_a_day_ago() {
    let (recs, cat) = catalog().await;
    let now = at(10 * 86_400);
    let day_and_more = Duration::from_secs(86_400 + 60);
    let mut tx = recs.begin(T).await.unwrap();
    let base = SessionRow {
        session_id: [0; 16],
        user_id: UserId(3),
        amr: vec!["pwd".into()],
        refresh_sha256: [1; 32],
        refresh_prev: None,
        impersonator_tenant: None,
        impersonator_user: None,
        reason: None,
        expires_at: now - day_and_more,
        revoked_at: None,
        client: "api".into(),
        last_used_at: None,
        created_at: now - Duration::from_secs(5 * 86_400),
        changed_version: 0,
    };
    // 103 rows expired over a day ago.
    for i in 0..103u8 {
        put_row(
            &mut *tx,
            [0, i, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0],
            &base,
        )
        .await;
    }
    // Revoked over a day ago, though not expired.
    let mut revoked_old = base.clone();
    revoked_old.expires_at = now + Duration::from_secs(3600);
    revoked_old.revoked_at = Some(now - day_and_more);
    put_row(&mut *tx, [1; 16], &revoked_old).await;
    // Revoked an hour ago, and expired an hour ago: kept.
    let mut recent = base.clone();
    recent.expires_at = now - Duration::from_secs(3600);
    recent.revoked_at = Some(now - Duration::from_secs(3600));
    put_row(&mut *tx, [2; 16], &recent).await;
    tx.commit().await.unwrap();
    insert(&cat, T, &new_session(9, 3, now)).await.unwrap();
    let mut tx = recs.begin(T).await.unwrap();
    let left: Vec<SessionRow> = TOKEN_SESSIONS.rows(&mut *tx, &Pk::new()).await.unwrap();
    // 104 candidates, 100 purged in key order: the last 3 expired rows
    // and the old revoked one stay, with the recent one and the new one.
    let ids: Vec<[u8; 16]> = left.iter().map(|r| r.session_id).collect();
    assert_eq!(ids.len(), 6, "{ids:?}");
    assert_eq!(ids[0][1], 100);
    assert!(ids.contains(&[1; 16]) && ids.contains(&[2; 16]) && ids.contains(&[9; 16]));
    // A second sign-in takes the rest.
    insert(&cat, T, &new_session(8, 3, now)).await.unwrap();
    let mut tx = recs.begin(T).await.unwrap();
    let left: Vec<SessionRow> = TOKEN_SESSIONS.rows(&mut *tx, &Pk::new()).await.unwrap();
    let ids: Vec<[u8; 16]> = left.iter().map(|r| r.session_id).collect();
    assert_eq!(ids, vec![[2; 16], [8; 16], [9; 16]]);
}

#[tokio::test]
async fn a_refresh_rotates_once_and_reuse_revokes() {
    let (recs, cat) = catalog().await;
    let t0 = at(0);
    insert(&cat, T, &new_session(1, 3, t0)).await.unwrap();
    let v = auth_version(&recs).await;
    // A wrong token: nothing changes.
    let r = rotate_in(&cat, T, [1; 16], [7; 32], [2; 32], RULES, at(60)).await;
    assert_eq!(r.unwrap(), Rotated::Invalid);
    assert_eq!(row(&recs, 1).await.unwrap().refresh_sha256, [1; 32]);
    // The current token: both hashes move, not an auth change.
    let r = rotate_in(&cat, T, [1; 16], [1; 32], [2; 32], RULES, at(60))
        .await
        .unwrap();
    assert_eq!(
        r,
        Rotated::Ok {
            tenant_id: T.0,
            user_id: Some(UserId(3)),
            created_at: crate::rec::to_micros(t0),
            amr: vec!["pwd".into()],
            client: "studio".into(),
        }
    );
    let after = row(&recs, 1).await.unwrap();
    assert_eq!(after.refresh_sha256, [2; 32]);
    assert_eq!(after.refresh_prev, Some([1; 32]));
    assert_eq!(after.last_used_at, Some(crate::rec::to_micros(at(60))));
    assert_eq!(after.changed_version, v);
    assert_eq!(auth_version(&recs).await, v);
    // The previous token again: theft; the session is revoked in an auth
    // change.
    let r = rotate_in(&cat, T, [1; 16], [1; 32], [3; 32], RULES, at(120)).await;
    assert_eq!(r.unwrap(), Rotated::Reused);
    let revoked = row(&recs, 1).await.unwrap();
    let v2 = auth_version(&recs).await;
    assert!(v2 > v);
    assert_eq!(revoked.revoked_at, Some(crate::rec::to_micros(at(120))));
    assert_eq!(revoked.changed_version, v2);
    assert_eq!(revoked.refresh_sha256, [2; 32]);
    // Revoked: even the current token is refused, with no change.
    let r = rotate_in(&cat, T, [1; 16], [2; 32], [3; 32], RULES, at(130)).await;
    assert_eq!(r.unwrap(), Rotated::Invalid);
    assert_eq!(auth_version(&recs).await, v2);
    // No such session.
    let r = rotate_in(&cat, T, [5; 16], [2; 32], [3; 32], RULES, at(130)).await;
    assert_eq!(r.unwrap(), Rotated::Invalid);
}

#[tokio::test]
async fn an_idle_session_ends_at_its_refresh() {
    let (recs, cat) = catalog().await;
    insert(&cat, T, &long_session(1)).await.unwrap();
    let v = auth_version(&recs).await;
    // No use for as long as the limit.
    let r = rotate_in(&cat, T, [1; 16], [1; 32], [2; 32], RULES, at(600)).await;
    assert_eq!(r.unwrap(), Rotated::Idle);
    let after = row(&recs, 1).await.unwrap();
    assert!(after.revoked_at.is_some());
    assert_eq!(after.refresh_sha256, [1; 32]);
    assert!(auth_version(&recs).await > v);
}

fn long_session(sid: u8) -> NewSession {
    let mut s = new_session(sid, 3, at(0));
    s.expires_at = at(100_000);
    s
}

#[tokio::test]
async fn a_session_past_its_age_or_expiry_ends_as_expired() {
    let (recs, cat) = catalog().await;
    insert(&cat, T, &long_session(1)).await.unwrap();
    let mut step = 0;
    let mut current = [1u8; 32];
    while step + 500 < 7200 {
        step += 500;
        let next = [u8::try_from(step / 500 + 1).unwrap(); 32];
        let r = rotate_in(&cat, T, [1; 16], current, next, RULES, at(step)).await;
        assert!(matches!(r.unwrap(), Rotated::Ok { .. }), "{step}");
        current = next;
    }
    let r = rotate_in(&cat, T, [1; 16], current, [99; 32], RULES, at(7200)).await;
    assert_eq!(r.unwrap(), Rotated::Expired);
    assert!(row(&recs, 1).await.unwrap().revoked_at.is_some());
    // Past `expires_at`, however young.
    let mut short = new_session(2, 3, at(0));
    short.expires_at = at(30);
    insert(&cat, T, &short).await.unwrap();
    let r = rotate_in(&cat, T, [2; 16], [1; 32], [2; 32], RULES, at(30)).await;
    assert_eq!(r.unwrap(), Rotated::Expired);
}

#[tokio::test]
async fn a_revoke_ends_a_live_session_once() {
    let (recs, cat) = catalog().await;
    insert(&cat, T, &new_session(1, 3, at(0))).await.unwrap();
    assert!(revoke_in_change(&cat, T, [1; 16], at(5)).await.unwrap());
    let v = auth_version(&recs).await;
    assert_eq!(row(&recs, 1).await.unwrap().changed_version, v);
    // Again, or an unknown session: false, and no version is used (the
    // next change follows directly).
    assert!(!revoke_in_change(&cat, T, [1; 16], at(6)).await.unwrap());
    assert!(!revoke_in_change(&cat, T, [2; 16], at(6)).await.unwrap());
    // A refused change rolls back and settles the version it was
    // given (a closed gap): no row moves to it.
    assert_eq!(
        row(&recs, 1).await.unwrap().revoked_at,
        Some(crate::rec::to_micros(at(5)))
    );
}

#[tokio::test]
async fn touches_move_last_use_of_live_sessions_unversioned() {
    let (recs, cat) = catalog().await;
    insert(&cat, T, &new_session(1, 3, at(0))).await.unwrap();
    insert(&cat, T, &new_session(2, 3, at(0))).await.unwrap();
    revoke_in_change(&cat, T, [2; 16], at(1)).await.unwrap();
    let v = auth_version(&recs).await;
    touch_in(&cat, T, [1; 16], at(50)).await.unwrap();
    touch_in(&cat, T, [2; 16], at(50)).await.unwrap();
    touch_in(&cat, T, [3; 16], at(50)).await.unwrap();
    assert_eq!(
        row(&recs, 1).await.unwrap().last_used_at,
        Some(crate::rec::to_micros(at(50)))
    );
    assert_eq!(
        row(&recs, 2).await.unwrap().last_used_at,
        Some(crate::rec::to_micros(at(0)))
    );
    assert!(row(&recs, 3).await.is_none());
    assert_eq!(auth_version(&recs).await, v);
    let s = read_session(&cat, T, [1; 16]).await.unwrap().unwrap();
    assert_eq!(s.user_id, UserId(3));
    assert!(read_session(&cat, T, [3; 16]).await.unwrap().is_none());
}

#[tokio::test]
async fn a_factor_reset_revokes_the_users_live_sessions() {
    let (recs, cat) = catalog().await;
    add_user(&cat, 3, "app").await;
    insert(&cat, T, &new_session(1, 3, at(0))).await.unwrap();
    insert(&cat, T, &new_session(2, 3, at(0))).await.unwrap();
    insert(&cat, T, &new_session(3, 4, at(0))).await.unwrap();
    revoke_in_change(&cat, T, [2; 16], at(1)).await.unwrap();
    let factors = crate::mfa::FactorStore::new(cat.clone());
    assert_eq!(factors.reset(T, UserId(3), at(9)).await.unwrap(), 0);
    let v = auth_version(&recs).await;
    let one = row(&recs, 1).await.unwrap();
    assert_eq!(one.revoked_at, Some(crate::rec::to_micros(at(9))));
    assert_eq!(one.changed_version, v);
    // Already revoked: left as it was.
    let two = row(&recs, 2).await.unwrap();
    assert_eq!(two.revoked_at, Some(crate::rec::to_micros(at(1))));
    assert!(two.changed_version < v);
    // Another user's: untouched.
    assert!(row(&recs, 3).await.unwrap().revoked_at.is_none());
}
