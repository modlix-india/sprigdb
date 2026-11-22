//! Issuers, claim mappings and the token-method writes on catalog records
//! (the in-memory store): ids, replacement by issuer, the `SET` column's
//! order, read order, change detection, and the user writes.

#![allow(clippy::unwrap_used)]

use sprig_catalog::records::RecordStore as _;
use sprig_catalog::records::mem::MemRecords;

use super::*;
use crate::token::sessions::tests::{T, add_user, at, auth_version, catalog};

fn mapping(priority: i16, kind: MappingKind, target: &str) -> ClaimMapping {
    ClaimMapping {
        priority,
        kind,
        claim: "sub".into(),
        match_kind: MatchKind::Any,
        match_value: None,
        target: target.into(),
    }
}

fn spec(issuer: &str, mappings: Vec<ClaimMapping>) -> IssuerSpec {
    IssuerSpec {
        issuer_id: None,
        kind: IssuerKind::Oidc,
        issuer: issuer.into(),
        jwks_url: None,
        audiences: vec!["sprig".into()],
        algorithms: vec!["RS256".into()],
        protocols: vec![
            TokenProtocol::Http,
            TokenProtocol::Mysql,
            TokenProtocol::Http,
        ],
        client_id: None,
        request_scopes: Some(vec!["openid".into()]),
        jit_users: false,
        max_lifetime_s: 3600,
        active: true,
        mappings,
    }
}

async fn issuers(recs: &MemRecords) -> Vec<IssuerSpec> {
    let mut tx = recs.begin(T).await.unwrap();
    read_issuers(&mut *tx).await.unwrap()
}

async fn changed(recs: &MemRecords, since: u64) -> bool {
    let mut tx = recs.begin(T).await.unwrap();
    changed_since(&mut *tx, since).await.unwrap()
}

#[tokio::test]
async fn issuers_get_ids_and_are_replaced_by_their_url() {
    let (recs, cat) = catalog().await;
    assert!(issuers(&recs).await.is_empty());
    assert!(!changed(&recs, 0).await);
    let a = spec(
        "https://a.example",
        vec![
            mapping(20, MappingKind::Role, "r20"),
            mapping(10, MappingKind::User, "u10"),
            mapping(20, MappingKind::Role, "r20b"),
        ],
    );
    let (ida, va) = put_issuer(&cat, T, &a).await.unwrap();
    let (idb, vb) = put_issuer(&cat, T, &spec("https://b.example", vec![]))
        .await
        .unwrap();
    assert_eq!((ida, idb), (1, 2));
    assert!(vb > va);
    let got = issuers(&recs).await;
    assert_eq!(got.len(), 2);
    assert_eq!(got[0].issuer_id, Some(1));
    // The SET column's order, each protocol once.
    assert_eq!(
        got[0].protocols,
        vec![TokenProtocol::Mysql, TokenProtocol::Http]
    );
    // Mappings by priority, then id (the order they were given).
    let targets: Vec<&str> = got[0].mappings.iter().map(|m| m.target.as_str()).collect();
    assert_eq!(targets, vec!["u10", "r20", "r20b"]);
    assert_eq!(got[0].request_scopes, Some(vec!["openid".to_owned()]));
    assert!(got[1].mappings.is_empty());
    // The same URL (trailing spaces aside, as the unique key compared it)
    // replaces the issuer and every mapping of it, keeping its id.
    let mut again = spec(
        "https://a.example  ",
        vec![mapping(5, MappingKind::User, "u5")],
    );
    again.active = false;
    // The key set's URL given: the issuer itself needn't parse as one.
    again.jwks_url = Some("https://a.example/keys".into());
    again.protocols = vec![TokenProtocol::Redis];
    let (id, v) = put_issuer(&cat, T, &again).await.unwrap();
    assert_eq!(id, 1);
    let got = issuers(&recs).await;
    assert_eq!(got.len(), 2);
    assert_eq!(got[0].issuer, "https://a.example  ");
    assert!(!got[0].active);
    assert_eq!(got[0].protocols, vec![TokenProtocol::Redis]);
    assert_eq!(got[0].mappings, vec![mapping(5, MappingKind::User, "u5")]);
    assert_eq!(got[1].issuer, "https://b.example");
    // Only the issuer's own mappings went.
    let mut tx = recs.begin(T).await.unwrap();
    let maps: Vec<MappingRow> = CLAIM_MAPPINGS.rows(&mut *tx, &Pk::new()).await.unwrap();
    assert_eq!(maps.len(), 1);
    assert_eq!(maps[0].changed_version, v);
    assert!(maps[0].mapping_id > 3, "mapping ids are never reused");
    // Another URL is another issuer.
    let (idc, _) = put_issuer(&cat, T, &spec("https://A.example", vec![]))
        .await
        .unwrap();
    assert_eq!(idc, 3);
}

#[tokio::test]
async fn changes_are_seen_after_their_version_only() {
    let (recs, cat) = catalog().await;
    let (_, v1) = put_issuer(&cat, T, &spec("https://a.example", vec![]))
        .await
        .unwrap();
    assert!(changed(&recs, v1 - 1).await);
    assert!(!changed(&recs, v1).await);
    let (_, v2) = put_issuer(
        &cat,
        T,
        &spec(
            "https://a.example",
            vec![mapping(1, MappingKind::User, "x")],
        ),
    )
    .await
    .unwrap();
    assert!(changed(&recs, v1).await);
    assert!(!changed(&recs, v2).await);
    // A mapping alone changed after `since` is a change too.
    let mut tx = recs.begin(T).await.unwrap();
    let mut row: IssuerRow = OIDC_ISSUERS
        .get(&mut *tx, &issuer_pk(1))
        .await
        .unwrap()
        .unwrap();
    row.changed_version = 0;
    OIDC_ISSUERS
        .put(&mut *tx, &issuer_pk(1), &row)
        .await
        .unwrap();
    tx.commit().await.unwrap();
    assert!(changed(&recs, v2 - 1).await);
    assert!(!changed(&recs, v2).await);
}

#[tokio::test]
async fn a_bad_spec_writes_nothing() {
    let (recs, cat) = catalog().await;
    let mut bad = spec("https://a.example", vec![]);
    bad.audiences.clear();
    let e = put_issuer(&cat, T, &bad).await.unwrap_err();
    assert_eq!(e.code(), Some(SprigCode::ConfigInvalid));
    assert!(issuers(&recs).await.is_empty());
    assert_eq!(auth_version(&recs).await, 0);
    migrate(&cat, T).await.unwrap();
}

async fn user(recs: &MemRecords, id: u32) -> UserRow {
    let mut tx = recs.begin(T).await.unwrap();
    AuthStore::new()
        .user(&mut *tx, UserId(id))
        .await
        .unwrap()
        .unwrap()
}

#[tokio::test]
async fn the_token_method_keeps_the_other_methods() {
    let (recs, cat) = catalog().await;
    add_user(&cat, 3, "app").await;
    let c = set_token_method(&cat, T, "app", "%", true).await.unwrap();
    assert_eq!(c.user_id, UserId(3));
    let u = user(&recs, 3).await;
    assert_eq!(
        u.auth_methods,
        AuthMethods::PASSWORD.union(AuthMethods::TOKEN)
    );
    assert_eq!(u.changed_version, c.auth_version);
    assert_eq!(auth_version(&recs).await, c.auth_version);
    let c = set_token_method(&cat, T, "app", "%", false).await.unwrap();
    assert_eq!(user(&recs, 3).await.auth_methods, AuthMethods::PASSWORD);
    assert_eq!(user(&recs, 3).await.changed_version, c.auth_version);
    // Unknown account (the host compares without case, the name exactly).
    let e = set_token_method(&cat, T, "App", "%", true)
        .await
        .unwrap_err();
    assert!(matches!(
        e.kind,
        sprig_common::ErrorKind::Backend { errno: 1396, .. }
    ));
    // A refused change rolls back and settles the version it was
    // given (a closed gap): no row moves to it.
}

#[tokio::test]
async fn a_just_in_time_user_has_only_the_token_method() {
    let (recs, cat) = catalog().await;
    add_user(&cat, 100, "taken").await;
    let c = create_jit_user(&cat, T, "alice", at(5)).await.unwrap();
    let u = user(&recs, c.user_id.0).await;
    assert_eq!(u.name, "alice");
    assert_eq!(u.host, "%");
    assert_eq!(u.auth_methods, AuthMethods::TOKEN);
    assert_eq!(u.source, UserSource::OidcJit);
    assert_eq!(u.changed_version, c.auth_version);
    // A live account of the name already there: 1396, and nothing new.
    let e = create_jit_user(&cat, T, "taken", at(6)).await.unwrap_err();
    assert!(matches!(
        e.kind,
        sprig_common::ErrorKind::Backend { errno: 1396, .. }
    ));
    let e = create_jit_user(&cat, T, "", at(6)).await.unwrap_err();
    assert_eq!(e.code(), Some(SprigCode::ConfigInvalid));
    // A refused change rolls back and settles the version it was
    // given (a closed gap): no row moves to it.
}
