#![allow(clippy::unwrap_used, clippy::too_many_lines)]

use sprig_catalog::RecordStore;
use sprig_catalog::records::mem::MemRecords;
use sprig_common::ids::TenantId;

use super::*;

const T: TenantId = TenantId(42);

async fn begin() -> Box<dyn RecordTx> {
    MemRecords::new().begin(T).await.unwrap()
}

/// The targets and actions `who` holds, in `rows`' order.
fn held(rows: &AuthzRows, who: Grantee) -> Vec<(PrivTarget, ActionSet)> {
    rows.privileges
        .iter()
        .filter(|p| p.grantee == who)
        .map(|p| (p.target.clone(), p.actions))
        .collect()
}

fn raw_privileges(rows: &[(Pk, PrivRec)]) -> Vec<PrivRec> {
    rows.iter().map(|(_, r)| r.clone()).collect()
}

async fn stored(tx: &mut dyn RecordTx) -> Vec<PrivRec> {
    raw_privileges(&PRIVILEGES.scan::<PrivRec>(tx, &Pk::new()).await.unwrap())
}

#[test]
fn named_patterns_round_trip() {
    for (d, t) in [("shop", "orders"), ("a`b", "c.d"), ("x", "``"), ("", "t")] {
        let p = named_pattern(d, t);
        assert_eq!(parse_named(&p), Some((d.to_owned(), t.to_owned())), "{p}");
    }
    assert_eq!(named_pattern("shop", "orders"), "`shop`.`orders`");
    assert_eq!(parse_named("shop"), None);
    assert_eq!(parse_named("`shop`.`t`x"), None);
    assert_eq!(parse_named(""), None);
}

#[test]
fn legacy_bits_are_everything_but_bypass() {
    // The bits format 1 gave every existing user.
    assert_eq!(LEGACY_ACTIONS.0, 4_026_531_839);
    assert!(LEGACY_ACTIONS.has(Action::TenantAdmin));
    assert!(LEGACY_ACTIONS.has(Action::GrantOption));
    assert!(LEGACY_ACTIONS.has(Action::Select));
    assert!(!LEGACY_ACTIONS.has(Action::BypassRls));
    assert!(AUTHZ_MODULE.migrations.is_empty());
    assert!(AuthzStore::new().migrated());
    assert_eq!(AuthzStore::new().format(), AUTHZ_FORMAT);
}

/// Every store operation, as the old test against MariaDB ran them.
#[tokio::test]
async fn every_store_operation_round_trips() {
    let mut tx = begin().await;
    let c = &mut *tx;
    let s = AuthzStore::new();
    let alice = Grantee::User(UserId(7));
    let by = Some(UserId(1));
    let sel = ActionSet::of(&[Action::Select]);
    let ins = ActionSet::of(&[Action::Insert]);
    let si = sel.union(ins);
    let (db, obj) = (DbId(3), ObjectId(41));
    let targets = [
        PrivTarget::Tenant,
        PrivTarget::Database("shop".into()),
        PrivTarget::Object { db, object: obj },
        PrivTarget::Column {
            db,
            object: obj,
            column: "price".into(),
        },
        PrivTarget::Restriction("hr".into()),
        PrivTarget::Named {
            db_key: "shop".into(),
            name_key: "gone".into(),
            column: None,
        },
        PrivTarget::Named {
            db_key: "shop".into(),
            name_key: "gone".into(),
            column: Some("c1".into()),
        },
        PrivTarget::GraphElement {
            db,
            graph: ObjectId(42),
            rels: false,
            name: "Person".into(),
        },
        PrivTarget::GraphElement {
            db,
            graph: ObjectId(42),
            rels: true,
            name: "KNOWS".into(),
        },
    ];
    // grant: one row per target, then again (the actions add up).
    for t in &targets {
        s.grant(c, alice, t, sel, by).await.unwrap();
    }
    for t in &targets {
        s.grant(c, alice, t, ins, None).await.unwrap();
    }
    let rows = s.load(c).await.unwrap();
    assert_eq!(
        held(&rows, alice),
        targets.iter().map(|t| (t.clone(), si)).collect::<Vec<_>>(),
        "every target loads back as granted"
    );
    // revoke: a part (an update) and the rest (a delete); a missing row.
    for t in &targets {
        assert_eq!(s.revoke(c, alice, t, ins).await.unwrap(), si, "{t:?}");
    }
    assert!(
        held(&s.load(c).await.unwrap(), alice)
            .iter()
            .all(|(_, a)| *a == sel)
    );
    for t in &targets {
        assert_eq!(s.revoke(c, alice, t, sel).await.unwrap(), sel, "{t:?}");
        assert_eq!(s.revoke(c, alice, t, sel).await.unwrap(), ActionSet::EMPTY);
    }
    assert!(held(&s.load(c).await.unwrap(), alice).is_empty());
    // Roles: create (and again: None), find, edges, defaults.
    let r1 = s.create_role(c, "reader", "%", by).await.unwrap().unwrap();
    let r2 = s
        .create_role(c, "writer", "%", None)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(s.create_role(c, "reader", "%", by).await.unwrap(), None);
    assert_eq!(s.role(c, "reader", "%").await.unwrap(), Some(r1));
    assert_eq!(s.role(c, "nobody", "%").await.unwrap(), None);
    s.grant_role(c, alice, r1, false, by).await.unwrap();
    s.grant_role(c, alice, r1, true, by).await.unwrap();
    s.grant_role(c, alice, r2, false, None).await.unwrap();
    s.grant_role(c, Grantee::Role(r2), r1, false, by)
        .await
        .unwrap();
    s.set_default_roles(c, UserId(7), &[r2]).await.unwrap();
    let rows = s.load(c).await.unwrap();
    assert_eq!(rows.roles.len(), 2);
    let edge = |g: Grantee, r: RoleId| {
        rows.edges
            .iter()
            .find(|e| e.grantee == g && e.role == r)
            .copied()
    };
    assert_eq!(
        edge(alice, r1),
        Some(EdgeRow {
            grantee: alice,
            role: r1,
            admin_option: true,
            is_default: false
        })
    );
    assert!(edge(alice, r2).is_some_and(|e| e.is_default && !e.admin_option));
    assert!(edge(Grantee::Role(r2), r1).is_some());
    assert!(s.revoke_role(c, alice, r2).await.unwrap());
    assert!(!s.revoke_role(c, alice, r2).await.unwrap());
    // forget_privileges and forget: a role's grants and the edges to it.
    let role2 = Grantee::Role(r2);
    s.grant(c, role2, &PrivTarget::Tenant, sel, by)
        .await
        .unwrap();
    s.forget_privileges(c, role2).await.unwrap();
    assert!(held(&s.load(c).await.unwrap(), role2).is_empty());
    s.grant(c, role2, &PrivTarget::Tenant, sel, by)
        .await
        .unwrap();
    s.forget(c, Grantee::Role(r1)).await.unwrap();
    s.forget(c, role2).await.unwrap();
    s.delete_role(c, r1).await.unwrap();
    s.delete_role(c, r2).await.unwrap();
    let rows = s.load(c).await.unwrap();
    assert!(rows.roles.is_empty() && rows.edges.is_empty(), "{rows:?}");
    // keep_by_name: an object's grants become named ones; forget_named.
    let col = PrivTarget::Column {
        db,
        object: obj,
        column: "price".into(),
    };
    let tbl = PrivTarget::Object { db, object: obj };
    s.grant(c, alice, &tbl, si, by).await.unwrap();
    s.grant(c, alice, &col, sel, by).await.unwrap();
    assert!(s.keep_by_name(c, db, obj, "shop", "items").await.unwrap());
    assert!(!s.keep_by_name(c, db, obj, "shop", "items").await.unwrap());
    let named = |column: Option<&str>| PrivTarget::Named {
        db_key: "shop".into(),
        name_key: "items".into(),
        column: column.map(str::to_owned),
    };
    let mut got = held(&s.load(c).await.unwrap(), alice);
    got.sort();
    let mut want = vec![(named(None), si), (named(Some("price")), sel)];
    want.sort();
    assert_eq!(got, want);
    s.forget_named(c, "shop", Some("items")).await.unwrap();
    assert!(held(&s.load(c).await.unwrap(), alice).is_empty());
    s.grant(c, alice, &named(None), sel, by).await.unwrap();
    s.forget_named(c, "shop", None).await.unwrap();
    assert!(held(&s.load(c).await.unwrap(), alice).is_empty());
    // forget_object and forget_database.
    s.grant(c, alice, &tbl, sel, by).await.unwrap();
    s.forget_object(c, db, obj).await.unwrap();
    assert!(held(&s.load(c).await.unwrap(), alice).is_empty());
    s.grant(c, alice, &PrivTarget::Database("shop".into()), sel, by)
        .await
        .unwrap();
    s.grant(c, alice, &col, sel, by).await.unwrap();
    s.forget_database(c, db, "shop").await.unwrap();
    assert!(held(&s.load(c).await.unwrap(), alice).is_empty());
    assert!(stored(c).await.is_empty());
    assert!(
        ROLE_EDGES
            .rows::<EdgeRec>(c, &Pk::new())
            .await
            .unwrap()
            .is_empty()
    );
    assert!(
        ROLES
            .rows::<RoleRec>(c, &Pk::new())
            .await
            .unwrap()
            .is_empty()
    );
}

#[tokio::test]
async fn a_grant_ors_into_the_row_of_its_unique_key() {
    let mut tx = begin().await;
    let c = &mut *tx;
    let s = AuthzStore::new();
    let alice = Grantee::User(UserId(7));
    let bob = Grantee::User(UserId(8));
    let sel = ActionSet::of(&[Action::Select]);
    let ins = ActionSet::of(&[Action::Insert]);
    let shop = PrivTarget::Database("shop".into());
    s.grant(c, alice, &shop, sel, Some(UserId(1)))
        .await
        .unwrap();
    s.grant(c, alice, &shop, ins, Some(UserId(2)))
        .await
        .unwrap();
    // The same name as a restriction, another grantee, a role with the
    // user's id, another database: each its own row.
    s.grant(c, alice, &PrivTarget::Restriction("shop".into()), sel, None)
        .await
        .unwrap();
    s.grant(c, bob, &shop, sel, None).await.unwrap();
    s.grant(c, Grantee::Role(RoleId(7)), &shop, sel, None)
        .await
        .unwrap();
    s.grant(c, alice, &PrivTarget::Database("Shop".into()), sel, None)
        .await
        .unwrap();
    let rows = stored(c).await;
    assert_eq!(rows.len(), 5);
    // Ids from the counter, in grant order; the merged row keeps its id
    // and takes the newer grantor.
    assert_eq!(
        rows.iter().map(|r| r.priv_id).collect::<Vec<_>>(),
        vec![1, 2, 3, 4, 5]
    );
    assert_eq!(rows[0].actions, sel.union(ins).0);
    assert_eq!(rows[0].granted_by, Some(2));
    assert_eq!(rows[1].restriction, 1);
    // Times are kept to the millisecond.
    assert_eq!(crate::store::micros(rows[0].granted_at) % 1000, 0);
    // Load returns them by id; a revoke of everything deletes the row and
    // its id is never handed out again.
    let loaded = s.load(c).await.unwrap();
    assert_eq!(
        loaded
            .privileges
            .iter()
            .map(|p| p.priv_id)
            .collect::<Vec<_>>(),
        vec![1, 2, 3, 4, 5]
    );
    assert_eq!(
        s.revoke(c, bob, &shop, sel).await.unwrap(),
        sel,
        "bob's row held SELECT"
    );
    s.grant(c, bob, &shop, sel, None).await.unwrap();
    assert_eq!(stored(c).await.last().unwrap().priv_id, 6);
}

#[tokio::test]
async fn load_skips_rows_this_build_does_not_use() {
    let mut tx = begin().await;
    let c = &mut *tx;
    let s = AuthzStore::new();
    let alice = Grantee::User(UserId(7));
    let sel = ActionSet::of(&[Action::Select]);
    s.grant(c, alice, &PrivTarget::Tenant, sel, None)
        .await
        .unwrap();
    let base = stored(c).await.remove(0);
    let put = |id: u64, f: &dyn Fn(&mut PrivRec)| {
        let mut r = base.clone();
        r.priv_id = id;
        f(&mut r);
        r
    };
    for r in [
        // Another tenant's grantee.
        put(10, &|r| r.grantee_tenant_id = 9),
        // A restriction below the database level.
        put(11, &|r| {
            r.level = "object".into();
            r.restriction = 1;
        }),
        // A level of a later release.
        put(12, &|r| r.level = "kv_prefix".into()),
    ] {
        PRIVILEGES.put(c, &priv_pk(r.priv_id), &r).await.unwrap();
    }
    // A role of another kind; an edge of another tenant's grantee.
    let mut role = RoleRec {
        role_id: 5,
        kind: "mongo".into(),
        name: "m".into(),
        host: "%".into(),
        db_id: 0,
        members_hash: None,
        created_by: None,
        created_at: now_ms(),
        last_used_at: None,
    };
    ROLES.put(c, &role_pk(5), &role).await.unwrap();
    role.role_id = 6;
    role.kind = SQL_ROLE.into();
    ROLES.put(c, &role_pk(6), &role).await.unwrap();
    s.grant_role(c, alice, RoleId(6), false, None)
        .await
        .unwrap();
    let pk = edge_pk(alice, RoleId(5));
    let mut e: EdgeRec = ROLE_EDGES
        .get(c, &edge_pk(alice, RoleId(6)))
        .await
        .unwrap()
        .unwrap();
    e.role_id = 5;
    e.grantee_tenant_id = 9;
    ROLE_EDGES.put(c, &pk, &e).await.unwrap();
    let rows = s.load(c).await.unwrap();
    assert_eq!(rows.privileges.len(), 1);
    assert_eq!(rows.privileges[0].target, PrivTarget::Tenant);
    assert_eq!(
        rows.roles.iter().map(|r| r.role_id).collect::<Vec<_>>(),
        vec![RoleId(6)]
    );
    assert_eq!(
        rows.edges.iter().map(|e| e.role).collect::<Vec<_>>(),
        vec![RoleId(6)]
    );
}

#[tokio::test]
async fn roles_are_unique_by_name_and_host_without_case() {
    let mut tx = begin().await;
    let c = &mut *tx;
    let s = AuthzStore::new();
    let r1 = s
        .create_role(c, "Reader", "%", None)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(r1, RoleId(1));
    // The host compares without case, the name exactly.
    let r2 = s
        .create_role(c, "Reader", "Example.COM", None)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        s.create_role(c, "Reader", "example.com", None)
            .await
            .unwrap(),
        None
    );
    let r3 = s
        .create_role(c, "reader", "%", None)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(s.role(c, "Reader", "EXAMPLE.com").await.unwrap(), Some(r2));
    assert_eq!(s.role(c, "READER", "%").await.unwrap(), None);
    // A dropped role's name is free again; its id is never reused.
    s.delete_role(c, r1).await.unwrap();
    let r4 = s
        .create_role(c, "Reader", "%", None)
        .await
        .unwrap()
        .unwrap();
    assert!(r4.0 > r3.0, "{r4:?} after {r3:?}");
    // Longer than the old columns: MariaDB's 1406.
    let long = "x".repeat(256);
    let e = s.create_role(c, &long, "%", None).await.unwrap_err();
    assert!(e.to_string().contains("1406"), "{e}");
    assert!(
        s.create_role(c, &"é".repeat(255), "%", None)
            .await
            .unwrap()
            .is_some()
    );
}

#[tokio::test]
async fn role_edges_keep_their_flags_and_load_in_key_order() {
    let mut tx = begin().await;
    let c = &mut *tx;
    let s = AuthzStore::new();
    let (r1, r2, r3) = (RoleId(1), RoleId(2), RoleId(3));
    let u9 = Grantee::User(UserId(9));
    let u300 = Grantee::User(UserId(300));
    // Granted out of order: users before roles, then by grantee and role.
    s.grant_role(c, Grantee::Role(r2), r1, false, None)
        .await
        .unwrap();
    s.grant_role(c, u300, r1, false, None).await.unwrap();
    s.grant_role(c, u9, r3, true, None).await.unwrap();
    s.grant_role(c, u9, r1, false, Some(UserId(1)))
        .await
        .unwrap();
    s.set_default_roles(c, UserId(9), &[r1, r3]).await.unwrap();
    // Granting again: the admin option only ever turns on; the default flag
    // and grantor stay.
    s.grant_role(c, u9, r3, false, Some(UserId(5)))
        .await
        .unwrap();
    s.grant_role(c, u9, r1, true, Some(UserId(5)))
        .await
        .unwrap();
    let e: EdgeRec = ROLE_EDGES.get(c, &edge_pk(u9, r1)).await.unwrap().unwrap();
    assert_eq!(e.granted_by, Some(1));
    let rows = s.load(c).await.unwrap();
    let got: Vec<_> = rows
        .edges
        .iter()
        .map(|e| (e.grantee, e.role, e.admin_option, e.is_default))
        .collect();
    assert_eq!(
        got,
        vec![
            (u9, r1, true, true),
            (u9, r3, true, true),
            (u300, r1, false, false),
            (Grantee::Role(r2), r1, false, false),
        ]
    );
    // SET DEFAULT ROLE replaces the defaults; other grantees keep theirs.
    s.set_default_roles(c, UserId(300), &[r1]).await.unwrap();
    s.set_default_roles(c, UserId(9), &[r3]).await.unwrap();
    let rows = s.load(c).await.unwrap();
    let defaults: Vec<_> = rows
        .edges
        .iter()
        .filter(|e| e.is_default)
        .map(|e| (e.grantee, e.role))
        .collect();
    assert_eq!(defaults, vec![(u9, r3), (u300, r1)]);
    // Forgetting a role removes the edges it holds and those granting it.
    s.forget(c, Grantee::Role(r2)).await.unwrap();
    s.forget(c, Grantee::Role(r1)).await.unwrap();
    let rows = s.load(c).await.unwrap();
    assert_eq!(
        rows.edges
            .iter()
            .map(|e| (e.grantee, e.role))
            .collect::<Vec<_>>(),
        vec![(u9, r3)]
    );
}

#[tokio::test]
async fn keep_by_name_merges_into_rows_held_by_name() {
    let mut tx = begin().await;
    let c = &mut *tx;
    let s = AuthzStore::new();
    let alice = Grantee::User(UserId(7));
    let bob = Grantee::User(UserId(8));
    let sel = ActionSet::of(&[Action::Select]);
    let ins = ActionSet::of(&[Action::Insert]);
    let (db, obj) = (DbId(3), ObjectId(41));
    let named = PrivTarget::Named {
        db_key: "shop".into(),
        name_key: "items".into(),
        column: None,
    };
    // Already held by name, by another grantor.
    s.grant(c, alice, &named, sel, Some(UserId(2)))
        .await
        .unwrap();
    s.grant(
        c,
        alice,
        &PrivTarget::Object { db, object: obj },
        ins,
        Some(UserId(1)),
    )
    .await
    .unwrap();
    s.grant(
        c,
        bob,
        &PrivTarget::Object { db, object: obj },
        sel,
        Some(UserId(1)),
    )
    .await
    .unwrap();
    // Another object of the database, and a graph row of the same object
    // id: untouched.
    s.grant(
        c,
        alice,
        &PrivTarget::Object {
            db,
            object: ObjectId(40),
        },
        sel,
        None,
    )
    .await
    .unwrap();
    let graph = PrivTarget::GraphElement {
        db,
        graph: obj,
        rels: false,
        name: "L".into(),
    };
    s.grant(c, alice, &graph, sel, None).await.unwrap();
    assert!(s.keep_by_name(c, db, obj, "shop", "items").await.unwrap());
    let rows = stored(c).await;
    let alice_named: Vec<_> = rows
        .iter()
        .filter(|r| r.held_by(alice) && r.name_pattern == "`shop`.`items`")
        .collect();
    assert_eq!(alice_named.len(), 1);
    assert_eq!(alice_named[0].actions, sel.union(ins).0);
    // The merged row keeps its grantor; a new one takes the source's.
    assert_eq!(alice_named[0].granted_by, Some(2));
    let bob_named = rows.iter().find(|r| r.held_by(bob)).unwrap();
    assert_eq!(bob_named.name_pattern, "`shop`.`items`");
    assert_eq!(bob_named.granted_by, Some(1));
    assert_eq!((bob_named.db_id, bob_named.object_id), (0, 0));
    // The graph row of the same id is deleted with the object (as the old
    // `DELETE ... WHERE db_id = ? AND object_id = ?` did); the other
    // object's row stays.
    let loaded = s.load(c).await.unwrap();
    assert!(held(&loaded, alice).iter().all(|(t, _)| *t != graph));
    assert!(held(&loaded, alice).iter().any(|(t, _)| *t
        == PrivTarget::Object {
            db,
            object: ObjectId(40)
        }));
    // A name pattern longer than the old column fails as MariaDB did.
    s.grant(c, alice, &PrivTarget::Object { db, object: obj }, sel, None)
        .await
        .unwrap();
    let long = "n".repeat(250);
    let e = s.keep_by_name(c, db, obj, "shop", &long).await.unwrap_err();
    assert!(e.to_string().contains("1406"), "{e}");
}

#[tokio::test]
async fn forget_named_matches_the_database_prefix_exactly() {
    let mut tx = begin().await;
    let c = &mut *tx;
    let s = AuthzStore::new();
    let alice = Grantee::User(UserId(7));
    let sel = ActionSet::of(&[Action::Select]);
    let named = |d: &str, t: &str, col: Option<&str>| PrivTarget::Named {
        db_key: d.into(),
        name_key: t.into(),
        column: col.map(str::to_owned),
    };
    for t in [
        named("shop", "a", None),
        named("shop", "a", Some("c")),
        named("shop", "b", None),
        named("shop2", "a", None),
        named("sh`op", "a", None),
        PrivTarget::Database("shop".into()),
    ] {
        s.grant(c, alice, &t, sel, None).await.unwrap();
    }
    s.forget_named(c, "shop", Some("a")).await.unwrap();
    let left = |rows: &AuthzRows| {
        held(rows, alice)
            .into_iter()
            .map(|(t, _)| t)
            .collect::<Vec<_>>()
    };
    assert_eq!(
        left(&s.load(c).await.unwrap()),
        vec![
            named("shop", "b", None),
            named("shop2", "a", None),
            named("sh`op", "a", None),
            PrivTarget::Database("shop".into()),
        ]
    );
    s.forget_named(c, "shop", None).await.unwrap();
    assert_eq!(
        left(&s.load(c).await.unwrap()),
        vec![
            named("shop2", "a", None),
            named("sh`op", "a", None),
            PrivTarget::Database("shop".into()),
        ]
    );
    s.forget_named(c, "sh`op", None).await.unwrap();
    assert_eq!(
        left(&s.load(c).await.unwrap()),
        vec![
            named("shop2", "a", None),
            PrivTarget::Database("shop".into())
        ]
    );
}

#[tokio::test]
async fn forget_database_takes_its_name_rows_and_object_rows() {
    let mut tx = begin().await;
    let c = &mut *tx;
    let s = AuthzStore::new();
    let alice = Grantee::User(UserId(7));
    let sel = ActionSet::of(&[Action::Select]);
    let db = DbId(3);
    for t in [
        PrivTarget::Database("shop".into()),
        PrivTarget::Restriction("shop".into()),
        PrivTarget::Database("other".into()),
        PrivTarget::Object {
            db,
            object: ObjectId(1),
        },
        PrivTarget::Column {
            db,
            object: ObjectId(1),
            column: "c".into(),
        },
        PrivTarget::Object {
            db: DbId(4),
            object: ObjectId(2),
        },
        PrivTarget::GraphElement {
            db,
            graph: ObjectId(5),
            rels: true,
            name: "R".into(),
        },
    ] {
        s.grant(c, alice, &t, sel, None).await.unwrap();
    }
    s.forget_database(c, db, "shop").await.unwrap();
    let left: Vec<_> = held(&s.load(c).await.unwrap(), alice)
        .into_iter()
        .map(|(t, _)| t)
        .collect();
    assert_eq!(
        left,
        vec![
            PrivTarget::Database("other".into()),
            PrivTarget::Object {
                db: DbId(4),
                object: ObjectId(2)
            },
            PrivTarget::GraphElement {
                db,
                graph: ObjectId(5),
                rels: true,
                name: "R".into(),
            },
        ]
    );
}
