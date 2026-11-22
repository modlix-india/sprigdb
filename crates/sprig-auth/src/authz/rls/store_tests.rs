//! [`RlsStore`] on the in-memory record store: what the old tables
//! enforced (a policy name unique per object, ids, compare-and-set,
//! changed-rows counts, the column limits) and the loads' order.

#![allow(clippy::unwrap_used)]

use sprig_catalog::RecordStore;
use sprig_catalog::records::mem::MemRecords;
use sprig_common::ids::TenantId;

use super::*;

const T: TenantId = TenantId(42);

fn new_policy<'a>(object: u64, name: &'a str, grantees: &'a [Grantee]) -> NewPolicy<'a> {
    NewPolicy {
        object: ObjectId(object),
        db: DbId(1),
        name,
        restrictive: false,
        commands: cmd::SELECT | cmd::DELETE,
        grantees,
        using_expr: Some("owner = CURRENT_USER()"),
        check_expr: None,
        using_mongo: None,
        check_mongo: None,
        by: UserId(1),
    }
}

async fn stored(tx: &mut dyn RecordTx, id: u32) -> PolicyRec {
    ROW_POLICIES.get(tx, &policy_pk(id)).await.unwrap().unwrap()
}

#[test]
fn the_module_has_no_migrations() {
    assert!(RLS_MODULE.migrations.is_empty());
    assert!(RlsStore::new().migrated());
    RlsStore::new().migrate();
}

#[tokio::test]
async fn policies_are_unique_per_object_and_load_by_object_and_name() {
    let recs = MemRecords::new();
    let mut tx = recs.begin(T).await.unwrap();
    let s = RlsStore::new();
    let g = [Grantee::Role(RoleId(3)), Grantee::User(UserId(7))];
    assert!(
        s.insert(&mut *tx, &new_policy(20, "b", &g), 5)
            .await
            .unwrap()
    );
    assert!(
        s.insert(&mut *tx, &new_policy(20, "a", &[]), 5)
            .await
            .unwrap()
    );
    assert!(
        s.insert(&mut *tx, &new_policy(10, "z", &[]), 5)
            .await
            .unwrap()
    );
    // The same name on the same table: refused; elsewhere, or another
    // case, a policy of its own.
    assert!(
        !s.insert(&mut *tx, &new_policy(20, "b", &[]), 6)
            .await
            .unwrap()
    );
    assert!(
        s.insert(&mut *tx, &new_policy(10, "b", &[]), 6)
            .await
            .unwrap()
    );
    assert!(
        s.insert(&mut *tx, &new_policy(20, "B", &[]), 6)
            .await
            .unwrap()
    );
    tx.commit().await.unwrap();
    let mut tx = recs.begin(T).await.unwrap();
    let rows = s.load(&mut *tx).await.unwrap();
    let got: Vec<_> = rows
        .policies
        .iter()
        .map(|p| (p.object_id.0, p.name.as_str(), p.policy_id))
        .collect();
    // Ids from the counter in insert order (the refused insert took none).
    assert_eq!(
        got,
        vec![
            (10, "b", 4),
            (10, "z", 3),
            (20, "B", 5),
            (20, "a", 2),
            (20, "b", 1)
        ]
    );
    let b = rows.policies.iter().find(|p| p.policy_id == 1).unwrap();
    assert_eq!(b.grantees, g.to_vec());
    assert_eq!(b.commands, cmd::SELECT | cmd::DELETE);
    assert!(!b.restrictive);
    assert_eq!(b.using_expr.as_deref(), Some("owner = CURRENT_USER()"));
    assert_eq!(b.check_expr, None);
    assert_eq!((b.created_by, b.changed_version), (UserId(1), 5));
    // Bits beyond the four commands aren't kept (the old SET column).
    let mut p = new_policy(30, "all", &[]);
    p.commands = 0xFF;
    p.restrictive = true;
    assert!(s.insert(&mut *tx, &p, 7).await.unwrap());
    let rows = s.load(&mut *tx).await.unwrap();
    let all = rows.policies.iter().find(|p| p.name == "all").unwrap();
    assert_eq!(all.commands, 15);
    assert!(all.restrictive);
}

#[tokio::test]
async fn policy_texts_keep_the_old_column_limits() {
    let recs = MemRecords::new();
    let mut tx = recs.begin(T).await.unwrap();
    let s = RlsStore::new();
    let long_name = "p".repeat(65);
    let e = s
        .insert(&mut *tx, &new_policy(1, &long_name, &[]), 1)
        .await
        .unwrap_err();
    assert!(e.to_string().contains("1406"), "{e}");
    assert!(
        s.insert(&mut *tx, &new_policy(1, &"é".repeat(64), &[]), 1)
            .await
            .unwrap()
    );
    let big = "x".repeat(65_536);
    let mut p = new_policy(1, "big", &[]);
    p.check_expr = Some(big.as_str());
    assert!(s.insert(&mut *tx, &p, 1).await.is_err());
    // A MongoDB form must be JSON, as the old JSON column checked.
    let mut p = new_policy(1, "m", &[]);
    p.using_mongo = Some("{not json");
    let e = s.insert(&mut *tx, &p, 1).await.unwrap_err();
    assert!(e.to_string().contains("4025"), "{e}");
    p.using_mongo = Some(r#"{"owner": {"$eq": 1}}"#);
    assert!(s.insert(&mut *tx, &p, 1).await.unwrap());
    // Too long on an existing policy only.
    assert!(
        !s.alter(
            &mut *tx,
            ObjectId(1),
            "none",
            None,
            Some(big.as_str()),
            None,
            2
        )
        .await
        .unwrap()
    );
    assert!(
        s.alter(
            &mut *tx,
            ObjectId(1),
            "m",
            None,
            Some(big.as_str()),
            None,
            2
        )
        .await
        .is_err()
    );
    assert!(
        s.rename(&mut *tx, ObjectId(1), "m", &long_name, 2)
            .await
            .is_err()
    );
}

#[tokio::test]
async fn alter_keeps_what_it_is_not_given_and_counts_changed_rows() {
    let recs = MemRecords::new();
    let mut tx = recs.begin(T).await.unwrap();
    let s = RlsStore::new();
    let mut p = new_policy(1, "p", &[]);
    p.using_mongo = Some(r#"{"a":1}"#);
    p.check_mongo = Some(r#"{"b":1}"#);
    p.check_expr = Some("b = 1");
    assert!(s.insert(&mut *tx, &p, 1).await.unwrap());
    // Grantees only: the expressions and their forms stay.
    let g = [Grantee::User(UserId(9))];
    assert!(
        s.alter(&mut *tx, ObjectId(1), "p", Some(&g), None, None, 2)
            .await
            .unwrap()
    );
    let r = stored(&mut *tx, 1).await;
    assert_eq!(r.grantees, grantees_json(&g));
    assert_eq!(r.using_mongo.as_deref(), Some(r#"{"a":1}"#));
    assert_eq!(r.check_expr.as_deref(), Some("b = 1"));
    assert_eq!(r.changed_version, 2);
    // A new USING drops its MongoDB form; the check's stays.
    assert!(
        s.alter(&mut *tx, ObjectId(1), "p", None, Some("a = 2"), None, 3)
            .await
            .unwrap()
    );
    let r = stored(&mut *tx, 1).await;
    assert_eq!(
        (r.using_expr.as_deref(), r.using_mongo.as_deref()),
        (Some("a = 2"), None)
    );
    assert_eq!(r.check_mongo.as_deref(), Some(r#"{"b":1}"#));
    // The same values at the same version change nothing: MariaDB counted
    // no row.
    assert!(
        !s.alter(&mut *tx, ObjectId(1), "p", None, Some("a = 2"), None, 3)
            .await
            .unwrap()
    );
    // No such policy (another table's name doesn't count).
    assert!(
        !s.alter(&mut *tx, ObjectId(2), "p", None, Some("x"), None, 4)
            .await
            .unwrap()
    );
}

#[tokio::test]
async fn recompiled_writes_only_over_the_text_it_read() {
    let recs = MemRecords::new();
    let mut tx = recs.begin(T).await.unwrap();
    let s = RlsStore::new();
    let mut p = new_policy(1, "p", &[]);
    p.using_expr = Some("old_a");
    p.using_mongo = Some(r#"{"a":1}"#);
    assert!(s.insert(&mut *tx, &p, 1).await.unwrap());
    let mut row = s.load(&mut *tx).await.unwrap().policies.remove(0);
    // As a load with a newer translator sees it.
    row.stored_exprs = Some((Some("old_a".into()), None));
    row.using_expr = Some("new_a".into());
    // Nothing stored to compare with: a no-op.
    let mut plain = row.clone();
    plain.stored_exprs = None;
    assert!(!s.recompiled(&mut *tx, &plain, 2).await.unwrap());
    assert!(s.recompiled(&mut *tx, &row, 2).await.unwrap());
    let r = stored(&mut *tx, 1).await;
    assert_eq!(r.using_expr.as_deref(), Some("new_a"));
    assert_eq!(r.using_mongo.as_deref(), Some(r#"{"a":1}"#));
    assert_eq!(r.changed_version, 2);
    // Run again (another gateway): the row no longer holds the old text.
    assert!(!s.recompiled(&mut *tx, &row, 3).await.unwrap());
    // A form changed since the load: no write.
    s.alter(&mut *tx, ObjectId(1), "p", None, Some("other"), None, 4)
        .await
        .unwrap();
    row.stored_exprs = Some((Some("other".into()), None));
    assert!(!s.recompiled(&mut *tx, &row, 5).await.unwrap());
    assert_eq!(stored(&mut *tx, 1).await.changed_version, 4);
    // A deleted policy: no write.
    assert!(s.delete(&mut *tx, ObjectId(1), "p").await.unwrap());
    assert!(!s.recompiled(&mut *tx, &row, 6).await.unwrap());
}

#[tokio::test]
async fn rename_delete_and_forget() {
    let recs = MemRecords::new();
    let mut tx = recs.begin(T).await.unwrap();
    let s = RlsStore::new();
    for (o, n) in [(1, "a"), (1, "b"), (2, "a")] {
        assert!(s.insert(&mut *tx, &new_policy(o, n, &[]), 1).await.unwrap());
    }
    let o1 = ObjectId(1);
    assert_eq!(s.rename(&mut *tx, o1, "x", "y", 2).await.unwrap(), None);
    assert_eq!(
        s.rename(&mut *tx, o1, "a", "b", 2).await.unwrap(),
        Some(false)
    );
    // A name another table uses is free here.
    assert_eq!(
        s.rename(&mut *tx, ObjectId(2), "a", "b", 2).await.unwrap(),
        Some(true)
    );
    assert_eq!(
        s.rename(&mut *tx, o1, "a", "c", 2).await.unwrap(),
        Some(true)
    );
    // To its own name at the version it has: no row changed.
    assert_eq!(s.rename(&mut *tx, o1, "c", "c", 2).await.unwrap(), None);
    assert_eq!(
        s.rename(&mut *tx, o1, "c", "c", 3).await.unwrap(),
        Some(true)
    );
    let r = stored(&mut *tx, 1).await;
    assert_eq!((r.name.as_str(), r.changed_version), ("c", 3));
    assert!(s.delete(&mut *tx, o1, "c").await.unwrap());
    assert!(!s.delete(&mut *tx, o1, "c").await.unwrap());
    // forget_objects: the policies and the switches of those objects only.
    s.touch(&mut *tx, o1, DbId(1), 4, Some((Some(true), None)))
        .await
        .unwrap();
    s.touch(&mut *tx, ObjectId(2), DbId(1), 4, None)
        .await
        .unwrap();
    s.forget_objects(&mut *tx, &[o1, ObjectId(99)])
        .await
        .unwrap();
    let rows = s.load(&mut *tx).await.unwrap();
    assert_eq!(
        rows.policies
            .iter()
            .map(|p| (p.object_id.0, p.name.as_str()))
            .collect::<Vec<_>>(),
        vec![(2, "b")]
    );
    assert_eq!(
        rows.security
            .iter()
            .map(|r| r.object_id)
            .collect::<Vec<_>>(),
        vec![ObjectId(2)]
    );
}

#[tokio::test]
async fn switches_upsert_and_checks_only_move_forward() {
    let recs = MemRecords::new();
    let mut tx = recs.begin(T).await.unwrap();
    let s = RlsStore::new();
    let o = ObjectId(5);
    // Checks of an object without a row: nothing.
    s.checks_deployed(&mut *tx, o, 3).await.unwrap();
    assert!(s.load(&mut *tx).await.unwrap().security.is_empty());
    s.touch(&mut *tx, o, DbId(1), 2, Some((Some(true), None)))
        .await
        .unwrap();
    let row = |rows: RlsRows| rows.security[0];
    let r = row(s.load(&mut *tx).await.unwrap());
    assert_eq!(
        r,
        SecurityRow {
            object_id: o,
            db_id: DbId(1),
            enabled: true,
            forced: false,
            checks_version: 0,
            changed_version: 2,
        }
    );
    assert!(!r.checks_current());
    // A policy change: the switches and the database stay.
    s.touch(&mut *tx, o, DbId(9), 3, None).await.unwrap();
    s.touch(&mut *tx, o, DbId(9), 4, Some((None, Some(true))))
        .await
        .unwrap();
    let r = row(s.load(&mut *tx).await.unwrap());
    assert_eq!((r.db_id, r.enabled, r.forced), (DbId(1), true, true));
    assert_eq!(r.changed_version, 4);
    s.checks_deployed(&mut *tx, o, 4).await.unwrap();
    s.checks_deployed(&mut *tx, o, 3).await.unwrap();
    let r = row(s.load(&mut *tx).await.unwrap());
    assert_eq!(r.checks_version, 4);
    assert!(r.checks_current());
    s.touch(&mut *tx, o, DbId(1), 5, Some((Some(false), Some(false))))
        .await
        .unwrap();
    let r = row(s.load(&mut *tx).await.unwrap());
    assert_eq!((r.enabled, r.forced, r.checks_version), (false, false, 4));
    // The state built from the records.
    tx.commit().await.unwrap();
    let mut tx = recs.begin(T).await.unwrap();
    let st = load_state(&mut *tx).await.unwrap();
    assert!(st.is_migrated());
    assert_eq!(st.security(o).map(|r| r.changed_version), Some(5));
    assert!(!st.any_enabled());
}
