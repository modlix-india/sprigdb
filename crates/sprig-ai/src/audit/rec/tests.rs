//! The records-backed stores over in-memory records.

use std::sync::Arc;

use serde_json::json;
use sprig_catalog::records::RecordStore;
use sprig_catalog::records::mem::MemRecords;
use sprig_common::ids::{TenantId, UserId};

use super::*;

fn usage(i: u64, o: u64) -> Usage {
    Usage {
        input_tokens: i,
        output_tokens: o,
        cache_read_tokens: 0,
        cache_write_tokens: 0,
    }
}

fn store() -> (Arc<dyn RecordStore>, RecAudit) {
    let r: Arc<dyn RecordStore> = Arc::new(MemRecords::new());
    let a = RecAudit::tenant(Arc::clone(&r), TenantId(7));
    (r, a)
}

#[tokio::test]
async fn conversations_belong_to_their_user_and_list_newest_first() {
    let (_, a) = store();
    let (ann, bob) = (UserId(1), UserId(2));
    let c1 = a.create(ann, Role::Data, "  first  ").await.unwrap();
    let c2 = a.create(ann, Role::Dba, "").await.unwrap();
    let c3 = a.create(bob, Role::Data, "bob's").await.unwrap();
    assert!(a.create(ann, Role::Admin, "x").await.is_err());
    assert_eq!(a.get(ann, c1).await.unwrap().unwrap().title, "first");
    assert_eq!(a.get(ann, c2).await.unwrap().unwrap().title, "Untitled");
    assert!(a.get(ann, c3).await.unwrap().is_none());
    // A message moves a conversation to the top.
    a.append(c1, MsgKind::User, &json!({"text": "hi"}), None)
        .await
        .unwrap();
    let l = a.list(ann, 10).await.unwrap();
    assert_eq!(l.iter().map(|c| c.id).collect::<Vec<_>>().len(), 2);
    assert!(l.iter().all(|c| c.user == 1));
    assert_eq!(a.list(ann, 1).await.unwrap().len(), 1);
    assert_eq!(a.list(bob, 10).await.unwrap()[0].id, c3);
}

#[tokio::test]
async fn messages_keep_their_order_bodies_and_hashes() {
    let (_, a) = store();
    let c = a.create(UserId(1), Role::Data, "q").await.unwrap();
    let h = [7u8; 32];
    let m1 = a
        .append(c, MsgKind::User, &json!({"text": "orders?"}), None)
        .await
        .unwrap();
    let m2 = a
        .append(
            c,
            MsgKind::SystemNote,
            &json!({"sha256": "x", "payload": {"a": 1}}),
            Some(h),
        )
        .await
        .unwrap();
    assert!(m2 > m1);
    let ms = a.messages(c).await.unwrap();
    assert_eq!(ms.len(), 2);
    assert_eq!(ms[0].kind, MsgKind::User);
    assert_eq!(ms[1].sent_hash, Some(h));
    assert_eq!(ms[1].body["payload"]["a"], 1);
    // Payloads go after their days, hashes stay.
    assert_eq!(a.purge_payloads(1).await.unwrap(), 0);
    tokio::time::sleep(std::time::Duration::from_millis(5)).await;
    assert_eq!(a.purge_payloads(0).await.unwrap(), 1);
    let ms = a.messages(c).await.unwrap();
    assert_eq!(ms[1].body["payload_purged"], true);
    assert_eq!(ms[1].body["sha256"], hex(&h));
    assert_eq!(ms[1].sent_hash, Some(h));
    assert_eq!(a.purge_payloads(0).await.unwrap(), 0);
}

#[tokio::test]
async fn usage_adds_up_per_user_and_for_the_tenant_functions_included() {
    let (r, a) = store();
    a.add_usage(UserId(1), Role::Data, &usage(100, 10))
        .await
        .unwrap();
    a.add_usage(UserId(1), Role::Data, &usage(5, 5))
        .await
        .unwrap();
    a.add_usage(UserId(2), Role::Dba, &usage(50, 0))
        .await
        .unwrap();
    a.add_usage_as(UserId(1), "functions", &usage(1, 1))
        .await
        .unwrap();
    let u = a.usage_today(UserId(1)).await.unwrap();
    assert_eq!((u.tenant, u.user), (172, 122));
    // Another tenant's records are its own.
    let other = RecAudit::tenant(r, TenantId(8));
    assert_eq!(other.usage_today(UserId(1)).await.unwrap().tenant, 0);
}

#[tokio::test]
async fn feedback_and_outcomes_are_the_owners() {
    let (_, a) = store();
    let (ann, bob) = (UserId(1), UserId(2));
    let c = a.create(ann, Role::Dba, "q").await.unwrap();
    let ans = a
        .append(
            c,
            MsgKind::Assistant,
            &json!({"role": "assistant", "content": []}),
            None,
        )
        .await
        .unwrap();
    a.append(
        c,
        MsgKind::Proposal,
        &json!({"id": "p-1", "kind": "sql", "body": {"sql": "CREATE INDEX i ON t (a)", "why": "faster lookups"}}),
        None,
    )
    .await
    .unwrap();
    assert_eq!(a.vote(bob, ans, 1, None).await.unwrap(), None);
    assert_eq!(
        a.vote(ann, ans, -1, Some(" too long ")).await.unwrap(),
        Some(Role::Dba)
    );
    assert!(a.proposal(bob, "p-1").await.unwrap().is_none());
    let f = a.proposal(ann, "p-1").await.unwrap().unwrap();
    assert_eq!((f.kind(), f.decided), ("sql", None));
    assert_eq!(
        a.outcome(ann, "p-1", Outcome::Accepted, None)
            .await
            .unwrap(),
        Some((Role::Dba, "sql".to_owned()))
    );
    assert_eq!(
        a.outcome(ann, "p-2", Outcome::Accepted, None)
            .await
            .unwrap(),
        None
    );
    let f = a.proposal(ann, "p-1").await.unwrap().unwrap();
    assert_eq!(f.decided, Some(Outcome::Accepted));
    let notes = a.recent(10).await.unwrap();
    assert_eq!(notes.len(), 2);
    assert_eq!(notes[0].outcome, Some(Outcome::Accepted));
    assert_eq!(notes[0].label.as_deref(), Some("faster lookups"));
    assert_eq!(notes[1].note.as_deref(), Some("too long"));
}

#[tokio::test]
async fn the_system_store_is_one_operators_and_keeps_admin_roles() {
    let r: Arc<dyn RecordStore> = Arc::new(MemRecords::new());
    let alice = RecAudit::system(Arc::clone(&r), " Acme.Alice ");
    let bob = RecAudit::system(Arc::clone(&r), "acme.bob");
    assert_eq!(alice.operator(), Some("acme.alice"));
    assert!(alice.create(UserId(0), Role::Data, "x").await.is_err());
    let c = alice.create(UserId(9), Role::Admin, "disk").await.unwrap();
    // The user id is ignored: the login decides.
    assert!(alice.get(UserId(1), c).await.unwrap().is_some());
    assert!(bob.get(UserId(9), c).await.unwrap().is_none());
    assert!(bob.list(UserId(9), 10).await.unwrap().is_empty());
    alice
        .add_usage(UserId(0), Role::Admin, &usage(10, 0))
        .await
        .unwrap();
    bob.add_usage(UserId(0), Role::Triage, &usage(5, 0))
        .await
        .unwrap();
    let u = bob.usage_today(UserId(3)).await.unwrap();
    assert_eq!((u.tenant, u.user), (15, 5));
    // A tenant's store never sees the system records.
    let t = RecAudit::tenant(r, TenantId(7));
    assert_eq!(t.usage_today(UserId(0)).await.unwrap().tenant, 0);
}

#[tokio::test]
async fn an_operators_admin_conversations_are_theirs_alone() {
    let r: Arc<dyn RecordStore> = Arc::new(MemRecords::new());
    let alice = RecAudit::system(Arc::clone(&r), "acme.alice");
    let c = alice.create(UserId(0), Role::Admin, "disk").await.unwrap();
    let ans = alice
        .append(
            c,
            MsgKind::Assistant,
            &json!({"role": "assistant", "content": []}),
            None,
        )
        .await
        .unwrap();
    alice
        .append(
            c,
            MsgKind::Proposal,
            &json!({"id": "p-a", "kind": "recommendation", "body": {"why": "x"}}),
            None,
        )
        .await
        .unwrap();
    // Another operator: no listing, no reading, no rating, no outcome.
    let bob = RecAudit::system(Arc::clone(&r), "acme.bob");
    assert!(bob.get(UserId(0), c).await.unwrap().is_none());
    assert!(bob.list(UserId(0), 100).await.unwrap().is_empty());
    assert_eq!(bob.vote(UserId(0), ans, 1, None).await.unwrap(), None);
    assert!(bob.proposal(UserId(0), "p-a").await.unwrap().is_none());
    assert_eq!(
        bob.outcome(UserId(0), "p-a", Outcome::Accepted, None)
            .await
            .unwrap(),
        None
    );
    // A login that differs only in case is the same operator (logins are
    // case-insensitive), any other login is not.
    assert!(
        RecAudit::system(Arc::clone(&r), "ACME.ALICE")
            .get(UserId(0), c)
            .await
            .unwrap()
            .is_some()
    );
    assert!(
        RecAudit::system(Arc::clone(&r), "acme.alice2")
            .get(UserId(0), c)
            .await
            .unwrap()
            .is_none()
    );
    // A tenant's store, whatever its tenant id (0 included) and user, never
    // sees the administration conversations: another module.
    for t in [0u64, 7, 38_306_000_000] {
        let ts = RecAudit::tenant(Arc::clone(&r), TenantId(t));
        for u in [0u32, 1] {
            assert!(
                ts.get(UserId(u), c).await.unwrap().is_none(),
                "tenant {t} user {u}"
            );
            assert!(ts.list(UserId(u), 100).await.unwrap().is_empty());
            assert!(ts.messages(c).await.unwrap().is_empty());
            assert_eq!(ts.vote(UserId(u), ans, 1, None).await.unwrap(), None);
            assert!(ts.proposal(UserId(u), "p-a").await.unwrap().is_none());
        }
    }
    // And the other way round: an operator's store sees no tenant's.
    let ts = RecAudit::tenant(Arc::clone(&r), TenantId(0));
    let tc = ts.create(UserId(1), Role::Data, "t").await.unwrap();
    assert!(
        alice
            .list(UserId(1), 100)
            .await
            .unwrap()
            .iter()
            .all(|x| x.id != tc || x.role == Role::Admin)
    );
    assert_eq!(alice.list(UserId(1), 100).await.unwrap().len(), 1);
}

#[tokio::test]
async fn the_list_is_newest_first_and_reads_only_what_it_returns() {
    let (_, a) = store();
    let ann = UserId(1);
    let mut ids = Vec::new();
    for i in 0..30 {
        ids.push(a.create(ann, Role::Data, &format!("c{i}")).await.unwrap());
    }
    // The oldest conversation gets a message: it moves to the top.
    tokio::time::sleep(std::time::Duration::from_millis(3)).await;
    a.append(ids[0], MsgKind::User, &json!({"text": "again"}), None)
        .await
        .unwrap();
    let top = a.list(ann, 5).await.unwrap();
    assert_eq!(top.len(), 5);
    assert_eq!(top[0].id, ids[0]);
    // Then the rest, newest (highest id on equal times) first.
    assert!(
        top.windows(2)
            .all(|w| (w[0].updated_ms, w[0].id.0) >= (w[1].updated_ms, w[1].id.0))
    );
    let all = a.list(ann, 100).await.unwrap();
    assert_eq!(all.len(), 30, "each conversation once, after its move");
    assert!(a.list(UserId(2), 100).await.unwrap().is_empty());
}

#[tokio::test]
async fn a_purge_goes_on_until_every_old_payload_is_gone() {
    let (_, a) = store();
    let c = a.create(UserId(1), Role::Data, "q").await.unwrap();
    let n = super::MAX_PURGE + 7;
    for _ in 0..n {
        a.append(
            c,
            MsgKind::SystemNote,
            &json!({"sha256": "x", "payload": {"a": 1}}),
            Some([3; 32]),
        )
        .await
        .unwrap();
    }
    assert!(a.has_payloads().await.unwrap());
    tokio::time::sleep(std::time::Duration::from_millis(5)).await;
    assert_eq!(a.purge_payloads(0).await.unwrap(), n as u64);
    assert!(!a.has_payloads().await.unwrap());
    assert!(
        a.messages(c)
            .await
            .unwrap()
            .iter()
            .all(|m| m.body.get("payload").is_none())
    );
}

#[tokio::test]
async fn recent_feedback_reads_only_the_newest_rows() {
    let (_, a) = store();
    let u = UserId(1);
    let c = a.create(u, Role::Data, "q").await.unwrap();
    let ans = a
        .append(
            c,
            MsgKind::Assistant,
            &json!({"role": "assistant", "content": []}),
            None,
        )
        .await
        .unwrap();
    assert!(a.recent(10).await.unwrap().is_empty());
    a.vote(u, ans, -1, Some("the oldest note")).await.unwrap();
    for _ in 0..super::MAX_FEEDBACK_SCAN {
        a.vote(u, ans, 1, None).await.unwrap();
    }
    // The oldest note is past the newest MAX_FEEDBACK_SCAN rows.
    assert!(a.recent(10).await.unwrap().is_empty());
    a.vote(u, ans, -1, Some("a new note")).await.unwrap();
    let notes = a.recent(10).await.unwrap();
    assert_eq!(notes.len(), 1);
    assert_eq!(notes[0].note.as_deref(), Some("a new note"));
}
