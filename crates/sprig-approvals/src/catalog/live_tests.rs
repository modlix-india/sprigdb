//! The catalog store on the in-memory record store: raises, refreshes,
//! concurrent raises, ignoring and suppression, versions, the due index,
//! filters and subscriptions.

#![allow(
    clippy::unwrap_used,
    clippy::too_many_lines,
    clippy::many_single_char_names,
    clippy::similar_names
)]

use std::sync::Arc;
use std::time::Duration;

use sprig_common::ids::{DbId, ObjectId, TenantId};

use super::*;
use crate::machine::{Act, TickChange};
use crate::model::{Action, Kind, Level, Mode, Subject};
use crate::store::Filter;

fn p(cols: &str, score: f64) -> Proposal {
    Proposal {
        kind: Kind::IndexCreate,
        subject: Subject {
            tenant: Some(TenantId(7)),
            db: Some(DbId(1)),
            object: Some(ObjectId(3)),
            ..Subject::default()
        },
        target: "t7/o3".into(),
        action: Action {
            text: format!("ALTER TABLE shop.orders ADD INDEX ({cols})"),
            machine: serde_json::json!({ "cols": cols }),
        },
        evidence: serde_json::json!({ "gain_ms_h": score }),
        score,
        summary: "an index".into(),
        emergency: false,
    }
}

#[tokio::test]
async fn catalog_store_round_trip() {
    let records: Arc<dyn RecordStore> = Arc::new(sprig_catalog::records::mem::MemRecords::new());
    let store = Arc::new(CatalogRecStore::new(Arc::clone(&records), TenantId(7)));
    let r = async move {
        let mut pol = Policy::fixed(Mode::Approve, Level::Table);
        pol.escalate_after = Some(Duration::from_secs(60));
        let a = store
            .raise(&p("a", 10.0), &pol, &[TenantId(1)], 1_000)
            .await
            .unwrap();
        let Verdict::Wait(id) = a.verdict.clone() else {
            panic!("{a:?}")
        };
        assert_eq!(id, RecId::Tenant(TenantId(7), 1));
        // Refresh: the same row.
        let b = store
            .raise(&p("a", 12.0), &pol, &[TenantId(1)], 2_000)
            .await
            .unwrap();
        assert_eq!(b.verdict, Verdict::Wait(id.clone()));
        let got = store.get(1).await.unwrap().unwrap();
        assert_eq!(got.refreshes, 1);
        assert_eq!(got.id, id);
        // Concurrent raises of new advice for another target: one row.
        let q = |n: u32| {
            let mut x = p("z", 5.0);
            x.target = "t7/o9".into();
            let store = &*store;
            let pol = &pol;
            async move { store.raise(&x, pol, &[], 3_000 + i64::from(n)).await }
        };
        let (r1, r2, r3) = tokio::join!(q(1), q(2), q(3));
        for r in [r1, r2, r3] {
            assert!(matches!(r.unwrap().verdict, Verdict::Wait(_)));
        }
        let live = store.list(&Filter::default()).await.unwrap();
        assert_eq!(live.len(), 2, "{live:?}");
        // Ignore, suppression, approval by version, the due index.
        let (ign, _) = crate::store::modify(&*store, &id, |r| {
            crate::machine::act(
                r,
                &Act::Ignore {
                    reason: "no".into(),
                    for_days: Some(2),
                    forever: false,
                },
                "ana",
                1,
                2.0,
                5_000,
            )
            .map(Some)
        })
        .await
        .unwrap();
        assert_eq!(ign.state, State::Ignored);
        let s = store.raise(&p("a", 15.0), &pol, &[], 6_000).await.unwrap();
        assert_eq!(s.verdict, Verdict::Suppressed(id.clone()));
        let s = store.raise(&p("a", 25.0), &pol, &[], 7_000).await.unwrap();
        let Verdict::Wait(id2) = s.verdict else {
            panic!()
        };
        assert_ne!(id2, id);
        let due = store.due(7_000 + 61_000_000, 10).await.unwrap();
        assert!(due.iter().any(|r| r.id == id2), "{due:?}");
        let mut r = due.into_iter().find(|r| r.id == id2).unwrap();
        let v = r.version;
        assert!(matches!(
            crate::machine::tick(&mut r, 7_000 + 61_000_000, Duration::from_secs(3600)),
            Some(TickChange::Escalated(_))
        ));
        assert!(store.put(&r, v).await.unwrap());
        assert!(!store.put(&r, v).await.unwrap(), "stale version refused");
        let f = Filter {
            states: vec![State::Ignored],
            ..Filter::default()
        };
        assert_eq!(store.list(&f).await.unwrap().len(), 1);
        let f = Filter {
            kinds: vec!["index".into()],
            all: true,
            ..Filter::default()
        };
        assert_eq!(store.list(&f).await.unwrap().len(), 3);
        // Subscriptions.
        let sub = Subscription {
            user: "ana".into(),
            email: "ana@example.com".into(),
            delivery: "immediate".into(),
            kinds: "*".into(),
            last_digest_us: None,
            updated_at_us: None,
        };
        store.set_subscription(&sub, 1).await.unwrap();
        store.digest_sent("ana", 9).await.unwrap();
        store.set_subscription(&sub, 10).await.unwrap();
        let subs = store.subscriptions().await.unwrap();
        assert_eq!(subs.len(), 1);
        assert_eq!(
            (subs[0].last_digest_us, subs[0].updated_at_us),
            (Some(9), Some(10))
        );
        // Closed items go after their time; a forever-ignored one stays.
        let n = store.purge(i64::MAX).await.unwrap();
        assert!(n >= 1, "the ignored item with an end goes");
        assert!(store.get(1).await.unwrap().is_none());
    };
    tokio::spawn(r).await.unwrap();
}
