//! The sink records on the in-memory record store.

#![allow(clippy::unwrap_used)]

use sprig_catalog::records::mem::MemRecords;

use super::*;
use crate::model::{ConnKind, Position, SinkRecord, TargetSpec};

const T: TenantId = TenantId(57_306_000_003);

fn conn(id: u64, name: &str) -> ConnectionDef {
    ConnectionDef {
        id: ConnectionId(id),
        db_id: 1,
        name: name.into(),
        kind: ConnKind::Webhook,
        options: serde_json::Map::new(),
        hosts: vec!["hooks.example.com".into()],
        residency_tags: None,
        owner_user_id: 7,
        secrets: Vec::new(),
    }
}

fn sink(id: u64, name: &str) -> SinkDef {
    let mut s = crate::probe::stand_in(1, name, TargetSpec::Webhook { path: None });
    s.id = SinkId(id);
    s.state = SinkState::Running;
    s
}

async fn change(
    m: &MemRecords,
    f: impl AsyncFnOnce(&mut dyn RecordTx) -> Result<()>,
) -> Result<()> {
    let mut tx = m.begin(T).await?;
    f(&mut *tx).await?;
    tx.commit().await
}

async fn load(m: &MemRecords) -> SinksState {
    let mut tx = m.begin(T).await.unwrap();
    SinksStore::load(&mut *tx).await.unwrap()
}

#[tokio::test]
async fn statements_follow_the_sql_rules() {
    let m = MemRecords::new();
    change(&m, async |r| {
        SinksStore::insert_connection(r, &conn(10, "hook")).await?;
        let v = SinksStore::next_secret_version(r, ConnectionId(10), "token").await?;
        assert_eq!(v, 1);
        SinksStore::put_secret(r, ConnectionId(10), "token", v, 1, b"sealed-1", 7).await?;
        SinksStore::insert_sink(r, &sink(20, "out")).await?;
        let u = Usage {
            db_id: 1,
            connection: "hook".into(),
            platform: false,
            user_id: 9,
        };
        SinksStore::set_usage(r, &u, true, 7).await?;
        SinksStore::set_usage(r, &u, true, 7).await
    })
    .await
    .unwrap();
    // Names are unique per database (as the unique keys were).
    let e = change(&m, async |r| {
        SinksStore::insert_connection(r, &conn(11, "hook")).await
    })
    .await
    .unwrap_err();
    assert_eq!(e.code(), Some(SprigCode::SinkExists));
    let e = change(&m, async |r| {
        SinksStore::insert_sink(r, &sink(21, "out")).await
    })
    .await
    .unwrap_err();
    assert_eq!(e.code(), Some(SprigCode::SinkExists));

    let st = load(&m).await;
    let c = st.connection(1, "hook").unwrap();
    assert_eq!(c.secrets.len(), 1);
    assert_eq!(
        (c.secrets[0].name.as_str(), c.secrets[0].version),
        ("token", 1)
    );
    assert_eq!(c.secrets[0].set_at.len(), 23);
    assert!(st.has_usage(1, "hook", false, 9));
    assert_eq!(st.usage.len(), 1);
    assert_eq!(st.sink(1, "out").unwrap().state, SinkState::Running);

    // Rotation: current -> previous -> retired (emptied).
    change(&m, async |r| {
        for v in 2..=3 {
            SinksStore::put_secret(r, ConnectionId(10), "token", v, 1, b"sealed-n", 7).await?;
        }
        Ok(())
    })
    .await
    .unwrap();
    let store = SinksStore::new(Arc::new(m.clone()), T);
    let mut s = store.sealed_secrets(ConnectionId(10), 24).await.unwrap();
    s.sort_by_key(|x| x.1);
    assert_eq!(s.len(), 2);
    assert_eq!((s[0].1, s[0].4), (2, true));
    assert_eq!(
        (s[1].1, s[1].4, s[1].3.as_slice()),
        (3, false, b"sealed-n".as_slice())
    );
    // A previous version past its window is no longer handed out.
    assert_eq!(
        store
            .sealed_secrets(ConnectionId(10), 0)
            .await
            .unwrap()
            .len(),
        1
    );

    // ALTER bumps the definition version; PAUSE sets the state.
    change(&m, async |r| {
        let mut s = sink(20, "out");
        s.owner_user_id = 8;
        SinksStore::update_sink(r, &s).await
    })
    .await
    .unwrap();
    store
        .set_state(SinkId(20), SinkState::Paused, Some("by hand"))
        .await
        .unwrap();
    let (state, reason, ver) = store.state_of(SinkId(20)).await.unwrap().unwrap();
    assert_eq!(
        (state, reason.as_deref(), ver),
        (SinkState::Paused, Some("by hand"), 1)
    );

    // Dropping the connection takes its secrets and grants.
    change(&m, async |r| {
        SinksStore::delete_connection(r, &conn(10, "hook")).await
    })
    .await
    .unwrap();
    let st = load(&m).await;
    assert!(st.connection(1, "hook").is_none());
    assert!(st.usage.is_empty());
    assert!(
        store
            .sealed_secrets(ConnectionId(10), 24)
            .await
            .unwrap()
            .is_empty()
    );
}

#[tokio::test]
async fn tasks_states_and_dead_letters() {
    let m = MemRecords::new();
    change(&m, async |r| {
        SinksStore::insert_sink(r, &sink(20, "out")).await
    })
    .await
    .unwrap();
    let store = SinksStore::new(Arc::new(m.clone()), T);
    let part = serde_json::json!({"partitions": [0, 1]});
    let e1 = store
        .claim_task(SinkId(20), TaskNo(0), &part, "w1")
        .await
        .unwrap();
    let e2 = store
        .claim_task(SinkId(20), TaskNo(0), &part, "w2")
        .await
        .unwrap();
    assert_eq!((e1, e2), (TaskEpoch(1), TaskEpoch(2)));
    // A stale epoch writes nothing.
    assert!(
        !store
            .report_task(SinkId(20), TaskNo(0), e1, TaskState::Failed, Some("x"), 5)
            .await
            .unwrap()
    );
    assert!(
        store
            .report_task(
                SinkId(20),
                TaskNo(0),
                e2,
                TaskState::Retrying,
                Some("boom"),
                5
            )
            .await
            .unwrap()
    );
    let t = store.tasks(SinkId(20)).await.unwrap();
    assert_eq!(t.len(), 1);
    assert_eq!(
        (t[0].attempts, t[0].delivered, t[0].worker_id.as_deref()),
        (1, 5, Some("w2"))
    );
    assert_eq!(t[0].last_error.as_deref(), Some("boom"));
    assert_eq!(
        store
            .task_claim(SinkId(20), TaskNo(0))
            .await
            .unwrap()
            .unwrap()
            .2,
        part
    );

    // A task's state change shows in states() and in the next snapshot.
    store
        .set_state(SinkId(20), SinkState::Failing, Some("down"))
        .await
        .unwrap();
    assert_eq!(
        store.states().await.unwrap()[&20],
        (SinkState::Failing, Some("down".to_owned()))
    );
    assert_eq!(
        load(&m).await.sink(1, "out").unwrap().state,
        SinkState::Failing
    );

    // Dead letters: written, listed, marked, replayed once.
    let info = crate::runtime::DeadInfo {
        reason: "403".into(),
        class: "permanent",
        attempts: 3,
    };
    for i in 0..3u8 {
        let rec = SinkRecord {
            idem_key: format!("k{i}"),
            key: Some(vec![0, i]),
            value: Some(vec![255, i, 0]),
            headers: vec![("h".to_owned(), b"v".to_vec())],
            op: None,
            before: None,
            after: None,
            ts_ms: 1000 + i64::from(i),
            pos: Position::Stream {
                stream: 41,
                partition: 2,
                offset: 100 + u64::from(i),
                epoch: 0,
            },
        };
        store.insert_dead(SinkId(20), &rec, &info).await.unwrap();
    }
    let d = store.dead_letters(SinkId(20), 10).await.unwrap();
    assert_eq!(d.len(), 3);
    assert_eq!(d[0].id + 2, d[2].id);
    assert_eq!(d[0].created_at.len(), 19);
    assert_eq!(
        store
            .mark_dead(SinkId(20), &[d[1].id], "replaying")
            .await
            .unwrap(),
        1
    );
    let back = store.take_replays(SinkId(20), 10).await.unwrap();
    assert_eq!(back.len(), 1);
    assert_eq!(back[0].idem_key, "k1");
    assert_eq!(back[0].value.as_deref(), Some([255u8, 1, 0].as_slice()));
    assert!(store.take_replays(SinkId(20), 10).await.unwrap().is_empty());
    assert_eq!(
        store.mark_dead(SinkId(20), &[], "discarded").await.unwrap(),
        2
    );

    // Dropping the sink takes its state and tasks.
    change(&m, async |r| SinksStore::delete_sink(r, SinkId(20)).await)
        .await
        .unwrap();
    store.forget_sink(SinkId(20)).await.unwrap();
    assert!(store.tasks(SinkId(20)).await.unwrap().is_empty());
    assert!(store.sink(SinkId(20)).await.unwrap().is_none());
    assert!(store.states().await.unwrap().is_empty());
}

#[test]
fn times_print_as_mariadb_did() {
    assert_eq!(utc_text(0), "1970-01-01 00:00:00.000");
    assert_eq!(utc_text(1_763_769_600_123), "2025-11-22 00:00:00.123");
    assert_eq!(utc_text(951_782_400_000), "2000-02-29 00:00:00.000");
}

#[tokio::test]
async fn statements_set_state_and_drop_after_their_commit() {
    use crate::sql::{Change, after_commit};
    let m = MemRecords::new();
    change(&m, async |r| SinksStore::insert_sink(r, &sink(30, "s")).await)
        .await
        .unwrap();
    let store = SinksStore::new(Arc::new(m.clone()), T);
    store
        .claim_task(SinkId(30), TaskNo(0), &serde_json::json!({}), "w")
        .await
        .unwrap();
    // A task's failure, then a statement's RESUME committed after it: the
    // statement's write lands last.
    store
        .set_state(SinkId(30), SinkState::Failing, Some("down"))
        .await
        .unwrap();
    after_commit(&store, &Change::State(SinkId(30), SinkState::Running, None))
        .await
        .unwrap();
    assert_eq!(store.states().await.unwrap()[&30].0, SinkState::Running);
    // A drop's change removes the definition; after its commit the task
    // and state rows go too, including a task claimed meanwhile.
    change(&m, async |r| SinksStore::delete_sink(r, SinkId(30)).await)
        .await
        .unwrap();
    store
        .claim_task(SinkId(30), TaskNo(1), &serde_json::json!({}), "late")
        .await
        .unwrap();
    after_commit(&store, &Change::DropSink(sink(30, "s")))
        .await
        .unwrap();
    assert!(store.tasks(SinkId(30)).await.unwrap().is_empty());
    let mut tx = m.begin(T).await.unwrap();
    assert!(STATES.rows::<StateRow>(&mut *tx, &Pk::new()).await.unwrap().is_empty());
}
