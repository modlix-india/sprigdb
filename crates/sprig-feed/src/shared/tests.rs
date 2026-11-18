#![allow(clippy::unwrap_used, clippy::many_single_char_names)]

use std::sync::atomic::{AtomicU64, AtomicUsize};

use sprig_binlog::{RowImage, Value};

use super::*;
use crate::source::{RawDdl, RawRow, RowKind, TableShape};

fn g(seq: u64) -> Gtid {
    Gtid {
        domain: 0,
        server: 1,
        seq,
    }
}

fn at(seq: u64) -> GtidPos {
    GtidPos::from_gtids([g(seq)])
}

fn row_in(schema: &str, table: &str) -> RawRow {
    RawRow {
        table: Arc::new(TableShape {
            schema: schema.into(),
            table: table.into(),
            columns: vec!["out_id".into()],
            key: vec![0],
        }),
        kind: RowKind::Insert,
        before: None,
        after: Some(RowImage(vec![Some(Value::Int(1))])),
        hint: None,
    }
}

fn row(schema: &str) -> RawRow {
    row_in(schema, "__sprig_qo")
}

fn txn(seq: u64, schemas: &[&str]) -> RawTxn {
    let mut t = RawTxn::new(g(seq), at(seq - 1), 0);
    t.rows = schemas.iter().map(|s| row(s)).collect();
    t
}

/// A cluster whose binlog grows while tests run; records every dump's
/// scope.
#[derive(Clone, Default)]
struct Fake {
    groups: Arc<Mutex<Vec<RawTxn>>>,
    grew: Arc<Notify>,
    opens: Arc<Mutex<Vec<DumpScope>>>,
    live: Arc<AtomicUsize>,
    /// Groups up to this one are purged: a dump from before it is refused
    /// (39800), as a node refuses a position its binary logs lack.
    purged: Arc<AtomicU64>,
    /// Dumps refused for it, by scope.
    refused: Arc<Mutex<Vec<DumpScope>>>,
}

impl Fake {
    fn with(groups: Vec<RawTxn>) -> Self {
        let f = Self::default();
        *f.groups.lock().unwrap() = groups;
        f
    }

    fn push(&self, t: RawTxn) {
        self.groups.lock().unwrap().push(t);
        self.grew.notify_waiters();
    }

    fn last(&self) -> u64 {
        self.groups.lock().unwrap().last().map_or(0, |t| t.gtid.seq)
    }

    fn opens(&self) -> Vec<DumpScope> {
        self.opens.lock().unwrap().clone()
    }

    fn shared_opens(&self) -> usize {
        self.opens().iter().filter(|s| s.only.is_none()).count()
    }

    fn catchup_opens(&self) -> usize {
        self.opens().iter().filter(|s| s.only.is_some()).count()
    }
}

struct FakeSrc {
    fake: Fake,
    scope: Arc<DumpScope>,
    pos: GtidPos,
    _live: LiveGuard,
}

struct LiveGuard(Arc<AtomicUsize>);

impl Drop for LiveGuard {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::SeqCst);
    }
}

/// What a dump with `scope` keeps of `t` (as `BinlogTxns` does).
fn cut(t: &RawTxn, scope: &DumpScope) -> Option<RawTxn> {
    let mut out = t.clone();
    out.rows
        .retain(|r| scope.keeps(&r.table.schema, &r.table.table));
    out.ddl = t.ddl.clone().filter(|d| scope.names_whole(&d.db, &d.sql));
    let empty = out.rows.is_empty() && out.ddl.is_none() && out.xa.is_none();
    (!empty).then_some(out)
}

#[async_trait::async_trait]
impl TxnSource for FakeSrc {
    async fn next(&mut self) -> Result<SourceItem> {
        loop {
            let grew = self.fake.grew.notified();
            let next = {
                let groups = self.fake.groups.lock().unwrap();
                groups.iter().find(|t| after(&self.pos, &t.gtid)).cloned()
            };
            match next {
                Some(t) => {
                    self.pos.update(t.gtid);
                    if let Some(c) = cut(&t, &self.scope) {
                        return Ok(SourceItem::Txn(c));
                    }
                }
                None => {
                    tokio::select! {
                        () = grew => {}
                        () = tokio::time::sleep(Duration::from_millis(200)) => {
                            return Ok(SourceItem::Idle { pos: self.pos.clone() });
                        }
                    }
                }
            }
        }
    }
}

#[async_trait::async_trait]
impl Clusters for Fake {
    async fn open(&self, _: &ClusterId, _: TenantId, _: &GtidPos) -> Result<Box<dyn TxnSource>> {
        unreachable!("the shared reader opens cluster-wide sources only")
    }

    async fn open_scope(
        &self,
        _: &ClusterId,
        scope: Arc<DumpScope>,
        from: &GtidPos,
    ) -> Result<Box<dyn TxnSource>> {
        let purged = self.purged.load(Ordering::SeqCst);
        if from.get(0).map_or(0, |g| g.seq) < purged {
            self.refused.lock().unwrap().push((*scope).clone());
            return Err(SprigError::new(
                SprigCode::FeedHistoryLost,
                format!("group {purged} and those before it are purged; dump from {from}"),
            ));
        }
        self.opens.lock().unwrap().push((*scope).clone());
        self.live.fetch_add(1, Ordering::SeqCst);
        Ok(Box::new(FakeSrc {
            fake: self.clone(),
            scope,
            pos: from.clone(),
            _live: LiveGuard(Arc::clone(&self.live)),
        }))
    }

    async fn position(&self, _: &ClusterId) -> Result<GtidPos> {
        Ok(at(self.last()))
    }
}

fn cluster() -> ClusterId {
    ClusterId::new("dc-a-c1").unwrap()
}

async fn take(src: &mut Box<dyn TxnSource>, n: usize) -> Vec<(u64, usize)> {
    let mut out = Vec::new();
    while out.len() < n {
        match tokio::time::timeout(Duration::from_secs(5), src.next())
            .await
            .expect("no item within 5 s")
            .unwrap()
        {
            SourceItem::Txn(t) => out.push((t.gtid.seq, t.rows.len())),
            SourceItem::Idle { .. } => {}
        }
    }
    out
}

async fn quiet(src: &mut Box<dyn TxnSource>, ms: u64) -> bool {
    tokio::time::timeout(Duration::from_millis(ms), async {
        loop {
            if let SourceItem::Txn(t) = src.next().await.unwrap() {
                return t.gtid.seq;
            }
        }
    })
    .await
    .is_err()
}

#[test]
fn groups_split_by_tenant_and_xa_follows_its_prepare() {
    let mut xa = HashMap::new();
    let t = txn(
        5,
        &["t7_d1_g1_s1", "t8_d2_g1_s1", "t7_d1_g1_s2", "t9_sprig"],
    );
    let parts = split(&t, &mut xa);
    let shape: Vec<(u64, usize)> = parts.iter().map(|(t, x)| (t.0, x.rows.len())).collect();
    // A catalog's rows go to its tenant; a Kept::Tables view drops them.
    assert_eq!(shape, vec![(7, 2), (8, 1), (9, 1)]);
    let tables = Kept::Tables(vec!["__sprig_qo".to_owned()].into());
    assert!(view(&parts[2].1, &tables).is_none());
    assert_eq!(view(&parts[2].1, &Kept::All).unwrap().rows.len(), 1);
    let mut p = txn(6, &["t7_d1_g1_s1"]);
    p.xa = Some(XaMark::Prepare("x1".into()));
    assert_eq!(split(&p, &mut xa).len(), 1);
    let mut c = txn(7, &[]);
    c.xa = Some(XaMark::Commit("x1".into()));
    let parts = split(&c, &mut xa);
    assert_eq!(parts.len(), 1);
    assert_eq!(parts[0].0, TenantId(7));
    assert!(xa.is_empty());
    // DDL goes to every tenant it names; statement DML to its database's.
    let mut d = txn(8, &[]);
    d.ddl = Some(RawDdl {
        db: "t3_d1_g1_s1".into(),
        sql: "RENAME TABLE a TO t4_d1_g1_s1.b".into(),
    });
    d.statement_dml = Some("t3_d1_g1_s1".into());
    let parts = split(&d, &mut xa);
    let who: Vec<(u64, bool, bool)> = parts
        .iter()
        .map(|(t, x)| (t.0, x.ddl.is_some(), x.statement_dml.is_some()))
        .collect();
    assert_eq!(who, vec![(3, true, true), (4, true, false)]);
    assert!(view(&parts[0].1, &tables).is_none());
    let a = at(5);
    let b = at(3);
    assert_eq!(older(&a, &b), b);
    assert_eq!(newer(&a, &b), a);
}

#[tokio::test]
async fn many_tenants_share_one_dump_and_late_starters_read_the_ring() {
    let fake = Fake::with(vec![
        txn(1, &["t1_d1_g1_s1"]),
        txn(2, &["t2_d1_g1_s1", "t1_d1_g1_s1"]),
        txn(3, &["t2_d1_g1_s1"]),
        txn(4, &["t1_d1_g1_s1"]),
    ]);
    let feed = SharedFeed::new(Arc::new(fake.clone()), SharedOpts::default());
    let shared = feed.view(Kept::Tables(vec!["__sprig_qo".to_owned()].into()));
    let c = cluster();
    // Tenant 1 from group 1 on; tenant 2 joins later from group 2 on.
    let mut s1 = shared.open(&c, TenantId(1), &at(0)).await.unwrap();
    assert_eq!(take(&mut s1, 3).await, vec![(1, 1), (2, 1), (4, 1)]);
    let mut s2 = shared.open(&c, TenantId(2), &at(1)).await.unwrap();
    // Tenant 2 started behind the hub: the ring serves it, and tenant 1
    // sees nothing twice.
    assert_eq!(take(&mut s2, 2).await, vec![(2, 1), (3, 1)]);
    assert!(quiet(&mut s1, 300).await, "tenant 1 got a group twice");
    assert_eq!(fake.shared_opens(), 1, "one dump, no rewind");
    assert_eq!(fake.catchup_opens(), 0);
    let st = feed.stats(&c).unwrap();
    assert_eq!((st.subscriptions, st.tenants, st.running), (2, 2, true));
    // 1,000 more tenants add subscriptions, not dumps.
    let mut subs = Vec::new();
    for t in 10..1_010 {
        subs.push(shared.open(&c, TenantId(t), &at(4)).await.unwrap());
    }
    assert_eq!(feed.stats(&c).unwrap().subscriptions, 1_002);
    assert_eq!(fake.shared_opens(), 1);
    // A new group for one of them reaches it alone.
    fake.push(txn(5, &["t500_d1_g1_s1"]));
    assert_eq!(take(&mut subs[490], 1).await, vec![(5, 1)]);
    assert!(quiet(&mut subs[0], 200).await);
    drop(subs);
}

#[tokio::test]
async fn a_slow_subscription_is_detached_and_comes_back_without_stalling_the_others() {
    let fake = Fake::with(vec![txn(1, &["t1_d1_g1_s1"])]);
    let opts = SharedOpts {
        sub_channel: 4,
        ..SharedOpts::default()
    };
    let feed = SharedFeed::new(Arc::new(fake.clone()), opts);
    let shared = feed.view(Kept::Tables(vec!["__sprig_qo".to_owned()].into()));
    let c = cluster();
    let mut fast = shared.open(&c, TenantId(1), &at(0)).await.unwrap();
    let mut slow = shared.open(&c, TenantId(2), &at(0)).await.unwrap();
    assert_eq!(take(&mut fast, 1).await, vec![(1, 1)]);
    // 100 groups for both: the slow one reads nothing meanwhile.
    for seq in 2..102 {
        fake.push(txn(seq, &["t1_d1_g1_s1", "t2_d1_g1_s1"]));
    }
    let got = take(&mut fast, 100).await;
    assert_eq!(got.first(), Some(&(2, 1)));
    assert_eq!(got.last(), Some(&(101, 1)));
    // The slow one now reads everything, in order, once.
    let got = take(&mut slow, 100).await;
    let seqs: Vec<u64> = got.iter().map(|x| x.0).collect();
    assert_eq!(seqs, (2..102).collect::<Vec<_>>());
    assert!(quiet(&mut slow, 300).await);
    assert_eq!(fake.shared_opens(), 1);
    assert_eq!(fake.catchup_opens(), 0, "the ring served the slow one");
}

/// A subscription that comes back further behind than
/// `max_backlog` but inside the ring reads its groups from the ring in
/// parts, while the hub reads on, and listens once it is close: no
/// catch-up dump, every group once and in order.
#[tokio::test]
async fn a_long_return_inside_the_ring_comes_in_parts_without_a_catch_up_dump() {
    let fake = Fake::with(vec![txn(1, &["t1_d1_g1_s1"])]);
    let opts = SharedOpts {
        sub_channel: 4,
        max_backlog: 10,
        ..SharedOpts::default()
    };
    let feed = SharedFeed::new(Arc::new(fake.clone()), opts);
    let shared = feed.view(Kept::Tables(vec!["__sprig_qo".to_owned()].into()));
    let c = cluster();
    let mut fast = shared.open(&c, TenantId(1), &at(0)).await.unwrap();
    let mut slow = shared.open(&c, TenantId(2), &at(0)).await.unwrap();
    assert_eq!(take(&mut fast, 1).await, vec![(1, 1)]);
    for seq in 2..102 {
        fake.push(txn(seq, &["t1_d1_g1_s1", "t2_d1_g1_s1"]));
    }
    assert_eq!(take(&mut fast, 100).await.len(), 100);
    // The slow one is detached, 96 groups behind (10 at a time); more
    // groups arrive while it reads.
    let mut seqs: Vec<u64> = take(&mut slow, 30).await.iter().map(|x| x.0).collect();
    for seq in 102..152 {
        fake.push(txn(seq, &["t1_d1_g1_s1", "t2_d1_g1_s1"]));
    }
    seqs.extend(take(&mut slow, 120).await.iter().map(|x| x.0));
    assert_eq!(seqs, (2..152).collect::<Vec<_>>());
    assert!(quiet(&mut slow, 300).await);
    assert_eq!(take(&mut fast, 50).await.len(), 50);
    assert_eq!(fake.shared_opens(), 1);
    assert_eq!(fake.catchup_opens(), 0, "the ring served it in parts");
    assert_eq!(
        feed.stats(&c).unwrap().subscriptions,
        2,
        "both listen again"
    );
}

#[tokio::test]
async fn behind_the_ring_a_catch_up_dump_reads_then_hands_over() {
    let fake = Fake::with((1..=50).map(|s| txn(s, &["t1_d1_g1_s1"])).collect());
    let opts = SharedOpts {
        ring_groups: 5,
        gather: Duration::ZERO,
        ..SharedOpts::default()
    };
    let feed = SharedFeed::new(Arc::new(fake.clone()), opts);
    let shared = feed.view(Kept::Tables(vec!["__sprig_qo".to_owned()].into()));
    let c = cluster();
    let mut a = shared.open(&c, TenantId(1), &at(0)).await.unwrap();
    assert_eq!(take(&mut a, 50).await.len(), 50);
    // A second subscription of the same tenant from group 10: the ring
    // holds 46..50 only.
    let mut b = shared.open(&c, TenantId(1), &at(10)).await.unwrap();
    let seqs: Vec<u64> = take(&mut b, 40).await.iter().map(|x| x.0).collect();
    assert_eq!(seqs, (11..=50).collect::<Vec<_>>());
    assert_eq!(fake.catchup_opens(), 1);
    assert_eq!(fake.opens()[1].only, Some(TenantId(1)));
    // New groups reach both through the hub; the catch-up dump closed.
    for s in 51..=60 {
        fake.push(txn(s, &["t1_d1_g1_s1"]));
    }
    let seqs: Vec<u64> = take(&mut b, 10).await.iter().map(|x| x.0).collect();
    assert_eq!(seqs, (51..=60).collect::<Vec<_>>());
    assert_eq!(take(&mut a, 10).await.len(), 10);
    // A consumer keeps reading: at the next check the hub takes over.
    assert!(quiet(&mut b, 500).await);
    assert_eq!(
        fake.live.load(Ordering::SeqCst),
        1,
        "only the hub's dump is open"
    );
    assert_eq!(feed.stats(&c).unwrap().catchups, 0);
}

#[tokio::test]
async fn subscriptions_arriving_together_move_a_gathering_hub_back() {
    let fake = Fake::with(
        (1..=20)
            .map(|s| txn(s, &["t1_d1_g1_s1", "t2_d1_g1_s1"]))
            .collect(),
    );
    let feed = SharedFeed::new(Arc::new(fake.clone()), SharedOpts::default());
    let shared = feed.view(Kept::Tables(vec!["__sprig_qo".to_owned()].into()));
    let c = cluster();
    // A worker restarts: each sequencer comes back from its own checkpoint.
    let mut a = shared.open(&c, TenantId(1), &at(15)).await.unwrap();
    let mut b = shared.open(&c, TenantId(2), &at(5)).await.unwrap();
    let sa: Vec<u64> = take(&mut a, 5).await.iter().map(|x| x.0).collect();
    let sb: Vec<u64> = take(&mut b, 15).await.iter().map(|x| x.0).collect();
    assert_eq!(sa, (16..=20).collect::<Vec<_>>());
    assert_eq!(sb, (6..=20).collect::<Vec<_>>());
    assert!(quiet(&mut a, 300).await);
    assert_eq!(fake.catchup_opens(), 0);
    assert!(fake.shared_opens() <= 2);
}

#[tokio::test]
async fn a_whole_tenant_view_gets_its_tables_and_ddl_and_widens_the_dump() {
    let mut g1 = txn(1, &["t1_d1_g1_s1"]);
    g1.rows.push(row_in("t1_d1_g1_s1", "orders"));
    g1.rows.push(row_in("t2_d1_g1_s1", "orders"));
    let mut g2 = RawTxn::new(g(2), at(1), 0);
    g2.ddl = Some(RawDdl {
        db: "t1_d1_g1_s1".into(),
        sql: "ALTER TABLE orders ADD c INT".into(),
    });
    let fake = Fake::with(vec![g1, g2]);
    let feed = SharedFeed::new(Arc::new(fake.clone()), SharedOpts::default());
    let outbox = feed.view(Kept::Tables(vec!["__sprig_qo".to_owned()].into()));
    let all = feed.view(Kept::All);
    let c = cluster();
    let mut q = outbox.open(&c, TenantId(1), &at(0)).await.unwrap();
    assert_eq!(take(&mut q, 1).await, vec![(1, 1)]);
    assert!(fake.opens()[0].tenants.is_empty());
    let mut w = all.open(&c, TenantId(1), &at(0)).await.unwrap();
    // Rows of every table of tenant 1 (not tenant 2's), then its DDL.
    let mut seen = Vec::new();
    while seen.len() < 2 {
        if let SourceItem::Txn(t) = tokio::time::timeout(Duration::from_secs(5), w.next())
            .await
            .unwrap()
            .unwrap()
        {
            seen.push((t.gtid.seq, t.rows.len(), t.ddl.is_some()));
        }
    }
    assert_eq!(seen, vec![(1, 2, false), (2, 0, true)]);
    // The hub reopens with tenant 1 kept whole, from where `w` started
    // (a replay, not a catch-up dump).
    assert!(quiet(&mut w, 500).await);
    let scopes = fake.opens();
    assert!(
        scopes
            .iter()
            .any(|s| s.only.is_none() && s.tenants.contains(&TenantId(1))),
        "the hub's dump keeps tenant 1 whole: {scopes:?}"
    );
    assert_eq!(
        fake.live.load(Ordering::SeqCst),
        1,
        "only the hub's dump is open"
    );
    // The outbox view still sees its row once, and no DDL.
    assert!(quiet(&mut q, 300).await);
    // A new group with rows of both kinds reaches each view as it keeps.
    let mut g3 = txn(3, &["t1_d1_g1_s1"]);
    g3.rows.push(row_in("t1_d1_g1_s1", "orders"));
    fake.push(g3);
    assert_eq!(take(&mut w, 1).await, vec![(3, 2)]);
    assert_eq!(take(&mut q, 1).await, vec![(3, 1)]);
}

/// Regression: a subscription registered behind a
/// gathering hub, waiting for it to move back, gets no idle position ahead
/// of the groups it hasn't had (its tail would skip them).
#[test]
fn no_idle_position_while_a_move_back_is_pending() {
    let mut st = HubState::default();
    let (tx_a, mut rx_a) = mpsc::channel(4);
    let (tx_b, mut rx_b) = mpsc::channel(4);
    st.register(
        TenantId(1),
        &Who {
            kept: Kept::All,
            idle_every: IDLE_FANOUT,
            consumer: UNNAMED,
        },
        at(5),
        tx_a,
        Arc::default(),
    );
    // B started at 0 behind the hub at 5: registered at 0, move back asked.
    st.register(
        TenantId(2),
        &Who {
            kept: Kept::All,
            idle_every: IDLE_FANOUT,
            consumer: UNNAMED,
        },
        at(0),
        tx_b,
        Arc::default(),
    );
    st.rewind = Some(at(0));
    st.idle_fanout(&at(5));
    assert!(rx_a.try_recv().is_err() && rx_b.try_recv().is_err());
    // Once the hub has moved back, it reads 1..5 again and sends positions.
    st.rewind = None;
    st.idle_fanout(&at(0));
    assert!(rx_b.try_recv().is_ok(), "B is at 0: position 0 is fine");
    assert!(rx_a.try_recv().is_err(), "A is at 5: nothing before it");
}

/// A new whole-tenant subscription a little behind the hub (it
/// read the cluster's position, or its checkpoint, just before the hub
/// read on) makes the hub read those groups again with the wider scope,
/// instead of opening a catch-up dump. Nobody gets a group twice, and the
/// ring keeps each group once.
#[tokio::test]
async fn a_new_whole_tenant_just_behind_the_hub_is_replayed_without_a_catch_up_dump() {
    let groups: Vec<RawTxn> = (1..=10)
        .map(|s| {
            let mut t = txn(s, &["t1_d1_g1_s1", "t3_d1_g1_s1"]);
            t.rows.push(row_in("t2_d1_g1_s1", "orders"));
            t
        })
        .collect();
    let fake = Fake::with(groups);
    let opts = SharedOpts {
        gather: Duration::ZERO,
        ..SharedOpts::default()
    };
    let feed = SharedFeed::new(Arc::new(fake.clone()), opts);
    let outbox = feed.view(Kept::Tables(vec!["__sprig_qo".to_owned()].into()));
    let all = feed.view(Kept::All);
    let c = cluster();
    let mut q = outbox.open(&c, TenantId(1), &at(0)).await.unwrap();
    assert_eq!(take(&mut q, 10).await.len(), 10);
    // Tenant 2's rows were never kept; it starts at 5, behind the hub.
    let mut w = all.open(&c, TenantId(2), &at(5)).await.unwrap();
    let seqs: Vec<u64> = take(&mut w, 5).await.iter().map(|x| x.0).collect();
    assert_eq!(seqs, (6..=10).collect::<Vec<_>>());
    assert!(quiet(&mut w, 300).await, "tenant 2 got a group twice");
    assert!(quiet(&mut q, 300).await, "tenant 1 got a group twice");
    assert_eq!(fake.catchup_opens(), 0, "no catch-up dump");
    let last = fake.opens().last().cloned().unwrap();
    assert!(last.only.is_none() && last.tenants.contains(&TenantId(2)));
    assert_eq!(fake.live.load(Ordering::SeqCst), 1, "one dump");
    // The ring kept each group once: tenant 3 from 2 reads 3..10 once.
    let mut r = outbox.open(&c, TenantId(3), &at(2)).await.unwrap();
    let seqs: Vec<u64> = take(&mut r, 8).await.iter().map(|x| x.0).collect();
    assert_eq!(seqs, (3..=10).collect::<Vec<_>>());
    assert!(quiet(&mut r, 300).await);
    assert_eq!(fake.catchup_opens(), 0);
    // New groups reach every view once.
    let mut g11 = txn(11, &["t1_d1_g1_s1", "t3_d1_g1_s1"]);
    g11.rows.push(row_in("t2_d1_g1_s1", "orders"));
    fake.push(g11);
    assert_eq!(take(&mut w, 1).await, vec![(11, 1)]);
    assert_eq!(take(&mut q, 1).await, vec![(11, 1)]);
    assert_eq!(take(&mut r, 1).await, vec![(11, 1)]);
    assert_eq!(feed.stats(&c).unwrap().catchups, 0);
}

/// Change streams resuming together after a gateway's restart,
/// each a little behind the hub, before the ring's start too: replays
/// chain (one under way is extended), and each subscription gets every
/// group of its tenant once, in order, with no catch-up dump.
#[tokio::test]
async fn replays_chain_for_whole_tenants_resuming_behind_the_hub() {
    let groups: Vec<RawTxn> = (1..=30)
        .map(|s| {
            let mut t = txn(s, &["t1_d1_g1_s1"]);
            for tenant in [2, 3, 4] {
                t.rows
                    .push(row_in(&format!("t{tenant}_d1_g1_s1"), "orders"));
            }
            t
        })
        .collect();
    let fake = Fake::with(groups);
    let opts = SharedOpts {
        gather: Duration::ZERO,
        ..SharedOpts::default()
    };
    let feed = SharedFeed::new(Arc::new(fake.clone()), opts);
    let outbox = feed.view(Kept::Tables(vec!["__sprig_qo".to_owned()].into()));
    let all = feed.view(Kept::All);
    let c = cluster();
    // The hub starts at 10 (its ring starts there) and reads to 30.
    let mut q = outbox.open(&c, TenantId(1), &at(10)).await.unwrap();
    assert_eq!(take(&mut q, 20).await.len(), 20);
    // Three streams resume at once, behind the hub, two before its ring.
    let mut w2 = all.open(&c, TenantId(2), &at(4)).await.unwrap();
    let mut w3 = all.open(&c, TenantId(3), &at(25)).await.unwrap();
    let mut w4 = all.open(&c, TenantId(4), &at(0)).await.unwrap();
    for (w, from) in [(&mut w2, 4), (&mut w3, 25), (&mut w4, 0)] {
        let seqs: Vec<u64> = take(w, 30 - from).await.iter().map(|x| x.0).collect();
        assert_eq!(seqs, ((from as u64 + 1)..=30).collect::<Vec<_>>());
        assert!(quiet(w, 200).await, "a group twice");
    }
    assert!(quiet(&mut q, 200).await, "tenant 1 got a group twice");
    assert_eq!(fake.catchup_opens(), 0, "no catch-up dump");
    // New groups reach everyone once.
    let mut g31 = txn(31, &["t1_d1_g1_s1"]);
    for tenant in [2, 3, 4] {
        g31.rows
            .push(row_in(&format!("t{tenant}_d1_g1_s1"), "orders"));
    }
    fake.push(g31);
    for w in [&mut q, &mut w2, &mut w3, &mut w4] {
        assert_eq!(take(w, 1).await, vec![(31, 1)]);
    }
    // A Tables subscription from before the ring still takes a catch-up
    // dump: the replay didn't put groups before the ring into it.
    let mut late = outbox.open(&c, TenantId(1), &at(2)).await.unwrap();
    let seqs: Vec<u64> = take(&mut late, 29).await.iter().map(|x| x.0).collect();
    assert_eq!(seqs, (3..=31).collect::<Vec<_>>());
    assert_eq!(fake.catchup_opens(), 1);
}

/// A small deterministic generator for the stress test below.
struct Lcg(u64);

impl Lcg {
    fn next(&mut self, n: u64) -> u64 {
        self.0 = self
            .0
            .wrapping_mul(6_364_136_223_846_793_005)
            .wrapping_add(1_442_695_040_888_963_407);
        (self.0 >> 33) % n.max(1)
    }
}

/// Whole-tenant subscriptions start at random points a little
/// behind a busy hub (replays, chained), read slowly (detached, back
/// through the ring or a catch-up dump), with a small ring. Each gets every
/// group of its tenant after its start once, in order.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn whole_tenants_starting_behind_a_busy_hub_get_every_group_once() {
    for seed in 1..=8u64 {
        let fake = Fake::with(Vec::new());
        let opts = SharedOpts {
            sub_channel: 3,
            ring_groups: 25,
            max_catchup: 2,
            gather: Duration::ZERO,
            ..SharedOpts::default()
        };
        let feed = SharedFeed::new(Arc::new(fake.clone()), opts);
        let all = feed.view(Kept::All);
        let c = cluster();
        let total = 240u64;
        let pusher = {
            let fake = fake.clone();
            tokio::spawn(async move {
                let mut r = Lcg(seed);
                for s in 1..=total {
                    let mut t = RawTxn::new(g(s), at(s - 1), 0);
                    for tenant in 1..=6 {
                        if r.next(3) == 0 {
                            t.rows
                                .push(row_in(&format!("t{tenant}_d1_g1_s1"), "orders"));
                        }
                    }
                    if t.rows.is_empty() {
                        t.rows.push(row_in("t9_d1_g1_s1", "orders"));
                    }
                    fake.push(t);
                    if r.next(4) == 0 {
                        tokio::time::sleep(Duration::from_millis(1)).await;
                    }
                }
            })
        };
        let mut readers = Vec::new();
        let mut r = Lcg(seed * 7919);
        for i in 0..14u64 {
            tokio::time::sleep(Duration::from_millis(r.next(15))).await;
            let tenant = 1 + r.next(6);
            let last = fake.last();
            let from = last.saturating_sub(r.next(30));
            let mut src = all.open(&c, TenantId(tenant), &at(from)).await.unwrap();
            let slow = r.next(3) == 0;
            readers.push(tokio::spawn(async move {
                let mut got = Vec::new();
                // Idle positions keep coming: stop 2 s after the last group.
                let mut quiet_since = tokio::time::Instant::now();
                loop {
                    if quiet_since.elapsed() > Duration::from_secs(2) {
                        return (i, tenant, from, got);
                    }
                    match tokio::time::timeout(Duration::from_secs(3), src.next()).await {
                        Ok(Ok(SourceItem::Txn(t))) => {
                            quiet_since = tokio::time::Instant::now();
                            got.push(t.gtid.seq);
                            if slow && t.gtid.seq % 5 == i % 5 {
                                tokio::time::sleep(Duration::from_millis(3)).await;
                            }
                        }
                        Ok(Ok(SourceItem::Idle { .. })) => {}
                        Ok(Err(e)) => panic!("reader {i}: {e}"),
                        Err(_) => return (i, tenant, from, got),
                    }
                }
            }));
        }
        pusher.await.unwrap();
        let groups = fake.groups.lock().unwrap().clone();
        for h in readers {
            let (i, tenant, from, got) = h.await.unwrap();
            let schema = format!("t{tenant}_d1_g1_s1");
            let want: Vec<u64> = groups
                .iter()
                .filter(|t| t.gtid.seq > from && t.rows.iter().any(|r| r.table.schema == schema))
                .map(|t| t.gtid.seq)
                .collect();
            assert_eq!(
                got, want,
                "seed {seed}, reader {i} (tenant {tenant} from {from})"
            );
        }
    }
}

/// A replay fills the new tenant's parts into the ring, so its
/// subscription, detached later as a slow reader, comes back through the
/// ring, not a catch-up dump.
#[tokio::test]
async fn a_replayed_tenant_detached_later_returns_through_the_ring() {
    let mk = |s: u64| {
        let mut t = txn(s, &["t1_d1_g1_s1"]);
        t.rows.push(row_in("t2_d1_g1_s1", "orders"));
        t
    };
    let fake = Fake::with((1..=10).map(mk).collect());
    let opts = SharedOpts {
        sub_channel: 2,
        gather: Duration::ZERO,
        ..SharedOpts::default()
    };
    let feed = SharedFeed::new(Arc::new(fake.clone()), opts);
    let outbox = feed.view(Kept::Tables(vec!["__sprig_qo".to_owned()].into()));
    let all = feed.view(Kept::All);
    let c = cluster();
    let mut q = outbox.open(&c, TenantId(1), &at(0)).await.unwrap();
    assert_eq!(take(&mut q, 10).await.len(), 10);
    // Tenant 2 starts at 5 (a replay), reads nothing while 10 more come:
    // its buffer of 2 fills and it is detached.
    let mut w = all.open(&c, TenantId(2), &at(5)).await.unwrap();
    tokio::time::sleep(Duration::from_millis(300)).await;
    for s in 11..=20 {
        fake.push(mk(s));
    }
    assert_eq!(take(&mut q, 10).await.len(), 10);
    let seqs: Vec<u64> = take(&mut w, 15).await.iter().map(|x| x.0).collect();
    assert_eq!(seqs, (6..=20).collect::<Vec<_>>());
    assert!(quiet(&mut w, 300).await);
    assert_eq!(fake.catchup_opens(), 0, "the ring served the return");
}

/// Idle positions a subscription gets within `dur`.
async fn idles(src: &mut Box<dyn TxnSource>, dur: Duration) -> Vec<GtidPos> {
    let mut out = Vec::new();
    let end = Instant::now() + dur;
    while let Ok(i) = tokio::time::timeout_at(end, src.next()).await {
        if let SourceItem::Idle { pos } = i.unwrap() {
            out.push(pos);
        }
    }
    out
}

/// A view may ask for idle positions sooner than every second
/// (the graph mirror's: a mirror read waits for it to reach the cluster's
/// position now, so others' writes mustn't hold it a second behind); the
/// other views keep one a second.
#[tokio::test(start_paused = true)]
async fn a_view_gets_idle_positions_as_often_as_it_asks() {
    let fake = Fake::default();
    let feed = SharedFeed::new(Arc::new(fake.clone()), SharedOpts::default());
    let fast = feed.view_every(Kept::All, IDLE_TICK);
    let slow = feed.view(Kept::All);
    // Tenant 2 writes a group every 50 ms for 3 s; tenants 1 and 3 never.
    let mut other = slow.open(&cluster(), TenantId(2), &at(0)).await.unwrap();
    let mut f = fast.open(&cluster(), TenantId(1), &at(0)).await.unwrap();
    let mut s = slow.open(&cluster(), TenantId(3), &at(0)).await.unwrap();
    let w = fake.clone();
    let writer = tokio::spawn(async move {
        for seq in 1..=60 {
            w.push(txn(seq, &["t2_d1_g1_s1"]));
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    });
    let span = Duration::from_millis(3_000);
    let (got, fi, si) = tokio::join!(
        take(&mut other, 60),
        idles(&mut f, span),
        idles(&mut s, span)
    );
    writer.await.unwrap();
    assert_eq!(got.len(), 60);
    assert!(
        fi.len() >= 10,
        "the fast view got {} idle positions",
        fi.len()
    );
    assert!(
        si.len() <= 4,
        "the default view got {} idle positions",
        si.len()
    );
    // Each position only grows, and the fast view's last is within a
    // tick or two of the writer's last group.
    assert!(fi.windows(2).all(|p| p[1].reached(&p[0])));
    let last = fi.last().unwrap().get(0).unwrap().seq;
    assert!(last >= 54, "the fast view's last position is {last}");
}

async fn seqs(src: &mut Box<dyn TxnSource>, n: usize) -> Vec<u64> {
    take(src, n).await.iter().map(|x| x.0).collect()
}

/// The first `Err` a source returns within 5 s, reading past anything
/// else.
async fn fails(src: &mut Box<dyn TxnSource>) -> SprigError {
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            if let Err(e) = src.next().await {
                return e;
            }
        }
    })
    .await
    .expect("no error within 5 s")
}

/// A worker's restart where one sequencer's checkpoint lies in purged
/// binlogs: the gathering hub moves back for it, finds that history lost,
/// and reads on instead of failing everyone. The lost subscription alone
/// gets 39800; the others catch up and join the hub, each group once, and
/// once caught up they share the one dump again. A retry of the lost one
/// never moves the hub back again.
#[tokio::test]
async fn a_lost_subscription_fails_alone_while_the_others_read_on() {
    let tenants = ["t1_d1_g1_s1", "t2_d1_g1_s1", "t3_d1_g1_s1"];
    let fake = Fake::with((1..=20).map(|s| txn(s, &tenants)).collect());
    fake.purged.store(5, Ordering::SeqCst);
    let feed = SharedFeed::new(Arc::new(fake.clone()), SharedOpts::default());
    let shared = feed.view(Kept::Tables(vec!["__sprig_qo".to_owned()].into()));
    let c = cluster();
    let mut a = shared.open(&c, TenantId(1), &at(15)).await.unwrap();
    let mut b = shared.open(&c, TenantId(2), &at(10)).await.unwrap();
    let mut lost = shared.open(&c, TenantId(3), &at(2)).await.unwrap();
    assert_eq!(seqs(&mut a, 5).await, (16..=20).collect::<Vec<_>>());
    assert_eq!(seqs(&mut b, 10).await, (11..=20).collect::<Vec<_>>());
    let e = fails(&mut lost).await;
    assert_eq!(e.code(), Some(SprigCode::FeedHistoryLost), "{e}");
    // Its retry takes a catch-up dump again (refused), not the hub back.
    let mut again = shared.open(&c, TenantId(3), &at(2)).await.unwrap();
    let e = fails(&mut again).await;
    assert_eq!(e.code(), Some(SprigCode::FeedHistoryLost), "{e}");
    let refused = fake.refused.lock().unwrap().clone();
    assert!(
        refused.iter().filter(|s| s.only.is_none()).count() <= 1,
        "the hub tried the lost position once: {refused:?}"
    );
    assert!(refused.iter().any(|s| s.only == Some(TenantId(3))));
    // New groups reach the others once, through the one shared dump.
    for s in 21..=25 {
        fake.push(txn(s, &tenants));
    }
    assert_eq!(seqs(&mut a, 5).await, (21..=25).collect::<Vec<_>>());
    assert_eq!(seqs(&mut b, 5).await, (21..=25).collect::<Vec<_>>());
    assert!(quiet(&mut a, 300).await, "tenant 1 got a group twice");
    assert!(quiet(&mut b, 300).await, "tenant 2 got a group twice");
    assert_eq!(
        fake.live.load(Ordering::SeqCst),
        1,
        "only the hub's dump is open"
    );
    assert_eq!(feed.stats(&c).unwrap().catchups, 0);
    assert_eq!(feed.stats(&c).unwrap().subscriptions, 2);
}

/// A new whole tenant just behind a running hub whose start is purged: the
/// replay it would get is refused, the hub goes back to where it was
/// reading, and the subscriptions there never notice; the new one alone
/// fails.
#[tokio::test]
async fn a_replay_into_lost_history_fails_only_its_subscription() {
    let groups: Vec<RawTxn> = (1..=10)
        .map(|s| {
            let mut t = txn(s, &["t1_d1_g1_s1"]);
            t.rows.push(row_in("t2_d1_g1_s1", "orders"));
            t
        })
        .collect();
    let fake = Fake::with(groups);
    let opts = SharedOpts {
        gather: Duration::ZERO,
        ..SharedOpts::default()
    };
    let feed = SharedFeed::new(Arc::new(fake.clone()), opts);
    let outbox = feed.view(Kept::Tables(vec!["__sprig_qo".to_owned()].into()));
    let all = feed.view(Kept::All);
    let c = cluster();
    let mut q = outbox.open(&c, TenantId(1), &at(0)).await.unwrap();
    assert_eq!(seqs(&mut q, 10).await, (1..=10).collect::<Vec<_>>());
    fake.purged.store(6, Ordering::SeqCst);
    let mut w = all.open(&c, TenantId(2), &at(3)).await.unwrap();
    let e = fails(&mut w).await;
    assert_eq!(e.code(), Some(SprigCode::FeedHistoryLost), "{e}");
    for s in 11..=13 {
        fake.push(txn(s, &["t1_d1_g1_s1"]));
    }
    assert_eq!(seqs(&mut q, 3).await, vec![11, 12, 13]);
    assert!(quiet(&mut q, 300).await, "tenant 1 got a group twice");
    assert_eq!(fake.live.load(Ordering::SeqCst), 1);
}

/// A subscription behind the hub that can be served: its catch-up dump
/// reads up to the hub and hands over with nothing skipped and nothing
/// twice, while groups keep arriving.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_catch_up_joins_the_hub_without_gaps_or_duplicates_under_load() {
    let fake = Fake::with(
        (1..=200)
            .map(|s| txn(s, &["t1_d1_g1_s1", "t2_d1_g1_s1"]))
            .collect(),
    );
    let opts = SharedOpts {
        ring_groups: 10,
        gather: Duration::ZERO,
        ..SharedOpts::default()
    };
    let feed = SharedFeed::new(Arc::new(fake.clone()), opts);
    let shared = feed.view(Kept::Tables(vec!["__sprig_qo".to_owned()].into()));
    let c = cluster();
    let mut a = shared.open(&c, TenantId(1), &at(0)).await.unwrap();
    assert_eq!(seqs(&mut a, 200).await.len(), 200);
    let f2 = fake.clone();
    let writer = tokio::spawn(async move {
        for s in 201..=600 {
            f2.push(txn(s, &["t1_d1_g1_s1", "t2_d1_g1_s1"]));
            if s % 20 == 0 {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        }
    });
    let mut b = shared.open(&c, TenantId(2), &at(50)).await.unwrap();
    let got = seqs(&mut b, 550).await;
    writer.await.unwrap();
    assert_eq!(got, (51..=600).collect::<Vec<_>>());
    assert!(quiet(&mut b, 400).await, "tenant 2 got a group twice");
    // Tenant 1, not read meanwhile, was detached and catches up too.
    assert_eq!(seqs(&mut a, 400).await, (201..=600).collect::<Vec<_>>());
    assert!(quiet(&mut a, 400).await, "tenant 1 got a group twice");
    assert!(fake.catchup_opens() >= 1);
    assert_eq!(feed.stats(&c).unwrap().catchups, 0);
    assert_eq!(
        fake.live.load(Ordering::SeqCst),
        1,
        "only the hub's dump is open"
    );
}

/// A group too large to hold reaches each tenant with only the tables of
/// its own it touched, and each view with the tables it keeps.
#[test]
fn an_overflow_is_split_by_tenant_and_cut_by_view() {
    let shape = |s: &str, t: &str| row_in(s, t).table;
    let mut t = RawTxn::new(g(1), at(0), 0);
    t.overflow = Some(Box::new(Overflow {
        tables: vec![
            shape("t1_d1_g1_s1", "orders"),
            shape("t1_d1_g1_s1", "__sprig_qo"),
            shape("t2_d1_g1_s1", "orders"),
        ],
        rows: 3_000_000,
        bytes: 900 << 20,
    }));
    let parts = split(&t, &mut HashMap::new());
    let tables = |p: &RawTxn| -> Vec<String> {
        p.overflow
            .as_ref()
            .map(|o| o.tables.iter().map(|t| t.table.clone()).collect())
            .unwrap_or_default()
    };
    let p1 = &parts.iter().find(|(x, _)| *x == TenantId(1)).unwrap().1;
    let p2 = &parts.iter().find(|(x, _)| *x == TenantId(2)).unwrap().1;
    assert_eq!(tables(p1), vec!["orders", "__sprig_qo"]);
    assert_eq!(tables(p2), vec!["orders"]);
    assert!(p1.rows.is_empty());
    let outbox = Kept::Tables(vec!["__sprig_qo".to_owned()].into());
    assert_eq!(tables(&view(p1, &outbox).unwrap()), vec!["__sprig_qo"]);
    assert!(
        view(p2, &outbox).is_none(),
        "tenant 2's outbox wasn't touched"
    );
    assert_eq!(tables(&view(p2, &Kept::All).unwrap()), vec!["orders"]);
}

fn big_txn(seq: u64, schema: &str, bytes: usize) -> RawTxn {
    let mut t = RawTxn::new(g(seq), at(seq - 1), 0);
    let mut r = row(schema);
    r.after = Some(RowImage(vec![Some(Value::String(vec![b'x'; bytes]))]));
    t.rows.push(r);
    t
}

/// A subscription that doesn't read holds at most its byte budget of
/// groups, however many its channel could take: past it, it is detached
/// (the hub goes on), and it comes back through the ring with every group
/// once and in order.
#[tokio::test]
async fn a_slow_subscription_holds_at_most_its_bytes() {
    let fake = Fake::with(vec![txn(1, &["t1_d1_g1_s1"])]);
    let opts = SharedOpts {
        sub_channel: 1_000,
        buffer_bytes: 20_000,
        ..SharedOpts::default()
    };
    let feed = SharedFeed::new(Arc::new(fake.clone()), opts);
    let shared = feed.view(Kept::Tables(vec!["__sprig_qo".to_owned()].into()));
    let c = cluster();
    let mut fast = shared.open(&c, TenantId(1), &at(0)).await.unwrap();
    let mut slow = shared.open(&c, TenantId(2), &at(0)).await.unwrap();
    assert_eq!(take(&mut fast, 1).await, vec![(1, 1)]);
    // 100 groups of about 1 KB for the slow one (100 KB in all, five
    // times its budget), each followed by one for the fast one.
    for i in 0..100 {
        fake.push(big_txn(2 + 2 * i, "t2_d1_g1_s1", 1_000));
        fake.push(txn(3 + 2 * i, &["t1_d1_g1_s1"]));
    }
    let got = take(&mut fast, 100).await;
    assert_eq!(got.last(), Some(&(201, 1)), "the hub never waited");
    let st = feed.stats(&c).unwrap();
    assert_eq!(st.subscriptions, 1, "the slow one was detached by bytes");
    let got = take(&mut slow, 100).await;
    let seqs: Vec<u64> = got.iter().map(|x| x.0).collect();
    assert_eq!(seqs, (0..100).map(|i| 2 + 2 * i).collect::<Vec<_>>());
    assert!(quiet(&mut slow, 300).await);
}
