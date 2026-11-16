//! A distributed transaction's branch connections go back to the pool, and
//! the next session that gets one sees nothing of the transaction: the
//! clock runs (`@@timestamp`), the lock waits are the server's, no
//! transaction is open. Two MariaDB servers stand for two clusters
//! without XA (Sprig 2PC holds a branch on each); the coordinator works on
//! the gateway's own `BackendSession`s, as a gateway session's does.
//!
//! ```sh
//! export SPRIG_TEST_TXN_POOL=mysql://root:sprig-dev-root@127.0.0.1:38311,mysql://root:sprig-dev-root@127.0.0.1:38321
//! cargo test -p sprig-gateway --test txn_pool_live -- --nocapture
//! ```
//!
//! A server with `wsrep_on` is a Galera cluster (the gateway's Galera
//! checks run too). The test uses schemas `txnpool_live_s1` and
//! `_s2` and drops exactly those.

#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::too_many_lines,
    clippy::missing_panics_doc,
    missing_docs
)]

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::sync::Arc;
use std::time::Duration;

use mysql_async::prelude::Queryable;
use mysql_async::{Opts, OptsBuilder, Pool};
use sprig_auth::accounts::BackendTarget;
use sprig_cluster::record::TopologyKind;
use sprig_common::SprigError;
use sprig_common::ids::{ClusterId, DbId, DcId, GroupId, NodeId, ShardId, TenantId};
use sprig_gateway::backend::settings::BackendSettings;
use sprig_gateway::backend::{
    BackendSession, Backends, ClusterView, FixedSettings, NodeRef, SessionOpts, StaticClusters,
    StmtInfo,
};
use sprig_proto_mysql::client::PacketKind;
use sprig_testkit::skip;
use sprig_testkit::util::TempDir;
use sprig_txn::decision::create_table_sql;
use sprig_txn::mysql::{MyAdmin, MyCluster};
use sprig_txn::{
    BeginOptions, ClusterAdmin, ClusterCaps, DbResult, DtidGen, Intent, NonXaPolicy, ShardRef,
    TxnBackend, TxnConfig, TxnCoordinator, TxnMode,
};

const TENANT: TenantId = TenantId(38_306_000_077);

struct Server {
    id: ClusterId,
    url: String,
    schema: String,
    shard: u32,
}

impl Server {
    fn shard_ref(&self) -> ShardRef {
        ShardRef {
            db: DbId(1),
            group: GroupId(1),
            shard: ShardId(self.shard),
            hash_point: u64::from(self.shard) << 40,
            schema: self.schema.clone(),
        }
    }
}

/// The coordinator's backend: a gateway `BackendSession` per branch, every
/// cluster without XA.
struct Gw {
    backends: Arc<Backends>,
    targets: HashMap<ClusterId, BackendTarget>,
    admin: Arc<MyAdmin>,
}

#[async_trait::async_trait]
impl TxnBackend for Gw {
    type Conn = BackendSession;

    fn caps(&self, _cluster: &ClusterId) -> ClusterCaps {
        ClusterCaps { xa: false }
    }

    async fn checkout(&self, cluster: &ClusterId) -> DbResult<BackendSession> {
        Ok(branch_session(self, cluster))
    }

    fn admin(&self) -> Arc<dyn ClusterAdmin> {
        self.admin.clone()
    }
}

/// A session as the gateway's coordinator checks one out: from the branch
/// pools.
fn branch_session(gw: &Gw, cluster: &ClusterId) -> BackendSession {
    let mut bs = gw
        .backends
        .session(gw.targets[cluster].clone(), SessionOpts::default());
    bs.set_txn_branch();
    bs
}

fn pool(url: &str) -> Pool {
    Pool::new(
        OptsBuilder::from_opts(Opts::from_url(url).unwrap()).pool_opts(
            mysql_async::PoolOpts::default()
                .with_constraints(mysql_async::PoolConstraints::new(0, 2).unwrap())
                .with_inactive_connection_ttl(Duration::from_secs(30)),
        ),
    )
}

/// `SHOW GLOBAL STATUS LIKE 'Connections'`: connections opened since start.
async fn connections(p: &Pool) -> u64 {
    let mut c = p.get_conn().await.unwrap();
    let r: Option<(String, u64)> = c
        .query_first("SHOW GLOBAL STATUS LIKE 'Connections'")
        .await
        .unwrap();
    r.unwrap().1
}

async fn rows(bs: &mut BackendSession, sql: &str) -> Result<Vec<Vec<Option<String>>>, SprigError> {
    bs.query(&StmtInfo::classify(sql.as_bytes()), sql.as_bytes())
        .await?;
    let mut ncols = 0;
    let mut out = Vec::new();
    let mut err = None;
    while let Some(p) = bs.next().await? {
        match p.kind {
            PacketKind::ColumnCount(n) => ncols = usize::try_from(n).unwrap(),
            PacketKind::Row => out.push(
                sprig_proto_mysql::resultset::decode_text_row(&p.payload, ncols)
                    .unwrap()
                    .into_iter()
                    .map(|v| v.map(|b| String::from_utf8_lossy(b).into_owned()))
                    .collect(),
            ),
            PacketKind::Err(e) => err = Some(e),
            _ => {}
        }
    }
    bs.finish().await?;
    assert!(err.is_none(), "{sql}: {err:?}");
    Ok(out)
}

async fn one(bs: &mut BackendSession, sql: &str) -> Vec<Option<String>> {
    rows(bs, sql)
        .await
        .unwrap_or_else(|e| panic!("{sql}: {e}"))
        .remove(0)
}

/// The session state a reused connection must show, as one row: its
/// thread, whether the clock runs, the lock waits against the server's,
/// whether a transaction is open.
const STATE: &str = "SELECT CONNECTION_ID(), \
     ABS(@@session.timestamp - UNIX_TIMESTAMP(SYSDATE(6))) < 1, \
     @@session.innodb_lock_wait_timeout, @@global.innodb_lock_wait_timeout, \
     @@session.lock_wait_timeout, @@global.lock_wait_timeout, \
     @@in_transaction, @@session.autocommit";

/// Checks a connection the branch pool hands out after a transaction (to
/// the next transaction's branch): it is one of the transaction's
/// (`threads`), and carries none of its settings.
async fn check_reused(gw: &Gw, s: &Server, threads: &BTreeSet<String>, what: &str) {
    let mut bs = branch_session(gw, &s.id);
    let r = one(&mut bs, STATE).await;
    let thread = r[0].clone().unwrap();
    println!("{what} on {}: {r:?}", s.id);
    assert!(
        threads.contains(&thread),
        "{what} on {}: thread {thread} isn't one of the transaction's {threads:?}: \
         the branch connection wasn't pooled",
        s.id
    );
    assert_eq!(r[1].as_deref(), Some("1"), "{what}: @@timestamp is fixed");
    assert_eq!(r[2], r[3], "{what}: innodb_lock_wait_timeout left changed");
    assert_eq!(r[4], r[5], "{what}: lock_wait_timeout left changed");
    assert_eq!(r[6].as_deref(), Some("0"), "{what}: a transaction is open");
    assert_eq!(r[7].as_deref(), Some("1"), "{what}: autocommit is off");
}

/// One transfer: takes 1 from the first server's row, gives it to the
/// second's; returns each branch's backend thread.
async fn transfer(
    t: &mut TxnCoordinator<Gw>,
    ss: &[Server],
    commit: bool,
) -> Vec<(ClusterId, String)> {
    t.begin(BeginOptions::default()).unwrap();
    let mut threads = Vec::new();
    for (i, s) in ss.iter().enumerate() {
        let b = t
            .branch(&s.id, &s.shard_ref(), Intent::Write)
            .await
            .unwrap();
        let delta = if i == 0 { -1 } else { 1 };
        rows(
            b.conn_mut(),
            &format!(
                "UPDATE `{}`.acct SET bal = bal + {delta}, at = NOW(6) WHERE id = 1",
                s.schema
            ),
        )
        .await
        .unwrap();
        let id = one(b.conn_mut(), "SELECT CONNECTION_ID()").await;
        threads.push((s.id.clone(), id[0].clone().unwrap()));
    }
    if commit {
        let o = t.commit().await.unwrap();
        assert_eq!(o.mode, TxnMode::Sprig2pc);
        assert_eq!(o.stragglers, 0);
    } else {
        t.rollback().await.unwrap();
    }
    threads
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn branch_connections_return_to_the_pool_clean() {
    let Some(v) = skip::var("SPRIG_TEST_TXN_POOL") else {
        skip::report_missing("SPRIG_TEST_TXN_POOL", "txn_pool_live");
        return;
    };
    let urls = skip::urls(&v);
    assert!(urls.len() >= 2, "two servers");
    let ss: Vec<Server> = urls
        .iter()
        .take(2)
        .enumerate()
        .map(|(i, u)| Server {
            id: ClusterId::new(sprig_testkit::unique_name("txp")).unwrap(),
            url: u.trim_end_matches('/').to_owned(),
            schema: format!("txnpool_live_s{}", i + 1),
            shard: u32::try_from(i + 1).unwrap(),
        })
        .collect();
    let pools: Vec<Pool> = ss.iter().map(|s| pool(&s.url)).collect();

    // Schemas, an account row on each, the coordinator's decision table.
    for (s, p) in ss.iter().zip(&pools) {
        let mut c = p.get_conn().await.unwrap();
        c.query_drop(format!("DROP DATABASE IF EXISTS `{}`", s.schema))
            .await
            .unwrap();
        c.query_drop(format!("CREATE DATABASE `{}`", s.schema))
            .await
            .unwrap();
        c.query_drop(format!(
            "CREATE TABLE `{}`.acct (id INT PRIMARY KEY, bal BIGINT NOT NULL, \
             at DATETIME(6) NULL) ENGINE=InnoDB",
            s.schema
        ))
        .await
        .unwrap();
        c.query_drop(format!(
            "INSERT INTO `{}`.acct (id, bal) VALUES (1, 1000)",
            s.schema
        ))
        .await
        .unwrap();
        c.query_drop(create_table_sql(&s.schema)).await.unwrap();
    }

    // The gateway's backends over the two servers, one cluster each.
    let dir = TempDir::new("sprig-txp").unwrap();
    let clusters = StaticClusters::new();
    let mut targets = HashMap::new();
    let mut my = BTreeMap::new();
    for (s, p) in ss.iter().zip(&pools) {
        let galera: Option<u8> = p
            .get_conn()
            .await
            .unwrap()
            .query_first("SELECT @@wsrep_on")
            .await
            .unwrap_or(None);
        let topology = if galera == Some(1) {
            TopologyKind::Galera
        } else {
            TopologyKind::SingleNode
        };
        let rest = s.url.strip_prefix("mysql://").unwrap();
        let (cred, addr) = rest.rsplit_once('@').unwrap();
        let (user, pass) = cred.split_once(':').unwrap();
        clusters.insert(ClusterView::new(
            s.id.clone(),
            topology,
            vec![NodeRef {
                node: NodeId::new("n1").unwrap(),
                addr: addr.to_owned(),
            }],
        ));
        let file = dir.join(format!("{}.toml", s.id));
        std::fs::write(
            &file,
            format!("user = \"{user}\"\npassword = \"{pass}\"\ntls = \"skip_verify\"\n"),
        )
        .unwrap();
        targets.insert(
            s.id.clone(),
            BackendTarget {
                cluster: s.id.clone(),
                user: user.to_owned(),
                credentials: format!("file:{}", file.display()).parse().unwrap(),
            },
        );
        let opts = Opts::from_url(&s.url).unwrap();
        my.insert(
            s.id.clone(),
            MyCluster {
                opts: opts.clone(),
                admin_opts: opts,
                caps: ClusterCaps { xa: false },
            },
        );
        println!("{}: {addr}, {topology:?}", s.id);
    }
    let backends = Backends::new(
        DcId::new("dc-a").unwrap(),
        clusters,
        FixedSettings::new(BackendSettings::default()),
    );
    backends.start();
    let gw = Arc::new(Gw {
        backends: Arc::clone(&backends),
        targets,
        admin: Arc::new(MyAdmin::new(&my)),
    });
    let mut t = TxnCoordinator::new(
        Arc::clone(&gw),
        Arc::new(DtidGen::new(77)),
        TENANT,
        TxnConfig {
            policy: NonXaPolicy::Sprig2pc,
            coordinator_id: "txn-pool-live".into(),
            ..TxnConfig::default()
        },
    );

    // 1. A committed transfer: each branch held a fixed clock and a lowered
    //    lock wait. A little later, the next session on each cluster gets
    //    the same connection back, with neither.
    let first = transfer(&mut t, &ss, true).await;
    tokio::time::sleep(Duration::from_millis(1500)).await;
    for s in &ss {
        let mine: BTreeSet<String> = first
            .iter()
            .filter(|(c, _)| c == &s.id)
            .map(|(_, t)| t.clone())
            .collect();
        check_reused(&gw, s, &mine, "after a commit").await;
    }

    // 2. A rolled-back transfer: the same.
    let rolled = transfer(&mut t, &ss, false).await;
    tokio::time::sleep(Duration::from_millis(1500)).await;
    for s in &ss {
        let mine: BTreeSet<String> = rolled
            .iter()
            .filter(|(c, _)| c == &s.id)
            .map(|(_, t)| t.clone())
            .collect();
        check_reused(&gw, s, &mine, "after a rollback").await;
    }

    // 3. Twenty transfers in a row reuse the same connections: one per
    //    cluster, and (almost) no new connections on the servers.
    let before: Vec<u64> = futures_join(&pools).await;
    let mut seen: BTreeMap<ClusterId, BTreeSet<String>> = BTreeMap::new();
    for _ in 0..20 {
        for (c, th) in transfer(&mut t, &ss, true).await {
            seen.entry(c).or_default().insert(th);
        }
    }
    let after: Vec<u64> = futures_join(&pools).await;
    let opened: Vec<u64> = after.iter().zip(&before).map(|(a, b)| a - b).collect();
    println!("20 transfers: backend threads per cluster {seen:?}; connections opened {opened:?}");
    for (c, ths) in &seen {
        assert!(
            ths.len() <= 2,
            "{c}: 20 transfers used {} connections",
            ths.len()
        );
    }
    for s in &ss {
        let all: BTreeSet<String> = seen[&s.id].clone();
        check_reused(&gw, s, &all, "after 20 transfers").await;
    }
    // The coordinator's admin pool may open one or two; a connection per
    // branch would be 20 on each server.
    assert!(
        opened.iter().all(|&n| n <= 4),
        "connections opened: {opened:?}"
    );

    // The money moved, and nothing is left behind.
    for (s, p) in ss.iter().zip(&pools) {
        let mut c = p.get_conn().await.unwrap();
        let bal: Option<i64> = c
            .query_first(format!("SELECT bal FROM `{}`.acct WHERE id = 1", s.schema))
            .await
            .unwrap();
        let want = if s.shard == 1 { 1000 - 21 } else { 1000 + 21 };
        assert_eq!(bal, Some(want), "{}", s.id);
        let left = gw
            .admin
            .redo_branches(&s.id)
            .await
            .unwrap()
            .iter()
            .filter(|r| r.statements > 0 || r.mark == Some(sprig_txn::redo::RedoMark::Done))
            .count();
        println!("{}: balance {bal:?}, redo rows left {left}", s.id);
        assert_eq!(left, 0, "{}: redo rows left", s.id);
    }

    drop(t);
    backends.shutdown();
    for (s, p) in ss.iter().zip(&pools) {
        let mut c = p.get_conn().await.unwrap();
        c.query_drop(format!("DROP DATABASE IF EXISTS `{}`", s.schema))
            .await
            .unwrap();
        drop(c);
        p.clone().disconnect().await.unwrap();
    }
}

async fn futures_join(pools: &[Pool]) -> Vec<u64> {
    let mut v = Vec::new();
    for p in pools {
        v.push(connections(p).await);
    }
    v
}
