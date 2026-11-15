//! Live: `SPRIG_TEST_MARIADB_SINGLE` (for example
//! `mysql://root:sprig-dev-root@127.0.0.1:13301`). Works in the schema
//! `ai_sys_live_<pid>`, which the store creates on first use, and drops it
//! afterwards.

use std::time::Duration;

use mysql_async::prelude::Queryable;
use serde_json::json;

use super::*;

#[tokio::test]
#[allow(clippy::too_many_lines)] // one round trip, in order
async fn live_system_audit_round_trip() {
    let Ok(url) = std::env::var("SPRIG_TEST_MARIADB_SINGLE") else {
        eprintln!("skipped: set SPRIG_TEST_MARIADB_SINGLE to run it");
        return;
    };
    let opts = mysql_async::OptsBuilder::from_opts(mysql_async::Opts::from_url(&url).unwrap())
        .pool_opts(
            mysql_async::PoolOpts::default()
                .with_constraints(mysql_async::PoolConstraints::new(0, 4).unwrap())
                .with_inactive_connection_ttl(Duration::from_secs(30)),
        );
    let pool = mysql_async::Pool::new(opts);
    let schema = format!("ai_sys_live_{}", std::process::id());
    let mut conn = pool.get_conn().await.unwrap();
    conn.query_drop(format!("DROP DATABASE IF EXISTS {schema}"))
        .await
        .unwrap();
    let ann = SystemAudit::in_schema(pool.clone(), schema.clone(), "Acme.Ann");
    let bob = SystemAudit::in_schema(pool.clone(), schema.clone(), "acme.bob");
    // The same login in another case is the same operator.
    let ann2 = SystemAudit::in_schema(pool.clone(), schema.clone(), "ACME.ANN");
    let anyone = UserId(0);
    let r = async {
        assert!(ann.create(anyone, Role::Data, "x").await.is_err());
        let c = ann
            .create(anyone, Role::Admin, "  Disk is filling  ")
            .await?;
        let other = bob.create(anyone, Role::Admin, "").await?;
        assert_eq!(ann.get(anyone, c).await?.unwrap().title, "Disk is filling");
        assert_eq!(ann2.get(anyone, c).await?.unwrap().role, Role::Admin);
        assert!(
            bob.get(anyone, c).await?.is_none(),
            "only the operator sees it"
        );
        assert!(ann.get(anyone, other).await?.is_none());
        assert_eq!(ann.list(anyone, 10).await?.len(), 1);
        assert_eq!(bob.list(anyone, 10).await?[0].title, "Untitled");
        ann.append(c, MsgKind::User, &json!({"text": "why?"}), None)
            .await?;
        let answer = ann
            .append(
                c,
                MsgKind::Assistant,
                &json!({"role": "assistant", "content": []}),
                None,
            )
            .await?;
        ann.append(
            c,
            MsgKind::Proposal,
            &json!({"id": "p-9", "kind": "recommendation", "body": {"why": "add a node"}}),
            None,
        )
        .await?;
        let hash = [9u8; 32];
        ann.append(
            c,
            MsgKind::SystemNote,
            &json!({"sha256": "09", "payload": "{}"}),
            Some(hash),
        )
        .await?;
        let msgs = ann.messages(c).await?;
        assert_eq!(msgs.len(), 4);
        assert_eq!(msgs[3].sent_hash, Some(hash));
        // Feedback: only on the operator's own answers and proposals.
        assert_eq!(bob.vote(anyone, answer, 1, None).await?, None);
        assert_eq!(
            ann.vote(anyone, answer, -1, Some(" vague ")).await?,
            Some(Role::Admin)
        );
        assert_eq!(
            bob.outcome(anyone, "p-9", Outcome::Accepted, None).await?,
            None
        );
        assert_eq!(
            ann.outcome(anyone, "p-9", Outcome::Dismissed, None).await?,
            Some((Role::Admin, "recommendation".to_owned()))
        );
        assert_eq!(ann.recent(10).await?.len(), 2);
        // Usage: the operator's own, and every operator's as the total.
        let u = Usage {
            input_tokens: 100,
            output_tokens: 30,
            cache_read_tokens: 900,
            cache_write_tokens: 50,
        };
        ann.add_usage(anyone, Role::Admin, &u).await?;
        ann2.add_usage(anyone, Role::Admin, &u).await?;
        bob.add_usage(anyone, Role::Admin, &u).await?;
        let today = ann.usage_today(UserId(77)).await?;
        assert_eq!((today.user, today.tenant), (360, 540));
        // The purge keeps the hash.
        assert_eq!(ann.purge_payloads(30).await?, 0);
        tokio::time::sleep(Duration::from_millis(20)).await;
        let mut pc = pool.get_conn().await.map_err(|e| store_error("t", e))?;
        assert_eq!(purge_payloads(&mut pc, &schema, 0).await?, 1);
        drop(pc);
        let msgs = ann.messages(c).await?;
        assert!(msgs[3].body.get("payload").is_none());
        assert_eq!(msgs[3].body["payload_purged"], true);
        assert_eq!(msgs[3].sent_hash, Some(hash));
        // Creating it again is a no-op.
        let mut pc = pool.get_conn().await.map_err(|e| store_error("t", e))?;
        ensure_schema(&mut pc, &schema).await?;
        drop(pc);
        assert_eq!(ann.list(anyone, 10).await?.len(), 1);
        Ok::<(), SprigError>(())
    }
    .await;
    conn.query_drop(format!("DROP DATABASE IF EXISTS {schema}"))
        .await
        .unwrap();
    drop(conn);
    pool.disconnect().await.unwrap();
    r.unwrap();
}
