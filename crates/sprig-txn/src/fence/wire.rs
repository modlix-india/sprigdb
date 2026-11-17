//! The holder calls a gateway makes to the relay writer's agent (its
//! internal service, over mutual TLS with `[tls]`): prepare, finished and
//! abandon. Plain text, one field per line, binary values in hex, so the
//! format needs no serializer:
//!
//! ```text
//! prepare                        finished                abandon
//! tenant <id>                    schema <hex>            tenant <id>
//! thread <backend thread id>     dtid <hex>              thread <id>
//! schema <hex>                                           dtid <hex>
//! dtid <hex>
//! gtrid <hex>
//! stmt <hex ctx> <hex sql>       (one line per statement, in order)
//! ```
//!
//! The answer is `ok`, or `err <errno> <message>` (with HTTP 409).

use sprig_common::ids::{TenantId, TxnId};

use crate::backend::DbError;
use crate::redo::RedoStmt;
use crate::xid::{Dtid, Gtrid, hex, unhex};

/// Route of the prepare call.
pub const PREPARE_ROUTE: &str = "/v1/relay/prepare";
/// Route of the finished call.
pub const FINISHED_ROUTE: &str = "/v1/relay/finished";
/// Route of the abandon call.
pub const ABANDON_ROUTE: &str = "/v1/relay/abandon";

/// The line a gateway sends first on a connection to the relay, naming
/// its tenant.
#[must_use]
pub fn preamble(tenant: TenantId) -> Vec<u8> {
    format!("SPRIGRELAY 1 {}\n", tenant.0).into_bytes()
}

/// A holder call.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum HolderCall {
    /// Register the holder, then insert its redo rows.
    Prepare {
        /// The tenant (the relayed connection's preamble).
        tenant: TenantId,
        /// The held branch's backend thread.
        thread: u64,
        /// The anchor schema.
        schema: String,
        /// The transaction.
        dtid: Dtid,
        /// Its gtrid.
        gtrid: Gtrid,
        /// The branch's writes.
        stmts: Vec<RedoStmt>,
    },
    /// The branch finished: delete its rows, drop the hold.
    Finished {
        /// The anchor schema.
        schema: String,
        /// The transaction.
        dtid: Dtid,
    },
    /// The coordinator gave the branch up: the holder is lost.
    Abandon {
        /// The tenant.
        tenant: TenantId,
        /// The held branch's backend thread.
        thread: u64,
        /// The transaction.
        dtid: Dtid,
    },
}

impl HolderCall {
    /// The route it is sent to.
    #[must_use]
    pub fn route(&self) -> &'static str {
        match self {
            HolderCall::Prepare { .. } => PREPARE_ROUTE,
            HolderCall::Finished { .. } => FINISHED_ROUTE,
            HolderCall::Abandon { .. } => ABANDON_ROUTE,
        }
    }

    /// The request body.
    #[must_use]
    pub fn encode(&self) -> String {
        match self {
            HolderCall::Prepare {
                tenant,
                thread,
                schema,
                dtid,
                gtrid,
                stmts,
            } => {
                let mut s = format!(
                    "prepare\ntenant {}\nthread {thread}\nschema {}\ndtid {}\ngtrid {}\n",
                    tenant.0,
                    hex(schema.as_bytes()),
                    hex(&dtid.to_bytes()),
                    hex(&gtrid.to_bytes())
                );
                for st in stmts {
                    s.push_str("stmt ");
                    s.push_str(&hex(st.ctx().as_bytes()));
                    s.push(' ');
                    s.push_str(&hex(st.sql.as_bytes()));
                    s.push('\n');
                }
                s
            }
            HolderCall::Finished { schema, dtid } => format!(
                "finished\nschema {}\ndtid {}\n",
                hex(schema.as_bytes()),
                hex(&dtid.to_bytes())
            ),
            HolderCall::Abandon {
                tenant,
                thread,
                dtid,
            } => format!(
                "abandon\ntenant {}\nthread {thread}\ndtid {}\n",
                tenant.0,
                hex(&dtid.to_bytes())
            ),
        }
    }

    /// Parses a request body.
    ///
    /// # Errors
    /// What is wrong with it.
    pub fn decode(body: &str) -> Result<Self, String> {
        let mut lines = body.lines();
        let kind = lines.next().ok_or("empty call")?;
        let (mut tenant, mut thread, mut schema, mut dtid, mut gtrid) =
            (None, None, None, None, None);
        let mut stmts = Vec::new();
        for line in lines {
            let (k, v) = line.split_once(' ').ok_or("a line without a value")?;
            match k {
                "tenant" => tenant = Some(TenantId(v.parse().map_err(|_| "bad tenant")?)),
                "thread" => thread = Some(v.parse::<u64>().map_err(|_| "bad thread")?),
                "schema" => schema = Some(text(v)?),
                "dtid" => {
                    let b: [u8; 16] = unhex(v).and_then(|b| b.try_into().ok()).ok_or("bad dtid")?;
                    dtid = Some(TxnId::from_bytes(b));
                }
                "gtrid" => {
                    gtrid = Some(
                        unhex(v)
                            .and_then(|b| Gtrid::from_bytes(&b))
                            .ok_or("bad gtrid")?,
                    );
                }
                "stmt" => {
                    let (ctx, sql) = v.split_once(' ').ok_or("bad stmt")?;
                    stmts.push(RedoStmt::from_ctx(text(sql)?, &text(ctx)?));
                }
                _ => return Err(format!("unknown field {k}")),
            }
        }
        let need = |what: &'static str| move || format!("no {what}");
        match kind {
            "prepare" => Ok(HolderCall::Prepare {
                tenant: tenant.ok_or_else(need("tenant"))?,
                thread: thread.ok_or_else(need("thread"))?,
                schema: schema.ok_or_else(need("schema"))?,
                dtid: dtid.ok_or_else(need("dtid"))?,
                gtrid: gtrid.ok_or_else(need("gtrid"))?,
                stmts,
            }),
            "finished" => Ok(HolderCall::Finished {
                schema: schema.ok_or_else(need("schema"))?,
                dtid: dtid.ok_or_else(need("dtid"))?,
            }),
            "abandon" => Ok(HolderCall::Abandon {
                tenant: tenant.ok_or_else(need("tenant"))?,
                thread: thread.ok_or_else(need("thread"))?,
                dtid: dtid.ok_or_else(need("dtid"))?,
            }),
            other => Err(format!("unknown call {other}")),
        }
    }
}

fn text(h: &str) -> Result<String, String> {
    unhex(h)
        .and_then(|b| String::from_utf8(b).ok())
        .ok_or_else(|| "bad hex text".to_owned())
}

/// The answer's body.
#[must_use]
pub fn encode_answer(r: &Result<(), DbError>) -> String {
    match r {
        Ok(()) => "ok".to_owned(),
        Err(e) => format!("err {} {}", e.errno, e.message.replace('\n', " ")),
    }
}

/// Parses an answer's body.
///
/// # Errors
/// The agent's error; a lost-connection error for a body that isn't an
/// answer (the call's outcome is then unknown).
pub fn decode_answer(body: &str) -> Result<(), DbError> {
    let body = body.trim();
    if body == "ok" {
        return Ok(());
    }
    let parsed = body.strip_prefix("err ").and_then(|rest| {
        let (n, msg) = rest.split_once(' ').unwrap_or((rest, ""));
        n.parse::<u16>().ok().map(|errno| DbError::new(errno, msg))
    });
    // Anything else (a proxy's page, a cut answer): the call's outcome is
    // unknown.
    Err(parsed.unwrap_or_else(|| DbError::lost(format!("relay agent: {body}"))))
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]

    use sprig_common::ids::{DbId, GroupId};

    use super::*;
    use crate::xid::{AnchorRef, DtidGen};

    #[test]
    fn calls_round_trip() {
        let dtid = DtidGen::new(7).next();
        let gtrid = Gtrid {
            tenant: TenantId(26_306_000_001),
            dtid,
            start_ms: 1_794_000_000_123,
            home: AnchorRef {
                db: DbId(1),
                group: GroupId(2),
                hash_point: 99,
            },
        };
        let stmts = vec![RedoStmt {
            sql: "UPDATE `t1_d1_g2_s1`.a SET v = 'x y\n' WHERE k = 1".into(),
            affected: 1,
            insert_id: 0,
            vars: vec![("sql_mode".into(), "STRICT_TRANS_TABLES".into())],
            charset: Some("utf8mb4".into()),
            timestamp_us: Some(1_794_000_000_123_456),
            db: Some("t1_d1_g1_s1".into()),
        }];
        for c in [
            HolderCall::Prepare {
                tenant: gtrid.tenant,
                thread: 4242,
                schema: "t1_d1_g2_s1".into(),
                dtid,
                gtrid,
                stmts,
            },
            HolderCall::Finished {
                schema: "t1_d1_g2_s1".into(),
                dtid,
            },
            HolderCall::Abandon {
                tenant: gtrid.tenant,
                thread: 4242,
                dtid,
            },
        ] {
            assert_eq!(HolderCall::decode(&c.encode()), Ok(c));
        }
        assert!(HolderCall::decode("prepare\ntenant 1\n").is_err());
        assert!(HolderCall::decode("format c:\n").is_err());
    }

    #[test]
    fn answers_round_trip() {
        assert_eq!(decode_answer(&encode_answer(&Ok(()))), Ok(()));
        let e = DbError::new(39448, "the tenant is fenced\non this node");
        assert_eq!(
            decode_answer(&encode_answer(&Err(e))),
            Err(DbError::new(39448, "the tenant is fenced on this node"))
        );
        assert!(decode_answer("502 Bad Gateway").unwrap_err().is_lost());
    }
}
