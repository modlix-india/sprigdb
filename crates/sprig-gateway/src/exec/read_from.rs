//! Reads from copies in the executor: `SET @sprig_read_token`, `SET
//! sprig.read_from`, and the token variable (see [`crate::reader`]).

use sprig_common::Result;
use sprig_proto_mysql::StatusFlags;
use sprig_proto_mysql::packets::OkPacket;

use super::PassSession;
use crate::backend::state::VarValue;
use crate::pipeline::{ExecCtx, ExecSummary, Request, ResultSink};
use crate::reader;

impl PassSession {
    /// Answers `SET @sprig_read_token = '<n>'` and `SET sprig.read_from =
    /// ...` of a tenant session itself; before any other statement that
    /// names `@sprig_read_token`, sets the variable to the session's token
    /// (its own last commit, or what a reader session read). `None` for any
    /// other request.
    pub(super) async fn read_token_set(
        &mut self,
        cx: &mut ExecCtx<'_>,
        req: &Request,
        out: &mut dyn ResultSink,
    ) -> Option<Result<ExecSummary>> {
        let Request::Query { sql, .. } = req else {
            return None;
        };
        self.tenant.as_ref()?;
        if let Some(from) = reader::parse_set_read_from(sql) {
            // A reader session's default; a session on the leader ships
            // its reads to a copy when it asks for `follower` (`nearest`
            // and `leader` read here: no copy is nearer or fresher).
            match &cx.session.reader {
                Some(r) => r.set_default(from),
                None => cx.session.ship.read_pref = Some(from),
            }
            return Some(self.read_from_ok(cx, out).await);
        }
        if let Some(t) = reader::parse_set_token(sql) {
            let now = match &cx.session.reader {
                Some(r) => {
                    r.raise(t);
                    r.token()
                }
                None => reader::raise_session_token(t),
            };
            self.bs(cx).vars_mut().user_vars.insert(
                reader::TOKEN_VAR.to_owned(),
                VarValue::Integer(now.to_string()),
            );
            return Some(self.read_from_ok(cx, out).await);
        }
        if reader::mentions_token(sql) {
            let t = match &cx.session.reader {
                Some(r) => r.token(),
                None => reader::session_token(),
            };
            self.bs(cx).vars_mut().user_vars.insert(
                reader::TOKEN_VAR.to_owned(),
                VarValue::Integer(t.to_string()),
            );
        }
        None
    }

    /// A read of a session on the leader that asks for a copy
    /// (`/*+ READ_FROM(follower) */`, or `follower` set for the session or
    /// the tenant): runs on a copy through the session's shadow connection
    /// ([`crate::ship`]), with the session's token. `None`: the request
    /// runs here (not such a read, inside a transaction, a statement that
    /// reads the session's own state, or no copy reachable).
    pub(super) async fn ship_read(
        &mut self,
        cx: &mut ExecCtx<'_>,
        req: &Request,
        out: &mut dyn ResultSink,
    ) -> Option<Result<ExecSummary>> {
        let Request::Query { sql, .. } = req else {
            return None;
        };
        if cx.session.reader.is_some() || !crate::forward::enabled() {
            return None;
        }
        // A session logged in with a token's claims (`SPRIG_CLAIM()`, token
        // roles) reads here: the copy's session would have none.
        if self.tenant.as_deref()?.rls.claims.is_some() {
            return None;
        }
        let tenant = cx.session.principal.as_ref()?.tenant?.0;
        let from = match reader::hint(sql) {
            Some(h) => h,
            None => cx.session.ship.default_from(tenant),
        };
        if !matches!(from, reader::ReadFrom::Follower(_)) {
            return None;
        }
        let status = self.status();
        if status.contains(StatusFlags::IN_TRANS)
            || !status.contains(StatusFlags::AUTOCOMMIT)
            || !self.eng.temps.is_empty()
            || !crate::ship::shippable(sql)
            || !cx.session.ship.may_try()
        {
            return None;
        }
        let login = cx.session.ship.mysql_login.clone()?;
        if cx.session.ship.mysql.is_none() {
            let o = crate::ship::Open {
                listener: login.listener,
                protocol: login.protocol,
                peer: cx.session.peer,
                tls: cx.session.tls.clone(),
                tenant,
                resume: String::new(),
                prelude: bytes::Bytes::new(),
                pick: u64::from(cx.session.peer.port()),
            };
            let principal = sprig_auth::handoff::PrincipalWire::of(cx.session.principal.as_ref()?);
            match crate::ship::MysqlShadow::open(&login, principal, o, cx.proto.caps, None).await {
                Ok(sh) => cx.session.ship.mysql = Some(sh),
                Err(e) => {
                    tracing::debug!(tenant, error = %e, "read for a copy runs on the leader");
                    cx.session.ship.failed();
                    crate::ship::count(login.protocol, "unavailable");
                    return None;
                }
            }
        }
        let token = reader::session_token();
        let db = self.current_logical();
        let vars = self.bs(cx).vars().vars.clone();
        let sh = cx.session.ship.mysql.as_mut()?;
        match sh.run(sql, token, db.as_deref(), &vars, status, out).await {
            Ok(crate::ship::Shipped::Done) => {
                crate::ship::count(login.protocol, "ok");
                Some(Ok(ExecSummary {
                    status: Some(status),
                    prepared: None,
                    db: Some(db),
                }))
            }
            Ok(crate::ship::Shipped::Fallback(e)) => {
                tracing::debug!(tenant, error = %e, "read for a copy runs on the leader");
                cx.session.ship.failed();
                crate::ship::count(login.protocol, "fallback");
                None
            }
            Err(e) => {
                cx.session.ship.failed();
                Some(Err(e))
            }
        }
    }

    async fn read_from_ok(
        &mut self,
        _cx: &mut ExecCtx<'_>,
        out: &mut dyn ResultSink,
    ) -> Result<ExecSummary> {
        let ok = OkPacket::new(self.status());
        out.ok(ok).await?;
        Ok(ExecSummary {
            status: Some(self.status()),
            prepared: None,
            db: Some(self.current_logical()),
        })
    }
}
