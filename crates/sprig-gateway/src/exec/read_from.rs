//! Reads from copies in the executor: `SET @sprig_read_token`, `SET
//! sprig.read_from`, and the token variable (see [`crate::reader`]).

use sprig_common::Result;
use sprig_proto_mysql::packets::OkPacket;

use super::PassSession;
use crate::backend::state::VarValue;
use crate::pipeline::{ExecCtx, ExecSummary, Request, ResultSink};
use crate::reader;

impl PassSession {
    /// Answers `SET @sprig_read_token = '<n>'` and `SET sprig.read_from =
    /// ...` of a tenant session itself; before any other statement that
    /// names `@sprig_read_token`, sets the variable to the session's token
    /// (a reader session's own, else every commit this node has made
    /// visible, which covers the session's). `None` for any other request.
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
            // A session on the leader reads there whatever it asks: no copy
            // is fresher.
            if let Some(r) = &cx.session.reader {
                r.set_default(from);
            }
            return Some(self.read_from_ok(cx, out).await);
        }
        if let Some(t) = reader::parse_set_token(sql) {
            let now = match &cx.session.reader {
                Some(r) => {
                    r.raise(t);
                    r.token()
                }
                None => t,
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
                None => reader::node_token(),
            };
            self.bs(cx).vars_mut().user_vars.insert(
                reader::TOKEN_VAR.to_owned(),
                VarValue::Integer(t.to_string()),
            );
        }
        None
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
