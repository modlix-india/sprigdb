//! Plain statements on engine tables that skip the gateway's text passes.
//!
//! Every client statement normally goes through the tenant pipeline's text
//! passes (exec/tenant.rs: the MySQL port's rewrites, the stream, typed
//! table, inheritance and search layers, row security, the name layer, the
//! privilege check's parse) before the engine sees it. For a plain
//! `INSERT ... VALUES` of literals, or a `SELECT`/`UPDATE`/`DELETE` of one
//! table by column values (`sprig_sql::compat::fastpath`), none of them
//! changes anything, yet each scans the whole text.
//!
//! The pipeline classifies such a text in one scan. The first statement of
//! a shape (its kind and table, in the session's context: current
//! database, lexer options, catalog version) goes the long way; if it
//! reaches the engine with its text unchanged by every pass, the shape is
//! learnt (`plancache::FastShapes`). Later statements of a learnt shape go
//! straight to the engine, after the checks that don't follow from the
//! shape: privileges (from the shape's needs), row security on the table,
//! a temporary table of the same name, inheritance and table groups.
//! Anything unusual (a refusal, a table no longer the engine's) goes the
//! long way, which answers it exactly as before.

use bytes::Bytes;
use sprig_catalog::ddl::database_key;
use sprig_common::Result;
use sprig_common::ids::ObjectId;
use sprig_sql::compat::fastpath::{self, FastStmt};

use crate::engine::catalog;
use crate::engine::plancache::Shape;
use crate::exec::PassSession;
use crate::exec::tenant::Step;
use crate::pipeline::{ExecCtx, ResultSink, RowFormat};

/// Turns the shortcut off for the process when set to `0` or `off`: for
/// measuring the long way against it on one build.
const SWITCH: &str = "SPRIGDB_STATEMENT_SHORTCUT";

/// Whether the shortcut is on (it is unless [`SWITCH`] turns it off).
fn enabled() -> bool {
    static ON: std::sync::OnceLock<bool> = std::sync::OnceLock::new();
    *ON.get_or_init(|| {
        !std::env::var(SWITCH)
            .is_ok_and(|v| v.trim() == "0" || v.trim().eq_ignore_ascii_case("off"))
    })
}

impl PassSession {
    /// The shape of a client text, when it may skip the text passes: a
    /// tenant with engine databases, and a text of one of
    /// `sprig_sql::compat::fastpath`'s shapes.
    pub(in crate::exec) fn fast_classify(&self, sql: &[u8]) -> Option<FastStmt> {
        if !enabled() {
            return None;
        }
        let ts = self.tenant.as_deref()?;
        if ts.types.internal || ts.principal.must_change_password {
            return None;
        }
        catalog::of(ts.catalog()?)?;
        fastpath::classify(sql, &self.lex_opts().0)
    }

    /// Hands `f` to the next text statement (`tenant_text`).
    pub(in crate::exec) fn fast_hint(&mut self, f: Option<FastStmt>) {
        self.eng.fast = f;
    }

    /// The hint for this statement, taken (a statement the gateway runs
    /// on its behalf never sees it).
    pub(in crate::exec) fn fast_take(&mut self) -> Option<FastStmt> {
        self.eng.fast.take()
    }

    /// Around one text statement: an enclosing statement's learning state
    /// is kept aside while a statement it runs goes through.
    pub(in crate::exec) fn fast_enter(&mut self) -> FastSaved {
        FastSaved(self.eng.fast_candidate.take(), self.eng.fast_learn.take())
    }

    /// See [`Self::fast_enter`].
    pub(in crate::exec) fn fast_leave(&mut self, saved: FastSaved) {
        self.eng.fast_candidate = saved.0;
        self.eng.fast_learn = saved.1;
    }

    /// The statement reached its last step with `sql` (the name layer's
    /// text) for the backend: when every pass left the client's text
    /// alone (`unchanged`, and `sql` is `entry`), a candidate shape is
    /// learnt if the engine runs it.
    pub(in crate::exec) fn fast_forwarding(&mut self, sql: &[u8], entry: &[u8], unchanged: bool) {
        let c = self.eng.fast_candidate.take();
        if unchanged && sql == entry {
            self.eng.fast_learn = c;
        }
    }

    /// The engine ran a statement on `object` under catalog version
    /// `version`: the shape waiting for it is learnt.
    pub(super) fn fast_learnt(&mut self, version: u64, object: ObjectId) {
        if let Some((v, shape)) = self.eng.fast_learn.take()
            && v == version
            && shape.object == object
        {
            self.eng.fast_shapes.learn(v, shape);
        }
    }

    /// Runs `text` (of shape `f`) on the engine without the text passes,
    /// when its shape is learnt and nothing else claims it. `None`: the
    /// statement goes the long way (nothing was sent).
    pub(in crate::exec) async fn engine_fast(
        &mut self,
        cx: &mut ExecCtx<'_>,
        text: &Bytes,
        f: &FastStmt,
        more: bool,
        format: RowFormat,
        out: &mut dyn ResultSink,
    ) -> Result<Option<Step>> {
        let lex = self.lex_opts().0;
        let persona = self.persona;
        let Some(current_name) = self.current_logical() else {
            return Ok(None);
        };
        let Some(ts) = self.tenant.as_deref_mut() else {
            return Ok(None);
        };
        let Ok(cat) = ts.refresh().await else {
            return Ok(None);
        };
        // A child tenant's adopted shares: the long way.
        if !cat.ancestry.is_empty() {
            return Ok(None);
        }
        let Some(eng) = catalog::of(&cat) else {
            return Ok(None);
        };
        let lctn = ts.lower_case_table_names();
        let Some(current) = cat
            .database(&database_key(&current_name, lctn))
            .map(|d| d.db_id)
        else {
            return Ok(None);
        };
        let Some(object) = eng
            .table(current, &f.table, lctn)
            .and_then(|p| p.as_ref().as_ref().ok().map(|td| td.object))
        else {
            return Ok(None);
        };
        // A temporary table of the same name hides the engine's.
        if ts.is_temp(Some(&current_name), &f.table) {
            return Ok(None);
        }
        // Row security on the table: the rewrite adds the session's
        // predicates.
        if crate::tenant::authz::privileges(ts, &cat)
            .row_policies()
            .is_some_and(|r| r.tables.contains_key(&object))
        {
            return Ok(None);
        }
        let shape = Shape::new(f.kind, object, current, lex);
        if !self.eng.fast_shapes.allows(cat.version, &shape) {
            self.eng.fast_candidate = Some((cat.version, shape));
            return Ok(None);
        }
        let Some(ts) = self.tenant.as_deref_mut() else {
            return Ok(None);
        };
        // A refusal is answered the long way, by the full check.
        if !crate::tenant::authz::allows_needs(ts, persona, &f.needs(), Some(&current_name)) {
            return Ok(None);
        }
        // What the long way does before the engine: the previous
        // statement's gateway warnings are gone, and so are its renames.
        ts.warnings = None;
        self.eng.diag.bypass(false);
        self.subst.clear();
        let r = Box::pin(self.engine_text(cx, text, Some(text), more, format, out)).await?;
        if r.is_none() {
            self.eng.fast_shapes.forget(&shape);
        }
        Ok(r)
    }
}

/// An enclosing statement's learning state (see
/// [`PassSession::fast_enter`]).
pub(in crate::exec) struct FastSaved(Option<(u64, Shape)>, Option<(u64, Shape)>);
