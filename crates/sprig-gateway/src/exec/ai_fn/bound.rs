//! Calls over row values bound by their answers (`sprig_sql::ai_fn::bind`):
//! in a `SELECT`'s `WHERE`, inside expressions of its list, in its
//! `ORDER BY`, and in a single-table `UPDATE ... SET`.
//!
//! ```text
//! UPDATE outside a transaction      → START TRANSACTION (COMMIT or ROLLBACK at the end)
//! row inputs                        → SELECT [DISTINCT] SHA2(x), x ... [FOR UPDATE]   (through this session)
//!   answered per call                 (functions::evaluate: settings, egress, cache, budgets, audit)
//! output inputs (SELECT)            → the statement, row calls bound, SHA2(x), x appended
//!   answered per call                 (a call already answered over the rows reuses those answers)
//! the statement                     → each call replaced by CASE SHA2(x) WHEN ... END
//!                                     (through this session: the router, row-level security)
//! ```
//!
//! Every read runs through this session, so names, privileges, row-level
//! security, sharding and the transaction apply as to the caller's own
//! statement; an `UPDATE`'s inputs are read `FOR UPDATE` in the
//! transaction the `UPDATE` then runs in, so they are the rows it changes.
//! More than [`MAX_ROWS`] rows of inputs, or answers larger than
//! [`MAX_BOUND_BYTES`], fail with 39601.

use std::collections::{HashMap, HashSet};

use async_trait::async_trait;
use bytes::Bytes;
use sprig_ai::functions::{self, FnSettings};
use sprig_common::{Result, SprigError};
use sprig_proto_mysql::command::QueryAttrs;
use sprig_proto_mysql::packets::OkPacket;
use sprig_proto_mysql::{ColumnDef, StatusFlags};
use sprig_sql::ai_fn::{self, ColumnAt, Level, Plan};

use super::{GatewayEnv, MAX_ROWS, PassSession, Step, budget_warning, literal};
use crate::pipeline::{EndOfRows, ExecCtx, ResultSink, RowFormat, RowValues, Wire};
use crate::tenant::session::LocalWarning;

/// The most answer text written into one statement.
const MAX_BOUND_BYTES: usize = 8 << 20;

/// A statement with bound calls, its constant calls answered.
pub(super) struct Bound<'a> {
    pub(super) plan: &'a Plan,
    /// Every call's arguments, checked.
    pub(super) calls: &'a [functions::Call],
    pub(super) env: &'a GatewayEnv,
    pub(super) settings: &'a FnSettings,
    pub(super) nbe: bool,
    /// One per call: the constant calls' values; the bound calls' lookups
    /// are filled in.
    pub(super) values: Vec<String>,
    pub(super) warnings: Vec<LocalWarning>,
}

fn limit(what: &str) -> SprigError {
    SprigError::standard(
        39601,
        *b"HY000",
        format!("ER_SPRIG_TRANSLATE_LIMIT: {what}"),
    )
}

/// A call's identity: equal calls are answered once.
type Key = (&'static str, Vec<Option<String>>, String);

impl PassSession {
    /// Runs a statement whose calls over rows are bound (module docs).
    #[allow(clippy::too_many_lines)] // transaction, reads, the statement, its answer
    pub(super) async fn tenant_ai_bound(
        &mut self,
        cx: &mut ExecCtx<'_>,
        text: &Bytes,
        attrs: &QueryAttrs,
        more: bool,
        out: &mut dyn ResultSink,
        mut b: Bound<'_>,
    ) -> Result<Step> {
        let plan = b.plan;
        let Some(bind) = plan.bind.as_ref() else {
            return Err(SprigError::internal("a statement without bound calls"));
        };
        let own = bind.update && {
            let s = self.status();
            s.contains(StatusFlags::AUTOCOMMIT) && !s.contains(StatusFlags::IN_TRANS)
        };
        if own && let Err(e) = self.run_checked(cx, "START TRANSACTION", true).await {
            return self.fail(out, &e).await;
        }
        let sql = match Box::pin(self.answer_bound(cx, text, &mut b)).await {
            Ok(()) => Bytes::from(plan.render(text, &b.values)),
            Err(e) => {
                if own {
                    let _ = self.run_checked(cx, "ROLLBACK", true).await;
                }
                return self.fail(out, &e).await;
            }
        };
        metrics::counter!("sprig_translator_rules_applied_total", "rule" => "ai_functions_bound")
            .increment(1);
        let step = if own {
            // The UPDATE's answer waits for the COMMIT.
            let mut held = Held::new(out.wire());
            let r = Box::pin(self.tenant_text(cx, &sql, attrs, false, RowFormat::Text, &mut held))
                .await;
            let done = match r {
                Ok(_) => held.error.take().map_or(Ok(()), Err),
                Err(e) => {
                    let _ = self.run_checked(cx, "ROLLBACK", true).await;
                    return Err(e);
                }
            };
            let committed = match done {
                Ok(()) => self.run_checked(cx, "COMMIT", true).await.map(|_| ()),
                Err(e) => {
                    let _ = self.run_checked(cx, "ROLLBACK", true).await;
                    Err(e)
                }
            };
            match committed {
                Ok(()) => {
                    let mut status = self.status();
                    if more {
                        status |= StatusFlags::MORE_RESULTS_EXISTS;
                    }
                    let mut ok = held.ok.take().unwrap_or_default();
                    ok.status = status;
                    ok.session_state = Bytes::new();
                    out.ok(ok).await?;
                    Step {
                        errored: false,
                        status: Some(status),
                    }
                }
                Err(e) => return self.fail(out, &e).await,
            }
        } else if bind.update {
            Box::pin(self.tenant_text(cx, &sql, attrs, more, RowFormat::Text, out)).await?
        } else {
            let mut named = Named {
                inner: out,
                names: &bind.names,
            };
            Box::pin(self.tenant_text(cx, &sql, attrs, more, RowFormat::Text, &mut named)).await?
        };
        if !b.warnings.is_empty()
            && !step.errored
            && let Some(ts) = self.tenant.as_deref_mut()
        {
            ts.warnings
                .get_or_insert_with(Vec::new)
                .extend(std::mem::take(&mut b.warnings));
        }
        Ok(step)
    }

    /// Reads the bound calls' inputs, answers them and puts each call's
    /// lookup in `b.values`.
    #[allow(clippy::too_many_lines)] // two levels: read, group by hash, answer
    async fn answer_bound(
        &mut self,
        cx: &mut ExecCtx<'_>,
        text: &[u8],
        b: &mut Bound<'_>,
    ) -> Result<()> {
        let plan = b.plan;
        let update = plan.bind.as_ref().is_some_and(|x| x.update);
        // Lookups by call, from the row inputs (a superset of the rows the
        // statement returns, so output calls equal to one reuse it).
        let mut known: HashMap<Key, String> = HashMap::new();
        for level in [Level::Row, Level::Output] {
            let (inputs, at) = plan.bound_inputs(text, level);
            if inputs.is_empty() {
                continue;
            }
            let key = |i: usize| -> Key {
                let c = &plan.calls[i];
                let x = at[i].map_or_else(String::new, |k| inputs[k].clone());
                (c.name, c.rest.clone(), x)
            };
            let todo: Vec<usize> = (0..plan.calls.len())
                .filter(|&i| plan.calls[i].level == Some(level))
                .collect();
            if level == Level::Output {
                for &i in &todo {
                    if let Some(v) = known.get(&key(i)) {
                        b.values[i].clone_from(v);
                    }
                }
                if todo.iter().all(|&i| known.contains_key(&key(i))) {
                    continue;
                }
            }
            let sql = match level {
                Level::Row => plan.row_keys_sql(text, MAX_ROWS + 1),
                Level::Output => plan.output_keys_sql(text, &b.values),
            };
            let Some(sql) = sql else { continue };
            let got = self.run_checked(cx, &sql, false).await?;
            if got.rows.len() > MAX_ROWS {
                return Err(limit(&if update {
                    format!(
                        "an UPDATE with AI functions may change at most {MAX_ROWS} rows; update in batches with a narrower WHERE"
                    )
                } else {
                    format!(
                        "AI functions over row values may read at most {MAX_ROWS} rows of inputs in one statement; add a LIMIT or a narrower WHERE"
                    )
                }));
            }
            // Each input's distinct values by hash (the last two columns
            // per input).
            let base = got
                .columns
                .len()
                .checked_sub(2 * inputs.len())
                .ok_or_else(|| SprigError::internal("the inputs' read lost its columns"))?;
            let mut per: Vec<Vec<(String, String)>> = vec![Vec::new(); inputs.len()];
            let mut seen: Vec<HashSet<Vec<u8>>> = vec![HashSet::new(); inputs.len()];
            for row in &got.rows {
                for k in 0..inputs.len() {
                    let (Some(Some(h)), Some(x)) =
                        (row.get(base + 2 * k), row.get(base + 2 * k + 1))
                    else {
                        continue;
                    };
                    if seen[k].insert(h.clone()) {
                        per[k].push((
                            String::from_utf8_lossy(h).into_owned(),
                            x.as_deref()
                                .map(|x| String::from_utf8_lossy(x).into_owned())
                                .unwrap_or_default(),
                        ));
                    }
                }
            }
            drop(got);
            for &i in &todo {
                let call_key = key(i);
                if let Some(v) = known.get(&call_key) {
                    b.values[i].clone_from(v);
                    continue;
                }
                let Some(slot) = at[i] else { continue };
                let xs: Vec<Option<String>> =
                    per[slot].iter().map(|(_, x)| Some(x.clone())).collect();
                let (answers, report) =
                    Box::pin(functions::evaluate(b.env, b.settings, &b.calls[i], &xs)).await?;
                b.warnings
                    .extend(budget_warning(b.calls[i].f, report.budget_nulls));
                let branches: Vec<(String, String)> = per[slot]
                    .iter()
                    .zip(answers)
                    .filter_map(|((h, _), a)| a.map(|a| (h.clone(), literal(Some(&a), b.nbe))))
                    .collect();
                let lookup = ai_fn::case_of(&inputs[slot], &branches);
                b.values[i].clone_from(&lookup);
                known.insert(call_key, lookup);
            }
        }
        let size: usize = b.values.iter().map(String::len).sum();
        if size > MAX_BOUND_BYTES {
            return Err(limit(&format!(
                "the answers of this statement's AI functions take more than {MAX_BOUND_BYTES} bytes; add a LIMIT or a narrower WHERE"
            )));
        }
        Ok(())
    }
}

/// An `UPDATE`'s answer, held until its transaction commits.
struct Held {
    wire: Wire,
    ok: Option<OkPacket>,
    error: Option<SprigError>,
}

impl Held {
    fn new(wire: Wire) -> Self {
        Self {
            wire,
            ok: None,
            error: None,
        }
    }

    fn rows() -> SprigError {
        SprigError::internal("rows from an UPDATE")
    }
}

#[async_trait]
impl ResultSink for Held {
    fn wire(&self) -> Wire {
        self.wire
    }
    async fn begin_rows(&mut self, _columns: &[ColumnDef], _format: RowFormat) -> Result<()> {
        Err(Self::rows())
    }
    async fn row(&mut self, _values: RowValues<'_>) -> Result<()> {
        Err(Self::rows())
    }
    async fn raw_row(&mut self, _payload: Bytes) -> Result<()> {
        Err(Self::rows())
    }
    async fn end_rows(&mut self, _end: EndOfRows) -> Result<()> {
        Err(Self::rows())
    }
    async fn ok(&mut self, ok: OkPacket) -> Result<()> {
        self.ok = Some(ok);
        Ok(())
    }
    async fn error(&mut self, err: &SprigError) -> Result<()> {
        self.error = Some(SprigError {
            kind: err.kind,
            message: err.message.clone(),
            retryable: err.retryable,
            source: None,
        });
        Ok(())
    }
    async fn column_list(&mut self, _columns: &[ColumnDef], _end: EndOfRows) -> Result<()> {
        Err(Self::rows())
    }
    async fn local_infile_request(&mut self, _file_name: &[u8]) -> Result<()> {
        Err(Self::rows())
    }
    async fn local_infile_read(&mut self) -> Result<Bytes> {
        Ok(Bytes::new())
    }
    async fn flush(&mut self) -> Result<()> {
        Ok(())
    }
    fn finished(&self) -> bool {
        self.ok.is_some() || self.error.is_some()
    }
}

/// The result's columns of items holding a bound call named by the
/// items' text (as without the lookups).
struct Named<'a> {
    inner: &'a mut dyn ResultSink,
    names: &'a [(ColumnAt, String)],
}

/// `columns` with the items' names.
fn renamed(columns: &[ColumnDef], names: &[(ColumnAt, String)]) -> Vec<ColumnDef> {
    let mut c = columns.to_vec();
    for (at, name) in names {
        if let Some(i) = at.index(c.len()) {
            c[i].name = Bytes::copy_from_slice(name.as_bytes());
        }
    }
    c
}

#[async_trait]
impl ResultSink for Named<'_> {
    fn wire(&self) -> Wire {
        self.inner.wire()
    }
    // `begin_rows_raw` keeps its default: decoded, then renamed here.
    async fn begin_rows(&mut self, columns: &[ColumnDef], format: RowFormat) -> Result<()> {
        self.inner
            .begin_rows(&renamed(columns, self.names), format)
            .await
    }
    async fn cursor_opened(&mut self, columns: &[ColumnDef], end: EndOfRows) -> Result<()> {
        self.inner
            .cursor_opened(&renamed(columns, self.names), end)
            .await
    }
    async fn row(&mut self, values: RowValues<'_>) -> Result<()> {
        self.inner.row(values).await
    }
    async fn raw_row(&mut self, payload: Bytes) -> Result<()> {
        self.inner.raw_row(payload).await
    }
    async fn end_rows(&mut self, end: EndOfRows) -> Result<()> {
        self.inner.end_rows(end).await
    }
    async fn ok(&mut self, ok: OkPacket) -> Result<()> {
        self.inner.ok(ok).await
    }
    async fn error(&mut self, err: &SprigError) -> Result<()> {
        self.inner.error(err).await
    }
    async fn column_list(&mut self, columns: &[ColumnDef], end: EndOfRows) -> Result<()> {
        self.inner.column_list(columns, end).await
    }
    async fn local_infile_request(&mut self, file_name: &[u8]) -> Result<()> {
        self.inner.local_infile_request(file_name).await
    }
    async fn local_infile_read(&mut self) -> Result<Bytes> {
        self.inner.local_infile_read().await
    }
    async fn flush(&mut self) -> Result<()> {
        self.inner.flush().await
    }
    fn finished(&self) -> bool {
        self.inner.finished()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn items_keep_their_names() {
        let cols: Vec<ColumnDef> = ["id", "(CASE SHA2(...) END)", "x"]
            .iter()
            .map(|n| ColumnDef {
                name: Bytes::copy_from_slice(n.as_bytes()),
                ..ColumnDef::default()
            })
            .collect();
        let r = renamed(
            &cols,
            &[
                (ColumnAt::FromStart(1), "UPPER(AI_SUMMARIZE(b))".to_owned()),
                (ColumnAt::FromEnd(0), "k".to_owned()),
                (ColumnAt::FromStart(7), "gone".to_owned()),
            ],
        );
        let names: Vec<&[u8]> = r.iter().map(|c| &c.name[..]).collect();
        assert_eq!(names, [&b"id"[..], b"UPPER(AI_SUMMARIZE(b))", b"k"]);
    }
}
