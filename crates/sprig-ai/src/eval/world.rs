//! The fake tool world: a [`ToolHost`] that answers from a case's world
//! entries (read tools, and proposal tools' checks) and records every call
//! it gets, and a provider wrapper that
//! records every body sent.

use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use serde_json::Value;
use sprig_common::{Result, SprigError};

use super::case::WorldEntry;
use crate::provider::{ChatOutcome, ChatRequest, ChatSink, Provider, ProviderKind};
use crate::tools::{ProposalCheck, ToolBody, ToolHost, ToolOutput, ToolSpec};

/// Serves a case's world, as the signed-in caller's host would.
#[derive(Debug)]
pub struct WorldHost {
    entries: Vec<WorldEntry>,
    /// The database the case's context names (`Database: shop`).
    db: Option<String>,
    /// The SQL of the case's expected calls: the tables and columns it
    /// names exist in the case's database.
    sql: Vec<String>,
    calls: Mutex<Vec<(String, Value)>>,
    checked: Mutex<Vec<(String, Value)>>,
}

impl WorldHost {
    /// A host serving `entries`.
    #[must_use]
    pub fn new(entries: Vec<WorldEntry>) -> Self {
        Self {
            entries,
            db: None,
            sql: Vec::new(),
            calls: Mutex::new(Vec::new()),
            checked: Mutex::new(Vec::new()),
        }
    }

    /// The same host for a case whose context is `context`: its
    /// `Database: <name>` line names the database `catalog` lists.
    #[must_use]
    pub fn with_context(mut self, context: Option<&str>) -> Self {
        self.db = context.and_then(|c| {
            c.lines().find_map(|l| {
                l.trim()
                    .strip_prefix("Database:")
                    .map(|d| d.trim().trim_end_matches('.').to_owned())
                    .filter(|d| !d.is_empty() && !d.contains(' '))
            })
        });
        self
    }

    /// The same host knowing the case's expected calls, whose SQL names
    /// tables and columns that exist.
    #[must_use]
    pub fn with_expected(mut self, calls: &[super::case::ExpectedCall]) -> Self {
        self.sql = calls
            .iter()
            .filter_map(|c| c.args.get("sql").and_then(Value::as_str).map(str::to_owned))
            .collect();
        self
    }

    /// Tables (and columns `alias.column`) the expected SQL names.
    fn sql_tables(&self) -> Vec<(String, Vec<String>)> {
        let mut out: Vec<(String, Vec<String>)> = Vec::new();
        for sql in &self.sql {
            let words: Vec<String> = sql
                .split(|c: char| c.is_whitespace() || matches!(c, ',' | '(' | ')' | ';'))
                .filter(|w| !w.is_empty())
                .map(|w| w.trim_matches('`').to_owned())
                .collect();
            let mut alias: Vec<(String, String)> = Vec::new();
            for (i, w) in words.iter().enumerate() {
                if !(w.eq_ignore_ascii_case("from") || w.eq_ignore_ascii_case("join")) {
                    continue;
                }
                let Some(t) = words.get(i + 1) else { continue };
                if !t.chars().all(|c| c.is_alphanumeric() || c == '_') || t.is_empty() {
                    continue;
                }
                if !out.iter().any(|(n, _)| n.eq_ignore_ascii_case(t)) {
                    out.push((t.clone(), Vec::new()));
                }
                alias.push((t.clone(), t.clone()));
                let mut a = words.get(i + 2);
                if a.is_some_and(|x| x.eq_ignore_ascii_case("as")) {
                    a = words.get(i + 3);
                }
                if let Some(a) = a
                    && a.chars().all(|c| c.is_alphanumeric() || c == '_')
                    && ![
                        "where", "join", "on", "group", "order", "left", "inner", "limit", "cross",
                        "right",
                    ]
                    .contains(&a.to_ascii_lowercase().as_str())
                {
                    alias.push((a.clone(), t.clone()));
                }
            }
            for w in &words {
                let Some((a, c)) = w.split_once('.') else {
                    continue;
                };
                let c: String = c
                    .chars()
                    .take_while(|x| x.is_alphanumeric() || *x == '_')
                    .collect();
                if c.is_empty() {
                    continue;
                }
                if let Some((_, t)) = alias.iter().find(|(x, _)| x.eq_ignore_ascii_case(a))
                    && let Some((_, cols)) = out.iter_mut().find(|(n, _)| n.eq_ignore_ascii_case(t))
                    && !cols.contains(&c)
                {
                    cols.push(c);
                }
            }
        }
        out
    }

    /// The tables of the case's database: its `describe` entries' tables
    /// with their columns, then the tables its expected SQL names.
    fn tables(&self) -> Vec<(String, Vec<(String, String)>)> {
        let mut all = self.described();
        for (t, cols) in self.sql_tables() {
            if !all.iter().any(|(n, _)| n.eq_ignore_ascii_case(&t)) {
                all.push((t, cols.into_iter().map(|c| (c, String::new())).collect()));
            }
        }
        all
    }

    /// The tables the case's `describe` entries describe, with their
    /// columns.
    fn described(&self) -> Vec<(String, Vec<(String, String)>)> {
        self.entries
            .iter()
            .filter(|e| e.tool == "describe")
            .filter_map(|e| {
                let Ok(ToolBody::Json(v)) = e.body() else {
                    return None;
                };
                let name = v["table"]
                    .as_str()
                    .or_else(|| e.when.as_ref().and_then(|w| w["table"].as_str()))?
                    .to_owned();
                let cols = v["columns"]
                    .as_array()
                    .map(|a| {
                        a.iter()
                            .filter_map(|c| {
                                Some((
                                    c["name"].as_str()?.to_owned(),
                                    c["type"].as_str().unwrap_or("").to_owned(),
                                ))
                            })
                            .collect()
                    })
                    .unwrap_or_default();
                Some((name, cols))
            })
            .collect()
    }

    /// What the real host would answer for a read the case has no entry
    /// for, from the case's own `describe` entries: `catalog` lists the
    /// tables, `relationships` infers joins from `<table>_id` columns.
    fn derived(&self, tool: &str, input: &Value) -> Option<ToolOutput> {
        let tables = self.tables();
        if tables.is_empty() {
            return None;
        }
        let db = input["db"]
            .as_str()
            .map(str::to_owned)
            .or_else(|| self.db.clone())?;
        match tool {
            "catalog" if input["db"].as_str().is_none() => {
                Some(ToolOutput::json(serde_json::json!({"databases": [db]})))
            }
            // A table the expected SQL names but no entry describes: the
            // columns that SQL uses.
            "describe" => {
                let t = input["table"].as_str()?;
                if self
                    .described()
                    .iter()
                    .any(|(n, _)| n.eq_ignore_ascii_case(t))
                {
                    return None;
                }
                let (name, cols) = tables
                    .iter()
                    .find(|(n, c)| n.eq_ignore_ascii_case(t) && !c.is_empty())?;
                Some(ToolOutput::json(serde_json::json!({
                    "db": db, "table": name,
                    "columns": cols.iter().map(|(c, _)| serde_json::json!({"name": c})).collect::<Vec<_>>(),
                })))
            }
            "catalog" => Some(ToolOutput::json(serde_json::json!({
                "db": db,
                "objects": tables.iter().map(|(t, _)| serde_json::json!({"name": t, "kind": "table"})).collect::<Vec<_>>(),
                "more": false,
            }))),
            "relationships" => {
                let mut joins = Vec::new();
                for (t, cols) in &tables {
                    for (c, _) in cols {
                        let Some(stem) = c.strip_suffix("_id") else {
                            continue;
                        };
                        let parent = tables.iter().find(|(p, pc)| {
                            (p == stem || *p == format!("{stem}s") || *p == format!("{stem}es"))
                                && pc.iter().any(|(n, _)| n == "id")
                        });
                        if let Some((p, _)) = parent {
                            joins.push(serde_json::json!({
                                "from": format!("{t}.{c}"), "to": format!("{p}.id"),
                                "source": "inferred",
                                "join": format!("JOIN {p} ON {p}.id = {t}.{c}"),
                            }));
                        }
                    }
                }
                let only = input["table"].as_str();
                if let Some(o) = only {
                    joins.retain(|j| {
                        j["from"]
                            .as_str()
                            .is_some_and(|f| f.starts_with(&format!("{o}.")))
                            || j["to"]
                                .as_str()
                                .is_some_and(|f| f.starts_with(&format!("{o}.")))
                    });
                }
                Some(ToolOutput::json(
                    serde_json::json!({"db": db, "joins": joins}),
                ))
            }
            _ => None,
        }
    }

    /// Every call the host got: tool and input, in order.
    #[must_use]
    pub fn calls(&self) -> Vec<(String, Value)> {
        self.calls
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone()
    }
}

#[async_trait]
impl ToolHost for WorldHost {
    async fn call(&self, spec: &ToolSpec, input: &Value) -> Result<ToolOutput> {
        self.calls
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .push((spec.name.to_owned(), input.clone()));
        let entry = self
            .entries
            .iter()
            .filter(|e| e.tool == spec.name)
            .find(|e| e.when.is_some() && e.serves(input))
            .or_else(|| {
                // A table's own entry, by the table its JSON names.
                input["table"].as_str().and_then(|t| {
                    self.entries.iter().find(|e| {
                        e.tool == spec.name
                            && e.when.is_none()
                            && e.json
                                .as_ref()
                                .and_then(|j| j["table"].as_str())
                                .is_some_and(|n| n.eq_ignore_ascii_case(t))
                    })
                })
            })
            .or_else(|| {
                // A derived description beats another table's entry.
                if spec.name == "describe" && self.derived(spec.name, input).is_some() {
                    return None;
                }
                self.entries
                    .iter()
                    .find(|e| e.tool == spec.name && e.when.is_none())
            });
        if entry.is_none()
            && let Some(out) = self.derived(spec.name, input)
        {
            return Ok(out);
        }
        let Some(e) = entry else {
            // A case that says nothing about the schema checks means its
            // statements are fine: the real check is deterministic, and
            // cases written before it existed assume valid DDL.
            if spec.name == "check_ddl" {
                return Ok(ToolOutput::json(serde_json::json!({
                    "passed": true,
                    "counts": {"errors": 0, "changes": 0, "notes": 0},
                    "findings": [],
                    "next": "It passes SprigDB's rules; propose it with propose_sql.",
                })));
            }
            return Err(world_error(format!(
                "{} found nothing for that input",
                spec.name
            )));
        };
        match e.body() {
            Ok(body) => Ok(ToolOutput {
                body,
                sources: e.sources.clone(),
            }),
            Err(m) => Err(world_error(m)),
        }
    }

    /// A proposal tool's check, from its world entry (`json` is the
    /// check's result, `error` refuses the proposal); none checks nothing.
    async fn check_proposal(&self, spec: &ToolSpec, input: &Value) -> Result<ProposalCheck> {
        self.checked
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .push((spec.name.to_owned(), input.clone()));
        let entry = self
            .entries
            .iter()
            .filter(|e| e.tool == spec.name)
            .find(|e| e.when.is_some() && e.serves(input))
            .or_else(|| {
                self.entries
                    .iter()
                    .find(|e| e.tool == spec.name && e.when.is_none())
            });
        match entry.map(WorldEntry::body) {
            None => Ok(ProposalCheck::default()),
            Some(Ok(ToolBody::Json(v))) => Ok(ProposalCheck {
                checks: Some(v),
                generated: None,
            }),
            Some(Ok(_)) => Err(world_error("a proposal check's world entry is json")),
            Some(Err(m)) => Err(world_error(m)),
        }
    }
}

/// A tool failure as the database would report it (`ER_UNKNOWN_ERROR`):
/// the model sees only the message.
fn world_error(m: impl Into<String>) -> SprigError {
    SprigError::standard(1105, *b"HY000", m)
}

/// Wraps a provider and keeps every body it sent.
pub struct Recording {
    inner: Arc<dyn Provider>,
    sent: Mutex<Vec<Arc<str>>>,
}

impl std::fmt::Debug for Recording {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Recording")
            .field("provider", &self.inner.name())
            .finish_non_exhaustive()
    }
}

impl Recording {
    /// Records what `inner` sends.
    #[must_use]
    pub fn new(inner: Arc<dyn Provider>) -> Self {
        Self {
            inner,
            sent: Mutex::new(Vec::new()),
        }
    }

    /// The bodies sent, oldest first.
    #[must_use]
    pub fn bodies(&self) -> Vec<Arc<str>> {
        self.sent
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone()
    }
}

#[async_trait]
impl Provider for Recording {
    fn kind(&self) -> ProviderKind {
        self.inner.kind()
    }

    fn name(&self) -> &str {
        self.inner.name()
    }

    async fn chat(&self, req: ChatRequest, out: &mut dyn ChatSink) -> Result<ChatOutcome> {
        let o = self.inner.chat(req, out).await?;
        self.sent
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .push(Arc::clone(&o.sent.body));
        Ok(o)
    }
}

/// The part of a sent body that carries the conversation: the system
/// prompt and the tool definitions (fixed text) are left out, in both the
/// Anthropic and the OpenAI-compatible shapes.
#[must_use]
pub fn conversation_part(body: &str) -> String {
    let Ok(mut v) = serde_json::from_str::<Value>(body) else {
        return body.to_owned();
    };
    if let Some(m) = v.as_object_mut() {
        m.remove("system");
        m.remove("tools");
        if let Some(Value::Array(msgs)) = m.get_mut("messages") {
            msgs.retain(|x| {
                !matches!(
                    x.get("role").and_then(Value::as_str),
                    Some("system" | "developer")
                )
            });
        }
    }
    v.to_string()
}
