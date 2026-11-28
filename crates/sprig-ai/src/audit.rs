//! Conversations, their messages and token usage: the [`AuditStore`] trait,
//! its in-memory store, and the tenant's catalog records ([`rec`], module
//! `ai`).
//!
//! - **What is kept.** Message bodies hold what the user saw; tool
//!   results their gated form (shape only, unless sharing is on). Each
//!   model call adds a `system_note` with the SHA-256 of the exact payload
//!   sent (`sent_hash`) and, for `ai.audit_payload_days`, the payload
//!   itself.
//! - **Usage.** `ai_usage` counts tokens per UTC day, user and role:
//!   input read or written uncached, and output. Budgets read it.
//! - **Whose.** A conversation belongs to one user; every read checks it.
//! - The audit stream gets one `ai.turn` event per turn: the user, the
//!   role, the tools called and the hashes sent ([`crate::turn`]).
//! - **The administration assistant's** conversations are kept the same
//!   way in the system records instead ([`system`], [`rec`]).

use std::collections::BTreeMap;
use std::sync::Mutex;

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sprig_catalog::migrate::{Migration, Module};
use sprig_common::ids::UserId;
use sprig_common::{Result, SprigError};

use crate::budget::UsageToday;
use crate::provider::Usage;
use crate::role::Role;

/// The catalog module.
pub const MODULE: &str = "ai";

/// The module's MariaDB migrations: none, its rows are engine records
/// ([`rec`]).
pub static AI_MIGRATIONS: &[Migration] = &[];

/// The module, for migrators.
pub const AI_MODULE: Module = Module {
    name: MODULE,
    migrations: AI_MIGRATIONS,
};

/// A conversation's id.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
pub struct ConvId(pub u64);

/// What a stored message is.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MsgKind {
    /// The user's message.
    User,
    /// The model's answer (its whole message: text, thinking, tool calls).
    Assistant,
    /// A tool call, as run.
    ToolCall,
    /// A tool's gated result.
    ToolResult,
    /// A proposal shown to the user.
    Proposal,
    /// A note: what was sent, a stop, an error.
    SystemNote,
}

impl MsgKind {
    /// The catalog's name.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            MsgKind::User => "user",
            MsgKind::Assistant => "assistant",
            MsgKind::ToolCall => "tool_call",
            MsgKind::ToolResult => "tool_result",
            MsgKind::Proposal => "proposal",
            MsgKind::SystemNote => "system_note",
        }
    }

    /// The kind named `s`.
    #[must_use]
    pub fn parse(s: &str) -> Option<Self> {
        [
            MsgKind::User,
            MsgKind::Assistant,
            MsgKind::ToolCall,
            MsgKind::ToolResult,
            MsgKind::Proposal,
            MsgKind::SystemNote,
        ]
        .into_iter()
        .find(|k| k.as_str() == s)
    }
}

/// A conversation's header.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Conversation {
    /// Its id.
    pub id: ConvId,
    /// Its owner.
    pub user: u32,
    /// `dba` or `data`.
    pub role: Role,
    /// Its title.
    pub title: String,
    /// Created, ms since the Unix epoch.
    pub created_ms: i64,
    /// Last changed, ms since the Unix epoch.
    pub updated_ms: i64,
}

/// One stored message.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct StoredMessage {
    /// Its id.
    pub id: u64,
    /// What it is.
    pub kind: MsgKind,
    /// Its body.
    pub body: Value,
    /// For a payload note, the SHA-256 of what was sent.
    pub sent_hash: Option<[u8; 32]>,
    /// When, ms since the Unix epoch.
    pub created_ms: i64,
}

/// Where conversations, messages and usage are kept.
#[async_trait]
pub trait AuditStore: Send + Sync {
    /// Starts a conversation for `user` (tenant roles only).
    async fn create(&self, user: UserId, role: Role, title: &str) -> Result<ConvId>;
    /// The conversation `id`, if it is `user`'s.
    async fn get(&self, user: UserId, id: ConvId) -> Result<Option<Conversation>>;
    /// `user`'s conversations, newest first.
    async fn list(&self, user: UserId, limit: u32) -> Result<Vec<Conversation>>;
    /// Appends a message; returns its id.
    async fn append(
        &self,
        conv: ConvId,
        kind: MsgKind,
        body: &Value,
        sent_hash: Option<[u8; 32]>,
    ) -> Result<u64>;
    /// The conversation's messages, oldest first.
    async fn messages(&self, conv: ConvId) -> Result<Vec<StoredMessage>>;
    /// Adds one model call's tokens to today's usage.
    async fn add_usage(&self, user: UserId, role: Role, usage: &Usage) -> Result<()>;
    /// Tokens used today by the tenant and by `user`.
    async fn usage_today(&self, user: UserId) -> Result<UsageToday>;
    /// Drops payloads older than `days` from their notes, keeping the
    /// hashes; returns how many.
    async fn purge_payloads(&self, days: u64) -> Result<u64>;
}

fn tenant_role(role: Role) -> Result<&'static str> {
    if role.is_tenant() {
        Ok(role.as_str())
    } else {
        Err(SprigError::internal(
            "only tenant assistants keep conversations in the catalog",
        ))
    }
}

fn title_of(t: &str) -> String {
    let t = t.trim();
    let t: String = t.chars().take(200).collect();
    if t.is_empty() {
        "Untitled".to_owned()
    } else {
        t
    }
}

// ---- in memory ----

#[derive(Default)]
pub(crate) struct Mem {
    pub(crate) convs: BTreeMap<u64, Conversation>,
    pub(crate) msgs: Vec<(u64, StoredMessage)>,
    usage: BTreeMap<(u32, Role), (u64, u64, u32)>,
    pub(crate) next: u64,
    /// Feedback rows ([`crate::feedback`]), oldest first.
    pub(crate) feedback: Vec<crate::feedback::MemFeedback>,
}

/// An in-memory store, for tests and evaluation runs.
#[derive(Default)]
pub struct MemoryAudit {
    inner: Mutex<Mem>,
    /// Keeps the administration roles' conversations instead of the
    /// tenant roles' (as [`rec::RecAudit::system`] does).
    system: bool,
}

impl std::fmt::Debug for MemoryAudit {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MemoryAudit").finish_non_exhaustive()
    }
}

impl MemoryAudit {
    pub(crate) fn lock(&self) -> std::sync::MutexGuard<'_, Mem> {
        self.inner
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    /// An in-memory store of administration conversations (`admin` and
    /// `triage`), like [`rec::RecAudit::system`].
    #[must_use]
    pub fn system() -> Self {
        Self {
            system: true,
            ..Self::default()
        }
    }

    /// Sets the tokens `user` used today (input, output), for tests.
    pub fn set_usage(&self, user: UserId, role: Role, input: u64, output: u64) {
        self.lock().usage.insert((user.0, role), (input, output, 1));
    }
}

#[async_trait]
impl AuditStore for MemoryAudit {
    async fn create(&self, user: UserId, role: Role, title: &str) -> Result<ConvId> {
        if self.system {
            system::system_role(role)?;
        } else {
            tenant_role(role)?;
        }
        let mut m = self.lock();
        m.next += 1;
        let id = m.next;
        m.convs.insert(
            id,
            Conversation {
                id: ConvId(id),
                user: user.0,
                role,
                title: title_of(title),
                created_ms: 0,
                updated_ms: 0,
            },
        );
        Ok(ConvId(id))
    }

    async fn get(&self, user: UserId, id: ConvId) -> Result<Option<Conversation>> {
        Ok(self
            .lock()
            .convs
            .get(&id.0)
            .filter(|c| c.user == user.0)
            .cloned())
    }

    async fn list(&self, user: UserId, limit: u32) -> Result<Vec<Conversation>> {
        Ok(self
            .lock()
            .convs
            .values()
            .rev()
            .filter(|c| c.user == user.0)
            .take(limit as usize)
            .cloned()
            .collect())
    }

    async fn append(
        &self,
        conv: ConvId,
        kind: MsgKind,
        body: &Value,
        sent_hash: Option<[u8; 32]>,
    ) -> Result<u64> {
        let mut m = self.lock();
        m.next += 1;
        let id = m.next;
        m.msgs.push((
            conv.0,
            StoredMessage {
                id,
                kind,
                body: body.clone(),
                sent_hash,
                created_ms: 0,
            },
        ));
        Ok(id)
    }

    async fn messages(&self, conv: ConvId) -> Result<Vec<StoredMessage>> {
        Ok(self
            .lock()
            .msgs
            .iter()
            .filter(|(c, _)| *c == conv.0)
            .map(|(_, m)| m.clone())
            .collect())
    }

    async fn add_usage(&self, user: UserId, role: Role, usage: &Usage) -> Result<()> {
        let mut m = self.lock();
        let e = m.usage.entry((user.0, role)).or_default();
        e.0 += usage.input_tokens + usage.cache_write_tokens;
        e.1 += usage.output_tokens;
        e.2 += 1;
        Ok(())
    }

    async fn usage_today(&self, user: UserId) -> Result<UsageToday> {
        let m = self.lock();
        let mut u = UsageToday::default();
        for ((who, _), (i, o, _)) in &m.usage {
            u.tenant += i + o;
            if *who == user.0 {
                u.user += i + o;
            }
        }
        Ok(u)
    }

    async fn purge_payloads(&self, _days: u64) -> Result<u64> {
        Ok(0)
    }
}

/// The provider history of a stored conversation, to continue it: the
/// user's texts, the model's answers as they were, and each tool result as
/// the model got it. A tool call left without a result (a turn that failed
/// while its tools ran) gets an error result, so the history stays valid.
#[must_use]
pub fn history(msgs: &[StoredMessage]) -> Vec<crate::provider::Message> {
    use crate::provider::{Block, Message, MsgRole};
    fn flush(out: &mut Vec<Message>, pending: &mut Vec<String>) {
        if pending.is_empty() {
            return;
        }
        let results = pending.drain(..).map(|id| Block::ToolResult {
            tool_use_id: id,
            content: crate::egress::wrap(
                &serde_json::json!({"error": "not run: the turn ended before this call finished"})
                    .to_string(),
            ),
            is_error: true,
        });
        match out.last_mut() {
            Some(m) if m.role == MsgRole::User => m.content.extend(results),
            _ => out.push(Message {
                role: MsgRole::User,
                content: results.collect(),
            }),
        }
    }
    let mut out: Vec<Message> = Vec::new();
    let mut pending: Vec<String> = Vec::new();
    for m in msgs {
        match m.kind {
            MsgKind::User => {
                flush(&mut out, &mut pending);
                let text = m.body["text"].as_str().unwrap_or_default().to_owned();
                match out.last_mut() {
                    Some(last) if last.role == MsgRole::User => {
                        last.content.push(Block::Text { text });
                    }
                    _ => out.push(Message::user(text)),
                }
            }
            MsgKind::Assistant => {
                flush(&mut out, &mut pending);
                let Ok(msg) = serde_json::from_value::<Message>(m.body.clone()) else {
                    continue;
                };
                pending = msg
                    .tool_calls()
                    .into_iter()
                    .map(|(id, _, _)| id.to_owned())
                    .collect();
                out.push(msg);
            }
            MsgKind::ToolResult => {
                let id = m.body["id"].as_str().unwrap_or_default().to_owned();
                let Some(at) = pending.iter().position(|p| *p == id) else {
                    continue;
                };
                pending.remove(at);
                let block = Block::ToolResult {
                    tool_use_id: id,
                    content: crate::egress::wrap(&m.body["result"].to_string()),
                    is_error: !m.body["ok"].as_bool().unwrap_or(false),
                };
                match out.last_mut() {
                    Some(last)
                        if last.role == MsgRole::User
                            && last
                                .content
                                .iter()
                                .all(|b| matches!(b, Block::ToolResult { .. })) =>
                    {
                        last.content.push(block);
                    }
                    _ => out.push(Message {
                        role: MsgRole::User,
                        content: vec![block],
                    }),
                }
            }
            MsgKind::ToolCall | MsgKind::Proposal | MsgKind::SystemNote => {}
        }
    }
    flush(&mut out, &mut pending);
    out
}

pub mod rec;
pub mod system;
