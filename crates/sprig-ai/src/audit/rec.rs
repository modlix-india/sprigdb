//! Conversations, messages, usage and feedback as the engine's catalog
//! records: a tenant's (module `ai`) and the administration assistant's
//! (module `ai_admin` in the system records, [`SYSTEM_TENANT`]).
//!
//! ```text
//! m/<module>/conv/<conv u64>                   ConvRec
//! m/<module>/recent/<owner><!updated ms u64><!conv u64>  ConvRec: an owner's conversations, newest first
//! m/<module>/msg/<conv u64><msg u64>           MsgRec
//! m/<module>/mconv/<msg u64>                   the message's conversation (u64 BE)
//! m/<module>/prop/<proposal id bytes>          PropRec: a proposal by the id the stream showed
//! m/<module>/pay/<created ms u64><msg u64>     the conversation (u64 BE) of a note still holding its payload
//! m/<module>/usage/<day u32><owner><role>      UsageRec
//! m/<module>/fb/<fb u64>                       FbRec
//! m/<module>/pout/<msg u64>                    the latest outcome recorded for a proposal (JSON string)
//! ctr/<module>.conv/, .msg/, .fb/              id counters
//! ```
//!
//! `<owner>` is a tenant user id (u32 BE) in a tenant's records and an
//! operator's login (`Pk::bytes`, as [`system::login_key`] keeps it) in the
//! system records. `<day>` is whole UTC days since 1970-01-01.
//!
//! - **Whose.** A tenant store checks every read against the caller's user
//!   id; a system store is opened for one operator's login and ignores the
//!   `UserId` arguments ([`super::system`]).
//! - **Usage.** One record per day, owner and role. [`RecAudit::usage_today`]
//!   sums the day's records: every owner's for the tenant (or the DC), the
//!   caller's own for the user. AI functions add theirs under the role
//!   `functions` ([`RecAudit::add_usage_as`]), so they share the budget.
//! - **Payloads.** A `system_note` with a hash and a `payload` is listed in
//!   `pay/` by its time, so [`AuditStore::purge_payloads`] reads a key range
//!   and rewrites each note to `{sha256, payload_purged: true}`.
//! - Every call is one short engine transaction of its own; nothing here
//!   bumps a catalog version.

use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use sprig_catalog::records::{Pk, RecordStore, RecordTx, RowTable, next_ids, prefix_end, scan_all};
use sprig_common::ids::{TenantId, UserId};
use sprig_common::{Result, SprigError};

use super::system::{login_key, system_role};
use super::{AuditStore, ConvId, Conversation, MsgKind, StoredMessage, tenant_role, title_of};
use crate::budget::UsageToday;
use crate::feedback::{FeedbackStore, Found, Note, Outcome, clean_note, note_of};
use crate::provider::Usage;
use crate::role::Role;

/// A tenant's module.
pub const MODULE: &str = "ai";
/// The administration assistant's module, in the system records.
pub const SYSTEM_MODULE: &str = "ai_admin";
/// Whose catalog records hold the DC's system stores (the administration
/// assistant's conversations): no tenant ever has id 0.
pub const SYSTEM_TENANT: TenantId = TenantId(0);

/// Notes one purge transaction rewrites at most; a purge runs transactions
/// until one comes back short.
const MAX_PURGE: usize = 5_000;
/// The newest feedback rows [`FeedbackStore::recent`] looks at (by id).
const MAX_FEEDBACK_SCAN: usize = 2_000;

/// A table of one of the two modules.
#[derive(Debug, Clone, Copy)]
struct Tables {
    module: &'static str,
    conv: RowTable,
    recent: RowTable,
    msg: RowTable,
    mconv: RowTable,
    prop: RowTable,
    pay: RowTable,
    usage: RowTable,
    fb: RowTable,
    pout: RowTable,
}

const TENANT_TABLES: Tables = tables(MODULE);
const SYSTEM_TABLES: Tables = tables(SYSTEM_MODULE);

const fn tables(module: &'static str) -> Tables {
    Tables {
        module,
        conv: RowTable::new(module, "conv"),
        recent: RowTable::new(module, "recent"),
        msg: RowTable::new(module, "msg"),
        mconv: RowTable::new(module, "mconv"),
        prop: RowTable::new(module, "prop"),
        pay: RowTable::new(module, "pay"),
        usage: RowTable::new(module, "usage"),
        fb: RowTable::new(module, "fb"),
        pout: RowTable::new(module, "pout"),
    }
}

impl Tables {
    fn counter(&self, what: &str) -> String {
        format!("{}.{what}", self.module)
    }
}

/// Whose conversations a store holds.
#[derive(Debug, Clone, PartialEq, Eq)]
enum Owner {
    /// A tenant's: each row names its user, checked on every read.
    Tenant,
    /// One operator's, in the system records.
    Operator(String),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct ConvRec {
    /// The tenant user (0 in the system records).
    user: u32,
    /// The operator's login (empty in a tenant's records).
    #[serde(default, skip_serializing_if = "String::is_empty")]
    operator: String,
    role: String,
    title: String,
    created_ms: i64,
    updated_ms: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct MsgRec {
    kind: String,
    body: Value,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    sent_hash: Option<String>,
    created_ms: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct PropRec {
    conv: u64,
    msg: u64,
}

#[derive(Debug, Clone, Copy, Default, Serialize, Deserialize)]
struct UsageRec {
    input: u64,
    output: u64,
    calls: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct FbRec {
    user: u32,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    operator: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    msg_id: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    proposal_id: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    vote: Option<i8>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    outcome: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    note: Option<String>,
    created_ms: i64,
}

fn now_ms() -> i64 {
    i64::try_from(
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis(),
    )
    .unwrap_or(i64::MAX)
}

/// Whole UTC days since 1970-01-01 at `ms`.
fn day_of(ms: i64) -> u32 {
    u32::try_from(ms.max(0) / 86_400_000).unwrap_or(u32::MAX)
}

fn hex(h: &[u8; 32]) -> String {
    use std::fmt::Write as _;
    let mut s = String::with_capacity(64);
    for b in h {
        let _ = write!(s, "{b:02x}");
    }
    s
}

fn unhex(s: &str) -> Option<[u8; 32]> {
    if s.len() != 64 {
        return None;
    }
    let mut out = [0u8; 32];
    for (i, o) in out.iter_mut().enumerate() {
        *o = u8::from_str_radix(s.get(i * 2..i * 2 + 2)?, 16).ok()?;
    }
    Some(out)
}

fn be_u64(b: &[u8]) -> Option<u64> {
    b.get(..8)
        .and_then(|s| <[u8; 8]>::try_from(s).ok())
        .map(u64::from_be_bytes)
}

/// The store over catalog records (module docs).
pub struct RecAudit {
    records: Arc<dyn RecordStore>,
    tenant: TenantId,
    owner: Owner,
    t: Tables,
}

impl std::fmt::Debug for RecAudit {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RecAudit")
            .field("tenant", &self.tenant)
            .field("owner", &self.owner)
            .finish_non_exhaustive()
    }
}

impl RecAudit {
    /// `tenant`'s conversations (the data and DBA assistants', MCP's,
    /// `ASK`'s and the digests'), in its catalog records.
    #[must_use]
    pub fn tenant(records: Arc<dyn RecordStore>, tenant: TenantId) -> Self {
        Self {
            records,
            tenant,
            owner: Owner::Tenant,
            t: TENANT_TABLES,
        }
    }

    /// `operator`'s administration conversations (an operator's login, or
    /// the incident agent's `sprig:incidents`), in the DC's system records.
    #[must_use]
    pub fn system(records: Arc<dyn RecordStore>, operator: &str) -> Self {
        Self {
            records,
            tenant: SYSTEM_TENANT,
            owner: Owner::Operator(login_key(operator)),
            t: SYSTEM_TABLES,
        }
    }

    /// The operator's login, as kept (`None` for a tenant's store).
    #[must_use]
    pub fn operator(&self) -> Option<&str> {
        match &self.owner {
            Owner::Operator(o) => Some(o),
            Owner::Tenant => None,
        }
    }

    async fn begin(&self) -> Result<Box<dyn RecordTx>> {
        self.records.begin(self.tenant).await
    }

    fn owner_pk(&self, user: UserId) -> Pk {
        match &self.owner {
            Owner::Tenant => Pk::new().u32(user.0),
            Owner::Operator(o) => Pk::new().bytes(o.as_bytes()),
        }
    }

    /// The key of `conv` in its owner's newest-first list: the owner, then
    /// the update time and the id, each inverted so a forward scan meets the
    /// newest first (ties: the higher id first).
    fn recent_pk(&self, user: UserId, updated_ms: i64, conv: u64) -> Pk {
        self.owner_pk(user)
            .u64(u64::MAX - u64::try_from(updated_ms.max(0)).unwrap_or(0))
            .u64(u64::MAX - conv)
    }

    /// The owner of `c` as [`Self::owner_pk`] takes it.
    fn owner_of(c: &ConvRec) -> UserId {
        UserId(c.user)
    }

    /// Whether any note still holds a payload (the purge's cheap check).
    ///
    /// # Errors
    /// The store's.
    pub async fn has_payloads(&self) -> Result<bool> {
        let mut tx = self.begin().await?;
        let lo = self.t.pay.prefix();
        let hi = prefix_end(&lo);
        Ok(!tx.scan(&lo, &hi, 1).await?.is_empty())
    }

    /// One purge transaction: up to [`MAX_PURGE`] notes older than `cut`
    /// (ms); returns (rewritten, looked at).
    async fn purge_batch(&self, cut: u64) -> Result<(u64, usize)> {
        let mut tx = self.begin().await?;
        let lo = self.t.pay.prefix();
        let hi = self.t.pay.key(&Pk::new().u64(cut));
        let due = tx.scan(&lo, &hi, MAX_PURGE).await?;
        let seen = due.len();
        let mut n = 0u64;
        for (k, v) in due {
            let (Some(conv), Some(msg)) = (
                be_u64(&v),
                k.len().checked_sub(8).and_then(|at| be_u64(&k[at..])),
            ) else {
                tx.delete(&k).await?;
                continue;
            };
            let mpk = Pk::new().u64(conv).u64(msg);
            if let Some(mut m) = self.t.msg.get_for_update::<MsgRec>(&mut *tx, &mpk).await?
                && m.body.get("payload").is_some()
            {
                m.body = json!({
                    "sha256": m.sent_hash.clone().unwrap_or_default(),
                    "payload_purged": true,
                });
                self.t.msg.put(&mut *tx, &mpk, &m).await?;
                n += 1;
            }
            tx.delete(&k).await?;
        }
        tx.commit().await?;
        Ok((n, seen))
    }

    fn check_role(&self, role: Role) -> Result<&'static str> {
        match self.owner {
            Owner::Tenant => tenant_role(role),
            Owner::Operator(_) => system_role(role),
        }
    }

    fn owns(&self, c: &ConvRec, user: UserId) -> bool {
        match &self.owner {
            Owner::Tenant => c.user == user.0,
            Owner::Operator(o) => c.operator == *o,
        }
    }

    fn conversation(id: u64, c: ConvRec) -> Option<Conversation> {
        Some(Conversation {
            id: ConvId(id),
            user: c.user,
            role: Role::parse(&c.role)?,
            title: c.title,
            created_ms: c.created_ms,
            updated_ms: c.updated_ms,
        })
    }

    async fn conv_of(
        &self,
        tx: &mut dyn RecordTx,
        user: UserId,
        id: u64,
    ) -> Result<Option<ConvRec>> {
        let c: Option<ConvRec> = self.t.conv.get(tx, &Pk::new().u64(id)).await?;
        Ok(c.filter(|c| self.owns(c, user)))
    }

    async fn msg_conv(&self, tx: &mut dyn RecordTx, msg: u64) -> Result<Option<u64>> {
        Ok(tx
            .get(&self.t.mconv.key(&Pk::new().u64(msg)))
            .await?
            .and_then(|v| be_u64(&v)))
    }

    /// Adds one call's tokens to today's usage under `role` as a label
    /// (`functions` for the AI functions in statements). The tokens count in
    /// [`AuditStore::usage_today`] like an assistant's.
    ///
    /// # Errors
    /// The store's (39070 when it can't be reached).
    pub async fn add_usage_as(&self, user: UserId, role: &str, usage: &Usage) -> Result<()> {
        let mut pk = Pk::new().u32(day_of(now_ms()));
        pk.0.extend_from_slice(&self.owner_pk(user).0);
        let pk = pk.bytes(role.as_bytes());
        let mut tx = self.begin().await?;
        let mut u: UsageRec = self
            .t
            .usage
            .get_for_update(&mut *tx, &pk)
            .await?
            .unwrap_or_default();
        u.input = u
            .input
            .saturating_add(usage.input_tokens + usage.cache_write_tokens);
        u.output = u.output.saturating_add(usage.output_tokens);
        u.calls = u.calls.saturating_add(1);
        self.t.usage.put(&mut *tx, &pk, &u).await?;
        tx.commit().await
    }

    /// The tokens used on day `day` (whole UTC days since the epoch) by
    /// everyone and by `user`.
    async fn usage_on(&self, day: u32, user: UserId) -> Result<UsageToday> {
        let mut tx = self.begin().await?;
        let under = Pk::new().u32(day);
        let mine = {
            let mut k = under.0.clone();
            k.extend_from_slice(&self.owner_pk(user).0);
            k
        };
        let mut out = UsageToday::default();
        for (pk, u) in self.t.usage.scan::<UsageRec>(&mut *tx, &under).await? {
            let n = u.input.saturating_add(u.output);
            out.tenant = out.tenant.saturating_add(n);
            if pk.0.starts_with(&mine) {
                out.user = out.user.saturating_add(n);
            }
        }
        Ok(out)
    }
}

#[async_trait]
impl AuditStore for RecAudit {
    async fn create(&self, user: UserId, role: Role, title: &str) -> Result<ConvId> {
        let role = self.check_role(role)?;
        let now = now_ms();
        let mut tx = self.begin().await?;
        let id = next_ids(&mut *tx, &self.t.counter("conv"), 1).await?;
        let (user_id, operator) = match &self.owner {
            Owner::Tenant => (user.0, String::new()),
            Owner::Operator(o) => (0, o.clone()),
        };
        let rec = ConvRec {
            user: user_id,
            operator,
            role: role.to_owned(),
            title: title_of(title),
            created_ms: now,
            updated_ms: now,
        };
        self.t.conv.put(&mut *tx, &Pk::new().u64(id), &rec).await?;
        self.t
            .recent
            .put(&mut *tx, &self.recent_pk(user, now, id), &rec)
            .await?;
        tx.commit().await?;
        Ok(ConvId(id))
    }

    async fn get(&self, user: UserId, id: ConvId) -> Result<Option<Conversation>> {
        let mut tx = self.begin().await?;
        Ok(self
            .conv_of(&mut *tx, user, id.0)
            .await?
            .and_then(|c| Self::conversation(id.0, c)))
    }

    async fn list(&self, user: UserId, limit: u32) -> Result<Vec<Conversation>> {
        let mut tx = self.begin().await?;
        // One range of the owner's newest-first list, `limit` records.
        let lo = self.t.recent.key(&self.owner_pk(user));
        let hi = prefix_end(&lo);
        let mut out = Vec::new();
        for (k, v) in tx.scan(&lo, &hi, limit as usize).await? {
            let Some(id) = k
                .len()
                .checked_sub(8)
                .and_then(|at| be_u64(&k[at..]))
                .map(|inv| u64::MAX - inv)
            else {
                continue;
            };
            let Ok(c) = serde_json::from_slice::<ConvRec>(&v) else {
                continue;
            };
            if self.owns(&c, user)
                && let Some(c) = Self::conversation(id, c)
            {
                out.push(c);
            }
        }
        Ok(out)
    }

    async fn append(
        &self,
        conv: ConvId,
        kind: MsgKind,
        body: &Value,
        sent_hash: Option<[u8; 32]>,
    ) -> Result<u64> {
        let now = now_ms();
        let mut tx = self.begin().await?;
        let cpk = Pk::new().u64(conv.0);
        let Some(mut c) = self
            .t
            .conv
            .get_for_update::<ConvRec>(&mut *tx, &cpk)
            .await?
        else {
            return Err(SprigError::internal(format!(
                "there is no conversation {}",
                conv.0
            )));
        };
        let id = next_ids(&mut *tx, &self.t.counter("msg"), 1).await?;
        let rec = MsgRec {
            kind: kind.as_str().to_owned(),
            body: body.clone(),
            sent_hash: sent_hash.as_ref().map(hex),
            created_ms: now,
        };
        self.t
            .msg
            .put(&mut *tx, &Pk::new().u64(conv.0).u64(id), &rec)
            .await?;
        tx.put(&self.t.mconv.key(&Pk::new().u64(id)), &conv.0.to_be_bytes())
            .await?;
        if kind == MsgKind::Proposal
            && let Some(pid) = body.get("id").and_then(Value::as_str)
        {
            self.t
                .prop
                .put(
                    &mut *tx,
                    &Pk::new().bytes(pid.as_bytes()),
                    &PropRec {
                        conv: conv.0,
                        msg: id,
                    },
                )
                .await?;
        }
        if kind == MsgKind::SystemNote && sent_hash.is_some() && body.get("payload").is_some() {
            tx.put(
                &self
                    .t
                    .pay
                    .key(&Pk::new().u64(u64::try_from(now).unwrap_or(0)).u64(id)),
                &conv.0.to_be_bytes(),
            )
            .await?;
        }
        // The owner's list: from the old update time to the new.
        let owner = Self::owner_of(&c);
        self.t
            .recent
            .delete(&mut *tx, &self.recent_pk(owner, c.updated_ms, conv.0))
            .await?;
        c.updated_ms = now;
        self.t.conv.put(&mut *tx, &cpk, &c).await?;
        self.t
            .recent
            .put(&mut *tx, &self.recent_pk(owner, now, conv.0), &c)
            .await?;
        tx.commit().await?;
        Ok(id)
    }

    async fn messages(&self, conv: ConvId) -> Result<Vec<StoredMessage>> {
        let mut tx = self.begin().await?;
        let rows = self
            .t
            .msg
            .scan::<MsgRec>(&mut *tx, &Pk::new().u64(conv.0))
            .await?;
        Ok(rows
            .into_iter()
            .filter_map(|(pk, m)| {
                Some(StoredMessage {
                    id: be_u64(pk.0.get(8..)?)?,
                    kind: MsgKind::parse(&m.kind)?,
                    body: m.body,
                    sent_hash: m.sent_hash.as_deref().and_then(unhex),
                    created_ms: m.created_ms,
                })
            })
            .collect())
    }

    async fn add_usage(&self, user: UserId, role: Role, usage: &Usage) -> Result<()> {
        self.add_usage_as(user, role.as_str(), usage).await
    }

    async fn usage_today(&self, user: UserId) -> Result<UsageToday> {
        self.usage_on(day_of(now_ms()), user).await
    }

    async fn purge_payloads(&self, days: u64) -> Result<u64> {
        let cut = now_ms()
            .saturating_sub(i64::try_from(days.saturating_mul(86_400_000)).unwrap_or(i64::MAX));
        let cut = u64::try_from(cut.max(0)).unwrap_or(0);
        // Short transactions until one comes back short, so no payload
        // outlives its retention however many notes an hour brings.
        let mut total = 0u64;
        loop {
            let (n, seen) = self.purge_batch(cut).await?;
            total += n;
            if seen < MAX_PURGE {
                return Ok(total);
            }
        }
    }
}

impl RecAudit {
    async fn record_feedback(&self, tx: &mut dyn RecordTx, user: UserId, f: FbRec) -> Result<()> {
        let id = next_ids(tx, &self.t.counter("fb"), 1).await?;
        let mut f = f;
        match &self.owner {
            Owner::Tenant => f.user = user.0,
            Owner::Operator(o) => o.clone_into(&mut f.operator),
        }
        self.t.fb.put(tx, &Pk::new().u64(id), &f).await
    }

    /// The proposal `proposal_id`'s conversation, message and stored body,
    /// if it is in one of `user`'s conversations.
    async fn find_proposal(
        &self,
        tx: &mut dyn RecordTx,
        user: UserId,
        proposal_id: &str,
    ) -> Result<Option<(ConvRec, u64, MsgRec)>> {
        let Some(p) = self
            .t
            .prop
            .get::<PropRec>(tx, &Pk::new().bytes(proposal_id.as_bytes()))
            .await?
        else {
            return Ok(None);
        };
        let Some(c) = self.conv_of(tx, user, p.conv).await? else {
            return Ok(None);
        };
        let Some(m) = self
            .t
            .msg
            .get::<MsgRec>(tx, &Pk::new().u64(p.conv).u64(p.msg))
            .await?
        else {
            return Ok(None);
        };
        Ok(Some((c, p.msg, m)))
    }
}

#[async_trait]
impl FeedbackStore for RecAudit {
    async fn vote(
        &self,
        user: UserId,
        msg_id: u64,
        vote: i8,
        note: Option<&str>,
    ) -> Result<Option<Role>> {
        let mut tx = self.begin().await?;
        let Some(conv) = self.msg_conv(&mut *tx, msg_id).await? else {
            return Ok(None);
        };
        let Some(c) = self.conv_of(&mut *tx, user, conv).await? else {
            return Ok(None);
        };
        let m: Option<MsgRec> = self
            .t
            .msg
            .get(&mut *tx, &Pk::new().u64(conv).u64(msg_id))
            .await?;
        if m.as_ref()
            .is_none_or(|m| m.kind != MsgKind::Assistant.as_str())
        {
            return Ok(None);
        }
        let Some(role) = Role::parse(&c.role) else {
            return Ok(None);
        };
        let f = FbRec {
            user: 0,
            operator: String::new(),
            msg_id: Some(msg_id),
            proposal_id: None,
            vote: Some(vote),
            outcome: None,
            note: clean_note(note),
            created_ms: now_ms(),
        };
        self.record_feedback(&mut *tx, user, f).await?;
        tx.commit().await?;
        Ok(Some(role))
    }

    async fn outcome(
        &self,
        user: UserId,
        proposal_id: &str,
        outcome: Outcome,
        note: Option<&str>,
    ) -> Result<Option<(Role, String)>> {
        let mut tx = self.begin().await?;
        let Some((c, msg, m)) = self.find_proposal(&mut *tx, user, proposal_id).await? else {
            return Ok(None);
        };
        let Some(role) = Role::parse(&c.role) else {
            return Ok(None);
        };
        let kind = m.body["kind"].as_str().unwrap_or_default().to_owned();
        let f = FbRec {
            user: 0,
            operator: String::new(),
            msg_id: None,
            proposal_id: Some(msg),
            vote: None,
            outcome: Some(outcome.as_str().to_owned()),
            note: clean_note(note),
            created_ms: now_ms(),
        };
        self.record_feedback(&mut *tx, user, f).await?;
        self.t
            .pout
            .put(&mut *tx, &Pk::new().u64(msg), outcome.as_str())
            .await?;
        tx.commit().await?;
        Ok(Some((role, kind)))
    }

    async fn recent(&self, limit: u32) -> Result<Vec<Note>> {
        let mut tx = self.begin().await?;
        // Only the newest ids, by the counter (ids only move up).
        let last = tx
            .get(&sprig_catalog::records::counter_key(&self.t.counter("fb")))
            .await?
            .and_then(|v| be_u64(&v))
            .unwrap_or(0);
        if last == 0 {
            return Ok(Vec::new());
        }
        let from = last.saturating_sub(MAX_FEEDBACK_SCAN as u64 - 1).max(1);
        let lo = self.t.fb.key(&Pk::new().u64(from));
        let hi = prefix_end(&self.t.fb.prefix());
        let rows = scan_all(&mut *tx, &lo, &hi).await?;
        let mut out = Vec::new();
        for (_, raw) in rows.into_iter().rev() {
            if out.len() >= limit as usize {
                break;
            }
            let Ok(f) = serde_json::from_slice::<FbRec>(&raw) else {
                continue;
            };
            if f.outcome.is_none() && f.note.is_none() {
                continue;
            }
            let body = match f.proposal_id {
                Some(msg) => match self.msg_conv(&mut *tx, msg).await? {
                    Some(conv) => self
                        .t
                        .msg
                        .get::<MsgRec>(&mut *tx, &Pk::new().u64(conv).u64(msg))
                        .await?
                        .map(|m| m.body),
                    None => None,
                },
                None => None,
            };
            out.push(note_of(f.vote, f.outcome.as_deref(), f.note, body.as_ref()));
        }
        Ok(out)
    }

    async fn proposal(&self, user: UserId, proposal_id: &str) -> Result<Option<Found>> {
        let mut tx = self.begin().await?;
        let Some((c, msg, m)) = self.find_proposal(&mut *tx, user, proposal_id).await? else {
            return Ok(None);
        };
        let Some(role) = Role::parse(&c.role) else {
            return Ok(None);
        };
        let decided = self
            .t
            .pout
            .get::<String>(&mut *tx, &Pk::new().u64(msg))
            .await?
            .as_deref()
            .and_then(Outcome::parse);
        Ok(Some(Found {
            msg_id: msg,
            role,
            proposal: m.body,
            decided,
        }))
    }
}

#[cfg(test)]
mod tests;
