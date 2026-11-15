//! The administration assistant's conversations, in the system schema
//! `__sprig_ai` ([`AI_SCHEMA`]) on one cluster of the DC.
//!
//! - **The same tables** as a tenant's `ai` module (`ai_conversations`,
//!   `ai_messages`, `ai_usage`, `ai_feedback`), except that a row belongs
//!   to an operator's login (`tenant.user`, lower case) instead of a
//!   tenant user id, and `role` is `admin` or `triage`.
//! - **Where.** The DC's system cluster: its lowest-numbered cluster that
//!   is active, draining or drained (never one that is still probing, was
//!   rejected or is retired). The controller and the gateways pick it the
//!   same way.
//! - **Who.** `sprig_ctl` owns the schema: the controller creates it (and
//!   so may the first gateway that needs it), the gateways read and write
//!   it for platform operators only, and no tenant account has a grant on
//!   it. `sprig_ctl` needs [`CTL_GRANT`].
//! - **Whose.** A [`SystemAudit`] is opened for one operator's login; it
//!   lists, reads and rates only that operator's conversations. The
//!   `UserId` arguments of [`AuditStore`] are ignored.
//! - **Usage and budgets.** [`AuditStore::usage_today`] counts every
//!   operator's tokens today as the "tenant" total and the operator's own
//!   as the user's, so the global `ai.budget.tenant_tokens_day` caps all
//!   administration turns of the DC and `ai.budget.user_tokens_day` each
//!   operator.
//! - **Retention.** The controller drops payloads older than the global
//!   `ai.audit_payload_days` every hour, keeping their hashes, as for the
//!   tenants.

use async_trait::async_trait;
use mysql_async::prelude::Queryable as _;
use serde_json::Value;
use sprig_common::ids::UserId;
use sprig_common::names::{AI_SCHEMA, quote};
use sprig_common::{Result, SprigCode, SprigError};
use tokio::sync::OnceCell;

use super::{
    AuditStore, ConvId, ConvRow, Conversation, MsgKind, MsgRow, StoredMessage, ms, purge_sql,
    title_of,
};
use crate::budget::UsageToday;
use crate::feedback::{FeedbackStore, Note, Outcome, clean_note};
use crate::provider::Usage;
use crate::role::Role;

/// The grant `sprig_ctl` needs on the schema, for messages.
pub const CTL_GRANT: &str = "GRANT ALL PRIVILEGES ON `__sprig_ai`.* TO 'sprig_ctl'@'%'";

/// The longest operator login kept.
pub const MAX_LOGIN: usize = 200;

/// The schema's statements, all `IF NOT EXISTS`, for `schema`
/// ([`AI_SCHEMA`] outside tests).
#[must_use]
pub fn ddl(schema: &str) -> Vec<String> {
    let s = quote(schema);
    vec![
        format!("CREATE DATABASE IF NOT EXISTS {s} DEFAULT CHARACTER SET utf8mb4"),
        format!(
            "CREATE TABLE IF NOT EXISTS {s}.`ai_conversations` (
  conv_id    BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
  operator   VARCHAR(200) NOT NULL,
  role       ENUM('admin','triage') NOT NULL,
  title      VARCHAR(200) NOT NULL,
  created_at DATETIME(3) NOT NULL,
  updated_at DATETIME(3) NOT NULL,
  KEY by_operator (operator, updated_at)
) ENGINE=InnoDB"
        ),
        format!(
            "CREATE TABLE IF NOT EXISTS {s}.`ai_messages` (
  msg_id     BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
  conv_id    BIGINT UNSIGNED NOT NULL,
  kind       ENUM('user','assistant','tool_call','tool_result','proposal','system_note') NOT NULL,
  body       JSON NOT NULL,
  sent_hash  BINARY(32) NULL,
  created_at DATETIME(3) NOT NULL,
  KEY by_conv (conv_id, msg_id)
) ENGINE=InnoDB"
        ),
        format!(
            "CREATE TABLE IF NOT EXISTS {s}.`ai_usage` (
  day           DATE NOT NULL,
  operator      VARCHAR(200) NOT NULL,
  role          VARCHAR(16) NOT NULL,
  input_tokens  BIGINT UNSIGNED NOT NULL,
  output_tokens BIGINT UNSIGNED NOT NULL,
  calls         INT UNSIGNED NOT NULL,
  PRIMARY KEY (day, operator, role)
) ENGINE=InnoDB"
        ),
        format!(
            "CREATE TABLE IF NOT EXISTS {s}.`ai_feedback` (
  fb_id       BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
  operator    VARCHAR(200) NOT NULL,
  msg_id      BIGINT UNSIGNED NULL,
  proposal_id BIGINT UNSIGNED NULL,
  vote        TINYINT NULL,
  outcome     ENUM('accepted','dismissed','failed') NULL,
  note        VARCHAR(500) NULL,
  created_at  DATETIME(3) NOT NULL
) ENGINE=InnoDB"
        ),
    ]
}

/// `Ok` for the roles kept here: `admin` and `triage`.
pub(crate) fn system_role(role: Role) -> Result<&'static str> {
    if role.is_tenant() {
        Err(SprigError::internal(
            "tenant assistants keep their conversations in the tenant's catalog",
        ))
    } else {
        Ok(role.as_str())
    }
}

/// The store's error for `e`: a missing grant names [`CTL_GRANT`].
#[must_use]
pub fn store_error(what: &str, e: mysql_async::Error) -> SprigError {
    let denied =
        matches!(&e, mysql_async::Error::Server(s) if matches!(s.code, 1044 | 1142 | 1227));
    if denied {
        return SprigError::new(
            SprigCode::ClusterPrecheckFailed,
            format!(
                "administration conversations: {what}: sprig_ctl may not manage {AI_SCHEMA} \
                 ({e}); run as an administrator: {CTL_GRANT}"
            ),
        );
    }
    SprigError::new(
        SprigCode::CatalogUnavailable,
        format!("administration conversations: {what}: the system cluster is unreachable"),
    )
    .with_source(e)
}

/// Creates `schema` and its tables when `ai_messages` is missing.
///
/// # Errors
/// [`store_error`]'s.
pub async fn ensure_schema(conn: &mut mysql_async::Conn, schema: &str) -> Result<()> {
    let have: Option<u64> = conn
        .exec_first(
            "SELECT COUNT(*) FROM information_schema.TABLES \
             WHERE TABLE_SCHEMA = ? AND TABLE_NAME = 'ai_messages'",
            (schema,),
        )
        .await
        .map_err(|e| store_error("looking for the schema", e))?;
    if have.unwrap_or(0) > 0 {
        return Ok(());
    }
    for sql in ddl(schema) {
        conn.query_drop(sql)
            .await
            .map_err(|e| store_error("creating the schema", e))?;
    }
    Ok(())
}

/// Drops payloads older than `days` from every operator's notes in
/// `schema`, keeping the hashes; returns how many.
///
/// # Errors
/// [`store_error`]'s.
pub async fn purge_payloads(conn: &mut mysql_async::Conn, schema: &str, days: u64) -> Result<u64> {
    let messages = format!("{}.`ai_messages`", quote(schema));
    conn.exec_drop(purge_sql(&messages), (days,))
        .await
        .map_err(|e| store_error("purging payloads", e))?;
    Ok(conn.affected_rows())
}

/// An operator's login as it is kept: trimmed, lower case, at most
/// [`MAX_LOGIN`] characters.
#[must_use]
pub fn login_key(login: &str) -> String {
    login
        .trim()
        .to_lowercase()
        .chars()
        .take(MAX_LOGIN)
        .collect()
}

/// One operator's view of the store (module docs).
pub struct SystemAudit {
    pool: mysql_async::Pool,
    schema: String,
    operator: String,
    ready: OnceCell<()>,
}

impl std::fmt::Debug for SystemAudit {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SystemAudit")
            .field("schema", &self.schema)
            .field("operator", &self.operator)
            .finish_non_exhaustive()
    }
}

impl SystemAudit {
    /// `operator`'s conversations in [`AI_SCHEMA`], reached through
    /// `pool` (`sprig_ctl` on the system cluster's writer).
    #[must_use]
    pub fn new(pool: mysql_async::Pool, operator: &str) -> Self {
        Self::in_schema(pool, AI_SCHEMA, operator)
    }

    /// The same in another schema (tests).
    #[must_use]
    pub fn in_schema(pool: mysql_async::Pool, schema: impl Into<String>, operator: &str) -> Self {
        Self {
            pool,
            schema: schema.into(),
            operator: login_key(operator),
            ready: OnceCell::new(),
        }
    }

    /// The operator's login, as kept.
    #[must_use]
    pub fn operator(&self) -> &str {
        &self.operator
    }

    fn t(&self, table: &str) -> String {
        format!("{}.{}", quote(&self.schema), quote(table))
    }

    async fn conn(&self) -> Result<mysql_async::Conn> {
        let mut conn = self
            .pool
            .get_conn()
            .await
            .map_err(|e| store_error("connecting", e))?;
        self.ready
            .get_or_try_init(|| ensure_schema(&mut conn, &self.schema))
            .await?;
        Ok(conn)
    }

    fn conv(r: ConvRow) -> Option<Conversation> {
        Some(Conversation {
            id: ConvId(r.0),
            user: r.1,
            role: Role::parse(&r.2)?,
            title: r.3,
            created_ms: r.4,
            updated_ms: r.5,
        })
    }

    async fn conv_rows(&self, where_: &str, params: mysql_async::Params) -> Result<Vec<ConvRow>> {
        let mut c = self.conn().await?;
        c.exec(
            format!(
                "SELECT conv_id, 0, role, title, {}, {} FROM {} WHERE operator = ? {where_}",
                ms("created_at"),
                ms("updated_at"),
                self.t("ai_conversations")
            ),
            params,
        )
        .await
        .map_err(|e| store_error("reading conversations", e))
    }
}

#[async_trait]
impl AuditStore for SystemAudit {
    async fn create(&self, _user: UserId, role: Role, title: &str) -> Result<ConvId> {
        let role = system_role(role)?;
        let mut c = self.conn().await?;
        c.exec_drop(
            format!(
                "INSERT INTO {} (operator, role, title, created_at, updated_at) \
                 VALUES (?, ?, ?, UTC_TIMESTAMP(3), UTC_TIMESTAMP(3))",
                self.t("ai_conversations")
            ),
            (self.operator.as_str(), role, title_of(title)),
        )
        .await
        .map_err(|e| store_error("starting a conversation", e))?;
        Ok(ConvId(c.last_insert_id().unwrap_or(0)))
    }

    async fn get(&self, _user: UserId, id: ConvId) -> Result<Option<Conversation>> {
        let rows = self
            .conv_rows("AND conv_id = ?", (self.operator.as_str(), id.0).into())
            .await?;
        Ok(rows.into_iter().find_map(Self::conv))
    }

    async fn list(&self, _user: UserId, limit: u32) -> Result<Vec<Conversation>> {
        let rows = self
            .conv_rows(
                "ORDER BY updated_at DESC, conv_id DESC LIMIT ?",
                (self.operator.as_str(), limit).into(),
            )
            .await?;
        Ok(rows.into_iter().filter_map(Self::conv).collect())
    }

    async fn append(
        &self,
        conv: ConvId,
        kind: MsgKind,
        body: &Value,
        sent_hash: Option<[u8; 32]>,
    ) -> Result<u64> {
        let mut c = self.conn().await?;
        c.exec_drop(
            format!(
                "INSERT INTO {} (conv_id, kind, body, sent_hash, created_at) \
                 VALUES (?, ?, ?, ?, UTC_TIMESTAMP(3))",
                self.t("ai_messages")
            ),
            (
                conv.0,
                kind.as_str(),
                body.to_string(),
                sent_hash.map(|h| h.to_vec()),
            ),
        )
        .await
        .map_err(|e| store_error("appending a message", e))?;
        let id = c.last_insert_id().unwrap_or(0);
        c.exec_drop(
            format!(
                "UPDATE {} SET updated_at = UTC_TIMESTAMP(3) WHERE conv_id = ?",
                self.t("ai_conversations")
            ),
            (conv.0,),
        )
        .await
        .map_err(|e| store_error("appending a message", e))?;
        Ok(id)
    }

    async fn messages(&self, conv: ConvId) -> Result<Vec<StoredMessage>> {
        let mut c = self.conn().await?;
        let rows: Vec<MsgRow> = c
            .exec(
                format!(
                    "SELECT msg_id, kind, body, sent_hash, {} FROM {} \
                     WHERE conv_id = ? ORDER BY msg_id",
                    ms("created_at"),
                    self.t("ai_messages")
                ),
                (conv.0,),
            )
            .await
            .map_err(|e| store_error("reading messages", e))?;
        Ok(rows
            .into_iter()
            .filter_map(|(id, kind, body, hash, at)| {
                Some(StoredMessage {
                    id,
                    kind: MsgKind::parse(&kind)?,
                    body: serde_json::from_str(&body).unwrap_or(Value::Null),
                    sent_hash: hash.and_then(|h| <[u8; 32]>::try_from(h.as_slice()).ok()),
                    created_ms: at,
                })
            })
            .collect())
    }

    async fn add_usage(&self, _user: UserId, role: Role, usage: &Usage) -> Result<()> {
        let mut c = self.conn().await?;
        c.exec_drop(
            format!(
                "INSERT INTO {} (day, operator, role, input_tokens, output_tokens, calls) \
                 VALUES (UTC_DATE(), ?, ?, ?, ?, 1) \
                 ON DUPLICATE KEY UPDATE input_tokens = input_tokens + VALUES(input_tokens), \
                 output_tokens = output_tokens + VALUES(output_tokens), calls = calls + 1",
                self.t("ai_usage")
            ),
            (
                self.operator.as_str(),
                role.as_str(),
                usage.input_tokens + usage.cache_write_tokens,
                usage.output_tokens,
            ),
        )
        .await
        .map_err(|e| store_error("recording usage", e))?;
        Ok(())
    }

    async fn usage_today(&self, _user: UserId) -> Result<UsageToday> {
        let mut c = self.conn().await?;
        let r: Option<(Option<u64>, Option<u64>)> = c
            .exec_first(
                format!(
                    "SELECT CAST(SUM(input_tokens + output_tokens) AS UNSIGNED), \
                     CAST(SUM(IF(operator = ?, input_tokens + output_tokens, 0)) AS UNSIGNED) \
                     FROM {} WHERE day = UTC_DATE()",
                    self.t("ai_usage")
                ),
                (self.operator.as_str(),),
            )
            .await
            .map_err(|e| store_error("reading usage", e))?;
        let (t, u) = r.unwrap_or_default();
        Ok(UsageToday {
            tenant: t.unwrap_or(0),
            user: u.unwrap_or(0),
        })
    }

    async fn purge_payloads(&self, days: u64) -> Result<u64> {
        let mut c = self.conn().await?;
        purge_payloads(&mut c, &self.schema, days).await
    }
}

#[async_trait]
impl FeedbackStore for SystemAudit {
    async fn vote(
        &self,
        _user: UserId,
        msg_id: u64,
        vote: i8,
        note: Option<&str>,
    ) -> Result<Option<Role>> {
        let mut c = self.conn().await?;
        let role: Option<String> = c
            .exec_first(
                format!(
                    "SELECT c.role FROM {} m JOIN {} c ON c.conv_id = m.conv_id \
                     WHERE m.msg_id = ? AND m.kind = 'assistant' AND c.operator = ?",
                    self.t("ai_messages"),
                    self.t("ai_conversations")
                ),
                (msg_id, self.operator.as_str()),
            )
            .await
            .map_err(|e| store_error("reading an answer", e))?;
        let Some(role) = role.as_deref().and_then(Role::parse) else {
            return Ok(None);
        };
        c.exec_drop(
            format!(
                "INSERT INTO {} (operator, msg_id, vote, note, created_at) \
                 VALUES (?, ?, ?, ?, UTC_TIMESTAMP(3))",
                self.t("ai_feedback")
            ),
            (self.operator.as_str(), msg_id, vote, clean_note(note)),
        )
        .await
        .map_err(|e| store_error("recording feedback", e))?;
        Ok(Some(role))
    }

    async fn outcome(
        &self,
        _user: UserId,
        proposal_id: &str,
        outcome: Outcome,
        note: Option<&str>,
    ) -> Result<Option<(Role, String)>> {
        let mut c = self.conn().await?;
        let row: Option<(u64, String, Option<String>)> = c
            .exec_first(
                format!(
                    "SELECT m.msg_id, c.role, JSON_UNQUOTE(JSON_EXTRACT(m.body, '$.kind')) \
                     FROM {} c JOIN {} m ON m.conv_id = c.conv_id \
                     WHERE c.operator = ? AND m.kind = 'proposal' \
                     AND JSON_UNQUOTE(JSON_EXTRACT(m.body, '$.id')) = ?",
                    self.t("ai_conversations"),
                    self.t("ai_messages")
                ),
                (self.operator.as_str(), proposal_id),
            )
            .await
            .map_err(|e| store_error("reading a proposal", e))?;
        let Some((msg_id, role, kind)) = row else {
            return Ok(None);
        };
        let Some(role) = Role::parse(&role) else {
            return Ok(None);
        };
        c.exec_drop(
            format!(
                "INSERT INTO {} (operator, proposal_id, outcome, note, created_at) \
                 VALUES (?, ?, ?, ?, UTC_TIMESTAMP(3))",
                self.t("ai_feedback")
            ),
            (
                self.operator.as_str(),
                msg_id,
                outcome.as_str(),
                clean_note(note),
            ),
        )
        .await
        .map_err(|e| store_error("recording feedback", e))?;
        Ok(Some((role, kind.unwrap_or_default())))
    }

    async fn recent(&self, limit: u32) -> Result<Vec<Note>> {
        let mut c = self.conn().await?;
        let rows: Vec<(Option<i8>, Option<String>, Option<String>, Option<String>)> = c
            .exec(
                format!(
                    "SELECT f.vote, f.outcome, f.note, CAST(m.body AS CHAR) FROM {} f \
                     LEFT JOIN {} m ON m.msg_id = f.proposal_id \
                     WHERE f.outcome IS NOT NULL OR f.note IS NOT NULL \
                     ORDER BY f.fb_id DESC LIMIT ?",
                    self.t("ai_feedback"),
                    self.t("ai_messages")
                ),
                (limit,),
            )
            .await
            .map_err(|e| store_error("reading feedback", e))?;
        Ok(rows
            .into_iter()
            .map(|(vote, outcome, note, body)| {
                let p: Option<Value> = body.and_then(|b| serde_json::from_str(&b).ok());
                crate::feedback::note_of(vote, outcome.as_deref(), note, p.as_ref())
            })
            .collect())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_schema_keeps_admin_roles_and_operator_logins() {
        let sql = ddl("__sprig_ai").join("\n");
        assert!(sql.starts_with("CREATE DATABASE IF NOT EXISTS `__sprig_ai`"));
        assert!(sql.contains("role       ENUM('admin','triage') NOT NULL"));
        assert!(sql.contains("PRIMARY KEY (day, operator, role)"));
        assert!(!sql.contains("user_id"));
        assert_eq!(ddl("x").len(), 5);
        assert!(system_role(Role::Admin).is_ok() && system_role(Role::Triage).is_ok());
        assert!(system_role(Role::Data).is_err() && system_role(Role::Dba).is_err());
        assert_eq!(login_key("  Acme.Alice "), "acme.alice");
        assert_eq!(login_key(&"x".repeat(300)).len(), MAX_LOGIN);
    }

    #[tokio::test]
    async fn the_memory_store_keeps_admin_conversations_only() {
        let m = crate::MemoryAudit::system();
        assert!(m.create(UserId(1), Role::Data, "x").await.is_err());
        let c = m.create(UserId(1), Role::Admin, "disk").await.unwrap();
        assert_eq!(
            m.get(UserId(1), c).await.unwrap().unwrap().role,
            Role::Admin
        );
        assert!(m.get(UserId(2), c).await.unwrap().is_none());
        let t = crate::MemoryAudit::default();
        assert!(t.create(UserId(1), Role::Admin, "x").await.is_err());
    }
}

#[cfg(test)]
mod live_tests;
