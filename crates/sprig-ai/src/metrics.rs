//! The `sprig_ai_*` metrics this crate records.
//!
//! - `sprig_ai_requests_total{role,provider,model,result}`: model calls;
//!   `result` is `ok`, `refused` or `error`.
//! - `sprig_ai_tokens_total{role,direction}`: `input`, `output`,
//!   `cache_read` or `cache_write`.
//! - `sprig_ai_turn_seconds{role}`: one turn, model calls and tools.
//! - `sprig_ai_tool_calls_total{tool,result}`: `ok`, `error`, `denied`
//!   or `proposed`.
//! - `sprig_ai_egress_reduced_total{role}`: tool results the egress gate
//!   reduced (rows or values withheld, or cut).
//! - `sprig_ai_proposals_total{kind,outcome}`: proposals; this crate
//!   records `shown`.
//! - `sprig_ai_budget_stops_total`: turns stopped by a daily budget.
//! - `sprig_ai_feedback_total{role,vote}`: thumbs up or down on answers.
//! - `sprig_ai_payloads_purged_total`: exact payloads dropped after
//!   `ai.audit_payload_days` (their hashes stay).
//! - `sprig_ai_mcp_requests_total{method,result}`: MCP requests;
//!   `result` is `ok`, `error` or `refused` (signed out, turned off,
//!   malformed).
//! - `sprig_ai_mcp_tool_calls_total{role,tool,result}`: MCP tool calls;
//!   `result` is `ok`, `error`, `denied`, `proposed` or `limited`.
//! - `sprig_ai_incidents_total{severity}`: advice items the administration
//!   agent filed.
//! - `sprig_ai_incident_triage_total{verdict}`: incidents triaged:
//!   `noise`, `needs_advice` or `critical` (triage skipped).
//! - `sprig_ai_incidents_held_total{reason}`: incidents that couldn't go
//!   further in a pass: `advice_cap`, `triage_cap`, `budget`, `no_model`,
//!   `error` or `expired` (pending for a day).
//! - `sprig_ai_intake_flushes_total{result}`: a process's writes of its
//!   error counts: `ok` or `error`.
//! - `sprig_ai_digests_total{result}`: tenant digests: `worded`, `facts`
//!   (stored with the facts alone: no model, a spent budget or a failed
//!   call) or `error`.
//! - `sprig_ai_digest_emails_total{result}`: digests emailed: `sent`,
//!   `skipped` (no SMTP or no subscriber) or `failed`.

use std::time::Duration;

use crate::provider::Usage;
use crate::role::Role;

/// One model call.
pub fn request(role: Role, provider: &str, model: &str, result: &'static str) {
    metrics::counter!(
        "sprig_ai_requests_total",
        "role" => role.as_str(),
        "provider" => provider.to_owned(),
        "model" => model.to_owned(),
        "result" => result
    )
    .increment(1);
}

/// One call's tokens.
pub fn tokens(role: Role, u: &Usage) {
    for (direction, n) in [
        ("input", u.input_tokens),
        ("output", u.output_tokens),
        ("cache_read", u.cache_read_tokens),
        ("cache_write", u.cache_write_tokens),
    ] {
        if n > 0 {
            metrics::counter!(
                "sprig_ai_tokens_total",
                "role" => role.as_str(),
                "direction" => direction
            )
            .increment(n);
        }
    }
}

/// One turn's duration.
pub fn turn(role: Role, took: Duration) {
    metrics::histogram!("sprig_ai_turn_seconds", "role" => role.as_str())
        .record(took.as_secs_f64());
}

/// One tool call.
pub fn tool_call(tool: &str, result: &'static str) {
    metrics::counter!(
        "sprig_ai_tool_calls_total",
        "tool" => tool.to_owned(),
        "result" => result
    )
    .increment(1);
}

/// A tool result the gate reduced.
pub fn egress_reduced(role: Role) {
    metrics::counter!("sprig_ai_egress_reduced_total", "role" => role.as_str()).increment(1);
}

/// A proposal and what became of it.
pub fn proposal(kind: &'static str, outcome: &'static str) {
    metrics::counter!(
        "sprig_ai_proposals_total",
        "kind" => kind,
        "outcome" => outcome
    )
    .increment(1);
}

/// A turn stopped by a budget.
pub fn budget_stop() {
    metrics::counter!("sprig_ai_budget_stops_total").increment(1);
}

/// A thumbs up or down; `vote` is `up` or `down`.
pub fn feedback(role: Role, vote: &'static str) {
    metrics::counter!(
        "sprig_ai_feedback_total",
        "role" => role.as_str(),
        "vote" => vote
    )
    .increment(1);
}

/// Payloads dropped by the retention purge.
pub fn payloads_purged(n: u64) {
    metrics::counter!("sprig_ai_payloads_purged_total").increment(n);
}

/// One MCP request; `method` is one of [`crate::mcp::METHODS`] or `other`.
pub fn mcp_request(method: &str, result: &'static str) {
    let method = crate::mcp::METHODS
        .iter()
        .find(|m| **m == method)
        .copied()
        .unwrap_or("other");
    metrics::counter!(
        "sprig_ai_mcp_requests_total",
        "method" => method,
        "result" => result
    )
    .increment(1);
}

/// One MCP tool call.
pub fn mcp_tool_call(role: Role, tool: &str, result: &'static str) {
    metrics::counter!(
        "sprig_ai_mcp_tool_calls_total",
        "role" => role.as_str(),
        "tool" => tool.to_owned(),
        "result" => result
    )
    .increment(1);
}

/// An advice item filed, by its severity.
pub fn incident(severity: &str) {
    let severity = crate::incidents::brief::SEVERITIES
        .iter()
        .find(|s| **s == severity)
        .copied()
        .unwrap_or("medium");
    metrics::counter!("sprig_ai_incidents_total", "severity" => severity).increment(1);
}

/// An incident triaged.
pub fn incident_triage(verdict: &'static str) {
    metrics::counter!("sprig_ai_incident_triage_total", "verdict" => verdict).increment(1);
}

/// An incident that couldn't go further in a pass.
pub fn incident_held(reason: &'static str) {
    metrics::counter!("sprig_ai_incidents_held_total", "reason" => reason).increment(1);
}

/// One write of a process's error counts.
pub fn intake_flush(result: &'static str) {
    metrics::counter!("sprig_ai_intake_flushes_total", "result" => result).increment(1);
}

/// One sampling pass of the metric history; `result` is `ok` or `error`.
pub fn history_sample(result: &'static str) {
    metrics::counter!("sprig_ai_history_samples_total", "result" => result).increment(1);
}

/// One finding of a health check rule.
pub fn health_finding(rule: &str) {
    let rule = [
        "shard_split",
        "cluster_full",
        "replica_lag",
        "cluster_health",
        "connections",
        "backup_age",
        "mover_backlog",
    ]
    .iter()
    .find(|r| **r == rule)
    .copied()
    .unwrap_or("other");
    metrics::counter!("sprig_ai_health_findings_total", "rule" => rule).increment(1);
}

/// One health warning filed; `worded` is `model` or `rules` (no model).
pub fn health_warning(worded: &'static str) {
    metrics::counter!("sprig_ai_health_warnings_total", "worded" => worded).increment(1);
}

/// One incident write-up; `result` is `drafted`, `held` or `failed`.
pub fn writeup(result: &'static str) {
    metrics::counter!("sprig_ai_writeups_total", "result" => result).increment(1);
}

/// One upgrade readiness check; `result` is `ok` or `error`.
pub fn upgrade_check(result: &'static str) {
    metrics::counter!("sprig_ai_upgrade_checks_total", "result" => result).increment(1);
}

/// One tenant digest; `result` is `worded`, `facts` or `error`.
pub fn digest(result: &'static str) {
    metrics::counter!("sprig_ai_digests_total", "result" => result).increment(1);
}

/// One digest email; `result` is `sent`, `skipped` or `failed`.
pub fn digest_email(result: &'static str) {
    metrics::counter!("sprig_ai_digest_emails_total", "result" => result).increment(1);
}
