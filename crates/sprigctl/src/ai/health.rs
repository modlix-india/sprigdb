//! `sprigctl ai health|metrics|decisions|upgrade-check`: the
//! administration agent's health findings, its metric history, the DC's
//! decision log and upgrade readiness, over the Admin API's `/admin/v1/ai/`.
//!
//! ```text
//! sprigctl ai health list
//! sprigctl ai health show <key>
//! sprigctl ai health check
//! sprigctl ai metrics sprig_backend_replica_lag_seconds --label cluster=c1 --days 7
//! sprigctl ai decisions "cluster c1 failover" --days 30
//! sprigctl ai upgrade-check --to 1.5.0 [--pack sprig-docs-1.5.0.pack] [--explain]
//! ```

use std::fmt::Write as _;

use clap::{Arg, ArgAction, ArgMatches, Command};
use serde_json::{Value, json};

use crate::client::AdminClient;
use crate::render::table;
use crate::settings::escape;
use crate::{Failed, Output, print};

/// `sprigctl ai health`.
pub(super) fn health_command() -> Command {
    Command::new("health")
        .about("Findings of the administration agent's health checks")
        .subcommand_required(true)
        .arg_required_else_help(true)
        .subcommand(Command::new("list").about("Findings, newest first"))
        .subcommand(
            Command::new("show")
                .about("One finding: what the rules found, the warning and what was sent")
                .arg(Arg::new("key").required(true).help("The finding's key")),
        )
        .subcommand(
            Command::new("check")
                .about("Run the health rules now and print what they find; nothing is filed"),
        )
}

/// `sprigctl ai metrics`.
pub(super) fn metrics_command() -> Command {
    Command::new("metrics")
        .about("A series of the administration agent's metric history")
        .arg(
            Arg::new("name")
                .required(true)
                .help("The series, such as sprig_backend_replica_lag_seconds"),
        )
        .arg(
            Arg::new("label")
                .long("label")
                .action(ArgAction::Append)
                .help("Only series with this label=value (repeat for more)"),
        )
        .arg(
            Arg::new("days")
                .long("days")
                .default_value("7")
                .value_parser(clap::value_parser!(u64).range(1..=30))
                .help("Days to show, 1 to 30"),
        )
}

/// `sprigctl ai decisions`.
pub(super) fn decisions_command() -> Command {
    Command::new("decisions")
        .about("The DC's decision log: failovers, data moves, incidents, findings and write-ups")
        .arg(
            Arg::new("subject")
                .help("Words to match (an id, a cluster, a kind); the newest when absent"),
        )
        .arg(
            Arg::new("days")
                .long("days")
                .default_value("30")
                .value_parser(clap::value_parser!(u64).range(1..=90))
                .help("Days to look back, 1 to 90"),
        )
}

/// `sprigctl ai upgrade-check`.
pub(super) fn upgrade_command() -> Command {
    Command::new("upgrade-check")
        .about("What upgrading to a version changes or breaks, given how this deployment is used")
        .arg(Arg::new("to").long("to").required(true).help("The target version"))
        .arg(
            Arg::new("pack")
                .long("pack")
                .help("The target version's docs pack file (else the controller looks for it)"),
        )
        .arg(
            Arg::new("explain")
                .long("explain")
                .action(ArgAction::SetTrue)
                .help("Also ask the administration model to explain the report (needs ai.model.admin)"),
        )
}

fn s(v: &Value, k: &str) -> String {
    match v.get(k) {
        Some(Value::String(x)) => x.clone(),
        Some(Value::Null) | None => String::new(),
        Some(x) => x.to_string(),
    }
}

fn findings_table(v: &Value) -> String {
    let rows: Vec<[String; 6]> = v["findings"]
        .as_array()
        .into_iter()
        .flatten()
        .map(|f| {
            [
                s(f, "key"),
                s(f, "rule"),
                s(f, "severity"),
                s(f, "status"),
                s(f, "last_found"),
                s(f, "text"),
            ]
        })
        .collect();
    if rows.is_empty() {
        return "no findings\n".to_owned();
    }
    table(
        ["KEY", "RULE", "SEVERITY", "STATUS", "LAST FOUND", "FINDING"],
        &rows,
    )
}

fn check_text(v: &Value) -> String {
    let mut t = String::new();
    for f in v["findings"].as_array().into_iter().flatten() {
        let _ = writeln!(
            t,
            "[{}] {}: {}",
            s(f, "severity"),
            s(f, "rule"),
            s(f, "text")
        );
    }
    if t.is_empty() {
        t.push_str("the rules find nothing\n");
    }
    t
}

#[allow(clippy::many_single_char_names)] // v, r, f, t, w: the reply, record, finding, text, warning
fn finding_text(v: &Value) -> String {
    let r = &v["finding"];
    let f = &r["finding"];
    let mut t = String::new();
    let _ = writeln!(t, "Finding {}", s(f, "key"));
    let _ = writeln!(t, "Rule:      {} ({})", s(f, "rule"), s(f, "severity"));
    let _ = writeln!(t, "Found:     {}", s(f, "text"));
    let _ = writeln!(t, "Status:    {}", s(r, "status"));
    for (label, k) in [("Advice id", "advice_id"), ("Reason", "reason")] {
        let x = s(r, k);
        if !x.is_empty() {
            let _ = writeln!(t, "{:<10} {x}", format!("{label}:"));
        }
    }
    let intake = s(r, "intake");
    if !intake.is_empty() {
        let _ = writeln!(t, "\nWhat was sent:\n{intake}");
    }
    let w = s(r, "warning");
    if !w.is_empty() {
        let _ = writeln!(t, "\nWarning:\n{w}");
    }
    t
}

fn trend_text(v: &Value) -> String {
    let mut t = format!("{}: {}\n", s(v, "name"), s(v, "about"));
    for sr in v["series"].as_array().into_iter().flatten() {
        let labels: Vec<String> = sr["labels"]
            .as_object()
            .into_iter()
            .flatten()
            .map(|(k, v)| format!("{k}={}", v.as_str().unwrap_or("")))
            .collect();
        let _ = writeln!(t, "\n{{{}}}", labels.join(","));
        for d in sr["days"].as_array().into_iter().flatten() {
            let _ = writeln!(
                t,
                "  {}  mean {}  min {}  max {}",
                s(d, "day"),
                s(d, "mean"),
                s(d, "min"),
                s(d, "max")
            );
        }
    }
    let note = s(v, "note");
    if !note.is_empty() {
        let _ = writeln!(t, "{note}");
    }
    t
}

fn decisions_text(v: &Value) -> String {
    let mut t = String::new();
    let note = s(v, "note");
    if !note.is_empty() {
        let _ = writeln!(t, "{note}");
    }
    for e in v["entries"].as_array().into_iter().flatten() {
        let _ = writeln!(
            t,
            "{}  {}  {}  {}\n    {}",
            match s(e, "at") {
                a if a.is_empty() => "(now)".to_owned(),
                a => a,
            },
            s(e, "id"),
            s(e, "kind"),
            s(e, "subject"),
            s(e, "summary")
        );
    }
    if t.is_empty() {
        t.push_str("no decisions\n");
    }
    t
}

/// The upgrade report as text.
pub(super) fn upgrade_text(v: &Value) -> String {
    let mut t = format!("{}\n", s(v, "summary"));
    for i in v["items"].as_array().into_iter().flatten() {
        let _ = writeln!(t, "- [{}] {}", s(i, "impact"), s(i, "text"));
    }
    let u = &v["usage"];
    let _ = writeln!(
        t,
        "\nRead: {} settings set away from their defaults; the workload of {} tenants ({} skipped).",
        u["settings_set"].as_array().map_or(0, Vec::len),
        s(u, "tenants_read"),
        s(u, "tenants_skipped")
    );
    let ex = s(v, "explanation");
    if !ex.is_empty() {
        let _ = writeln!(t, "\nExplanation (advice only):\n{ex}");
    }
    let why = s(v, "explanation_unavailable");
    if !why.is_empty() {
        let _ = writeln!(t, "\nNo explanation: {why}");
    }
    t
}

/// Runs `sprigctl ai health|metrics|decisions|upgrade-check`.
///
/// # Errors
/// The API's; a pack that can't be read.
pub(super) async fn run(
    c: &AdminClient,
    cmd: &str,
    m: &ArgMatches,
    out: Output,
) -> Result<(), Failed> {
    match cmd {
        "health" => match m.subcommand() {
            Some(("list", _)) => {
                let r: Value = c.get("/admin/v1/ai/health").await?;
                print(out, &r, findings_table);
            }
            Some(("show", sub)) => {
                let key = sub.get_one::<String>("key").cloned().unwrap_or_default();
                let r: Value = c
                    .get(&format!("/admin/v1/ai/health/{}", escape(&key)))
                    .await?;
                print(out, &r, finding_text);
            }
            Some(("check", _)) => {
                let r: Value = c.post("/admin/v1/ai/health:check", &json!({})).await?;
                print(out, &r, check_text);
            }
            _ => return Err(Failed("sprigctl ai health list|show|check".into())),
        },
        "metrics" => {
            let mut q = format!(
                "name={}&days={}",
                escape(m.get_one::<String>("name").map_or("", String::as_str)),
                m.get_one::<u64>("days").copied().unwrap_or(7)
            );
            for l in m.get_many::<String>("label").into_iter().flatten() {
                let _ = write!(q, "&label={}", escape(l));
            }
            let r: Value = c.get(&format!("/admin/v1/ai/metrics?{q}")).await?;
            print(out, &r, trend_text);
        }
        "decisions" => {
            let subject = m.get_one::<String>("subject").cloned().unwrap_or_default();
            let r: Value = c
                .get(&format!(
                    "/admin/v1/ai/decisions?subject={}&days={}",
                    escape(&subject),
                    m.get_one::<u64>("days").copied().unwrap_or(30)
                ))
                .await?;
            print(out, &r, decisions_text);
        }
        "upgrade-check" => {
            let to = m.get_one::<String>("to").cloned().unwrap_or_default();
            let mut body = json!({"to": to, "explain": m.get_flag("explain")});
            if let Some(p) = m.get_one::<String>("pack") {
                let bytes = std::fs::read(p)
                    .map_err(|e| Failed(format!("the docs pack {p} can't be read: {e}")))?;
                let cat = sprig_controller::ai_health::catalog_of_pack_bytes(&bytes)
                    .map_err(|e| Failed(e.message))?;
                body["catalog"] =
                    serde_json::to_value(cat).map_err(|e| Failed(format!("the catalog: {e}")))?;
            }
            let r: Value = c.post("/admin/v1/ai:upgradeCheck", &body).await?;
            print(out, &r, upgrade_text);
        }
        _ => return Err(Failed(format!("sprigctl ai {cmd}?"))),
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn reports_read_as_text() {
        let v = json!({"summary": "Upgrade from 1.4 to 1.5: 1 that break", "items": [
            {"impact": "breaks", "subject": "a.b", "text": "`a.b` is set here"}],
            "usage": {"settings_set": [{"key": "a.b", "scope": "global"}], "tenants_read": 3, "tenants_skipped": 0},
            "explanation_unavailable": "ai.model.admin is empty"});
        let t = upgrade_text(&v);
        assert!(t.contains("- [breaks] `a.b` is set here"));
        assert!(t.contains("1 settings set away"));
        assert!(t.contains("No explanation: ai.model.admin is empty"));
        let d = decisions_text(
            &json!({"entries": [{"id": "cf-1a2b3c4d-7", "at": "2026-11-14T03:12Z",
            "kind": "cluster.failover.promote", "subject": "cluster c2", "summary": "promoted"}], "note": ""}),
        );
        assert!(d.starts_with("2026-11-14T03:12Z  cf-1a2b3c4d-7"));
        assert_eq!(
            check_text(&json!({"findings": []})),
            "the rules find nothing\n"
        );
    }
}
